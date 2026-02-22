import 'dotenv/config';
import express from 'express';
import morgan from 'morgan';
import multer from 'multer';
import path from 'path';
import { fileURLToPath } from 'url';
import { WebSocketServer, WebSocket } from 'ws';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

app.use(morgan('dev'));
app.use(express.json({ limit: '25mb' }));
app.use(express.static(path.join(__dirname, '../web')));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, '../web/index.html'));
});

const PORT = process.env.PORT ? Number(process.env.PORT) : 8080;

function nowMs() {
  return Number(process.hrtime.bigint() / 1000000n);
}

function jsonError(res, status, message, extra = {}) {
  res.status(status).json({ error: message, ...extra });
}

app.get('/health', (req, res) => {
  res.json({ ok: true });
});

app.post('/api/v1/translate/text', async (req, res) => {
  const { text, source_lang, target_lang, stream = false, model } = req.body || {};
  if (!text || !source_lang || !target_lang) {
    return jsonError(res, 400, 'Missing required fields: text, source_lang, target_lang');
  }

  const apiKey = process.env.DOUBAO_API_KEY;
  if (!apiKey) return jsonError(res, 500, 'Missing env DOUBAO_API_KEY');

  const prompt = `请将以下${source_lang}句子翻译成${target_lang}。\n要求：只返回翻译结果，不要有其他解释。\n\n原文：${text}\n\n翻译：`;

  const url = 'https://ark.cn-beijing.volces.com/api/v3/chat/completions';
  const body = {
    model: model || process.env.DOUBAO_MODEL || 'doubao-seed-1-6-flash-250828',
    stream: !!stream,
    max_output_tokens: 1024,
    temperature: 0.1,
    thinking: { type: 'disabled' },
    messages: [{ role: 'user', content: prompt }]
  };

  const t0 = nowMs();
  const ac = new AbortController();
  const timeoutMs = 30000;
  const to = setTimeout(() => ac.abort(new Error('timeout')), timeoutMs);

  try {
    const r = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`
      },
      body: JSON.stringify(body),
      signal: ac.signal
    });

    if (!r.ok) {
      const txt = await r.text().catch(() => '');
      return jsonError(res, 502, `Doubao error: ${r.status}`, { detail: txt });
    }

    if (!stream) {
      const t1 = nowMs();
      const data = await r.json();
      const t2 = nowMs();
      const translation = data?.choices?.[0]?.message?.content ?? '';
      return res.json({
        translation,
        raw: data,
        timing: { ttfb_ms: t1 - t0, total_ms: t2 - t0 }
      });
    }

    res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('Connection', 'keep-alive');

    const reader = r.body.getReader();
    const decoder = new TextDecoder('utf-8');

    let tFirst = null;
    let buf = '';

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      if (tFirst === null) tFirst = nowMs();
      buf += decoder.decode(value, { stream: true });

      // Doubao stream is typically SSE-like: lines with "data: {...}" and "data: [DONE]".
      // We'll forward chunks as-is for the browser to parse.
      res.write(buf);
      buf = '';
    }

    const tEnd = nowMs();
    res.write(`\n\n`);
    res.write(`event: timing\ndata: ${JSON.stringify({ ttfb_ms: (tFirst ?? tEnd) - t0, total_ms: tEnd - t0 })}\n\n`);
    res.end();
  } catch (e) {
    const tEnd = nowMs();
    return jsonError(res, 500, `Translate failed: ${String(e?.message || e)}`, {
      timing: { total_ms: tEnd - t0 }
    });
  } finally {
    clearTimeout(to);
  }
});

app.post('/api/v1/asr', upload.single('audio'), async (req, res) => {
  const { model = 'deepgram', audio_url, language = 'zh', include_raw } = req.body || {};

  let audio_base64 = req.file?.buffer
    ? req.file.buffer.toString('base64')
    : req.body?.audio_base64;

  console.log(`[ASR] Model: ${model}, Lang: ${language}, Raw: ${include_raw}`);
  console.log(`[ASR] Data: url=${!!audio_url}, b64=${!!audio_base64}, file=${!!req.file}`);

  if (!audio_url && !audio_base64) {
    return jsonError(res, 400, 'Provide audio_url or audio_base64, or upload an audio file', {
      received_body_keys: Object.keys(req.body || {}),
      has_file: !!req.file
    });
  }

  const t0 = nowMs();
  const ac = new AbortController();
  const timeoutMs = 60000;
  const to = setTimeout(() => ac.abort(new Error('timeout')), timeoutMs);

  try {
    if (model === 'deepgram') {
      const apiKey = process.env.DEEPGRAM_API_KEY;
      if (!apiKey) return jsonError(res, 500, 'Missing env DEEPGRAM_API_KEY');

      const qs = new URLSearchParams({
        model: process.env.DEEPGRAM_MODEL || 'nova-2',
        smart_format: 'true',
        diarize: 'false',
        paragraphs: 'true',
        language
      });
      const url = `https://api.deepgram.com/v1/listen?${qs.toString()}`;

      const r = await fetch(url, {
        method: 'POST',
        headers: {
          Authorization: `Token ${apiKey}`,
          'Content-Type': audio_url ? 'application/json' : 'application/octet-stream'
        },
        body: audio_url
          ? JSON.stringify({ url: audio_url })
          : Buffer.from(audio_base64, 'base64'),
        signal: ac.signal
      });

      const t1 = nowMs();
      if (!r.ok) {
        const txt = await r.text().catch(() => '');
        return jsonError(res, 502, `Deepgram error: ${r.status}`, { detail: txt });
      }

      const data = await r.json();
      const t2 = nowMs();
      const alt = data?.results?.channels?.[0]?.alternatives?.[0];
      const text = alt?.transcript ?? '';
      const confidence = alt?.confidence ?? null;
      const resp = { text, confidence, timing: { ttfb_ms: t1 - t0, total_ms: t2 - t0 } };
      if (String(include_raw).toLowerCase() === 'true') resp.raw = data;
      return res.json(resp);
    }

    if (model === 'qwen3-asr-flash') {
      const apiKey = process.env.DASHSCOPE_API_KEY;
      if (!apiKey) return jsonError(res, 500, 'Missing env DASHSCOPE_API_KEY');

      const url = 'https://dashscope-intl.aliyuncs.com/api/v1/services/aigc/multimodal-generation/generation';
      const audio = audio_url
        ? audio_url
        : `data:audio/wav;base64,${audio_base64}`;

      const body = {
        model: 'qwen3-asr-flash',
        input: {
          messages: [
            { role: 'system', content: [{ text: '' }] },
            { role: 'user', content: [{ audio }] }
          ]
        },
        parameters: {
          asr_options: { enable_itn: false },
          language
        }
      };

      const r = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${apiKey}`
        },
        body: JSON.stringify(body),
        signal: ac.signal
      });

      const t1 = nowMs();
      if (!r.ok) {
        const txt = await r.text().catch(() => '');
        return jsonError(res, 502, `DashScope ASR error: ${r.status}`, { detail: txt });
      }

      const data = await r.json();
      const t2 = nowMs();
      const text = data?.output?.text ?? data?.output?.choices?.[0]?.message?.content?.[0]?.text ?? '';
      const resp = { text, confidence: 1.0, timing: { ttfb_ms: t1 - t0, total_ms: t2 - t0 } };
      if (String(include_raw).toLowerCase() === 'true') resp.raw = data;
      return res.json(resp);
    }

    if (model === 'stream_in_stream_out') {
      return jsonError(res, 501, 'ASR stream-in is reserved (vendor TBD)');
    }

    return jsonError(res, 400, `Unknown ASR model: ${model}`);
  } catch (e) {
    const tEnd = nowMs();
    return jsonError(res, 500, `ASR failed: ${String(e?.message || e)}`, {
      timing: { total_ms: tEnd - t0 }
    });
  } finally {
    clearTimeout(to);
  }
});

app.post('/api/v1/tts', async (req, res) => {
  const { text, lang, voice = 'Cherry', model } = req.body || {};
  if (!text || !lang) return jsonError(res, 400, 'Missing required fields: text, lang');

  const apiKey = process.env.DASHSCOPE_API_KEY;
  if (!apiKey) return jsonError(res, 500, 'Missing env DASHSCOPE_API_KEY');

  const url = 'https://dashscope-intl.aliyuncs.com/api/v1/services/aigc/multimodal-generation/generation';
  const language_type_map = {
    zh: 'Chinese',
    en: 'English',
    ja: 'Japanese',
    ko: 'Korean',
    es: 'Spanish',
    fr: 'French',
    de: 'German'
  };

  const body = {
    model: model || 'qwen3-tts-flash',
    input: {
      text,
      voice,
      language_type: language_type_map[lang] || 'English'
    },
    parameters: {
      format: 'mp3',
      sample_rate: 24000
    }
  };

  const t0 = nowMs();
  const ac = new AbortController();
  const timeoutMs = 60000;
  const to = setTimeout(() => ac.abort(new Error('timeout')), timeoutMs);

  try {
    const r = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`
      },
      body: JSON.stringify(body),
      signal: ac.signal
    });

    const t1 = nowMs();
    if (!r.ok) {
      const txt = await r.text().catch(() => '');
      return jsonError(res, 502, `DashScope TTS error: ${r.status}`, { detail: txt });
    }

    const data = await r.json();
    const t2 = nowMs();

    const audioUrl = data?.output?.audio?.url ?? null;
    const audioDataB64 = data?.output?.audio?.data ?? null;

    return res.json({
      audio_url: audioUrl,
      audio_base64: audioDataB64,
      format: data?.output?.audio?.url?.toLowerCase?.().includes('.mp3') ? 'mp3' : 'wav',
      raw: data,
      timing: { ttfb_ms: t1 - t0, total_ms: t2 - t0 }
    });
  } catch (e) {
    const tEnd = nowMs();
    return jsonError(res, 500, `TTS failed: ${String(e?.message || e)}`, { timing: { total_ms: tEnd - t0 } });
  } finally {
    clearTimeout(to);
  }
});

app.get('/api/v1/media', async (req, res) => {
  const url = req.query?.url;
  if (!url || typeof url !== 'string') return jsonError(res, 400, 'Missing query param: url');

  const t0 = nowMs();
  const ac = new AbortController();
  const timeoutMs = 60000;
  const to = setTimeout(() => ac.abort(new Error('timeout')), timeoutMs);

  try {
    const r = await fetch(url, { signal: ac.signal });
    if (!r.ok) {
      const txt = await r.text().catch(() => '');
      return jsonError(res, 502, `Media fetch error: ${r.status}`, { detail: txt });
    }

    const ct = r.headers.get('content-type') || 'application/octet-stream';
    res.setHeader('Content-Type', ct);
    const cl = r.headers.get('content-length');
    if (cl) res.setHeader('Content-Length', cl);
    res.setHeader('Cache-Control', 'private, max-age=300');

    const reader = r.body.getReader();
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      res.write(Buffer.from(value));
    }
    res.end();
  } catch (e) {
    return jsonError(res, 500, `Media proxy failed: ${String(e?.message || e)}`, { timing: { total_ms: nowMs() - t0 } });
  } finally {
    clearTimeout(to);
  }
});

app.post('/api/v1/tts/stream', async (req, res) => {
  const { text, lang, voice = 'Cherry', model } = req.body || {};
  if (!text || !lang) return jsonError(res, 400, 'Missing required fields: text, lang');

  const apiKey = process.env.DASHSCOPE_API_KEY;
  if (!apiKey) return jsonError(res, 500, 'Missing env DASHSCOPE_API_KEY');

  const url = 'https://dashscope-intl.aliyuncs.com/api/v1/services/aigc/multimodal-generation/generation';
  const language_type_map = {
    zh: 'Chinese',
    en: 'English',
    ja: 'Japanese',
    ko: 'Korean',
    es: 'Spanish',
    fr: 'French',
    de: 'German'
  };

  const body = {
    model: model || 'qwen3-tts-flash',
    input: {
      text,
      voice,
      language_type: language_type_map[lang] || 'English'
    },
    parameters: {
      format: 'wav', // 实际上 DashScope SSE 会返回 PCM 字节流
      sample_rate: 24000
    }
  };

  const t0 = nowMs();
  const ac = new AbortController();
  const timeoutMs = 60000;
  const to = setTimeout(() => ac.abort(new Error('timeout')), timeoutMs);

  try {
    const r = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
        'X-DashScope-SSE': 'enable'
      },
      body: JSON.stringify(body),
      signal: ac.signal
    });

    if (!r.ok) {
      const txt = await r.text().catch(() => '');
      return jsonError(res, 502, `DashScope TTS stream error: ${r.status}`, { detail: txt });
    }

    res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('Connection', 'keep-alive');

    const reader = r.body.getReader();
    const decoder = new TextDecoder('utf-8');

    let tFirst = null;
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      if (tFirst === null) tFirst = nowMs();
      const chunk = decoder.decode(value, { stream: true });
      res.write(chunk);
    }

    const tEnd = nowMs();
    res.write(`\n\n`);
    res.write(`event: timing\ndata: ${JSON.stringify({ ttfb_ms: (tFirst ?? tEnd) - t0, total_ms: tEnd - t0 })}\n\n`);
    res.end();
  } catch (e) {
    const tEnd = nowMs();
    return jsonError(res, 500, `TTS stream failed: ${String(e?.message || e)}`, { timing: { total_ms: tEnd - t0 } });
  } finally {
    clearTimeout(to);
  }
});


// Debug endpoints to verify WS upgrade headers reach the app.
app.get('/api/v1/asr/realtime/debug', (req, res) => {
  const h = req.headers || {};
  res.json({
    ok: true,
    path: req.path,
    method: req.method,
    headers: {
      host: h.host,
      connection: h.connection,
      upgrade: h.upgrade,
      origin: h.origin,
      'sec-websocket-key': h['sec-websocket-key'],
      'sec-websocket-version': h['sec-websocket-version'],
      'sec-websocket-protocol': h['sec-websocket-protocol'],
      'x-forwarded-for': h['x-forwarded-for'],
      'x-forwarded-proto': h['x-forwarded-proto'],
      'x-forwarded-host': h['x-forwarded-host']
    }
  });
});

app.get('/api/v1/debug/env', (req, res) => {
  res.json({
    ok: true,
    port: process.env.PORT ? Number(process.env.PORT) : 8080,
    env: {
      NODE_ENV: process.env.NODE_ENV || null,
      DOUBAO_MODEL: process.env.DOUBAO_MODEL || null,
      DOUBAO_API_BASE: process.env.DOUBAO_API_BASE || null,
      QWEN_REALTIME_WS_URL: process.env.QWEN_REALTIME_WS_URL || null,
      has_DOUBAO_API_KEY: !!process.env.DOUBAO_API_KEY,
      has_DASHSCOPE_API_KEY: !!process.env.DASHSCOPE_API_KEY,
      has_DEEPGRAM_API_KEY: !!process.env.DEEPGRAM_API_KEY
    }
  });
});

const server = app.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
});

server.on('upgrade', (request, socket, head) => {
  const pathname = new URL(request.url, `http://${request.headers.host}`).pathname;
  console.log(`[TERMINAL] Upgrade request for ${pathname}`);
  if (pathname !== '/api/v1/asr/realtime') {
    console.log(`[TERMINAL] Rejecting upgrade for ${pathname}`);
    socket.destroy();
  }
});

// --- Real-time ASR WebSocket Proxy (Qwen-ASR Realtime) ---
// Browser connects to: ws://localhost:8787/api/v1/asr/realtime
// Server connects to DashScope: wss://dashscope-intl.aliyuncs.com/api-ws/v1/realtime
const rtasrWss = new WebSocketServer({ server, path: '/api/v1/asr/realtime' });

rtasrWss.on('connection', (clientWs) => {
  console.log('[RT-ASR] client connected');
  let upstream = null;

  // Session context to store mode and language preferences
  const uiConfig = {
    mode: 'dual_button',
    leftLang: 'zh',
    rightLang: 'en'
  };

  function decideSideAndDirection(leftLang, rightLang, detectedLang) {
    if (!detectedLang) return { side: 'left', fromLang: leftLang, toLang: rightLang };
    // Exact match
    if (detectedLang === leftLang) return { side: 'left', fromLang: leftLang, toLang: rightLang };
    if (detectedLang === rightLang) return { side: 'right', fromLang: rightLang, toLang: leftLang };
    // Prefix match (e.g., 'en-US' matches 'en')
    if (detectedLang.startsWith(leftLang)) return { side: 'left', fromLang: leftLang, toLang: rightLang };
    if (detectedLang.startsWith(rightLang)) return { side: 'right', fromLang: rightLang, toLang: leftLang };
    // Fallback
    return { side: 'left', fromLang: detectedLang, toLang: rightLang };
  }

  function sendClient(obj) {
    try {
      clientWs.send(JSON.stringify(obj));
    } catch {}
  }

  clientWs.on('message', (buf) => {
    const msgStr = Buffer.isBuffer(buf) ? buf.toString('utf8') : String(buf);

    let msg;
    try {
      msg = JSON.parse(msgStr);
    } catch {
      return sendClient({ type: 'error', error: { message: 'Invalid JSON from client' } });
    }

    if (!upstream) {
      // First message must be session.update
      if (msg?.type !== 'session.update') {
        return sendClient({ type: 'error', error: { message: 'First message must be session.update' } });
      }

      const s = msg.session || {};
      // Update uiConfig from session.update, but keep previous values if not provided
      if (s.mode) uiConfig.mode = s.mode;
      if (s.left_lang || s.leftLang) uiConfig.leftLang = s.left_lang || s.leftLang;
      if (s.right_lang || s.rightLang) uiConfig.rightLang = s.right_lang || s.rightLang;

      const apiKey = process.env.DASHSCOPE_API_KEY;
      if (!apiKey) {
        return sendClient({ type: 'error', error: { message: 'Missing env DASHSCOPE_API_KEY' } });
      }

      // Use model from session.update or default to qwen3-asr-flash-realtime
      const modelName = s.model || 'qwen3-asr-flash-realtime';
      const baseUrl = process.env.QWEN_REALTIME_WS_URL || 'wss://dashscope-intl.aliyuncs.com/api-ws/v1/realtime';
      const url = `${baseUrl}?model=${modelName}`;
      
      console.log(`[RT-ASR] connecting to ${url} with uiConfig:`, uiConfig);
      upstream = new WebSocket(url, {
        headers: {
          Authorization: `Bearer ${apiKey}`
        }
      });

      upstream.on('unexpected-response', (req, res) => {
        let body = '';
        res.on('data', chunk => body += chunk);
        res.on('end', () => {
          console.error(`[RT-ASR] Upstream handshake failed. Status: ${res.statusCode}, Body: ${body}`);
          sendClient({ 
            type: 'error', 
            error: { 
              message: `Upstream Handshake failed: ${res.statusCode}`,
              detail: body
            } 
          });
        });
      });

      upstream.on('open', () => {
        console.log('[RT-ASR] upstream open');
        upstream.send(JSON.stringify(msg));
      });

      upstream.on('message', (data) => {
        try {
          const text = typeof data === 'string' ? data : data.toString('utf8');
          let parsed;
          try {
            parsed = JSON.parse(text);
          } catch {
            return clientWs.send(text);
          }

          // Append UI logic for completed transcription
          if (parsed.type === 'conversation.item.input_audio_transcription.completed') {
            const detectedLang = parsed.language;
            const { side, fromLang, toLang } = decideSideAndDirection(uiConfig.leftLang, uiConfig.rightLang, detectedLang);
            parsed.ui_side = side;
            parsed.ui_source_lang = fromLang;
            parsed.ui_target_lang = toLang;
            parsed.ui_mode = uiConfig.mode;
          }

          clientWs.send(JSON.stringify(parsed));
        } catch (e) {
          console.error('[RT-ASR] error processing upstream message:', e);
        }
      });

      upstream.on('close', (code, reason) => {
        console.log(`[RT-ASR] upstream closed. code=${code}, reason=${reason}`);
        sendClient({ type: 'session.finished', reason: String(reason) });
        try { clientWs.close(); } catch {}
      });

      upstream.on('error', (err) => {
        console.error('[RT-ASR] upstream error details:', err);
        sendClient({ type: 'error', error: { message: `Upstream error: ${err.message}` } });
      });

      return;
    }

    if (upstream.readyState === WebSocket.OPEN) {
      upstream.send(JSON.stringify(msg));
    }
  });

  clientWs.on('close', () => {
    console.log('[RT-ASR] client closed');
    try { upstream?.terminate(); } catch {}
    upstream = null;
  });
});
