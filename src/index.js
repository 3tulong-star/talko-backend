import 'dotenv/config';
import express from 'express';
import morgan from 'morgan';
import multer from 'multer';
import path from 'path';
import { fileURLToPath } from 'url';
import { WebSocketServer, WebSocket } from 'ws';
import admin from 'firebase-admin';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// --- Firebase Admin bootstrap (env -> cert) ---
async function initFirebaseAdmin() {
  if (admin.apps?.length) return { ok: true, already: true };

  const json = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
  if (!json) {
    console.error('[Firebase] Missing env FIREBASE_SERVICE_ACCOUNT_JSON');
    return { ok: false, reason: 'missing FIREBASE_SERVICE_ACCOUNT_JSON' };
  }

  let serviceAccount;
  try {
    serviceAccount = JSON.parse(json);
  } catch (e) {
    console.error('[Firebase] FIREBASE_SERVICE_ACCOUNT_JSON is not valid JSON');
    return { ok: false, reason: 'invalid FIREBASE_SERVICE_ACCOUNT_JSON (not JSON)' };
  }

  try {
    admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
    console.log('[Firebase] Admin SDK initialized successfully (cert from env)');
    return { ok: true };
  } catch (e) {
    console.error('[Firebase] Initialization error:', e);
    return { ok: false, reason: String(e?.message || e) };
  }
}

const fbInitPromise = initFirebaseAdmin();

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

app.use(morgan('dev'));
app.use(express.json({ limit: '25mb' }));
app.use(express.static(path.join(__dirname, '../web')));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, '../web/index.html'));
});

const PORT = process.env.PORT ? Number(process.env.PORT) : 8080;
const DEFAULT_USAGE_LIMIT_SECONDS = 60 * 60;

function nowMs() {
  return Number(process.hrtime.bigint() / 1000000n);
}

function jsonError(res, status, message, extra = {}) {
  res.status(status).json({ error: message, ...extra });
}

function getAuthToken(req) {
  const h = req.headers?.authorization;
  if (!h || typeof h !== 'string') return null;
  const m = h.match(/^Bearer\s+(.+)$/i);
  return m ? m[1] : null;
}

async function authMiddleware(req, res, next) {
  const token = getAuthToken(req);
  if (!token) return jsonError(res, 401, 'Missing Authorization: Bearer <token>');

  const init = await fbInitPromise;
  if (!init.ok) {
    return jsonError(res, 500, 'Firebase Admin not initialized', { reason: init.reason });
  }

  try {
    const decoded = await admin.auth().verifyIdToken(token);
    req.user = { uid: decoded.uid, decoded };
    return next();
  } catch (e) {
    return jsonError(res, 401, 'Invalid Firebase ID token', { detail: String(e?.message || e) });
  }
}

function retentionForPlan(plan) {
  if (plan === 'pro' || plan === 'pro_plus') return 30;
  return 7;
}

function toISOStringSafe(value) {
  if (!value) return null;
  try {
    if (value instanceof Date) return value.toISOString();
    if (typeof value?.toDate === 'function') return value.toDate().toISOString();
    if (typeof value?._seconds === 'number') return new Date(value._seconds * 1000).toISOString();
    return null;
  } catch {
    return null;
  }
}

function mapConversationDoc(doc) {
  const data = doc.data() || {};
  return {
    id: doc.id,
    ...data,
    createdAtISO: toISOStringSafe(data.createdAt),
    updatedAtISO: toISOStringSafe(data.updatedAt),
    lastMessageAtISO: toISOStringSafe(data.lastMessageAt),
    expireAtISO: toISOStringSafe(data.expireAt)
  };
}

function mapMessageDoc(doc) {
  const data = doc.data() || {};
  return {
    id: doc.id,
    ...data,
    createdAtISO: toISOStringSafe(data.createdAt),
    expireAtISO: toISOStringSafe(data.expireAt)
  };
}

async function getUserPlan(uid) {
  // For now: default free. Read from Firestore once RevenueCat webhook is set up.
  return 'free';
}

function getLangPairKey(source, target) {
  return `${String(source || '').toLowerCase()}->${String(target || '').toLowerCase()}`;
}

function getJsonMapFromEnv(envName) {
  const raw = process.env[envName];
  if (!raw) return {};
  try {
    const parsed = JSON.parse(raw);
    return (parsed && typeof parsed === 'object') ? parsed : {};
  } catch {
    return {};
  }
}

function resolveASRProvider({ requestedProvider, leftLang, rightLang }) {
  if (requestedProvider) return String(requestedProvider).toLowerCase();
  const pairMap = getJsonMapFromEnv('ASR_PROVIDER_MAP_JSON');
  const pair = getLangPairKey(leftLang, rightLang);
  return String(pairMap[pair] || process.env.DEFAULT_ASR_PROVIDER || 'qwen').toLowerCase();
}

function normalizeDeepgramLanguage(lang) {
  const v = String(lang || '').trim().toLowerCase();
  if (!v) return null;
  if (v === 'zh' || v.startsWith('zh-')) return 'zh';
  if (v === 'en' || v.startsWith('en-')) return 'en';
  if (v === 'es' || v.startsWith('es-')) return 'es';
  if (v === 'fr' || v.startsWith('fr-')) return 'fr';
  if (v === 'de' || v.startsWith('de-')) return 'de';
  if (v === 'hi' || v.startsWith('hi-')) return 'hi';
  if (v === 'ru' || v.startsWith('ru-')) return 'ru';
  if (v === 'pt' || v.startsWith('pt-')) return 'pt';
  if (v === 'ja' || v.startsWith('ja-')) return 'ja';
  if (v === 'it' || v.startsWith('it-')) return 'it';
  if (v === 'nl' || v.startsWith('nl-')) return 'nl';
  if (v === 'ko' || v.startsWith('ko-')) return 'ko';
  return v;
}

function resolveDeepgramModel({ requestedModel, languageHint }) {
  const language = normalizeDeepgramLanguage(languageHint);
  if (requestedModel) return String(requestedModel);

  const byLangMap = getJsonMapFromEnv('DEEPGRAM_MODEL_MAP_JSON');
  if (language && byLangMap[language]) return String(byLangMap[language]);

  // Nova-3 does not support zh; fallback to 2-general for Chinese.
  if (language === 'zh') return process.env.DEEPGRAM_ZH_MODEL || '2-general';

  return process.env.DEEPGRAM_ASR_MODEL || 'nova-3';
}

async function transcribeWithDeepgramFromPCM({ audioPcmChunks, languageHint, model }) {
  const apiKey = process.env.DEEPGRAM_API_KEY;
  if (!apiKey) throw new Error('missing_deepgram_api_key');

  const joined = Buffer.concat(audioPcmChunks);
  if (!joined.length) return '';

  const wav = pcm16ToWavBuffer(joined, 16000, 1, 16);
  const normalizedLanguage = normalizeDeepgramLanguage(languageHint);
  const dgModel = resolveDeepgramModel({ requestedModel: model, languageHint: normalizedLanguage });

  const url = new URL(process.env.DEEPGRAM_ASR_URL || 'https://api.deepgram.com/v1/listen');
  url.searchParams.set('model', dgModel);
  if (normalizedLanguage) url.searchParams.set('language', normalizedLanguage);
  url.searchParams.set('smart_format', 'true');
  url.searchParams.set('interim_results', 'true');
  url.searchParams.set('punctuate', 'true');
  url.searchParams.set('endpointing', String(process.env.DEEPGRAM_ENDPOINTING_MS || 300));

  const resp = await fetch(url.toString(), {
    method: 'POST',
    headers: {
      Authorization: `Token ${apiKey}`,
      'Content-Type': 'audio/wav'
    },
    body: wav
  });

  if (!resp.ok) {
    const detail = await resp.text().catch(() => '');
    throw new Error(`deepgram_transcribe_failed_${resp.status}:${detail}`);
  }

  const data = await resp.json();
  const transcript = String(data?.results?.channels?.[0]?.alternatives?.[0]?.transcript || '').trim();
  console.log(`[Deepgram] model=${dgModel} lang=${normalizedLanguage || 'auto'} chars=${transcript.length}`);
  return transcript;
}

function resolveTranslationProvider({ requestedProvider, sourceLang, targetLang }) {
  if (requestedProvider) return String(requestedProvider).toLowerCase();
  const pairMap = getJsonMapFromEnv('TRANSLATION_PROVIDER_MAP_JSON');
  const pair = getLangPairKey(sourceLang, targetLang);
  return String(pairMap[pair] || process.env.DEFAULT_TRANSLATION_PROVIDER || 'doubao').toLowerCase();
}

const QWEN_TTS_LANG_WHITELIST = new Set(['zh', 'en', 'es', 'ru', 'it', 'fr', 'ko', 'ja', 'de', 'pt']);

function normalizeTtsLang(lang) {
  const v = String(lang || '').toLowerCase();
  if (v.startsWith('zh')) return 'zh';
  if (v.startsWith('en')) return 'en';
  if (v.startsWith('es')) return 'es';
  if (v.startsWith('ru')) return 'ru';
  if (v.startsWith('it')) return 'it';
  if (v.startsWith('fr')) return 'fr';
  if (v.startsWith('ko')) return 'ko';
  if (v.startsWith('ja')) return 'ja';
  if (v.startsWith('de')) return 'de';
  if (v.startsWith('pt')) return 'pt';
  return v;
}

function pcm16ToWav(pcmBuffer, sampleRate = 16000, channels = 1) {
  const bitsPerSample = 16;
  const byteRate = sampleRate * channels * bitsPerSample / 8;
  const blockAlign = channels * bitsPerSample / 8;
  const dataSize = pcmBuffer.length;
  const buffer = Buffer.alloc(44 + dataSize);

  buffer.write('RIFF', 0);
  buffer.writeUInt32LE(36 + dataSize, 4);
  buffer.write('WAVE', 8);
  buffer.write('fmt ', 12);
  buffer.writeUInt32LE(16, 16);
  buffer.writeUInt16LE(1, 20);
  buffer.writeUInt16LE(channels, 22);
  buffer.writeUInt32LE(sampleRate, 24);
  buffer.writeUInt32LE(byteRate, 28);
  buffer.writeUInt16LE(blockAlign, 32);
  buffer.writeUInt16LE(bitsPerSample, 34);
  buffer.write('data', 36);
  buffer.writeUInt32LE(dataSize, 40);

  pcmBuffer.copy(buffer, 44);
  return buffer;
}

function pcm16ToWavBuffer(pcmBuffer, sampleRate = 16000, channels = 1, bitsPerSample = 16) {
  const byteRate = sampleRate * channels * (bitsPerSample / 8);
  const blockAlign = channels * (bitsPerSample / 8);
  const dataSize = pcmBuffer.length;
  const wav = Buffer.alloc(44 + dataSize);

  wav.write('RIFF', 0);
  wav.writeUInt32LE(36 + dataSize, 4);
  wav.write('WAVE', 8);
  wav.write('fmt ', 12);
  wav.writeUInt32LE(16, 16);
  wav.writeUInt16LE(1, 20);
  wav.writeUInt16LE(channels, 22);
  wav.writeUInt32LE(sampleRate, 24);
  wav.writeUInt32LE(byteRate, 28);
  wav.writeUInt16LE(blockAlign, 32);
  wav.writeUInt16LE(bitsPerSample, 34);
  wav.write('data', 36);
  wav.writeUInt32LE(dataSize, 40);
  pcmBuffer.copy(wav, 44);

  return wav;
}

async function transcribeWithOpenAIFromPCM({ audioPcmChunks, languageHint }) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) throw new Error('missing_openai_api_key');

  const joined = Buffer.concat(audioPcmChunks);
  if (!joined.length) return '';

  const wav = pcm16ToWavBuffer(joined, 16000, 1, 16);
  const form = new FormData();
  form.append('model', process.env.OPENAI_ASR_MODEL || 'gpt-4o-transcribe');
  if (languageHint) form.append('language', languageHint);
  form.append('file', new Blob([wav], { type: 'audio/wav' }), 'audio.wav');

  const resp = await fetch(process.env.OPENAI_ASR_URL || 'https://api.openai.com/v1/audio/transcriptions', {
    method: 'POST',
    headers: { Authorization: `Bearer ${apiKey}` },
    body: form
  });

  if (!resp.ok) {
    const detail = await resp.text().catch(() => '');
    throw new Error(`openai_transcribe_failed_${resp.status}:${detail}`);
  }

  const data = await resp.json();
  return String(data?.text || '').trim();
}

async function synthesizeWithQwenTTS({ text, lang = 'en', voice = 'Cherry', model }) {
  const apiKey = process.env.DASHSCOPE_API_KEY;
  if (!apiKey) throw new Error('missing_dashscope_api_key');

  const effectiveModel = model || process.env.QWEN_TTS_MODEL || 'qwen3-tts-flash-realtime';
  if (!String(effectiveModel).includes('realtime')) {
    throw new Error(`qwen_tts_realtime_required:model=${effectiveModel}`);
  }

  return await synthesizeWithQwenRealtimeTTS({ text, lang, voice, model: effectiveModel });
}

function synthesizeWithQwenRealtimeTTS({ text, lang = 'en', voice = 'Cherry', model = 'qwen3-tts-vc-realtime-2026-01-15' }) {
  const apiKey = process.env.DASHSCOPE_API_KEY;
  if (!apiKey) return Promise.reject(new Error('missing_dashscope_api_key'));

  const wsBase = process.env.QWEN_TTS_REALTIME_WS_URL || 'wss://dashscope-intl.aliyuncs.com/api-ws/v1/realtime';
  const wsUrl = `${wsBase}?model=${encodeURIComponent(model)}`;
  console.log(`[TTS-RT] connect ws=${wsBase} model=${model} lang=${lang}`);

  return new Promise((resolve, reject) => {
    const chunks = [];
    let settled = false;
    const ws = new WebSocket(wsUrl, { headers: { Authorization: `Bearer ${apiKey}` } });

    const done = (err, payload) => {
      if (settled) return;
      settled = true;
      try { ws.close(); } catch {}
      if (err) reject(err);
      else resolve(payload);
    };

    ws.on('open', () => {
      ws.send(JSON.stringify({
        event_id: `event_${Date.now()}`,
        type: 'session.update',
        session: {
          mode: 'server_commit',
          voice,
          language_type: 'Auto',
          response_format: 'pcm',
          sample_rate: 24000
        }
      }));

      ws.send(JSON.stringify({ type: 'input_text_buffer.append', text }));
      ws.send(JSON.stringify({ type: 'session.finish' }));
    });

    ws.on('message', (raw) => {
      try {
        const msg = JSON.parse(raw.toString('utf8'));
        if (msg.type === 'response.audio.delta' && msg.delta) {
          chunks.push(Buffer.from(msg.delta, 'base64'));
        } else if (msg.type === 'session.finished') {
          const pcm = Buffer.concat(chunks);
          if (!pcm.length) return done(new Error('qwen_tts_realtime_empty_audio'));
          const wav = pcm16ToWavBuffer(pcm, 24000, 1, 16);
          return done(null, {
            audioBase64: wav.toString('base64'),
            format: 'wav',
            provider: 'qwen',
            model,
            lang,
            voice
          });
        } else if (msg.type === 'error') {
          const e = msg.error?.message || JSON.stringify(msg.error || msg);
          return done(new Error(`qwen_tts_realtime_error:${e}`));
        }
      } catch {
        // ignore non-json frame
      }
    });

    ws.on('error', (e) => done(new Error(`qwen_tts_realtime_ws_error:${String(e?.message || e)}`)));
    ws.on('close', () => {
      if (!settled) done(new Error('qwen_tts_realtime_closed_early'));
    });

    setTimeout(() => done(new Error('qwen_tts_realtime_timeout')), Number(process.env.QWEN_TTS_REALTIME_TIMEOUT_MS || 20000));
  });
}

function streamQwenRealtimeTTS({ text, lang = 'en', voice = 'Cherry', model = 'qwen3-tts-flash-realtime', onChunk, onDone, onError }) {
  const apiKey = process.env.DASHSCOPE_API_KEY;
  if (!apiKey) {
    onError?.(new Error('missing_dashscope_api_key'));
    return () => {};
  }

  const wsBase = process.env.QWEN_TTS_REALTIME_WS_URL || 'wss://dashscope-intl.aliyuncs.com/api-ws/v1/realtime';
  const wsUrl = `${wsBase}?model=${encodeURIComponent(model)}`;
  const ws = new WebSocket(wsUrl, { headers: { Authorization: `Bearer ${apiKey}` } });
  let closed = false;

  const finish = (fn, arg) => {
    if (closed) return;
    closed = true;
    try { ws.close(); } catch {}
    fn?.(arg);
  };

  ws.on('open', () => {
    ws.send(JSON.stringify({
      event_id: `event_${Date.now()}`,
      type: 'session.update',
      session: {
        mode: 'server_commit',
        voice,
        language_type: 'Auto',
        response_format: 'pcm',
        sample_rate: 24000
      }
    }));
    ws.send(JSON.stringify({ type: 'input_text_buffer.append', text }));
    ws.send(JSON.stringify({ type: 'session.finish' }));
  });

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString('utf8'));
      if (msg.type === 'response.audio.delta' && msg.delta) {
        onChunk?.(msg.delta);
      } else if (msg.type === 'session.finished') {
        finish(onDone);
      } else if (msg.type === 'error') {
        const detail = msg.error?.message || JSON.stringify(msg.error || msg);
        finish(onError, new Error(`qwen_tts_realtime_error:${detail}`));
      }
    } catch {
      // ignore non-json frame
    }
  });

  ws.on('error', (e) => finish(onError, new Error(`qwen_tts_realtime_ws_error:${String(e?.message || e)}`)));
  ws.on('close', () => finish(onError, new Error('qwen_tts_realtime_closed_early')));

  const timeout = setTimeout(() => finish(onError, new Error('qwen_tts_realtime_timeout')), Number(process.env.QWEN_TTS_REALTIME_TIMEOUT_MS || 20000));

  return () => {
    clearTimeout(timeout);
    finish();
  };
}

function getWsTokenFromReq(req) {
  try {
    const u = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const qToken = u.searchParams.get('token');
    if (qToken) return qToken;
  } catch {}

  const h = req.headers?.authorization;
  if (typeof h === 'string') {
    const m = h.match(/^Bearer\s+(.+)$/i);
    if (m) return m[1];
  }
  return null;
}

function isGuestWsReq(req) {
  try {
    const u = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    return u.searchParams.get('guest') === '1';
  } catch {
    return false;
  }
}

async function verifyUserFromWsReq(req) {
  const token = getWsTokenFromReq(req);
  if (!token) throw new Error('missing_token');

  const init = await fbInitPromise;
  if (!init.ok) throw new Error('firebase_not_ready');

  const decoded = await admin.auth().verifyIdToken(token);
  return decoded.uid;
}

async function getUserUsage(uid) {
  const db = admin.firestore();
  const ref = db.collection('users').doc(uid);
  const snap = await ref.get();
  const data = snap.exists ? (snap.data() || {}) : {};

  const usageSecondsTotal = Number(data.usageSecondsTotal || 0);
  const usageLimitSeconds = Number(data.usageLimitSeconds || DEFAULT_USAGE_LIMIT_SECONDS);
  const remainingSeconds = Math.max(0, usageLimitSeconds - usageSecondsTotal);

  return {
    usageSecondsTotal,
    usageLimitSeconds,
    remainingSeconds
  };
}

async function addUserUsage(uid, secondsToAdd) {
  const safeSeconds = Math.max(0, Math.floor(secondsToAdd));
  if (!safeSeconds) return;

  const db = admin.firestore();
  const ref = db.collection('users').doc(uid);

  await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const data = snap.exists ? (snap.data() || {}) : {};
    const usageSecondsTotal = Number(data.usageSecondsTotal || 0);
    const usageLimitSeconds = Number(data.usageLimitSeconds || DEFAULT_USAGE_LIMIT_SECONDS);

    tx.set(ref, {
      usageSecondsTotal: usageSecondsTotal + safeSeconds,
      usageLimitSeconds,
      usageUpdatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
  });
}

app.get('/health', async (req, res) => {
  const init = await fbInitPromise;
  res.json({ ok: true, firebase_admin: init.ok ? 'ready' : 'not_ready' });
});

app.get('/api/v1/me', authMiddleware, async (req, res) => {
  const uid = req.user.uid;
  const plan = await getUserPlan(uid);
  res.json({ uid, plan, retention_days: retentionForPlan(plan) });
});

app.get('/api/v1/usage/me', authMiddleware, async (req, res) => {
  try {
    const uid = req.user.uid;
    const usage = await getUserUsage(uid);
    res.json({ uid, ...usage });
  } catch (e) {
    return jsonError(res, 500, 'Failed to read usage', { detail: String(e?.message || e) });
  }
});

// --- History APIs ---
app.post('/api/v1/history/conversations', authMiddleware, async (req, res) => {
  const uid = req.user.uid;
  const { langLeft, langRight, title } = req.body || {};
  if (!langLeft || !langRight) return jsonError(res, 400, 'Missing required fields: langLeft, langRight');

  const plan = await getUserPlan(uid);
  const retentionDays = retentionForPlan(plan);
  const now = new Date();
  const expireAt = new Date(now.getTime() + retentionDays * 24 * 60 * 60 * 1000);

  try {
    const db = admin.firestore();
    const ref = db.collection('users').doc(uid).collection('conversations').doc();
    const doc = {
      title: title || null,
      langLeft,
      langRight,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      lastMessageAt: null,
      isArchived: false,
      expireAt: admin.firestore.Timestamp.fromDate(expireAt)
    };
    await ref.set(doc);
    res.json({ conversationId: ref.id });
  } catch (e) {
    return jsonError(res, 500, 'Failed to create conversation', { detail: String(e?.message || e) });
  }
});

app.post('/api/v1/history/conversations/:conversationId/messages', authMiddleware, async (req, res) => {
  const uid = req.user.uid;
  const { conversationId } = req.params;
  const { side, sourceLang, targetLang, originalText, translatedText } = req.body || {};

  if (!side || !sourceLang || !targetLang || !originalText) {
    return jsonError(res, 400, 'Missing required fields');
  }

  const plan = await getUserPlan(uid);
  const retentionDays = retentionForPlan(plan);
  const now = new Date();
  const expireAt = new Date(now.getTime() + retentionDays * 24 * 60 * 60 * 1000);

  try {
    const db = admin.firestore();
    const convRef = db.collection('users').doc(uid).collection('conversations').doc(conversationId);
    const msgRef = convRef.collection('messages').doc();
    
    const msgDoc = {
      side,
      sourceLang,
      targetLang,
      originalText,
      translatedText: translatedText || null,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      expireAt: admin.firestore.Timestamp.fromDate(expireAt)
    };

    await db.runTransaction(async (tx) => {
      const convSnap = await tx.get(convRef);
      if (!convSnap.exists) throw new Error('not_found');
      tx.set(msgRef, msgDoc);
      tx.update(convRef, {
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        lastMessageAt: admin.firestore.FieldValue.serverTimestamp()
      });
    });
    res.json({ messageId: msgRef.id });
  } catch (e) {
    const status = e.message === 'not_found' ? 404 : 500;
    return jsonError(res, status, 'Failed to add message', { detail: String(e?.message || e) });
  }
});

app.get('/api/v1/history/conversations', authMiddleware, async (req, res) => {
  const uid = req.user.uid;
  const limit = Math.min(Number(req.query?.limit || 20), 50);
  try {
    const db = admin.firestore();
    const qs = await db.collection('users').doc(uid).collection('conversations')
      .orderBy('updatedAt', 'desc').limit(limit).get();
    const conversations = qs.docs.map(mapConversationDoc);
    res.json({ conversations });
  } catch (e) {
    return jsonError(res, 500, 'Failed to list conversations', { detail: String(e?.message || e) });
  }
});

app.get('/api/v1/history/conversations/:conversationId/messages', authMiddleware, async (req, res) => {
  const uid = req.user.uid;
  const { conversationId } = req.params;
  const limit = Math.min(Number(req.query?.limit || 50), 200);
  try {
    const db = admin.firestore();
    const qs = await db.collection('users').doc(uid).collection('conversations').doc(conversationId)
      .collection('messages').orderBy('createdAt', 'asc').limit(limit).get();
    const messages = qs.docs.map(mapMessageDoc);
    res.json({ messages });
  } catch (e) {
    return jsonError(res, 500, 'Failed to list messages', { detail: String(e?.message || e) });
  }
});

// --- Original Translate & ASR APIs ---
app.post('/api/v1/tts', async (req, res) => {
  const { text, lang = 'en', voice = 'Cherry', provider = 'qwen', model, stream = false } = req.body || {};
  if (!text) return jsonError(res, 400, 'Missing required field: text');

  console.log(`[TTS] request provider=${provider} model=${model || process.env.QWEN_TTS_MODEL || 'qwen3-tts-flash-realtime'} lang=${lang} text_len=${String(text).length}`);

  const selectedProvider = String(provider || 'qwen').toLowerCase();
  if (selectedProvider !== 'qwen') {
    return jsonError(res, 400, 'Unsupported tts provider', { provider: selectedProvider });
  }

  const normalizedLang = normalizeTtsLang(lang);
  const effectiveModel = model || process.env.QWEN_TTS_MODEL || 'qwen3-tts-flash-realtime';
  if (!String(effectiveModel).includes('realtime')) {
    return jsonError(res, 400, 'Only qwen realtime tts models are supported now', { model: effectiveModel });
  }
  if (!QWEN_TTS_LANG_WHITELIST.has(normalizedLang)) {
    return jsonError(res, 400, 'Unsupported language for qwen realtime tts', {
      lang,
      normalized_lang: normalizedLang,
      supported_langs: Array.from(QWEN_TTS_LANG_WHITELIST)
    });
  }

  try {
    const t0 = nowMs();

    if (stream) {
      res.setHeader('Content-Type', 'application/x-ndjson; charset=utf-8');
      res.setHeader('Cache-Control', 'no-cache, no-transform');
      res.setHeader('Connection', 'keep-alive');
      res.setHeader('X-Accel-Buffering', 'no');
      if (typeof res.flushHeaders === 'function') res.flushHeaders();
      res.write(JSON.stringify({ type: 'start', provider: 'qwen', model: effectiveModel }) + '\n');

      const stop = streamQwenRealtimeTTS({
        text,
        lang: normalizedLang,
        voice,
        model: effectiveModel,
        onChunk: (delta) => {
          res.write(JSON.stringify({ type: 'audio.delta', delta }) + '\n');
        },
        onDone: () => {
          res.write(JSON.stringify({ type: 'done' }) + '\n');
          res.end();
          console.log(`[TTS] stream success provider=qwen model=${effectiveModel} lang=${normalizedLang} ms=${nowMs() - t0}`);
        },
        onError: (e) => {
          res.write(JSON.stringify({ type: 'error', detail: String(e?.message || e) }) + '\n');
          res.end();
          console.error(`[TTS] stream failed provider=qwen model=${effectiveModel} lang=${normalizedLang} err=${String(e?.message || e)}`);
        }
      });

      // Don't listen on req.close here: request body finishes quickly and would cancel stream early.
      res.on('close', () => {
        try { stop?.(); } catch {}
      });
      return;
    }

    const result = await synthesizeWithQwenTTS({ text, lang: normalizedLang, voice, model: effectiveModel });
    console.log(`[TTS] success provider=${result?.provider || 'qwen'} model=${result?.model || effectiveModel} lang=${normalizedLang} ms=${nowMs() - t0}`);
    res.json(result);
  } catch (e) {
    console.error(`[TTS] failed provider=qwen model=${effectiveModel} lang=${normalizedLang} err=${String(e?.message || e)}`);
    return jsonError(res, 500, 'TTS failed', { detail: String(e?.message || e) });
  }
});

app.post('/api/v1/translate/text', async (req, res) => {
  const { text, source_lang, target_lang, stream = false, model, provider } = req.body || {};
  if (!text || !source_lang || !target_lang) {
    return jsonError(res, 400, 'Missing required fields: text, source_lang, target_lang');
  }

  const selectedProvider = resolveTranslationProvider({
    requestedProvider: provider,
    sourceLang: source_lang,
    targetLang: target_lang
  });

  const prompt = [
    'You are a professional spoken-language translation engine.',
    `Target language: ${target_lang}`,
    `Reference source language (may be inaccurate): ${source_lang}`,
    '',
    'Rules:',
    '1) Prioritize understanding via the reference source language; if the input is clearly in another language, auto-detect first, then translate.',
    '2) Output only the final translation. No explanations, no quotes, no prefixes/suffixes.',
    '3) Preserve numbers, proper nouns, time, currency, and units.',
    '4) Naturalize for spoken conversation when helpful, but do not change meaning.',
    '5) If the input is already in the target language, output it unchanged.',
    '',
    `Source text: ${text}`,
    'Translation:'
  ].join('\n');

  let url;
  let body;
  let headers;

  if (selectedProvider === 'openai') {
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) return jsonError(res, 500, 'Missing env OPENAI_API_KEY');
    url = process.env.OPENAI_TRANSLATE_URL || 'https://api.openai.com/v1/chat/completions';
    body = {
      model: model || process.env.OPENAI_TRANSLATE_MODEL || 'gpt-4o-mini',
      stream: !!stream,
      temperature: 0.1,
      messages: [{ role: 'user', content: prompt }]
    };
    headers = { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` };
  } else if (selectedProvider === 'minimax') {
    const apiKey = process.env.MINIMAX_API_KEY;
    if (!apiKey) return jsonError(res, 500, 'Missing env MINIMAX_API_KEY');

    url = process.env.MINIMAX_TRANSLATE_URL || 'https://api.minimaxi.com/v1/text/chatcompletion_v2';
    body = {
      model: model || process.env.MINIMAX_TRANSLATE_MODEL || 'MiniMax-M2.5',
      messages: [{ role: 'user', content: prompt }],
      temperature: 0.1,
      stream: !!stream
    };
    headers = {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${apiKey}`
    };
  } else if (selectedProvider === 'google_basic') {
    const apiKey = process.env.GOOGLE_TRANSLATE_API_KEY;
    if (!apiKey) return jsonError(res, 500, 'Missing env GOOGLE_TRANSLATE_API_KEY');
    if (stream) return jsonError(res, 400, 'Google Translation Basic streaming is not enabled');

    url = process.env.GOOGLE_TRANSLATE_URL || 'https://translation.googleapis.com/language/translate/v2';
    body = {
      q: text,
      source: source_lang,
      target: target_lang,
      format: 'text'
    };
    headers = {
      'Content-Type': 'application/json; charset=utf-8'
    };

    const sep = url.includes('?') ? '&' : '?';
    url = `${url}${sep}key=${encodeURIComponent(apiKey)}`;
  } else if (selectedProvider === 'gemini') {
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) return jsonError(res, 500, 'Missing env GEMINI_API_KEY');

    const geminiModel = model || process.env.GEMINI_TRANSLATE_MODEL || 'gemini-3.1-flash-lite';
    if (stream) return jsonError(res, 400, 'Gemini translation streaming is not enabled yet');

    url = process.env.GEMINI_TRANSLATE_URL || `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(geminiModel)}:generateContent`;
    body = {
      contents: [
        {
          parts: [{ text: prompt }]
        }
      ],
      generationConfig: {
        temperature: 0.1
      }
    };
    headers = {
      'Content-Type': 'application/json',
      'x-goog-api-key': apiKey
    };
  } else if (selectedProvider === 'qwen') {
    const apiKey = process.env.DASHSCOPE_API_KEY;
    if (!apiKey) return jsonError(res, 500, 'Missing env DASHSCOPE_API_KEY');

    const qwenModel = model || process.env.QWEN_TRANSLATE_MODEL || 'qwen3.5-flash';
    url = process.env.QWEN_TRANSLATE_URL || 'https://dashscope-intl.aliyuncs.com/compatible-mode/v1/chat/completions';
    body = {
      model: qwenModel,
      stream: !!stream,
      top_p: 0.8,
      temperature: 0.7,
      enable_search: false,
      enable_thinking: false,
      thinking_budget: 4000,
      result_format: 'message',
      messages: [{ role: 'user', content: prompt }]
    };
    headers = {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${apiKey}`
    };
  } else {
    const apiKey = process.env.DOUBAO_API_KEY;
    if (!apiKey) return jsonError(res, 500, 'Missing env DOUBAO_API_KEY');
    url = process.env.DOUBAO_TRANSLATE_URL || 'https://ark.cn-beijing.volces.com/api/v3/chat/completions';
    body = {
      model: model || process.env.DOUBAO_MODEL || 'doubao-seed-1-6-flash-250828',
      stream: !!stream,
      max_output_tokens: 1024,
      temperature: 0.1,
      thinking: { type: 'disabled' },
      messages: [{ role: 'user', content: prompt }]
    };
    headers = { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` };
  }

  const t0 = nowMs();
  const traceId = `tr_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
  const ac = new AbortController();
  const to = setTimeout(() => ac.abort(new Error('timeout')), 30000);

  console.log(`[Translate][trace] id=${traceId} in provider=${selectedProvider} source=${source_lang} target=${target_lang} chars=${String(text).length} stream=${!!stream}`);

  try {
    const upstreamStartMs = nowMs();
    const r = await fetch(url, {
      method: 'POST',
      headers,
      body: JSON.stringify(body),
      signal: ac.signal
    });
    const upstreamTtfbMs = nowMs() - upstreamStartMs;

    if (!r.ok) {
      const detail = await r.text().catch(() => '');
      const backendTotalMs = nowMs() - t0;
      console.error(`[Translate][trace] id=${traceId} upstream_error provider=${selectedProvider} status=${r.status} upstream_ttfb_ms=${upstreamTtfbMs} backend_total_ms=${backendTotalMs}`);
      res.setHeader('X-Trace-Id', traceId);
      res.setHeader('X-Provider', selectedProvider);
      return jsonError(res, 502, `${selectedProvider} error: ${r.status}`, { detail });
    }

    if (!stream) {
      const decodeStartMs = nowMs();
      const data = await r.json();
      let translation = data?.choices?.[0]?.message?.content ?? '';

      if (selectedProvider === 'minimax') {
        translation = data?.choices?.[0]?.message?.content
          || data?.reply
          || data?.base_resp?.status_msg
          || translation
          || '';
      } else if (selectedProvider === 'gemini') {
        translation = data?.candidates?.[0]?.content?.parts?.map((p) => p?.text || '').join('')
          || data?.candidates?.[0]?.content?.parts?.[0]?.text
          || '';
      } else if (selectedProvider === 'qwen') {
        translation = data?.choices?.[0]?.message?.content
          || data?.output?.choices?.[0]?.message?.content
          || data?.output_text
          || '';
      } else if (selectedProvider === 'google_basic') {
        translation = data?.data?.translations?.[0]?.translatedText
          || data?.translations?.[0]?.translatedText
          || '';
      }

      const usedModel = selectedProvider === 'gemini'
        ? (model || process.env.GEMINI_TRANSLATE_MODEL || 'gemini-3.1-flash-lite')
        : (selectedProvider === 'qwen'
          ? (model || process.env.QWEN_TRANSLATE_MODEL || 'qwen3.5-flash')
          : (selectedProvider === 'google_basic'
            ? 'google-translate-v2-basic'
            : (body?.model || model || null)));

      const decodeMs = nowMs() - decodeStartMs;
      const backendTotalMs = nowMs() - t0;
      console.log(`[Translate][trace] id=${traceId} done provider=${selectedProvider} model=${usedModel || 'unknown'} upstream_ttfb_ms=${upstreamTtfbMs} decode_ms=${decodeMs} backend_total_ms=${backendTotalMs}`);

      res.setHeader('X-Trace-Id', traceId);
      res.setHeader('X-Provider', selectedProvider);
      if (usedModel) {
        res.setHeader('X-Model', String(usedModel));
      }
      res.setHeader('X-Upstream-Latency-Ms', String(upstreamTtfbMs));
      res.setHeader('X-Backend-Latency-Ms', String(backendTotalMs));

      return res.json({
        translation,
        provider: selectedProvider,
        model: usedModel,
        trace_id: traceId,
        timing: {
          upstream_ttfb_ms: upstreamTtfbMs,
          decode_ms: decodeMs,
          backend_total_ms: backendTotalMs
        }
      });
    }

    res.setHeader('X-Trace-Id', traceId);
    res.setHeader('X-Provider', selectedProvider);
    res.setHeader('X-Upstream-Latency-Ms', String(upstreamTtfbMs));
    res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
    const reader = r.body.getReader();
    const decoder = new TextDecoder('utf-8');
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      res.write(decoder.decode(value, { stream: true }));
    }
    const backendTotalMs = nowMs() - t0;
    console.log(`[Translate][trace] id=${traceId} stream_done provider=${selectedProvider} upstream_ttfb_ms=${upstreamTtfbMs} backend_total_ms=${backendTotalMs}`);
    res.end();
  } catch (e) {
    const backendTotalMs = nowMs() - t0;
    console.error(`[Translate][trace] id=${traceId} failed provider=${selectedProvider} backend_total_ms=${backendTotalMs} err=${String(e?.message || e)}`);
    return jsonError(res, 500, `Translate failed: ${String(e?.message || e)}`);
  } finally {
    clearTimeout(to);
  }
});

// --- Real-time ASR WebSocket Proxy (Qwen-ASR Realtime) ---
const server = app.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
});

const rtasrWss = new WebSocketServer({ server, path: '/api/v1/asr/realtime' });

rtasrWss.on('connection', async (clientWs, req) => {
  let upstream = null;
  let uid = null;
  let wsStartedAtMs = nowMs();
  let initialSessionUpdateMsg = null;
  const pendingClientMsgs = [];
  const uiConfig = { mode: 'dual_button', leftLang: 'zh', rightLang: 'en' };
  const isGuest = isGuestWsReq(req);

  // OpenAI/Deepgram pseudo-streaming state
  let asrProvider = 'qwen';
  let openaiAudioChunks = [];
  let openaiLanguageHint = null;
  let openaiPartialInFlight = false;
  let openaiLastPartialAt = 0;
  let openaiLastPartialText = '';
  let openaiItemId = `item_openai_${Math.random().toString(36).slice(2, 10)}`;

  let deepgramUpstream = null;
  let deepgramLanguageHint = null;
  let deepgramModelHint = null;
  let deepgramItemId = `item_deepgram_${Math.random().toString(36).slice(2, 10)}`;
  let deepgramLastInterimText = '';
  let deepgramClosing = false;
  let pendingDeepgramFrames = [];

  if (!isGuest) {
    try {
      uid = await verifyUserFromWsReq(req);
      const usage = await getUserUsage(uid);
      if (usage.remainingSeconds <= 0) {
        clientWs.send(JSON.stringify({
          type: 'error',
          error: { code: 'usage_limit_exceeded', message: 'Usage limit exceeded (60 minutes).' }
        }));
        clientWs.close(1008, 'usage_limit_exceeded');
        return;
      }
    } catch (e) {
      clientWs.send(JSON.stringify({
        type: 'error',
        error: { code: 'unauthorized', message: `Unauthorized: ${String(e?.message || e)}` }
      }));
      clientWs.close(1008, 'unauthorized');
      return;
    }
  }

  function decideSideAndDirection(leftLang, rightLang, detectedLang) {
    if (!detectedLang) return { side: 'left', fromLang: leftLang, toLang: rightLang };
    if (detectedLang === leftLang) return { side: 'left', fromLang: leftLang, toLang: rightLang };
    if (detectedLang === rightLang) return { side: 'right', fromLang: rightLang, toLang: leftLang };
    if (detectedLang.startsWith(leftLang)) return { side: 'left', fromLang: leftLang, toLang: rightLang };
    if (detectedLang.startsWith(rightLang)) return { side: 'right', fromLang: rightLang, toLang: leftLang };
    return { side: 'left', fromLang: detectedLang, toLang: rightLang };
  }

  clientWs.on('message', async (buf) => {
    let msg;
    try {
      msg = JSON.parse(buf.toString('utf8'));
    } catch { return; }

    // Deepgram path (true realtime WS upstream)
    if (asrProvider === 'deepgram') {
      if (!deepgramUpstream) {
        if (msg?.type !== 'session.update') {
          return;
        }

        const s = msg.session || {};
        if (s.left_lang || s.leftLang) uiConfig.leftLang = s.left_lang || s.leftLang;
        if (s.right_lang || s.rightLang) uiConfig.rightLang = s.right_lang || s.rightLang;
        deepgramLanguageHint = uiConfig.leftLang || 'en';
        deepgramModelHint = s.model || resolveDeepgramModel({ requestedModel: null, languageHint: deepgramLanguageHint });

        const dgModel = resolveDeepgramModel({ requestedModel: deepgramModelHint, languageHint: deepgramLanguageHint });
        const dgLang = normalizeDeepgramLanguage(deepgramLanguageHint) || 'en';
        const dgBase = process.env.DEEPGRAM_WS_URL || 'wss://api.deepgram.com/v1/listen';
        const dgUrl = new URL(dgBase);
        dgUrl.searchParams.set('model', dgModel);
        dgUrl.searchParams.set('language', dgLang);
        dgUrl.searchParams.set('encoding', 'linear16');
        dgUrl.searchParams.set('sample_rate', '16000');
        dgUrl.searchParams.set('channels', '1');
        dgUrl.searchParams.set('interim_results', 'true');
        dgUrl.searchParams.set('vad_events', 'true');
        dgUrl.searchParams.set('punctuate', 'true');
        dgUrl.searchParams.set('smart_format', 'true');
        dgUrl.searchParams.set('endpointing', String(process.env.DEEPGRAM_ENDPOINTING_MS || 300));
        dgUrl.searchParams.set('utterance_end_ms', String(process.env.DEEPGRAM_UTTERANCE_END_MS || 1000));

        const dgKey = process.env.DEEPGRAM_API_KEY;
        if (!dgKey) {
          clientWs.send(JSON.stringify({
            type: 'error',
            error: { code: 'server_misconfigured', message: 'Missing env DEEPGRAM_API_KEY' }
          }));
          clientWs.close(1011, 'server_misconfigured');
          return;
        }

        deepgramClosing = false;
        deepgramUpstream = new WebSocket(dgUrl.toString(), {
          headers: { Authorization: `Token ${dgKey}` }
        });

        deepgramUpstream.on('open', () => {
          clientWs.send(JSON.stringify({ type: 'session.ready', provider: 'deepgram' }));
          for (const frame of pendingDeepgramFrames) {
            try { deepgramUpstream.send(frame); } catch {}
          }
          pendingDeepgramFrames = [];
        });

        deepgramUpstream.on('message', (data) => {
          try {
            const parsed = JSON.parse(data.toString('utf8'));

            if (parsed?.type === 'UtteranceEnd') {
              if (deepgramLastInterimText) {
                const { side, fromLang, toLang } = decideSideAndDirection(uiConfig.leftLang, uiConfig.rightLang, deepgramLanguageHint || uiConfig.leftLang);
                clientWs.send(JSON.stringify({
                  type: 'conversation.item.input_audio_transcription.completed',
                  item_id: deepgramItemId,
                  transcript: deepgramLastInterimText,
                  language: deepgramLanguageHint || uiConfig.leftLang,
                  ui_side: side,
                  ui_source_lang: fromLang,
                  ui_target_lang: toLang,
                  ui_mode: uiConfig.mode,
                  provider: 'deepgram',
                  model: dgModel
                }));
                deepgramItemId = `item_deepgram_${Math.random().toString(36).slice(2, 10)}`;
                deepgramLastInterimText = '';
              }
              return;
            }

            const transcript = String(parsed?.channel?.alternatives?.[0]?.transcript || '').trim();
            if (!transcript) return;

            const isFinal = !!parsed.is_final;
            const speechFinal = !!parsed.speech_final;
            if (isFinal) {
              const { side, fromLang, toLang } = decideSideAndDirection(uiConfig.leftLang, uiConfig.rightLang, deepgramLanguageHint || uiConfig.leftLang);
              clientWs.send(JSON.stringify({
                type: 'conversation.item.input_audio_transcription.completed',
                item_id: deepgramItemId,
                transcript,
                language: deepgramLanguageHint || uiConfig.leftLang,
                ui_side: side,
                ui_source_lang: fromLang,
                ui_target_lang: toLang,
                ui_mode: uiConfig.mode,
                provider: 'deepgram',
                model: dgModel
              }));
              deepgramItemId = `item_deepgram_${Math.random().toString(36).slice(2, 10)}`;
              deepgramLastInterimText = '';
            } else {
              deepgramLastInterimText = transcript;
              clientWs.send(JSON.stringify({
                type: 'conversation.item.input_audio_transcription.text',
                item_id: deepgramItemId,
                content_index: 0,
                text: transcript,
                stash: '',
                language: deepgramLanguageHint || uiConfig.leftLang,
                provider: 'deepgram',
                model: dgModel,
                speech_final: speechFinal
              }));
            }
          } catch {
            // ignore parse errors
          }
        });

        deepgramUpstream.on('error', (e) => {
          clientWs.send(JSON.stringify({
            type: 'error',
            error: { code: 'deepgram_asr_exception', message: String(e?.message || e) }
          }));
          clientWs.close(1011, 'deepgram_asr_exception');
        });

        deepgramUpstream.on('close', () => {
          deepgramUpstream = null;
          if (!deepgramClosing && clientWs.readyState === WebSocket.OPEN) {
            clientWs.send(JSON.stringify({ type: 'session.finished' }));
          }
        });

        return;
      }

      if (msg?.type === 'input_audio_buffer.append' && msg.audio) {
        try {
          const pcm = Buffer.from(String(msg.audio), 'base64');
          if (deepgramUpstream.readyState === WebSocket.OPEN) {
            deepgramUpstream.send(pcm);
          } else if (deepgramUpstream.readyState === WebSocket.CONNECTING) {
            pendingDeepgramFrames.push(pcm);
          }
        } catch {}
        return;
      }

      if (msg?.type === 'input_audio_buffer.commit') {
        // Deepgram finalizes by endpointing/silence; no explicit commit frame required.
        return;
      }

      if (msg?.type === 'session.finish') {
        deepgramClosing = true;
        try {
          if (deepgramUpstream.readyState === WebSocket.OPEN) {
            deepgramUpstream.send(JSON.stringify({ type: 'Finalize' }));
          }
        } catch {}
        try {
          deepgramUpstream?.close();
        } catch {}
        deepgramUpstream = null;
        clientWs.send(JSON.stringify({ type: 'session.finished' }));
        clientWs.close();
        return;
      }

      if (msg?.type === 'session.update') {
        const s = msg.session || {};
        if (s.left_lang || s.leftLang) uiConfig.leftLang = s.left_lang || s.leftLang;
        if (s.right_lang || s.rightLang) uiConfig.rightLang = s.right_lang || s.rightLang;
        deepgramLanguageHint = uiConfig.leftLang || 'en';
      }
      return;
    }

    // OpenAI gpt-4o-transcribe path (pseudo-streaming via periodic partial transcription)
    if (asrProvider === 'openai') {
      if (msg?.type === 'input_audio_buffer.append' && msg.audio) {
        openaiAudioChunks.push(String(msg.audio));

        const now = Date.now();
        const partialIntervalMs = Number(process.env.OPENAI_PARTIAL_INTERVAL_MS || 1200);
        const minChunksForPartial = Number(process.env.OPENAI_PARTIAL_MIN_CHUNKS || 8);

        if (!openaiPartialInFlight && openaiAudioChunks.length >= minChunksForPartial && (now - openaiLastPartialAt) >= partialIntervalMs) {
          openaiPartialInFlight = true;
          openaiLastPartialAt = now;

          const snapshotBuffers = openaiAudioChunks.map((b64) => Buffer.from(b64, 'base64'));
          transcribeWithOpenAIFromPCM({
            audioPcmChunks: snapshotBuffers,
            languageHint: openaiLanguageHint || 'en'
          })
            .then((partialText) => {
              const text = String(partialText || '').trim();
              if (!text || text === openaiLastPartialText) return;
              openaiLastPartialText = text;

              clientWs.send(JSON.stringify({
                type: 'conversation.item.input_audio_transcription.text',
                item_id: openaiItemId,
                content_index: 0,
                text,
                stash: '',
                language: openaiLanguageHint || uiConfig.leftLang,
                provider: 'openai'
              }));
            })
            .catch(() => {})
            .finally(() => {
              openaiPartialInFlight = false;
            });
        }
        return;
      }

      if (msg?.type === 'input_audio_buffer.commit' || msg?.type === 'session.finish') {
        try {
          const audioBuffers = openaiAudioChunks.map((b64) => Buffer.from(b64, 'base64'));
          openaiAudioChunks = [];

          const transcript = await transcribeWithOpenAIFromPCM({
            audioPcmChunks: audioBuffers,
            languageHint: openaiLanguageHint || 'en'
          });
          const { side, fromLang, toLang } = decideSideAndDirection(uiConfig.leftLang, uiConfig.rightLang, openaiLanguageHint || uiConfig.leftLang);

          clientWs.send(JSON.stringify({
            type: 'conversation.item.input_audio_transcription.completed',
            item_id: openaiItemId,
            transcript,
            language: openaiLanguageHint || uiConfig.leftLang,
            ui_side: side,
            ui_source_lang: fromLang,
            ui_target_lang: toLang,
            ui_mode: uiConfig.mode,
            provider: 'openai'
          }));

          openaiItemId = `item_openai_${Math.random().toString(36).slice(2, 10)}`;
          openaiLastPartialText = '';

          clientWs.send(JSON.stringify({ type: 'session.finished' }));
          if (msg?.type === 'session.finish') {
            clientWs.close();
          }
          return;
        } catch (e) {
          clientWs.send(JSON.stringify({
            type: 'error',
            error: { code: 'openai_asr_exception', message: String(e?.message || e) }
          }));
          clientWs.close(1011, 'openai_asr_exception');
          return;
        }
      }

      if (msg?.type === 'session.update') {
        const s = msg.session || {};
        if (s.left_lang || s.leftLang) uiConfig.leftLang = s.left_lang || s.leftLang;
        if (s.right_lang || s.rightLang) uiConfig.rightLang = s.right_lang || s.rightLang;
        openaiLanguageHint = uiConfig.leftLang || 'en';
      }
      return;
    }

    if (!upstream) {
      if (msg?.type === 'session.update') {
        initialSessionUpdateMsg = msg;
        const s = msg.session || {};
        if (s.mode) uiConfig.mode = s.mode;
        if (s.left_lang || s.leftLang) uiConfig.leftLang = s.left_lang || s.leftLang;
        if (s.right_lang || s.rightLang) uiConfig.rightLang = s.right_lang || s.rightLang;

        const requestedProvider = s.provider || null;
        const selectedProvider = resolveASRProvider({
          requestedProvider,
          leftLang: uiConfig.leftLang,
          rightLang: uiConfig.rightLang
        });

        asrProvider = selectedProvider;

        if (asrProvider === 'openai') {
          openaiAudioChunks = [];
          openaiLanguageHint = uiConfig.leftLang || 'en';
          openaiPartialInFlight = false;
          openaiLastPartialAt = 0;
          openaiLastPartialText = '';
          openaiItemId = `item_openai_${Math.random().toString(36).slice(2, 10)}`;

          clientWs.send(JSON.stringify({ type: 'session.ready', provider: 'openai' }));
          return;
        }

        if (asrProvider === 'deepgram') {
          deepgramLanguageHint = uiConfig.leftLang || 'en';
          deepgramModelHint = s.model || resolveDeepgramModel({ requestedModel: null, languageHint: deepgramLanguageHint });
          deepgramItemId = `item_deepgram_${Math.random().toString(36).slice(2, 10)}`;
          deepgramLastInterimText = '';
          pendingDeepgramFrames = [];
          deepgramClosing = false;

          const dgModel = resolveDeepgramModel({ requestedModel: deepgramModelHint, languageHint: deepgramLanguageHint });
          const dgLang = normalizeDeepgramLanguage(deepgramLanguageHint) || 'en';
          const dgBase = process.env.DEEPGRAM_WS_URL || 'wss://api.deepgram.com/v1/listen';
          const dgUrl = new URL(dgBase);
          dgUrl.searchParams.set('model', dgModel);
          dgUrl.searchParams.set('language', dgLang);
          dgUrl.searchParams.set('encoding', 'linear16');
          dgUrl.searchParams.set('sample_rate', '16000');
          dgUrl.searchParams.set('channels', '1');
          dgUrl.searchParams.set('interim_results', 'true');
          dgUrl.searchParams.set('punctuate', 'true');
          dgUrl.searchParams.set('smart_format', 'true');
          dgUrl.searchParams.set('endpointing', String(process.env.DEEPGRAM_ENDPOINTING_MS || 300));

          const dgKey = process.env.DEEPGRAM_API_KEY;
          if (!dgKey) {
            clientWs.send(JSON.stringify({
              type: 'error',
              error: { code: 'server_misconfigured', message: 'Missing env DEEPGRAM_API_KEY' }
            }));
            clientWs.close(1011, 'server_misconfigured');
            return;
          }

          deepgramUpstream = new WebSocket(dgUrl.toString(), {
            headers: { Authorization: `Token ${dgKey}` }
          });

          deepgramUpstream.on('open', () => {
            clientWs.send(JSON.stringify({ type: 'session.ready', provider: 'deepgram' }));
            for (const frame of pendingDeepgramFrames) {
              try { deepgramUpstream.send(frame); } catch {}
            }
            pendingDeepgramFrames = [];
          });

          deepgramUpstream.on('message', (data) => {
            try {
              const parsed = JSON.parse(data.toString('utf8'));
              const transcript = String(parsed?.channel?.alternatives?.[0]?.transcript || '').trim();
              if (!transcript) return;

              if (parsed.is_final) {
                const { side, fromLang, toLang } = decideSideAndDirection(uiConfig.leftLang, uiConfig.rightLang, deepgramLanguageHint || uiConfig.leftLang);
                clientWs.send(JSON.stringify({
                  type: 'conversation.item.input_audio_transcription.completed',
                  item_id: deepgramItemId,
                  transcript,
                  language: deepgramLanguageHint || uiConfig.leftLang,
                  ui_side: side,
                  ui_source_lang: fromLang,
                  ui_target_lang: toLang,
                  ui_mode: uiConfig.mode,
                  provider: 'deepgram',
                  model: dgModel
                }));
                deepgramItemId = `item_deepgram_${Math.random().toString(36).slice(2, 10)}`;
              } else {
                clientWs.send(JSON.stringify({
                  type: 'conversation.item.input_audio_transcription.text',
                  item_id: deepgramItemId,
                  content_index: 0,
                  text: transcript,
                  stash: '',
                  language: deepgramLanguageHint || uiConfig.leftLang,
                  provider: 'deepgram',
                  model: dgModel
                }));
              }
            } catch {}
          });

          deepgramUpstream.on('error', (e) => {
            clientWs.send(JSON.stringify({
              type: 'error',
              error: { code: 'deepgram_asr_exception', message: String(e?.message || e) }
            }));
            clientWs.close(1011, 'deepgram_asr_exception');
          });

          deepgramUpstream.on('close', () => {
            deepgramUpstream = null;
          });

          return;
        }

        const apiKey = process.env.DASHSCOPE_API_KEY;
        if (!apiKey) {
          clientWs.send(JSON.stringify({
            type: 'error',
            error: { code: 'server_misconfigured', message: 'Missing env DASHSCOPE_API_KEY' }
          }));
          clientWs.close(1011, 'server_misconfigured');
          return;
        }

        const modelName = s.model || process.env.QWEN_REALTIME_MODEL || 'qwen3-asr-flash-realtime';
        const baseUrl = process.env.QWEN_REALTIME_WS_URL || 'wss://dashscope-intl.aliyuncs.com/api-ws/v1/realtime';
        const url = `${baseUrl}?model=${modelName}`;

        upstream = new WebSocket(url, { headers: { Authorization: `Bearer ${apiKey}` } });
        upstream.on('open', () => {
          if (initialSessionUpdateMsg) {
            upstream.send(JSON.stringify(initialSessionUpdateMsg));
          }
          for (const queued of pendingClientMsgs) {
            upstream.send(JSON.stringify(queued));
          }
          pendingClientMsgs.length = 0;
        });
        upstream.on('message', (data) => {
          try {
            const parsed = JSON.parse(data.toString('utf8'));
            if (parsed.type === 'conversation.item.input_audio_transcription.text') {
              const { side, fromLang, toLang } = decideSideAndDirection(uiConfig.leftLang, uiConfig.rightLang, parsed.language);
              parsed.ui_side = side;
              parsed.ui_source_lang = fromLang;
              parsed.ui_target_lang = toLang;
              parsed.ui_mode = uiConfig.mode;
            }
            if (parsed.type === 'conversation.item.input_audio_transcription.completed') {
              const { side, fromLang, toLang } = decideSideAndDirection(uiConfig.leftLang, uiConfig.rightLang, parsed.language);
              parsed.ui_side = side;
              parsed.ui_source_lang = fromLang;
              parsed.ui_target_lang = toLang;
              parsed.ui_mode = uiConfig.mode;
            }
            clientWs.send(JSON.stringify(parsed));
          } catch {
            clientWs.send(data);
          }
        });
        upstream.on('close', () => clientWs.close());
        upstream.on('error', () => clientWs.close());
        return;
      }

      pendingClientMsgs.push(msg);
      return;
    }

    if (upstream.readyState === WebSocket.OPEN) {
      upstream.send(JSON.stringify(msg));
    } else {
      pendingClientMsgs.push(msg);
    }
  });

  clientWs.on('close', async () => {
    upstream?.terminate();
    upstream = null;

    if (uid && !isGuest) {
      const durationSec = Math.max(0, Math.ceil((nowMs() - wsStartedAtMs) / 1000));
      try {
        await addUserUsage(uid, durationSec);
      } catch (e) {
        console.error('[Usage] Failed to add usage:', e);
      }
    }
  });
});
