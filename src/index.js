import 'dotenv/config';
import express from 'express';
import morgan from 'morgan';
import multer from 'multer';
import path from 'path';
import { fileURLToPath } from 'url';
import { WebSocketServer, WebSocket } from 'ws';
import fs from 'fs/promises';
import admin from 'firebase-admin';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// --- Firebase Admin bootstrap (env -> file) ---
async function ensureFirebaseCredentialsFile() {
  const json = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
  if (!json) return { ok: false, reason: 'missing FIREBASE_SERVICE_ACCOUNT_JSON' };

  const configuredPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
  const preferredPath = configuredPath && configuredPath.trim().length > 0 ? configuredPath : '/tmp/firebase-service-account.json';

  try {
    const dir = path.dirname(preferredPath);
    await fs.mkdir(dir, { recursive: true }).catch(() => {});
    await fs.writeFile(preferredPath, json, { encoding: 'utf8' });
    process.env.GOOGLE_APPLICATION_CREDENTIALS = preferredPath;
    return { ok: true, path: preferredPath, fallback: false };
  } catch (e) {
    const fallbackPath = '/tmp/firebase-service-account.json';
    try {
      await fs.writeFile(fallbackPath, json, { encoding: 'utf8' });
      process.env.GOOGLE_APPLICATION_CREDENTIALS = fallbackPath;
      return { ok: true, path: fallbackPath, fallback: true, error: String(e?.message || e) };
    } catch (e2) {
      return { ok: false, reason: 'failed to write fallback file', error: String(e2?.message || e2) };
    }
  }
}

async function initFirebaseAdmin() {
  if (admin.apps?.length) return { ok: true, already: true };

  const cred = await ensureFirebaseCredentialsFile();
  if (!cred.ok) {
    console.error('[Firebase] Credentials not found:', cred.reason);
    return { ok: false, reason: cred.reason };
  }

  try {
    admin.initializeApp({ credential: admin.credential.applicationDefault() });
    console.log('[Firebase] Admin SDK initialized successfully using:', cred.path);
    return { ok: true, cred };
  } catch (e) {
    console.error('[Firebase] Initialization error:', e);
    return { ok: false, reason: String(e?.message || e), cred };
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

async function getUserPlan(uid) {
  // For now: default free. Read from Firestore once RevenueCat webhook is set up.
  return 'free';
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
    const conversations = qs.docs.map(d => ({ id: d.id, ...d.data() }));
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
    const messages = qs.docs.map(d => ({ id: d.id, ...d.data() }));
    res.json({ messages });
  } catch (e) {
    return jsonError(res, 500, 'Failed to list messages', { detail: String(e?.message || e) });
  }
});

// --- Original Translate & ASR APIs ---
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
  const to = setTimeout(() => ac.abort(new Error('timeout')), 30000);

  try {
    const r = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
      body: JSON.stringify(body),
      signal: ac.signal
    });

    if (!r.ok) return jsonError(res, 502, `Doubao error: ${r.status}`);

    if (!stream) {
      const data = await r.json();
      const translation = data?.choices?.[0]?.message?.content ?? '';
      return res.json({ translation, timing: { total_ms: nowMs() - t0 } });
    }

    res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
    const reader = r.body.getReader();
    const decoder = new TextDecoder('utf-8');
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      res.write(decoder.decode(value, { stream: true }));
    }
    res.end();
  } catch (e) {
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

rtasrWss.on('connection', (clientWs) => {
  let upstream = null;
  const uiConfig = { mode: 'dual_button', leftLang: 'zh', rightLang: 'en' };

  function decideSideAndDirection(leftLang, rightLang, detectedLang) {
    if (!detectedLang) return { side: 'left', fromLang: leftLang, toLang: rightLang };
    if (detectedLang === leftLang) return { side: 'left', fromLang: leftLang, toLang: rightLang };
    if (detectedLang === rightLang) return { side: 'right', fromLang: rightLang, toLang: leftLang };
    if (detectedLang.startsWith(leftLang)) return { side: 'left', fromLang: leftLang, toLang: rightLang };
    if (detectedLang.startsWith(rightLang)) return { side: 'right', fromLang: rightLang, toLang: leftLang };
    return { side: 'left', fromLang: detectedLang, toLang: rightLang };
  }

  clientWs.on('message', (buf) => {
    let msg;
    try {
      msg = JSON.parse(buf.toString('utf8'));
    } catch { return; }

    if (!upstream) {
      if (msg?.type !== 'session.update') return;
      const s = msg.session || {};
      if (s.mode) uiConfig.mode = s.mode;
      if (s.left_lang || s.leftLang) uiConfig.leftLang = s.left_lang || s.leftLang;
      if (s.right_lang || s.rightLang) uiConfig.rightLang = s.right_lang || s.rightLang;

      const apiKey = process.env.DASHSCOPE_API_KEY;
      const modelName = s.model || 'qwen3-asr-flash-realtime';
      const baseUrl = process.env.QWEN_REALTIME_WS_URL || 'wss://dashscope-intl.aliyuncs.com/api-ws/v1/realtime';
      const url = `${baseUrl}?model=${modelName}`;

      upstream = new WebSocket(url, { headers: { Authorization: `Bearer ${apiKey}` } });
      upstream.on('open', () => upstream.send(JSON.stringify(msg)));
      upstream.on('message', (data) => {
        try {
          const parsed = JSON.parse(data.toString('utf8'));
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
    if (upstream.readyState === WebSocket.OPEN) upstream.send(JSON.stringify(msg));
  });

  clientWs.on('close', () => {
    upstream?.terminate();
    upstream = null;
  });
});
