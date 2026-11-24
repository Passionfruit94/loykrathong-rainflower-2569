// Rainflower - ลอยกระทงออนไลน์ : backend
// ไม่มี dependency ภายนอก ใช้ Node.js 18+ เท่านั้น
//
//   node server.js          -> http://localhost:3000
//   PORT=8080 node server.js
//
// API
//   POST /api/float    ลอยกระทง  body: {id?, t, n, w}
//   GET  /api/stream   Server-Sent Events (เรียลไทม์: hello / float / online)
//   GET  /api/stats    {total, online}

'use strict';
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const PORT = Number(process.env.PORT) || 3000;
const HOST = process.env.HOST || '0.0.0.0';
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, 'krathong.json');
const PUBLIC_DIR = path.join(__dirname, 'public');
const TRUST_PROXY = process.env.TRUST_PROXY === '1'; // อยู่หลัง nginx / cloudflare ให้ตั้งเป็น 1

const KEEP_RECENT = 200;            // เก็บกระทงล่าสุดกี่อัน
const REPLAY_WINDOW_MS = 90_000;    // ผู้เข้าใหม่จะเห็นกระทงที่ลอยไปไม่เกินกี่ ms
const REPLAY_MAX = 25;
const RATE_LIMIT = { windowMs: 10_000, max: 4 }; // ต่อ IP
const MAX_BODY = 4 * 1024;
const KRATHONG_TYPES = 5;           // ต้องตรงกับจำนวนแบบกระทงในหน้าเว็บ

// ---------- Storage ----------
let state = { total: 0, recent: [] };
try {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  const raw = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
  if (Number.isFinite(raw.total)) state.total = Math.max(0, Math.floor(raw.total));
  if (Array.isArray(raw.recent)) state.recent = raw.recent.slice(-KEEP_RECENT);
} catch (e) {
  if (e.code !== 'ENOENT') console.warn('อ่านไฟล์ข้อมูลไม่ได้ เริ่มใหม่:', e.message);
}

let saveTimer = null;
function saveNow() {
  clearTimeout(saveTimer);
  saveTimer = null;
  const tmp = DATA_FILE + '.tmp';
  try {
    fs.writeFileSync(tmp, JSON.stringify(state));
    fs.renameSync(tmp, DATA_FILE); // เขียนแบบ atomic กันไฟล์พังตอนปิดเครื่อง
  } catch (e) {
    console.error('บันทึกข้อมูลไม่สำเร็จ:', e.message);
  }
}
function scheduleSave() {
  if (!saveTimer) saveTimer = setTimeout(saveNow, 1000);
}

// ---------- Helpers ----------
const clean = (v, max) =>
  String(v ?? '')
    .replace(/[\u0000-\u001f\u007f​-‏‪-‮⁦-⁩]/g, ' ') // control + bidi
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, max);

const recentIds = new Set(state.recent.map((k) => k.id));

const hits = new Map(); // ip -> [timestamps]
function limited(ip) {
  const now = Date.now();
  const arr = (hits.get(ip) || []).filter((t) => now - t < RATE_LIMIT.windowMs);
  if (arr.length >= RATE_LIMIT.max) {
    hits.set(ip, arr);
    return true;
  }
  arr.push(now);
  hits.set(ip, arr);
  return false;
}
setInterval(() => {
  const now = Date.now();
  for (const [ip, arr] of hits) {
    const f = arr.filter((t) => now - t < RATE_LIMIT.windowMs);
    f.length ? hits.set(ip, f) : hits.delete(ip);
  }
}, 60_000).unref();

function clientIp(req) {
  if (TRUST_PROXY) {
    const xf = req.headers['x-forwarded-for'];
    if (xf) return String(xf).split(',')[0].trim();
  }
  return req.socket.remoteAddress || 'unknown';
}

function json(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
    'Content-Length': Buffer.byteLength(body),
  });
  res.end(body);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', (c) => {
      size += c.length;
      if (size > MAX_BODY) {
        reject(Object.assign(new Error('too large'), { status: 413 }));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

// ---------- Realtime (SSE) ----------
const clients = new Set();

function sse(res, event, data) {
  res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
}
function broadcast(event, data) {
  for (const res of clients) {
    try { sse(res, event, data); } catch { /* ปล่อยให้ close handler จัดการ */ }
  }
}
setInterval(() => {
  for (const res of clients) {
    try { res.write(': ping\n\n'); } catch { /* ignore */ }
  }
}, 25_000).unref();

function handleStream(req, res) {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no', // ปิด buffering ของ nginx
  });
  res.write('retry: 3000\n\n');

  clients.add(res);
  const now = Date.now();
  const replay = state.recent
    .filter((k) => now - k.ts < REPLAY_WINDOW_MS)
    .slice(-REPLAY_MAX)
    .map((k) => ({ id: k.id, t: k.t, n: k.n, w: k.w, age: now - k.ts }));
  sse(res, 'hello', { total: state.total, online: clients.size, recent: replay });
  broadcast('online', { online: clients.size });

  req.on('close', () => {
    clients.delete(res);
    broadcast('online', { online: clients.size });
  });
}

// ---------- API ----------
async function handleFloat(req, res) {
  const ip = clientIp(req);
  if (limited(ip)) return json(res, 429, { ok: false, error: 'rate_limited' });

  let body;
  try {
    body = JSON.parse(await readBody(req));
  } catch (e) {
    return json(res, e.status || 400, { ok: false, error: e.status === 413 ? 'too_large' : 'bad_json' });
  }
  if (!body || typeof body !== 'object') return json(res, 400, { ok: false, error: 'bad_json' });

  const t = Number(body.t);
  if (!Number.isInteger(t) || t < 0 || t >= KRATHONG_TYPES) return json(res, 400, { ok: false, error: 'bad_type' });

  // id ที่ client สร้างเอง ใช้เพื่อให้ผู้ส่งไม่เห็นกระทงซ้ำตอนได้รับ event กลับมา
  let id = typeof body.id === 'string' && /^[a-z0-9]{6,40}$/.test(body.id) ? body.id : '';
  if (!id || recentIds.has(id)) id = crypto.randomUUID().replace(/-/g, '').slice(0, 20);

  const k = {
    id,
    t,
    n: clean(body.n, 20) || 'กระทงนิรนาม',
    w: clean(body.w, 80) || 'ขอให้ทุกคนมีความสุข',
    ts: Date.now(),
  };

  state.total += 1;
  state.recent.push(k);
  recentIds.add(k.id);
  if (state.recent.length > KEEP_RECENT) {
    const old = state.recent.splice(0, state.recent.length - KEEP_RECENT);
    for (const o of old) recentIds.delete(o.id);
  }
  scheduleSave();

  broadcast('float', { id: k.id, t: k.t, n: k.n, w: k.w, total: state.total });
  json(res, 200, { ok: true, id: k.id, total: state.total });
}

// ---------- Static files ----------
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.json': 'application/json; charset=utf-8',
  '.woff2': 'font/woff2',
};

function serveStatic(req, res, pathname) {
  let rel;
  try { rel = decodeURIComponent(pathname); } catch { res.writeHead(400); return res.end(); }
  if (rel.endsWith('/')) rel += 'index.html';
  const file = path.normalize(path.join(PUBLIC_DIR, rel));
  if (file !== PUBLIC_DIR && !file.startsWith(PUBLIC_DIR + path.sep)) {
    res.writeHead(403);
    return res.end('Forbidden');
  }
  fs.readFile(file, (err, buf) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      return res.end('Not found');
    }
    const ext = path.extname(file).toLowerCase();
    res.writeHead(200, {
      'Content-Type': MIME[ext] || 'application/octet-stream',
      'Cache-Control': ext === '.html' ? 'no-cache' : 'public, max-age=3600',
      'X-Content-Type-Options': 'nosniff',
    });
    res.end(req.method === 'HEAD' ? undefined : buf);
  });
}

// ---------- Server ----------
const server = http.createServer(async (req, res) => {
  const { pathname } = new URL(req.url, 'http://localhost');
  try {
    if (pathname === '/api/stream' && req.method === 'GET') return handleStream(req, res);
    if (pathname === '/api/float' && req.method === 'POST') return await handleFloat(req, res);
    if (pathname === '/api/stats' && req.method === 'GET') return json(res, 200, { total: state.total, online: clients.size });
    if (pathname.startsWith('/api/')) return json(res, 404, { ok: false, error: 'not_found' });
    if (req.method === 'GET' || req.method === 'HEAD') return serveStatic(req, res, pathname);
    res.writeHead(405);
    res.end();
  } catch (e) {
    console.error(e);
    if (!res.headersSent) json(res, 500, { ok: false, error: 'server_error' });
    else res.end();
  }
});
server.keepAliveTimeout = 65_000;

server.listen(PORT, HOST, () => {
  console.log(`ลอยกระทง Rainflower พร้อมแล้ว: http://localhost:${PORT}  (กระทงทั้งหมด ${state.total})`);
});

function shutdown() {
  console.log('\nกำลังบันทึกข้อมูลและปิดเซิร์ฟเวอร์...');
  saveNow();
  for (const res of clients) { try { res.end(); } catch { /* ignore */ } }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 1500).unref();
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

module.exports = { server }; // เผื่อใช้ทดสอบ
