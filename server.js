const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 10000;
const MAX_BUFFER = 2 * 1024 * 1024;

// panel-only caps. /device is never blocked by these.
const MAX_PANELS = 60;
const MAX_PER_IP = 24;

const IDLE_MS = 300000;      // 5 min no traffic -> drop
const PING_MS = 25000;

const devices = new Map();   // id -> {ws, info, panels:Set}
const ips = new Map();       // ip -> panel count

const server = http.createServer((req, res) => {
  let u;
  try { u = new URL(req.url, 'http://x'); } catch { res.writeHead(400); return res.end('bad url'); }

  if (u.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain', 'Cache-Control': 'no-store' });
    return res.end('ok');
  }

  if (u.pathname === '/api/list') {
    const out = [];
    for (const [id, d] of devices)
      out.push({ id, model: d.info.model, sdk: d.info.sdk, panels: d.panels.size });
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    return res.end(JSON.stringify({ total: out.length, devices: out }));
  }

  if (u.pathname === '/' || u.pathname === '/index.html' || u.pathname === '/panel') {
    fs.readFile(path.join(__dirname, 'public', 'index.html'), (e, b) => {
      if (e) { res.writeHead(500); return res.end('missing index.html'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
      res.end(b);
    });
    return;
  }
  res.writeHead(404); res.end('404');
});

const wss = new WebSocketServer({ noServer: true, maxPayload: 16 * 1024 * 1024 });

server.on('upgrade', (req, sock, head) => {
  let u;
  try { u = new URL(req.url, 'http://x'); } catch { return sock.destroy(); }

  const isPanel = u.pathname === '/panel';
  const isDevice = u.pathname === '/device';
  if (!isPanel && !isDevice) return sock.destroy();

  const ip = (req.headers['x-forwarded-for'] || sock.remoteAddress || '?')
    .toString().split(',')[0].trim();

  // limits apply to panels only - devices always get through
  if (isPanel) {
    let panels = 0;
    for (const ws of wss.clients) if (ws._panel) panels++;
    if (panels >= MAX_PANELS) return deny(sock, 503, 'Service Unavailable', 'panel limit');

    const n = ips.get(ip) || 0;
    if (n >= MAX_PER_IP) return deny(sock, 429, 'Too Many Requests', 'too many panels from ip');
    ips.set(ip, n + 1);
  }

  wss.handleUpgrade(req, sock, head, ws => {
    ws._ip = ip;
    ws._panel = isPanel;
    ws._alive = true;
    wss.emit('connection', ws, u);
  });
});

function deny(sock, code, text, msg) {
  const body = msg || text;
  sock.write(
    'HTTP/1.1 ' + code + ' ' + text + '\r\n' +
    'Connection: close\r\n' +
    'Content-Type: text/plain\r\n' +
    'Content-Length: ' + Buffer.byteLength(body) + '\r\n\r\n' +
    body
  );
  sock.destroy();
}

function listPayload() {
  const out = [];
  for (const [id, d] of devices)
    out.push({ id, model: d.info.model, sdk: d.info.sdk, panels: d.panels.size, last: d.info.last });
  return JSON.stringify({ t: 'list', devices: out, total: out.length });
}

function broadcastList() {
  const msg = listPayload();
  for (const ws of wss.clients) if (ws._panel) safeSend(ws, msg);
}

function safeSend(ws, data) {
  if (!ws || ws.readyState !== 1) return false;
  if (ws.bufferedAmount > MAX_BUFFER) return false;
  try { ws.send(data); return true; } catch { return false; }
}

wss.on('connection', (ws, u) => {
  ws.on('pong', () => { ws._alive = true; });
  ws.on('error', () => {});

  ws.on('close', () => {
    if (ws._panel) {
      const n = (ips.get(ws._ip) || 1) - 1;
      if (n <= 0) ips.delete(ws._ip); else ips.set(ws._ip, n);
      broadcastList();
    }
  });

  if (u.pathname === '/device') return onDevice(ws, u);
  onPanel(ws);
});

/* ---------------- device ---------------- */

function onDevice(ws, u) {
  const id = (u.searchParams.get('id') || '').slice(0, 40)
    || crypto.randomBytes(4).toString('hex');

  const info = {
    model: (u.searchParams.get('m') || '?').slice(0, 60),
    sdk: parseInt(u.searchParams.get('a') || '0', 10) || 0,
    last: Date.now()
  };

  const old = devices.get(id);
  if (old && old.ws !== ws) {
    // hand over attached panels so they are not left hanging
    for (const p of old.panels) {
      p._attached = id;
      safeSend(p, JSON.stringify({ t: 'attached', id, model: info.model }));
    }
    try { old.ws.close(4000, 'replaced'); } catch {}
  }

  const entry = { ws, info, panels: (old && old.panels) || new Set() };
  devices.set(id, entry);
  ws._dev = id;

  ws.on('message', (data, bin) => {
    ws._alive = true;
    entry.info.last = Date.now();

    if (bin) {
      if (!data.length) return;
      const noDrop = data[0] === 4;   // channel 4 never drops
      for (const p of entry.panels) {
        if (!noDrop && p.bufferedAmount > MAX_BUFFER) continue;
        safeSend(p, data);
      }
      return;
    }

    const s = data.toString();
    for (const p of entry.panels) safeSend(p, s);
  });

  ws.on('close', () => {
    if (devices.get(id) !== entry) return;   // already replaced
    devices.delete(id);
    for (const p of entry.panels) {
      p._attached = null;
      safeSend(p, JSON.stringify({ t: 'err', m: 'device gone', id }));
      safeSend(p, JSON.stringify({ t: 'device.gone', id }));
    }
    entry.panels.clear();
    broadcastList();
  });

  broadcastList();
}

/* ---------------- panel ---------------- */

function onPanel(ws) {
  let attached = null;
  safeSend(ws, listPayload());

  ws.on('message', (data, bin) => {
    ws._alive = true;

    if (bin) {
      const d = attached && devices.get(attached);
      if (d) safeSend(d.ws, data);
      return;
    }

    let m;
    try { m = JSON.parse(data.toString()); } catch { return; }
    if (!m || typeof m !== 'object') return;

    if (m.t === 'list') { safeSend(ws, listPayload()); return; }

    if (m.t === 'attach') {
      const d = devices.get(m.id);
      if (!d) { safeSend(ws, JSON.stringify({ t: 'err', m: 'device offline' })); return; }

      if (attached) {
        const p = devices.get(attached);
        if (p) { p.panels.delete(ws); safeSend(p.ws, '{"t":"panel.detach"}'); }
      }

      attached = m.id;
      ws._attached = attached;
      d.panels.add(ws);
      safeSend(ws, JSON.stringify({ t: 'attached', id: m.id, model: d.info.model }));
      safeSend(d.ws, '{"t":"panel.attach"}');
      broadcastList();
      return;
    }

    if (m.t === 'detach') {
      const d = attached && devices.get(attached);
      if (d) { d.panels.delete(ws); safeSend(d.ws, '{"t":"panel.detach"}'); }
      attached = null;
      ws._attached = null;
      broadcastList();
      return;
    }

    if (!attached) { safeSend(ws, '{"t":"err","m":"not attached"}'); return; }

    const d = devices.get(attached);
    if (!d) {
      attached = null;
      ws._attached = null;
      safeSend(ws, '{"t":"err","m":"device offline"}');
      broadcastList();
      return;
    }
    safeSend(d.ws, JSON.stringify(m));
  });

  ws.on('close', () => {
    const d = attached && devices.get(attached);
    if (d) {
      d.panels.delete(ws);
      safeSend(d.ws, '{"t":"panel.detach"}');
    }
    attached = null;
  });
}

/* ---------------- housekeeping ---------------- */

setInterval(() => {
  const now = Date.now();

  for (const ws of wss.clients) {
    if (ws.readyState !== 1) continue;
    if (ws._alive === false) {
      try { ws.terminate(); } catch {}
      continue;
    }
    ws._alive = false;
    try { ws.ping(); } catch {}
  }

  let changed = false;
  for (const [id, d] of devices) {
    if (d.ws.readyState !== 1 || now - d.info.last > IDLE_MS) {
      devices.delete(id);
      try { d.ws.terminate(); } catch {}
      changed = true;
    }
  }
  if (changed) broadcastList();
}, PING_MS);

server.listen(PORT, '0.0.0.0', () => console.log('relay on ' + PORT));

function shutdown() {
  try { server.close(); } catch {}
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
