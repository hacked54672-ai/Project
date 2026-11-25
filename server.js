const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 10000;
const MAX_BUFFER = 2 * 1024 * 1024;
const MAX_PANELS = 24;
const MAX_PER_IP = 8;

const devices = new Map();   // id -> {ws, info, panels:Set}
const ips = new Map();       // ip -> count

const server = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x');
  if (u.pathname === '/health') { res.writeHead(200); return res.end('ok'); }

  if (u.pathname === '/api/list') {
    const out = [];
    for (const [id, d] of devices)
      out.push({ id, model: d.info.model, sdk: d.info.sdk, panels: d.panels.size });
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    return res.end(JSON.stringify({ total: out.length, devices: out }));
  }

  if (u.pathname === '/' || u.pathname === '/index.html') {
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
  const u = new URL(req.url, 'http://x');
  if (u.pathname !== '/panel' && u.pathname !== '/device') return sock.destroy();

  const ip = (req.headers['x-forwarded-for'] || sock.remoteAddress || '?')
    .toString().split(',')[0].trim();
  const n = ips.get(ip) || 0;
  if (n >= MAX_PER_IP) return deny(sock, 429, 'too many connections from ip');

  if (u.pathname === '/panel') {
    let panels = 0;
    for (const ws of wss.clients) if (ws._panel) panels++;
    if (panels >= MAX_PANELS) return deny(sock, 503, 'panel limit');
  }

  ips.set(ip, n + 1);
  sock._ip = ip;
  wss.handleUpgrade(req, sock, head, ws => {
    ws._ip = ip;
    wss.emit('connection', ws, u);
  });
});

function deny(sock, code, msg) {
  sock.write(`HTTP/1.1 ${code} ${msg}\r\nConnection: close\r\n\r\n`);
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
  if (old) { try { old.ws.close(4000, 'replaced'); } catch {} }

  const entry = { ws, info, panels: new Set() };
  devices.set(id, entry);
  ws._dev = id;

  ws.on('message', (data, bin) => {
    entry.info.last = Date.now();
    if (bin) {
      for (const p of entry.panels) {
        if (data[0] !== 4 && p.bufferedAmount > MAX_BUFFER) continue;
        safeSend(p, data);
      }
      return;
    }
    const s = data.toString();
    for (const p of entry.panels) safeSend(p, s);
  });

  ws.on('close', () => {
    if (devices.get(id) === entry) devices.delete(id);
    broadcastList();
  });
  ws.on('error', () => {});
  broadcastList();
}

/* ---------------- panel ---------------- */

function onPanel(ws) {
  ws._panel = true;
  let attached = null;
  safeSend(ws, listPayload());

  ws.on('message', (data, bin) => {
    if (bin) {
      const d = attached && devices.get(attached);
      if (d) safeSend(d.ws, data);
      return;
    }
    let m;
    try { m = JSON.parse(data.toString()); } catch { return; }

    if (m.t === 'list') { safeSend(ws, listPayload()); return; }

    if (m.t === 'attach') {
      const d = devices.get(m.id);
      if (!d) { safeSend(ws, '{"t":"err","m":"device offline"}'); return; }
      if (attached) {
        const p = devices.get(attached);
        if (p) { p.panels.delete(ws); safeSend(p.ws, '{"t":"panel.detach"}'); }
      }
      attached = m.id;
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
      broadcastList();
      return;
    }

    const d = attached && devices.get(attached);
    if (d) safeSend(d.ws, JSON.stringify(m));
  });

  ws.on('close', () => {
    if (attached) {
      const d = devices.get(attached);
      if (d) { d.panels.delete(ws); safeSend(d.ws, '{"t":"panel.detach"}'); }
    }
    broadcastList();
  });
  ws.on('error', () => {});
}

/* ---------------- housekeeping ---------------- */

setInterval(() => {
  for (const ws of wss.clients) {
    if (ws._panel || ws.readyState !== 1) continue;
    try { ws.ping(); } catch {}
  }
  const now = Date.now();
  for (const [id, d] of devices) {
    if (d.ws.readyState !== 1 || now - d.info.last > 120000) {
      devices.delete(id);
      try { d.ws.terminate(); } catch {}
      broadcastList();
    }
  }
}, 25000);

wss.on('connection', ws => {
  ws.on('close', () => {
    const ip = ws._ip;
    if (!ip) return;
    const n = (ips.get(ip) || 1) - 1;
    if (n <= 0) ips.delete(ip); else ips.set(ip, n);
  });
});

server.listen(PORT, '0.0.0.0', () => console.log('relay on ' + PORT));