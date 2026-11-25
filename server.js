'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const TOKEN = process.env.TOKEN || 'lisa123';
const PUB = path.join(__dirname, 'public');

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.png': 'image/png',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon'
};

const server = http.createServer((req, res) => {
  let p = decodeURIComponent((req.url || '/').split('?')[0]);
  if (p === '/health') { res.writeHead(200); return res.end('ok'); }
  if (p === '/') p = '/index.html';
  const file = path.join(PUB, path.normalize(p).replace(/^(\.\.[\/\\])+/, ''));
  fs.readFile(file, (e, b) => {
    if (e) { res.writeHead(404); return res.end('404'); }
    res.writeHead(200, {
      'Content-Type': MIME[path.extname(file)] || 'application/octet-stream',
      'Cache-Control': 'no-store'
    });
    res.end(b);
  });
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 8 * 1024 * 1024 });

const devices = new Map();   // id -> { ws, name, model, batt, sdk }
const panels = new Set();    // { ws, dev }
let seq = 1;

function safe(ws, str) { try { if (ws.readyState === 1) ws.send(str); } catch (e) {} }

function devList() {
  const out = [];
  for (const [id, d] of devices)
    out.push({ id, name: d.name, model: d.model, batt: d.batt, online: d.ws.readyState === 1 });
  return out;
}

function pushDevices() {
  const m = JSON.stringify({ t: 'devices', list: devList() });
  for (const p of panels) safe(p.ws, m);
}

function toPanels(dev, str) {
  for (const p of panels) if (p.dev === dev) safe(p.ws, str);
}

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://x');
  const role = url.searchParams.get('role');
  const token = url.searchParams.get('token') || '';
  if (TOKEN && token !== TOKEN) { ws.close(4001, 'bad token'); return; }

  if (role === 'device') {
    const id = (url.searchParams.get('id') || '').slice(0, 40) || ('d' + (seq++) + Math.random().toString(36).slice(2, 6));
    const rec = {
      ws,
      name: url.searchParams.get('name') || 'device',
      model: url.searchParams.get('model') || '',
      batt: -1,
      sdk: 0
    };
    devices.set(id, rec);
    ws.send(JSON.stringify({ t: 'welcome', id }));
    pushDevices();
    console.log('device up', id, rec.name);

    ws.on('message', (buf) => {
      let o; try { o = JSON.parse(buf.toString()); } catch (e) { return; }
      if (o.t === 'hello') {
        rec.name = o.name || rec.name;
        rec.model = o.model || rec.model;
        rec.batt = typeof o.batt === 'number' ? o.batt : rec.batt;
        rec.sdk = o.sdk || rec.sdk;
        pushDevices();
        return;
      }
      if (o.t === 'stat') {
        if (typeof o.batt === 'number') rec.batt = o.batt;
        pushDevices();
        return;
      }
      toPanels(id, buf.toString());
    });

    ws.on('close', () => {
      devices.delete(id);
      pushDevices();
      console.log('device down', id);
    });
    ws.on('error', () => {});
    return;
  }

  // panel
  const p = { ws, dev: null };
  panels.add(p);
  safe(ws, JSON.stringify({ t: 'devices', list: devList() }));

  ws.on('message', (buf) => {
    let o; try { o = JSON.parse(buf.toString()); } catch (e) { return; }
    if (o.t === 'pick') {
      p.dev = o.dev || null;
      const d = p.dev && devices.get(p.dev);
      safe(ws, JSON.stringify({ t: 'picked', dev: p.dev, ok: !!d }));
      return;
    }
    if (o.t === 'cmd' && o.dev) {
      const d = devices.get(o.dev);
      if (!d) { safe(ws, JSON.stringify({ t: 'res', id: o.id, ok: false, err: 'offline' })); return; }
      safe(d.ws, JSON.stringify(Object.assign({}, o, { t: 'cmd' })));
    }
  });

  ws.on('close', () => panels.delete(p));
  ws.on('error', () => {});
});

setInterval(() => {
  for (const [, d] of devices) safe(d.ws, '{"t":"ping"}');
}, 25000);

server.listen(PORT, () => console.log('lisa cloud on :' + PORT));