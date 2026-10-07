// Monopoly online mod - room relay (v2): one host + up to 3 guests per room (seats 0..3).
// It forwards messages and never interprets game data. Rooms live in memory only.
//
// Client -> relay (JSON text frames):
//   {t:'host', name}               create a room                     -> {t:'hosted', code, seat:0, token}
//   {t:'list'}                     list open rooms                   -> {t:'rooms', r:[{c,n,m,h}]}
//   {t:'join', code, name}         join as the next free seat        -> {t:'joined', seat, token, n, names}
//   {t:'rejoin', code, seat, token} reclaim a dropped seat           -> {t:'joined', seat, token, rejoin:true}
//   {t:'relay', to?, ...}          guest -> host only; host -> `to` (seat number) or every guest
//   {t:'close'}                    host: stop accepting joiners (game started)
//   {t:'leave'}                    leave the room
// Relay -> client: peer_joined{seat,name}, peer_left{seat}, peer_back{seat}, host_left, error{m}
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 10000;
const MAX_SEATS = 4;
const CODE_LEN = 4;
const MAX_MSG = 64 * 1024;
const ROOM_IDLE_MS = 60 * 60 * 1000;
const REJOIN_GRACE_MS = 3 * 60 * 1000;

/** code -> { seats:[{ws,name,token,gone:timestamp|null}|null x4], open, created, last } */
const rooms = new Map();

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/healthz') {
    res.writeHead(200, { 'content-type': 'text/plain' });
    res.end('monopoly relay ok rooms=' + rooms.size + '\n');
  } else { res.writeHead(404); res.end(); }
});
const wss = new WebSocketServer({ server, maxPayload: MAX_MSG });

const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const newToken = () => crypto.randomBytes(6).toString('hex');
const count = (r) => r.seats.filter(Boolean).length;

function newCode() {
  for (let i = 0; i < 300; i++) {
    const c = String(Math.floor(Math.random() * 10 ** CODE_LEN)).padStart(CODE_LEN, '0');
    if (!rooms.has(c)) return c;
  }
  return null;
}

function names(r) { return r.seats.map((s) => (s ? s.name : '')); }

function closeRoom(code, why) {
  const r = rooms.get(code); if (!r) return;
  r.seats.forEach((s) => { if (s && s.ws) { send(s.ws, { t: why || 'host_left' }); s.ws.room = null; try { s.ws.close(); } catch (e) {} } });
  rooms.delete(code);
}

function leave(ws, final) {
  const code = ws.room; if (!code) return;
  const r = rooms.get(code); ws.room = null; if (!r) return;
  const seat = ws.seat; const s = r.seats[seat];
  if (!s || s.ws !== ws) return;
  if (seat === 0) { closeRoom(code, 'host_left'); return; }
  if (final || r.open) { r.seats[seat] = null; }            // lobby: seat is simply freed
  else { s.ws = null; s.gone = Date.now(); }                // match running: keep the seat for a rejoin
  send(r.seats[0] && r.seats[0].ws, { t: 'peer_left', seat, names: names(r) });
}

wss.on('connection', (ws) => {
  ws.isAlive = true; ws.room = null; ws.seat = -1;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (data, isBinary) => {
    if (isBinary) return;
    let m; try { m = JSON.parse(data.toString()); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    const room = ws.room ? rooms.get(ws.room) : null;
    switch (m.t) {
      case 'host': {
        if (ws.room) return;
        const code = newCode(); if (!code) return send(ws, { t: 'error', m: 'no free room code' });
        const token = newToken();
        rooms.set(code, { seats: [{ ws, name: String(m.name || 'Host').slice(0, 20), token, gone: null }, null, null, null], open: true, created: Date.now(), last: Date.now() });
        ws.room = code; ws.seat = 0;
        send(ws, { t: 'hosted', code, seat: 0, token });
        break;
      }
      case 'list': {
        const out = [];
        for (const [c, r] of rooms) if (r.open && count(r) < MAX_SEATS) out.push({ c, n: count(r), m: MAX_SEATS, h: r.seats[0].name });
        send(ws, { t: 'rooms', r: out.slice(0, 10) });
        break;
      }
      case 'join': {
        if (ws.room) return;
        const code = String(m.code || ''); const r = rooms.get(code);
        if (!r) return send(ws, { t: 'error', m: 'room not found' });
        if (!r.open) return send(ws, { t: 'error', m: 'game already started' });
        const seat = r.seats.findIndex((s, i) => i > 0 && !s);
        if (seat < 0) return send(ws, { t: 'error', m: 'room is full' });
        const token = newToken(); const name = String(m.name || 'Guest').slice(0, 20);
        r.seats[seat] = { ws, name, token, gone: null }; ws.room = code; ws.seat = seat; r.last = Date.now();
        send(ws, { t: 'joined', code, seat, token, n: count(r), names: names(r) });
        send(r.seats[0].ws, { t: 'peer_joined', seat, name, n: count(r), names: names(r) });
        break;
      }
      case 'rejoin': {
        const r = rooms.get(String(m.code || '')); const seat = Number(m.seat);
        const s = r && r.seats[seat];
        if (!s || s.token !== m.token || s.ws) return send(ws, { t: 'error', m: 'cannot rejoin' });
        s.ws = ws; s.gone = null; ws.room = String(m.code); ws.seat = seat;
        send(ws, { t: 'joined', code: String(m.code), seat, token: s.token, rejoin: true, n: count(r), names: names(r) });
        send(r.seats[0].ws, { t: 'peer_back', seat });
        if (s.queue) { for (const q of s.queue) send(ws, q); s.queue = null; }      // replay what the host sent meanwhile, in order
        break;
      }
      case 'close': {
        if (room && ws.seat === 0) room.open = false;
        break;
      }
      case 'relay': {
        if (process.env.RELAY_DEBUG) console.log('relay from seat', ws.seat, 'k=', m.k, 'len=', data.length);
        if (!room) return; room.last = Date.now();
        if (ws.seat === 0) {
          m.from = 0;
          const deliver = (s) => { if (!s) return; if (s.ws) send(s.ws, m); else if (s.gone) { (s.queue = s.queue || []).push(m); if (s.queue.length > 5000) s.queue.shift(); } };
          if (m.to !== undefined && m.to !== null) deliver(room.seats[m.to]);
          else room.seats.forEach((s, i) => { if (i > 0) deliver(s); });
        } else { m.from = ws.seat; send(room.seats[0].ws, m); }
        break;
      }
      case 'kick': {                                   // host removes a player (setup screen)
        if (!room || ws.seat !== 0) return;
        const seat = Number(m.seat); const t = room.seats[seat];
        if (!(seat > 0 && t)) return;
        const tw = t.ws; room.seats[seat] = null;
        if (tw) { tw.room = null; send(tw, { t: 'kicked' }); try { tw.close(); } catch (e) {} }
        send(ws, { t: 'peer_left', seat, names: names(room) });
        break;
      }
      case 'leave': leave(ws, true); break;
      case 'ping': send(ws, { t: 'pong' }); break;
    }
  });
  ws.on('close', () => leave(ws, false));
  ws.on('error', () => leave(ws, false));
});

setInterval(() => {
  wss.clients.forEach((ws) => { if (!ws.isAlive) return ws.terminate(); ws.isAlive = false; ws.ping(); });
  const now = Date.now();
  for (const [c, r] of rooms) {
    if (now - r.last > ROOM_IDLE_MS) { closeRoom(c, 'host_left'); continue; }
    r.seats.forEach((s, i) => {                       // drop seats whose rejoin window expired
      if (s && s.gone && now - s.gone > REJOIN_GRACE_MS) { r.seats[i] = null; send(r.seats[0].ws, { t: 'peer_gone', seat: i }); }
    });
  }
}, 15000);

server.listen(PORT, '0.0.0.0', () => console.log('relay v2 listening on ' + PORT));
