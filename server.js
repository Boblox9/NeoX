const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;
const DATA_FILE = path.join(__dirname, 'data.json');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 8e6 }); // до 8 МБ на сообщение (файлы/голос)

app.use(express.static(__dirname));

// ---------- Хранилище ----------
let data = { users: {}, messages: {} };
try {
  if (fs.existsSync(DATA_FILE)) data = JSON.parse(fs.readFileSync(DATA_FILE));
} catch (e) { console.error('data.json повреждён, начинаем с нуля'); }

function save() {
  try { fs.writeFileSync(DATA_FILE, JSON.stringify(data)); }
  catch (e) { console.error('save error', e); }
}
setInterval(save, 5000);

// ---------- Онлайн ----------
const uidConns = new Map(); // uid -> Set<socketId>
function isOnline(uid) { return uidConns.has(uid) && uidConns.get(uid).size > 0; }
function usersList() {
  return Object.values(data.users).map(u => ({ ...u, online: isOnline(u.uid) }));
}
function broadcastState() {
  io.emit('state', { users: usersList(), messages: data.messages });
}

// ---------- Socket.IO ----------
io.on('connection', (socket) => {
  let uid = null;

  socket.on('hello', ({ uid: u, name, color }) => {
    if (!u || !name) return;
    uid = u;
    if (!uidConns.has(uid)) uidConns.set(uid, new Set());
    uidConns.get(uid).add(socket.id);
    data.users[uid] = { uid, name, color, lastSeen: Date.now() };
    save();
    socket.emit('state', { users: usersList(), messages: data.messages });
    io.emit('userUpdate', { user: { ...data.users[uid], online: true } });
  });

  socket.on('message', ({ room, msg }) => {
    if (!room || !msg) return;
    msg.id = msg.id || 'm_' + Math.random().toString(36).slice(2, 10);
    if (!data.messages[room]) data.messages[room] = [];
    data.messages[room].push(msg);
    if (data.messages[room].length > 1000)
      data.messages[room] = data.messages[room].slice(-1000);
    save();
    io.emit('message', { room, msg });
  });

  socket.on('messageUpdate', ({ room, id, patch }) => {
    const arr = data.messages[room] || [];
    const i = arr.findIndex(m => m.id === id);
    if (i > -1) {
      arr[i] = { ...arr[i], ...patch };
      save();
      io.emit('messageUpdate', { room, id, patch });
    }
  });

  socket.on('messageDelete', ({ room, id }) => {
    data.messages[room] = (data.messages[room] || []).filter(m => m.id !== id);
    save();
    io.emit('messageDelete', { room, id });
  });

  socket.on('typing', ({ room, isTyping }) => {
    if (!uid) return;
    socket.broadcast.emit('typing', { room, uid, isTyping });
  });

  socket.on('disconnect', () => {
    if (!uid) return;
    const set = uidConns.get(uid);
    if (set) { set.delete(socket.id); if (!set.size) uidConns.delete(uid); }
    if (data.users[uid]) data.users[uid].lastSeen = Date.now();
    io.emit('userUpdate', { user: { ...data.users[uid], online: isOnline(uid) } });
  });
});

server.listen(PORT, () => console.log('NeoX: http://localhost:' + PORT));