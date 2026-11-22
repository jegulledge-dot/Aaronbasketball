
'use strict';

const http = require('http');
const crypto = require('crypto');

const rooms = new Map();
const PORT = Number(process.env.PORT || 8080);

function send(socket, obj) {
  if (!socket || socket.destroyed) return;

  const data = Buffer.from(JSON.stringify(obj));
  let header;

  if (data.length < 126) {
    header = Buffer.from([129, data.length]);
  } else if (data.length < 65536) {
    header = Buffer.alloc(4);
    header[0] = 129;
    header[1] = 126;
    header.writeUInt16BE(data.length, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 129;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(data.length), 2);
  }

  socket.write(Buffer.concat([header, data]));
}

function closeRoom(code) {
  const room = rooms.get(code);
  if (!room) return;

  rooms.delete(code);

  for (const socket of [room.host, room.guest]) {
    if (socket && !socket.destroyed) {
      send(socket, {
        type: 'ended',
        reason: 'Other player disconnected'
      });
    }
  }
}

const server = http.createServer((req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/plain',
    'Access-Control-Allow-Origin': '*'
  });

  res.end('CART3R multiplayer relay running\n');
});

server.on('upgrade', (req, socket) => {
  if (
    req.headers.upgrade?.toLowerCase() !== 'websocket' ||
    !req.headers['sec-websocket-key']
  ) {
    socket.destroy();
    return;
  }

  const accept = crypto
    .createHash('sha1')
    .update(
      req.headers['sec-websocket-key'] +
      '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'
    )
    .digest('base64');

  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    'Sec-WebSocket-Accept: ' + accept + '\r\n\r\n'
  );

  let buffer = Buffer.alloc(0);
  let roomCode = null;
  let role = null;
  let last = 0;

  socket.on('data', chunk => {
    buffer = Buffer.concat([buffer, chunk]);

    while (buffer.length >= 2) {
      const op = buffer[0] & 15;
      const masked = !!(buffer[1] & 128);
      let len = buffer[1] & 127;
      let offset = 2;

      if (len === 126) {
        if (buffer.length < 4) break;
        len = buffer.readUInt16BE(2);
        offset = 4;
      } else if (len === 127) {
        if (buffer.length < 10) break;

        const big = buffer.readBigUInt64BE(2);
        if (big > 200000n) {
          socket.destroy();
          return;
        }

        len = Number(big);
        offset = 10;
      }

      if (!masked) {
        socket.destroy();
        return;
      }

      if (len > 200000) {
        socket.destroy();
        return;
      }

      if (buffer.length < offset + 4 + len) break;

      const mask = buffer.subarray(offset, offset + 4);
      const body = Buffer.from(
        buffer.subarray(offset + 4, offset + 4 + len)
      );

      buffer = buffer.subarray(offset + 4 + len);

      for (let i = 0; i < body.length; i++) {
        body[i] ^= mask[i % 4];
      }

      if (op === 8) {
        socket.end();
        return;
      }

      if (op === 9) {
        socket.write(Buffer.from([0x8a, 0]));
        continue;
      }

      if (op !== 1) continue;

      let msg;

      try {
        msg = JSON.parse(body.toString('utf8'));
      } catch {
        continue;
      }

      if (!roomCode) {
        if (msg.type === 'create') {
          let code;

          do {
            code = crypto.randomBytes(3)
              .toString('hex')
              .toUpperCase();
          } while (rooms.has(code));

          roomCode = code;
          role = 'host';

          rooms.set(code, {
            host: socket,
            guest: null
          });

          send(socket, {
            type: 'created',
            code,
            role
          });

        } else if (msg.type === 'join') {
          const code = String(msg.code || '')
            .trim()
            .toUpperCase();

          const room = rooms.get(code);

          if (!room || room.guest) {
            send(socket, {
              type: 'error',
              message: 'Room not found or full'
            });
            continue;
          }

          roomCode = code;
          role = 'guest';
          room.guest = socket;

          send(socket, {
            type: 'joined',
            code,
            role
          });

          send(room.host, { type: 'ready' });
          send(socket, { type: 'ready' });
        }

        continue;
      }

      const room = rooms.get(roomCode);
      if (!room) continue;

      const now = Date.now();
      if (now - last < 12) continue;
      last = now;

      if (
        role === 'host' &&
        msg.type === 'state' &&
        room.guest
      ) {
        send(room.guest, {
          type: 'state',
          state: msg.state
        });
      }

      if (
        role === 'guest' &&
        msg.type === 'input'
      ) {
        send(room.host, {
          type: 'input',
          input: msg.input
        });
      }
    }
  });

  socket.on('close', () => {
    if (roomCode) closeRoom(roomCode);
  });

  socket.on('error', () => {
    if (roomCode) closeRoom(roomCode);
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('CART3R relay listening on port ' + PORT);
});
