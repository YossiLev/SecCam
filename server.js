/**
 * StreamHub Server
 * Accepts up to 6 mobile camera streams over WebSocket.
 * Saves each stream in 1-hour .webm chunks.
 * Serves viewer page for remote internet access.
 * Relays voice messages back to the originating phone.
 */

import express, { json, static as expressStatic } from 'express';
import { createServer } from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import { v4 as uuidv4 } from 'uuid';
import { existsSync, mkdirSync, readdirSync, statSync, createWriteStream } from 'fs';
import { join, basename, dirname } from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';
import os from 'node:os';
import { createConnection } from 'node:net';
import 'dotenv/config'; // Automatically loads the .env file

// Recreate __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// ── Config ──────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 8085;
const PASSWORD = process.env.PASSWORD || 'changeme';
const MAX_PHONES = 6;
const CHUNK_DURATION_MS = 60 * 60 * 1000; // 1 hour
const RECORDINGS_DIR = join(__dirname, 'recordings');

if (!existsSync(RECORDINGS_DIR)) mkdirSync(RECORDINGS_DIR, { recursive: true });

// ── State ────────────────────────────────────────────────────────────────────
// phones[slotIndex] = { ws, id, name, chunkFile, chunkStart, fileStream, viewers: Set }
const phones = new Array(MAX_PHONES).fill(null);

// viewers[viewerWs] = { phoneSlot, ws }
const viewers = new Map();

// pendingViewers awaiting phone slot info
// voiceQueue: per-phone queue of { data, from }

// ── Express App ──────────────────────────────────────────────────────────────
const app = express();
app.use(json());
app.use(expressStatic(join(__dirname, './viewer')));

// Serve mobile app as well (for convenience)
app.use('/mobile', expressStatic(join(__dirname, './mobile')));

// Auth middleware for REST endpoints
function requireAuth(req, res, next) {
  console.log(`[requireAuth] ${req.query.password}`)
  const pw = req.headers['x-password'] || req.query.password;
  if (pw !== PASSWORD) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

// List available slots / streams
app.get('/api/slots', requireAuth, (req, res) => {
  const slots = phones.map((p, i) => p
    ? { slot: i, id: p.id, name: p.name, connected: true, connectedAt: p.connectedAt }
    : { slot: i, connected: false }
  );
  res.json(slots);
});

// List recordings for a given slot
app.get('/api/recordings/:slot', requireAuth, (req, res) => {
  const slot = parseInt(req.params.slot);
  if (isNaN(slot) || slot < 0 || slot >= MAX_PHONES)
    return res.status(400).json({ error: 'Invalid slot' });

  const dir = join(RECORDINGS_DIR, String(slot));
  if (!existsSync(dir)) return res.json([]);

  const files = readdirSync(dir)
    .filter(f => f.endsWith('.webm'))
    .map(f => {
      const stat = statSync(join(dir, f));
      return { filename: f, size: stat.size, mtime: stat.mtime };
    })
    .sort((a, b) => new Date(b.mtime) - new Date(a.mtime));

  res.json(files);
});

// Download a specific recording chunk
app.get('/api/recordings/:slot/:filename', requireAuth, (req, res) => {
  const slot = parseInt(req.params.slot);
  const file = basename(req.params.filename); // prevent traversal
  if (isNaN(slot) || slot < 0 || slot >= MAX_PHONES)
    return res.status(400).json({ error: 'Invalid slot' });

  const filepath = join(RECORDINGS_DIR, String(slot), file);
  if (!existsSync(filepath)) return res.status(404).json({ error: 'Not found' });
  res.download(filepath);
});

// ── HTTP Server + WebSocket ───────────────────────────────────────────────────
const httpServer = createServer(app);

// Two WSS paths: /stream (phones) and /view (viewers)
const wssPhones = new WebSocketServer({ noServer: true });
const wssViewers = new WebSocketServer({ noServer: true });

httpServer.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const pw = url.searchParams.get('password');

  if (pw !== PASSWORD) {
    socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
    socket.destroy();
    return;
  }

  if (url.pathname === '/stream') {
    wssPhones.handleUpgrade(req, socket, head, ws => wssPhones.emit('connection', ws, req));
  } else if (url.pathname === '/view') {
    wssViewers.handleUpgrade(req, socket, head, ws => wssViewers.emit('connection', ws, req));
  } else {
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
    socket.destroy();
  }
});

// ── Phone Connection Handler ─────────────────────────────────────────────────
wssPhones.on('connection', (ws, req) => {
  console.log(`[connection]`);
  const url = new URL(req.url, `http://${req.headers.host}`);
  console.log(`[connection] url=${url}`);

  const requestedName = url.searchParams.get('name') || 'Phone';

  // Find a free slot
  const slot = phones.findIndex(p => p === null);
  if (slot === -1) {
    ws.close(1013, 'Server full: maximum 6 phones connected');
    return;
  }

  const id = uuidv4();
  const phone = {
    ws, id, slot,
    name: `${requestedName} (Slot ${slot + 1})`,
    connectedAt: new Date().toISOString(),
    chunkStart: Date.now(),
    fileStream: null,
    chunkTimer: null,
    viewers: new Set(),
    // Rolling buffer so late-joining viewers get the WebM init segment + recent data
    initBuffer: [],   // chunks from stream start (the WebM header lives here)
    recentBuffer: [], // last ~3 seconds of chunks after init
    initDone: false,
    recentBytes: 0
  };
  phones[slot] = phone;

  console.log(`[Phone] Connected: slot=${slot} name="${phone.name}" id=${id}`);

  // Ensure recording dir
  const slotDir = join(RECORDINGS_DIR, String(slot));
  if (!existsSync(slotDir)) mkdirSync(slotDir, { recursive: true });

  // Open first chunk file
  openNewChunk(phone);

  // Notify all viewers of phone list update
  broadcastSlotUpdate();

  // Send slot assignment to phone
  ws.send(JSON.stringify({ type: 'assigned', slot, id, name: phone.name }));

  ws.on('message', (data, isBinary) => {
    if (!isBinary) {
      // Text control message from phone
      try {
        const msg = JSON.parse(data.toString());
        handlePhoneMessage(phone, msg);
      } catch (e) { /* ignore */ }
      return;
    }

    // Binary: video chunk — write to file
    const buf = Buffer.from(data);
    if (phone.fileStream) {
      phone.fileStream.write(buf);
    }

    // Keep a rolling buffer so late-joining viewers can catch up.
    // The first ~3 seconds are treated as "init" (contains the WebM header).
    // After that we keep a sliding window of recent chunks (~4 MB cap).
    const INIT_WINDOW_MS = 3000;
    const RECENT_MAX_BYTES = 4 * 1024 * 1024; // 4 MB
    if (!phone.initDone) {
      phone.initBuffer.push(buf);
      if (Date.now() - phone.chunkStart >= INIT_WINDOW_MS) {
        phone.initDone = true;
      }
    } else {
      phone.recentBuffer.push(buf);
      phone.recentBytes += buf.length;
      // Trim oldest chunks if over budget
      while (phone.recentBytes > RECENT_MAX_BYTES && phone.recentBuffer.length > 1) {
        phone.recentBytes -= phone.recentBuffer.shift().length;
      }
    }

    // Relay live to any viewers watching this slot
    phone.viewers.forEach(vws => {
      if (vws.readyState === WebSocket.OPEN) {
        vws.send(buf, { binary: true });
      }
    });
  });

  ws.on('close', () => {
    console.log(`[Phone] Disconnected: slot=${slot}`);
    closeChunk(phone);
    if (phone.chunkTimer) clearInterval(phone.chunkTimer);
    // Notify viewers on this slot that stream ended
    phone.viewers.forEach(vws => {
      if (vws.readyState === WebSocket.OPEN)
        vws.send(JSON.stringify({ type: 'stream_ended', slot }));
    });
    phones[slot] = null;
    broadcastSlotUpdate();
  });

  ws.on('error', err => {
    console.error(`[Phone] Error slot=${slot}:`, err.message);
  });
});

function handlePhoneMessage(phone, msg) {
  // Currently phones can send: { type: 'ping' }
  if (msg.type === 'ping') {
    phone.ws.send(JSON.stringify({ type: 'pong' }));
  }
  // Phone tells us the exact codec it chose — store it and forward to all viewers
  if (msg.type === 'mime' && msg.mime) {
    phone.mime = msg.mime;
    console.log(`[Phone] slot=${phone.slot} mime="${msg.mime}"`);
    const payload = JSON.stringify({ type: 'mime', mime: msg.mime });
    phone.viewers.forEach(vws => {
      if (vws.readyState === WebSocket.OPEN) vws.send(payload);
    });
  }
}

// ── Chunk File Management ────────────────────────────────────────────────────
function openNewChunk(phone) {
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const filename = `chunk_${ts}.webm`;
  const filepath = join(RECORDINGS_DIR, String(phone.slot), filename);
  phone.fileStream = createWriteStream(filepath, { flags: 'a' });
  phone.currentFile = filename;
  phone.chunkStart = Date.now();
  console.log(`[Rec] Opened chunk: slot=${phone.slot} file=${filename}`);

  // Schedule next chunk rotation
  phone.chunkTimer = setTimeout(() => {
    if (phones[phone.slot] === phone) {
      closeChunk(phone);
      openNewChunk(phone);
    }
  }, CHUNK_DURATION_MS);
}

function closeChunk(phone) {
  if (phone.fileStream) {
    phone.fileStream.end();
    phone.fileStream = null;
    console.log(`[Rec] Closed chunk: slot=${phone.slot} file=${phone.currentFile}`);
  }
  if (phone.chunkTimer) {
    clearTimeout(phone.chunkTimer);
    phone.chunkTimer = null;
  }
}

// ── Viewer Connection Handler ─────────────────────────────────────────────────
wssViewers.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const slot = parseInt(url.searchParams.get('slot') ?? '-1');

  const viewer = { ws, slot };
  viewers.set(ws, viewer);

  console.log(`[Viewer] Connected: slot=${slot}`);

  // Subscribe to phone's live viewer set
  if (slot >= 0 && slot < MAX_PHONES && phones[slot]) {
    const phone = phones[slot];
    phones[slot].viewers.add(ws);

    // Send mime FIRST so viewer sets up SourceBuffer with the correct codec
    // before any binary data arrives
    if (phone.mime) {
      ws.send(JSON.stringify({ type: 'mime', mime: phone.mime }));
    }

    ws.send(JSON.stringify({ type: 'stream_active', slot, name: phone.name }));

    // Replay buffered data so the viewer gets the WebM header + recent frames
    const replay = [...phone.initBuffer, ...phone.recentBuffer];
    console.log(`[Viewer] Replaying ${replay.length} buffered chunks to new viewer on slot=${slot} mime="${phone.mime}"`);
    for (const chunk of replay) {
      if (ws.readyState === WebSocket.OPEN) ws.send(chunk, { binary: true });
    }
  } else {
    ws.send(JSON.stringify({ type: 'stream_inactive', slot }));
  }

  // Send current slot list
  ws.send(JSON.stringify({ type: 'slots', slots: getSlotsPayload() }));

  ws.on('message', (data, isBinary) => {
    if (isBinary) {
      // Binary: voice message audio — relay to the phone on `slot`
      if (slot >= 0 && slot < MAX_PHONES && phones[slot]) {
        const phone = phones[slot];
        if (phone.ws.readyState === WebSocket.OPEN) {
          // Prefix with a JSON header as a text frame, then the audio binary
          phone.ws.send(JSON.stringify({ type: 'voice_incoming' }));
          phone.ws.send(data, { binary: true });
        }
      }
      return;
    }

    try {
      const msg = JSON.parse(data.toString());
      if (msg.type === 'ping') ws.send(JSON.stringify({ type: 'pong' }));
      if (msg.type === 'change_slot') {
        const newSlot = parseInt(msg.slot);
        // Unsubscribe from old slot
        if (viewer.slot >= 0 && viewer.slot < MAX_PHONES && phones[viewer.slot]) {
          phones[viewer.slot].viewers.delete(ws);
        }
        viewer.slot = newSlot;
        if (newSlot >= 0 && newSlot < MAX_PHONES && phones[newSlot]) {
          const phone = phones[newSlot];
          phone.viewers.add(ws);
          ws.send(JSON.stringify({ type: 'stream_active', slot: newSlot, name: phone.name }));
          // Replay buffer for the new slot
          const replay = [...phone.initBuffer, ...phone.recentBuffer];
          for (const chunk of replay) {
            if (ws.readyState === WebSocket.OPEN) ws.send(chunk, { binary: true });
          }
        } else {
          ws.send(JSON.stringify({ type: 'stream_inactive', slot: newSlot }));
        }
      }
    } catch (e) { /* ignore */ }
  });

  ws.on('close', () => {
    console.log(`[Viewer] Disconnected: slot=${slot}`);
    if (slot >= 0 && slot < MAX_PHONES && phones[slot]) {
      phones[slot].viewers.delete(ws);
    }
    viewers.delete(ws);
  });

  ws.on('error', err => console.error('[Viewer] Error:', err.message));
});

// ── Helpers ──────────────────────────────────────────────────────────────────
function getSlotsPayload() {
  return phones.map((p, i) => p
    ? { slot: i, connected: true, name: p.name, id: p.id, connectedAt: p.connectedAt }
    : { slot: i, connected: false }
  );
}

function broadcastSlotUpdate() {
  const payload = JSON.stringify({ type: 'slots', slots: getSlotsPayload() });
  viewers.forEach(({ ws }) => {
    if (ws.readyState === WebSocket.OPEN) ws.send(payload);
  });
}

// ── Start ────────────────────────────────────────────────────────────────────
httpServer.listen(PORT, '0.0.0.0', () => {
  const ifaces = os.networkInterfaces();
  let localIP = 'localhost';
  Object.values(ifaces).flat().forEach(i => {
    if (i.family === 'IPv4' && !i.internal) localIP = i.address;
  });

  console.log(`\n╔══════════════════════════════════════════╗`);
  console.log(`║         StreamHub Server Running         ║`);
  console.log(`╠══════════════════════════════════════════╣`);
  console.log(`║  Local:   http://${localIP}:${PORT}`.padEnd(44) + `║`);
  console.log(`║  Port:    ${PORT}`.padEnd(44) + `║`);
  console.log(`║  Password: ${PASSWORD}`.padEnd(44) + `║`);
  console.log(`║  Max phones: ${MAX_PHONES}`.padEnd(44) + `║`);
  console.log(`╠══════════════════════════════════════════╣`);
  console.log(`║  Mobile app: http://${localIP}:${PORT}/mobile`.padEnd(44) + `║`);
  console.log(`║  Viewer:     http://${localIP}:${PORT}/`.padEnd(44) + `║`);
  console.log(`╚══════════════════════════════════════════╝\n`);
  console.log(`Recordings will be saved to: ${RECORDINGS_DIR}\n`);
});
