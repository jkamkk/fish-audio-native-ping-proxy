import http from "node:http";
import crypto from "node:crypto";
import WebSocket from "ws";
import { decode, encode } from "@msgpack/msgpack";

const PORT = Number(process.env.PORT || 10000);
const FISH_URL = "wss://api.fish.audio/v1/tts/live";
const KEEPALIVE_MS = 10000;
const AUDIO_IDLE_MS = 1200;
const SEGMENT_TIMEOUT_MS = 45000;
const sessions = new Map();

const REQUEST_FIELDS = [
  "max_new_tokens", "temperature", "top_p", "repetition_penalty",
  "min_chunk_length", "references", "reference_id", "prosody",
  "chunk_length", "condition_on_previous_chunks", "normalize",
  "early_stop_threshold", "format", "sample_rate", "mp3_bitrate",
  "opus_bitrate", "latency",
];

function json(res, value, status = 200) {
  const body = JSON.stringify(value);
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
    "Content-Length": Buffer.byteLength(body),
  });
  res.end(body);
}

function bytes(value) {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (Buffer.isBuffer(value)) return new Uint8Array(value);
  return null;
}

function authorization(req) {
  const value = req.headers.authorization || "";
  return /^Bearer\s+\S+$/i.test(value) ? value : "";
}

function copyConfig(source) {
  const config = { text: "" };
  for (const field of REQUEST_FIELDS) {
    if (source[field] !== undefined && source[field] !== null) config[field] = source[field];
  }
  return config;
}

function sessionName(auth) {
  return crypto.createHash("sha256").update(auth).digest("hex");
}

function wav(pieces, sampleRate = 44100, channels = 1, bits = 16) {
  const dataLength = pieces.reduce((sum, part) => sum + part.byteLength, 0);
  const header = Buffer.alloc(44);
  const blockAlign = channels * bits / 8;
  header.write("RIFF", 0);
  header.writeUInt32LE(36 + dataLength, 4);
  header.write("WAVE", 8);
  header.write("fmt ", 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20);
  header.writeUInt16LE(channels, 22);
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(sampleRate * blockAlign, 28);
  header.writeUInt16LE(blockAlign, 32);
  header.writeUInt16LE(bits, 34);
  header.write("data", 36);
  header.writeUInt32LE(dataLength, 40);
  return Buffer.concat([header, ...pieces.map((part) => Buffer.from(part))]);
}

class Session {
  constructor(auth, model) {
    this.auth = auth;
    this.model = model;
    this.ws = null;
    this.signature = "";
    this.current = null;
    this.queue = Promise.resolve();
    this.keepalive = null;
    this.lastUsed = Date.now();
  }

  async ensure(config) {
    const signature = JSON.stringify({ model: this.model, config: {
      reference_id: config.reference_id,
      references: config.references,
      format: "pcm",
      sample_rate: config.sample_rate || 44100,
      prosody: config.prosody,
    }});
    if (this.ws?.readyState === WebSocket.OPEN && this.signature === signature) return;
    this.close();
    this.signature = signature;
    this.ws = await new Promise((resolve, reject) => {
      const ws = new WebSocket(FISH_URL, {
        headers: { Authorization: this.auth, model: this.model },
      });
      const fail = (error) => { try { ws.close(); } catch {} reject(error); };
      ws.once("open", () => resolve(ws));
      ws.once("error", fail);
    });
    this.ws.binaryType = "arraybuffer";
    this.ws.on("message", (data) => this.onMessage(data));
    this.ws.on("error", (error) => this.fail(error));
    this.ws.on("close", () => this.fail(new Error("Fish WebSocket closed")));
    this.ws.send(encode({ event: "start", request: {
      ...config,
      format: "pcm",
      sample_rate: config.sample_rate || 44100,
    }}));
    this.keepalive = setInterval(() => {
      if (this.ws?.readyState === WebSocket.OPEN) this.ws.ping();
    }, KEEPALIVE_MS);
  }

  onMessage(data) {
    let message;
    try { message = decode(new Uint8Array(data)); } catch (error) { this.fail(error); return; }
    if (!this.current) return;
    if (message?.event === "audio") {
      const audio = bytes(message.audio);
      if (!audio?.byteLength) return;
      this.current.parts.push(audio);
      this.current.bytes += audio.byteLength;
      clearTimeout(this.current.idle);
      this.current.idle = setTimeout(() => this.finish(), AUDIO_IDLE_MS);
    } else if (message?.event === "error") {
      this.fail(new Error(message.error || "Fish TTS error"));
    }
  }

  fail(error) {
    if (this.current) {
      clearTimeout(this.current.idle);
      clearTimeout(this.current.timeout);
      if (!this.current.res.writableEnded) this.current.res.destroy(error);
      this.current.resolve();
      this.current = null;
    }
    this.close();
  }

  finish() {
    const job = this.current;
    if (!job) return;
    this.current = null;
    clearTimeout(job.idle);
    clearTimeout(job.timeout);
    if (!job.res.writableEnded) {
      job.res.write(wav(job.parts, job.sampleRate));
      job.res.end();
    }
    job.resolve();
  }

  close() {
    if (this.keepalive) clearInterval(this.keepalive);
    this.keepalive = null;
    if (this.ws) {
      try { this.ws.close(); } catch {}
    }
    this.ws = null;
  }

  enqueue(job) {
    const run = this.queue.then(() => this.process(job));
    this.queue = run.catch(() => undefined);
    return run;
  }

  async process({ body, req, res }) {
    this.lastUsed = Date.now();
    const config = copyConfig(body);
    await this.ensure(config);
    await new Promise((resolve, reject) => {
      this.current = {
        res,
        resolve,
        reject,
        parts: [],
        bytes: 0,
        idle: null,
        timeout: setTimeout(() => this.fail(new Error("segment timeout")), SEGMENT_TIMEOUT_MS),
        sampleRate: config.sample_rate || 44100,
      };
      res.writeHead(200, {
        "Content-Type": "audio/wav",
        "Cache-Control": "no-store",
        "X-Fish-Bridge-Mode": "render-native-ping-pcm-wav",
      });
      try {
        this.ws.send(encode({ event: "text", text: body.text }));
        this.ws.send(encode({ event: "flush" }));
      } catch (error) {
        reject(error);
        this.fail(error);
      }
    });
  }
}

async function readJson(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

const server = http.createServer(async (req, res) => {
  if (req.method === "GET" && req.url === "/health") {
    return json(res, { ok: true, service: "fish-native-ping-proxy" });
  }
  if (req.method !== "POST" || !req.url.startsWith("/v1/tts")) {
    return json(res, { error: "POST /v1/tts required" }, 404);
  }
  const auth = authorization(req);
  if (!auth) return json(res, { error: "Missing Fish Audio API key" }, 401);
  let body;
  try { body = await readJson(req); } catch { return json(res, { error: "Invalid JSON" }, 400); }
  if (!body?.text) return json(res, { error: "Missing text" }, 400);
  const model = req.headers.model || "s2.1-pro-free";
  const key = sessionName(auth);
  let session = sessions.get(key);
  if (!session) {
    session = new Session(auth, model);
    sessions.set(key, session);
  }
  try {
    await session.enqueue({ body, req, res });
  } catch (error) {
    if (!res.headersSent) json(res, { error: String(error.message || error) }, 502);
  }
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`fish-native-ping-proxy listening on ${PORT}`);
});

setInterval(() => {
  const expiry = Date.now() - 30 * 60 * 1000;
  for (const [key, session] of sessions) {
    if (session.lastUsed < expiry && !session.current) {
      session.close();
      sessions.delete(key);
    }
  }
}, 5 * 60 * 1000);
