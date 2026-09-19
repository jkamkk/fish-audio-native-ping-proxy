# Fish Audio Native Ping Proxy

Render Free-compatible Node.js proxy for the Fish Audio Live WebSocket.

- Receives the same JSON `POST /v1/tts` request as the current Worker bridge.
- Keeps one Fish WebSocket per Fish API key.
- Sends native WebSocket Ping frames every 10 seconds.
- Returns PCM audio wrapped as WAV for the novel-reader PCM mode.

Deploy as a Render **Web Service** with:

```text
Build Command: npm install
Start Command: npm start
```

The public service URL can then replace the Cloudflare Worker bridge URL in the Surge module.
