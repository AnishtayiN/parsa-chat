# Parsa Chat — چت پارسا

A full-featured realtime chat room that runs **100% free** on Cloudflare:

- **Server** — Cloudflare Worker + Durable Objects (SQLite storage, WebSocket Hibernation API). Instant delivery, per-room history, presence.
- **Web client** — served by the Worker itself at `/` (Persian, RTL, dark/light themes).
- **Windows app** — Electron 22 (last Electron line that still runs on **Windows 7**; also 8/10/11). Source in `windows/`.

## Features (v2.0)

- Multiple rooms (any name), optional **room password**, first member becomes **admin** (pin, delete any message, kick/ban, set password)
- Reply/quote, edit, delete, **emoji reactions**, **typing indicator**, **read receipts** (✓/✓✓)
- **Pinned message**, **search** in history, load-older pagination, date separators, unread divider
- **Image messages** (auto-compressed), **voice messages** (record in browser), **file sharing** (≤ 1 MB)
- Colored avatars, member list, emoji picker, sound + desktop notifications, auto-reconnect
- History keeps the last 1000 messages per room in the object's SQLite database

## How it works

- Each room is one Durable Object instance (`ChatRoom`), addressed by room name.
- Clients connect to `wss://<worker-host>/ws?room=<room>&name=<username>[&pass=<password>]`.
- Protocol is JSON. Client → server: `msg`, `media`, `edit`, `delete`, `react`, `typing`, `seen`, `pin`, `unpin`, `search`, `history_before`, `set_password`, `kick`. Server → client: `history`, `msg`, `edited`, `deleted`, `reaction`, `typing`, `seen`, `pinned`, `presence`, `search_results`, `older`, `system`, `error`, `kicked`.
- Identity is name-based (no accounts) — fine for a friends chat; don't use it for anything sensitive.

## Deploy the server

With Wrangler:

```bash
cd worker
npx wrangler deploy
```

Or via the Cloudflare API (multipart upload; first deploy needs the Durable Object binding + `new_sqlite_classes` migration in the metadata, later deploys omit migrations). Enable the workers.dev subdomain with **POST** `/workers/scripts/<name>/subdomain` `{"enabled":true}` (PUT is rejected for API tokens).

## Build the Windows app

```bash
cd windows
npm install
node apply-nsis-patch.js   # Linux only: lets electron-builder skip wine
npm run dist:win
```

Electron 22.3.27 is pinned for Windows 7 compatibility. The bundled `index.html` is the same client the Worker serves, with the WebSocket host hardcoded.

## Tests

```bash
cd tools
npm install
node test-ws.cjs   # basic: connect, presence, messages, history
node test-v2.cjs   # full: reply/edit/delete/react/typing/seen/pin/search/media/password/kick
```

## Free-tier budget (Cloudflare Workers Free)

100,000 requests/day, ~2M incoming WebSocket messages/day (20:1 billing), 5 GB SQLite storage — far more than a friends chat room needs. Idle connections hibernate and cost nothing.
