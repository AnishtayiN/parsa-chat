# Parsa Chat — چت پارسا

A realtime chat room that runs **100% free** on Cloudflare:

- **Server** — Cloudflare Worker + Durable Objects (SQLite storage, WebSocket Hibernation API). Instant message delivery, message history per room, online-presence list.
- **Web client** — served by the Worker itself at `/` (Persian, RTL, dark).
- **Android app** — native Kotlin (OkHttp WebSocket), Persian RTL UI. Source in `android/`.
- **Windows app** — Electron 22 (last Electron line that still runs on **Windows 7**; also runs on 8/10/11). Source in `windows/`.

## How it works

- Each room is one Durable Object instance (`ChatRoom`), addressed by room name.
- Clients connect to `wss://<worker-host>/ws?room=<room>&name=<username>`.
- Protocol is JSON: `msg` (chat message), `history` (last 50 on join), `presence` (online users), `system` (join/leave notices).
- History keeps the last 500 messages per room in the object's SQLite database.

## Deploy the server

With Wrangler:

```bash
cd worker
npx wrangler deploy
```

Or via the Cloudflare API (multipart upload with Durable Object binding + `new_sqlite_classes` migration in the metadata).

## Build the apps

- Android: open `android/` in Android Studio, or run `./gradlew assembleDebug` (JDK 17 + Android SDK). The app connects to the Worker URL configured in `MainActivity.kt`.
- Windows: `cd windows && npm install && npm run dist` (electron-builder → NSIS installer). Electron 22 requires no special flags for Windows 7 support.

## Free-tier budget (Cloudflare Workers Free)

100,000 requests/day, ~2M incoming WebSocket messages/day (20:1 billing), 5 GB SQLite storage — far more than a friends chat room needs. Idle connections hibernate and cost nothing.
