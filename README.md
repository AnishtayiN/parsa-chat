# Parsa Chat — چت پارسا

A full-featured realtime chat room that runs **100% free** on Cloudflare:

- **Server** — Cloudflare Worker + Durable Objects (SQLite storage, WebSocket Hibernation API). Instant delivery, per-room history, presence.
- **Web client** — served by the Worker itself at `/` (Persian, RTL, dark/light themes).
- **Windows app** — Electron 22 (last Electron line that still runs on **Windows 7**; also 8/10/11). Source in `windows/`.
- **Android app** — native WebView shell (Java, no frameworks) running on **Android 5.0+**. Source in `android/`. Signed APK is attached to each release.

## Features (v3.0 — Telegram-style)

- **Chat list sidebar** with live room previews, **unread badges** and per-room **drafts** (background "lite" connections keep every saved room live without showing you online there)
- **Polls** (single/multi choice, live results, close by creator/admin), **forwarding** between rooms, **multiple pinned messages** (up to 20) with a cycling pin bar
- Reply/quote (swipe-to-reply on touch), edit (ArrowUp), delete, **emoji reactions** (double-tap = ❤️), long-press/right-click **context menu**, typing indicator, read receipts (✓/✓✓)
- **Voice messages** with waveform, playback speed (1x/1.5x/2x) and live recording waveform; **image messages** with fullscreen lightbox (zoom + download); **file sharing** (≤ 1 MB)
- Telegram-style bubbles with tails, chat **wallpaper**, dark/light themes, jumbo emoji (sticker-style), date chips, unread divider, scroll-to-bottom FAB
- Rooms: optional **password**, **room topic**, first member becomes **admin**; admins can **promote/demote** other admins, kick/ban and **unban**, pin, delete any message
- Profiles: **bio** and **avatar color** per member, online/last-seen status, member panel
- Categorized **emoji picker** with recents, in-chat search, load-older pagination, sounds + notifications, auto-reconnect
- Messages up to 4096 chars; history keeps the last 1000 messages per room in SQLite

## How it works

- Each room is one Durable Object instance (`ChatRoom`), addressed by room name.
- Clients connect to `wss://<worker-host>/ws?room=<room>&name=<username>[&pass=<password>][&lite=1]`. `lite=1` is for the chat-list background connections: they receive messages but stay invisible in presence and don't broadcast join/leave.
- Protocol is JSON. Client → server: `msg`, `media`, `poll`, `vote`, `close_poll`, `edit`, `delete`, `react`, `typing`, `seen`, `pin`, `unpin`, `search`, `history_before`, `set_password`, `kick`, `unban`, `promote`, `demote`, `set_topic`, `set_bio`, `set_color`. Server → client: `history`, `msg`, `poll`, `edited`, `deleted`, `reaction`, `typing`, `seen`, `pinned`, `presence`, `members`, `topic`, `password`, `search_results`, `older`, `system`, `error`, `kicked`.
- Identity is name-based (no accounts) — fine for a friends chat; don't use it for anything sensitive.

## Client single source

The whole UI lives in **`worker/client.html`**. After editing it, run:

```bash
node tools/sync-client.mjs
```

This embeds it into `worker/worker.js` (served at `/`) and copies it with the hardcoded WebSocket host into `windows/index.html` and `android/app/src/main/assets/index.html`. Never edit those three copies by hand.

## Deploy the server

With Wrangler:

```bash
cd worker
npx wrangler deploy
```

Or via the Cloudflare API (multipart upload). The metadata must include the Durable Object binding and the migration object — note the API shape is `"migrations":{"new_tag":"v1","steps":[{"new_sqlite_classes":["ChatRoom"]}]}` (an array is rejected; re-sending an already-applied tag is fine). Enable the workers.dev subdomain with **POST** `/workers/scripts/<name>/subdomain` `{"enabled":true}` (PUT is rejected for API tokens).

## Build the Windows app

```bash
cd windows
npm install
node apply-nsis-patch.js   # Linux only: lets electron-builder skip wine
npm run dist:win
```

Electron 22.3.27 is pinned for Windows 7 compatibility. The bundled `index.html` is the same client the Worker serves, with the WebSocket host hardcoded.

## Build the Android app

```bash
cd android
./gradlew assembleRelease   # output: app/build/outputs/apk/release/ParsaChat-<version>.apk
```

Requires JDK 17 and the Android SDK (platform 34). Without signing env vars the APK is debug-signed so it still installs. The app is a WebView shell around the same bundled client (`app/src/main/assets/index.html`, WebSocket host hardcoded), served from a secure `https://appassets.androidplatform.net` origin so **voice messages** work. Native bridges provide:

- microphone permission + `getUserMedia` for voice recording
- file picker for image/file messages
- saving received files to the public **Downloads** folder (API 29+) or the app's external Downloads dir (older)
- native notifications for new messages while the app is in the background (with Persian app name «چت پارسا»)
- external links open in the browser; back button closes open panels first (via the client's `__pcBack` hook), then press-again-to-exit

CI: the **Build Android APK and upload to release** workflow (manual dispatch, tag input) builds on `ubuntu-latest`, signs with the keystore stored in repo secrets (`ANDROID_KEYSTORE_BASE64`, `ANDROID_KEYSTORE_PASSWORD`, `ANDROID_KEY_ALIAS`, `ANDROID_KEY_PASSWORD`) and uploads `ParsaChat-<version>.apk` to the release.

## Tests

```bash
cd tools
npm install
node test-ws.cjs   # basic: connect, presence, messages, history
node test-v2.cjs   # v2: reply/edit/delete/react/typing/seen/pin/search/media/password/kick
node test-v3.cjs   # v3: polls/forward/multi-pin/promote/demote/unban/topic/bio/color/lite/password
# override host: PC_HOST=<worker-host> node test-v3.cjs
```

## Free-tier budget (Cloudflare Workers Free)

100,000 requests/day, ~2M incoming WebSocket messages/day (20:1 billing), 5 GB SQLite storage — far more than a friends chat room needs. Idle connections hibernate and cost nothing.
