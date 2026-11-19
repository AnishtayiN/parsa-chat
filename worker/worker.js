// Parsa Chat v2.0 — realtime chat room server
// Cloudflare Worker + Durable Objects (SQLite) + WebSocket Hibernation API
// Features: rooms, history, presence, reply, edit, delete, reactions, typing,
// read receipts, pinned message, search, image/voice/file messages,
// room password, admin (first member) with kick.

const VERSION = "2.0.0";
const MAX_HISTORY_SEND = 50;
const MAX_HISTORY_KEEP = 1000;
const MAX_TEXT_LEN = 2000;
const MAX_NAME_LEN = 24;
const MAX_ROOM_LEN = 32;
const MAX_MEDIA_B64 = 1500000; // ~1.1 MB binary
const PALETTE = ["#e5484d","#f76b15","#ffb224","#46a758","#12a594","#0090ff","#6e56cf","#d6409f","#8e4ec6","#30a46c"];

function cleanText(s, maxLen, fallback) {
  if (typeof s !== "string") return fallback;
  const t = s.replace(/[\u0000-\u001F\u007F]/g, "").trim().slice(0, maxLen);
  return t || fallback;
}
function colorFor(name) {
  let h = 0;
  for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) >>> 0;
  return PALETTE[h % PALETTE.length];
}
async function sha256hex(s) {
  const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s));
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

export class ChatRoom {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.sql = ctx.storage.sql;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS messages (" +
      "id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, " +
      "kind TEXT NOT NULL DEFAULT 'text', text TEXT NOT NULL DEFAULT '', " +
      "data TEXT, reply_to INTEGER, ts INTEGER NOT NULL, " +
      "edited INTEGER NOT NULL DEFAULT 0, deleted INTEGER NOT NULL DEFAULT 0, " +
      "pinned INTEGER NOT NULL DEFAULT 0)"
    );
    // v1 -> v2 migration: add missing columns
    const cols = this.sql.exec("PRAGMA table_info(messages)").toArray().map((r) => r.name);
    const add = (col, def) => { if (!cols.includes(col)) this.sql.exec("ALTER TABLE messages ADD COLUMN " + col + " " + def); };
    add("kind", "TEXT NOT NULL DEFAULT 'text'");
    add("data", "TEXT");
    add("reply_to", "INTEGER");
    add("edited", "INTEGER NOT NULL DEFAULT 0");
    add("deleted", "INTEGER NOT NULL DEFAULT 0");
    add("pinned", "INTEGER NOT NULL DEFAULT 0");
    this.sql.exec("CREATE TABLE IF NOT EXISTS reactions (msg_id INTEGER NOT NULL, name TEXT NOT NULL, emoji TEXT NOT NULL, PRIMARY KEY (msg_id, name, emoji))");
    this.sql.exec("CREATE TABLE IF NOT EXISTS members (name TEXT PRIMARY KEY, color TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'member', last_seen_id INTEGER NOT NULL DEFAULT 0, banned INTEGER NOT NULL DEFAULT 0, joined_ts INTEGER NOT NULL)");
    this.sql.exec("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)");
  }

  // ---------- helpers ----------
  metaGet(key) {
    const r = this.sql.exec("SELECT value FROM meta WHERE key = ?", key).toArray();
    return r.length ? r[0].value : null;
  }
  metaSet(key, value) {
    if (value === null) this.sql.exec("DELETE FROM meta WHERE key = ?", key);
    else this.sql.exec("INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", key, value);
  }
  member(name) {
    const r = this.sql.exec("SELECT * FROM members WHERE name = ?", name).toArray();
    return r.length ? r[0] : null;
  }
  reactionsFor(msgId) {
    const rows = this.sql.exec("SELECT emoji, name FROM reactions WHERE msg_id = ?", msgId).toArray();
    const byEmoji = new Map();
    for (const r of rows) {
      if (!byEmoji.has(r.emoji)) byEmoji.set(r.emoji, []);
      byEmoji.get(r.emoji).push(r.name);
    }
    return [...byEmoji.entries()].map(([emoji, users]) => ({ emoji, count: users.length, users }));
  }
  msgObj(row) {
    return {
      id: row.id, name: row.name, kind: row.kind,
      text: row.deleted ? "" : row.text,
      data: row.deleted ? null : row.data,
      replyTo: row.reply_to, ts: row.ts,
      edited: !!row.edited, deleted: !!row.deleted,
      reactions: this.reactionsFor(row.id),
    };
  }
  getMsg(id) {
    const r = this.sql.exec("SELECT * FROM messages WHERE id = ?", id).toArray();
    return r.length ? r[0] : null;
  }
  pinnedMsg() {
    const r = this.sql.exec("SELECT * FROM messages WHERE pinned = 1 AND deleted = 0 ORDER BY id DESC LIMIT 1").toArray();
    return r.length ? this.msgObj(r[0]) : null;
  }
  onlineUsers() {
    const seen = new Map();
    for (const ws of this.ctx.getWebSockets()) {
      const att = ws.deserializeAttachment() || {};
      if (att.name) seen.set(att.name, att);
    }
    return [...seen.values()].map((a) => ({ name: a.name, color: a.color, role: a.role }));
  }
  broadcast(obj, except) {
    const s = JSON.stringify(obj);
    for (const ws of this.ctx.getWebSockets()) {
      if (ws === except) continue;
      try { ws.send(s); } catch { /* gone */ }
    }
  }
  broadcastPresence(except) {
    this.broadcast({ type: "presence", users: this.onlineUsers() }, except);
  }
  sendTo(ws, obj) { try { ws.send(JSON.stringify(obj)); } catch { /* gone */ } }

  // ---------- connection ----------
  async fetch(request) {
    const url = new URL(request.url);
    const room = cleanText(url.searchParams.get("room"), MAX_ROOM_LEN, "عمومی");
    const name = cleanText(url.searchParams.get("name"), MAX_NAME_LEN, "مهمان");

    const passHash = this.metaGet("pass_hash");
    if (passHash) {
      const given = url.searchParams.get("pass") || "";
      const givenHash = await sha256hex(given + "::" + room);
      if (givenHash !== passHash) {
        const pair = new WebSocketPair();
        this.ctx.acceptWebSocket(pair[1]);
        pair[1].send(JSON.stringify({ type: "error", text: "رمز روم اشتباهه" }));
        pair[1].close(4403, "bad password");
        return new Response(null, { status: 101, webSocket: pair[0] });
      }
    }

    let mem = this.member(name);
    if (mem && mem.banned) {
      const pair = new WebSocketPair();
      this.ctx.acceptWebSocket(pair[1]);
      pair[1].send(JSON.stringify({ type: "error", text: "از این روم اخراج شدی" }));
      pair[1].close(4403, "banned");
      return new Response(null, { status: 101, webSocket: pair[0] });
    }
    if (!mem) {
      const count = this.sql.exec("SELECT COUNT(*) AS c FROM members").toArray()[0].c;
      const role = count === 0 ? "admin" : "member";
      this.sql.exec(
        "INSERT INTO members (name, color, role, joined_ts) VALUES (?, ?, ?, ?)",
        name, colorFor(name), role, Date.now()
      );
      mem = this.member(name);
    }

    const pair = new WebSocketPair();
    const client = pair[0], server = pair[1];
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ name, color: mem.color, role: mem.role, room });

    const rows = this.sql.exec(
      "SELECT * FROM messages ORDER BY id DESC LIMIT ?", MAX_HISTORY_SEND
    ).toArray().reverse();
    const members = this.sql.exec("SELECT name, color, role, last_seen_id FROM members WHERE banned = 0").toArray();
    this.sendTo(server, {
      type: "history",
      messages: rows.map((r) => this.msgObj(r)),
      pinned: this.pinnedMsg(),
      members,
      you: { name, color: mem.color, role: mem.role },
      hasPassword: !!passHash,
    });
    this.broadcastPresence();
    this.broadcast({ type: "system", text: name + " وارد شد" }, server);
    return new Response(null, { status: 101, webSocket: client });
  }

  // ---------- messages from clients ----------
  async webSocketMessage(ws, raw) {
    let d;
    try { d = JSON.parse(raw); } catch { return; }
    if (!d || typeof d.type !== "string") return;
    const att = ws.deserializeAttachment() || {};
    const name = att.name || "مهمان";
    const isAdmin = att.role === "admin";

    switch (d.type) {
      case "msg": {
        const text = typeof d.text === "string" ? d.text.trim().slice(0, MAX_TEXT_LEN) : "";
        if (!text) return;
        const replyTo = Number.isInteger(d.replyTo) ? d.replyTo : null;
        const ts = Date.now();
        this.sql.exec("INSERT INTO messages (name, kind, text, reply_to, ts) VALUES (?, 'text', ?, ?, ?)", name, text, replyTo, ts);
        const id = this.sql.exec("SELECT last_insert_rowid() AS id").toArray()[0].id;
        this.prune();
        this.broadcast({ type: "msg", message: this.msgObj(this.getMsg(id)) });
        break;
      }
      case "media": {
        const kind = ["image", "voice", "file"].includes(d.kind) ? d.kind : null;
        if (!kind || typeof d.data !== "string" || d.data.length > MAX_MEDIA_B64) {
          this.sendTo(ws, { type: "error", text: "فایل خیلی بزرگه یا نامعتبره" });
          return;
        }
        const caption = typeof d.text === "string" ? d.text.trim().slice(0, 200) : "";
        const replyTo = Number.isInteger(d.replyTo) ? d.replyTo : null;
        const ts = Date.now();
        this.sql.exec("INSERT INTO messages (name, kind, text, data, reply_to, ts) VALUES (?, ?, ?, ?, ?, ?)", name, kind, caption, d.data, replyTo, ts);
        const id = this.sql.exec("SELECT last_insert_rowid() AS id").toArray()[0].id;
        this.prune();
        this.broadcast({ type: "msg", message: this.msgObj(this.getMsg(id)) });
        break;
      }
      case "edit": {
        const row = this.getMsg(d.id);
        if (!row || row.deleted || row.name !== name || row.kind !== "text") return;
        const text = typeof d.text === "string" ? d.text.trim().slice(0, MAX_TEXT_LEN) : "";
        if (!text) return;
        this.sql.exec("UPDATE messages SET text = ?, edited = 1 WHERE id = ?", text, row.id);
        this.broadcast({ type: "edited", id: row.id, text });
        break;
      }
      case "delete": {
        const row = this.getMsg(d.id);
        if (!row || row.deleted) return;
        if (row.name !== name && !isAdmin) return;
        this.sql.exec("UPDATE messages SET deleted = 1, text = '', data = NULL WHERE id = ?", row.id);
        this.broadcast({ type: "deleted", id: row.id });
        if (row.pinned) this.broadcast({ type: "pinned", message: this.pinnedMsg() });
        break;
      }
      case "react": {
        const row = this.getMsg(d.id);
        const emoji = typeof d.emoji === "string" ? d.emoji.slice(0, 8) : "";
        if (!row || row.deleted || !emoji) return;
        const existing = this.sql.exec("SELECT 1 FROM reactions WHERE msg_id = ? AND name = ? AND emoji = ?", row.id, name, emoji).toArray();
        if (existing.length) this.sql.exec("DELETE FROM reactions WHERE msg_id = ? AND name = ? AND emoji = ?", row.id, name, emoji);
        else this.sql.exec("INSERT INTO reactions (msg_id, name, emoji) VALUES (?, ?, ?)", row.id, name, emoji);
        this.broadcast({ type: "reaction", id: row.id, reactions: this.reactionsFor(row.id) });
        break;
      }
      case "typing": {
        this.broadcast({ type: "typing", name, on: !!d.on }, ws);
        break;
      }
      case "seen": {
        const upTo = Number.isInteger(d.upToId) ? d.upToId : 0;
        if (!upTo) return;
        this.sql.exec("UPDATE members SET last_seen_id = MAX(last_seen_id, ?) WHERE name = ?", upTo, name);
        this.broadcast({ type: "seen", name, upToId: upTo }, ws);
        break;
      }
      case "pin": {
        if (!isAdmin) return;
        const row = this.getMsg(d.id);
        if (!row || row.deleted) return;
        this.sql.exec("UPDATE messages SET pinned = 0");
        this.sql.exec("UPDATE messages SET pinned = 1 WHERE id = ?", row.id);
        this.broadcast({ type: "pinned", message: this.pinnedMsg() });
        break;
      }
      case "unpin": {
        if (!isAdmin) return;
        this.sql.exec("UPDATE messages SET pinned = 0");
        this.broadcast({ type: "pinned", message: null });
        break;
      }
      case "search": {
        const q = typeof d.q === "string" ? d.q.trim().slice(0, 100) : "";
        if (!q) return;
        const rows = this.sql.exec(
          "SELECT * FROM messages WHERE deleted = 0 AND kind = 'text' AND text LIKE ? ORDER BY id DESC LIMIT 30",
          "%" + q.replace(/[%_]/g, "") + "%"
        ).toArray().reverse();
        this.sendTo(ws, { type: "search_results", q, messages: rows.map((r) => this.msgObj(r)) });
        break;
      }
      case "history_before": {
        const before = Number.isInteger(d.beforeId) ? d.beforeId : 0;
        if (!before) return;
        const rows = this.sql.exec(
          "SELECT * FROM messages WHERE id < ? ORDER BY id DESC LIMIT ?", before, MAX_HISTORY_SEND
        ).toArray().reverse();
        this.sendTo(ws, { type: "older", messages: rows.map((r) => this.msgObj(r)) });
        break;
      }
      case "set_password": {
        if (!isAdmin) return;
        const pw = typeof d.password === "string" ? d.password.slice(0, 64) : "";
        if (!pw) { this.metaSet("pass_hash", null); this.broadcast({ type: "system", text: "رمز روم برداشته شد" }); }
        else {
          const hash = await sha256hex(pw + "::" + (att.room || "عمومی"));
          this.metaSet("pass_hash", hash);
          this.broadcast({ type: "system", text: "رمز روم تنظیم شد" });
        }
        break;
      }
      case "kick": {
        if (!isAdmin) return;
        const target = cleanText(d.name, MAX_NAME_LEN, "");
        if (!target || target === name) return;
        this.sql.exec("UPDATE members SET banned = 1 WHERE name = ?", target);
        for (const s of this.ctx.getWebSockets()) {
          const a = s.deserializeAttachment() || {};
          if (a.name === target) { this.sendTo(s, { type: "kicked" }); try { s.close(4403, "kicked"); } catch { /* gone */ } }
        }
        this.broadcast({ type: "system", text: target + " از روم اخراج شد" });
        this.broadcastPresence();
        break;
      }
    }
  }

  prune() {
    this.sql.exec(
      "DELETE FROM messages WHERE pinned = 0 AND id NOT IN (SELECT id FROM messages ORDER BY id DESC LIMIT ?)",
      MAX_HISTORY_KEEP
    );
  }

  async webSocketClose(ws) {
    const att = ws.deserializeAttachment() || {};
    if (att.name) {
      this.broadcast({ type: "typing", name: att.name, on: false }, ws);
      this.broadcast({ type: "system", text: att.name + " خارج شد" }, ws);
    }
    this.broadcastPresence(ws);
  }
  async webSocketError(ws) { this.broadcastPresence(ws); }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/ws") {
      if (request.headers.get("Upgrade") !== "websocket") {
        return new Response("Expected WebSocket upgrade", { status: 426 });
      }
      const room = cleanText(url.searchParams.get("room"), MAX_ROOM_LEN, "عمومی");
      const id = env.CHAT_ROOM.idFromName(room);
      return env.CHAT_ROOM.get(id).fetch(request);
    }
    if (url.pathname === "/health") {
      return Response.json({ ok: true, app: "parsa-chat", version: VERSION });
    }
    if (url.pathname === "/" || url.pathname === "/index.html") {
      return new Response(WEB_HTML, { headers: { "content-type": "text/html; charset=utf-8" } });
    }
    return new Response("Not found", { status: 404 });
  },
};

// ---------------- Web client v2 (served at /) ----------------
const WEB_HTML = `<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
<title>چت پارسا</title>
<style>
  :root {
    --bg:#0f1115; --panel:#171a21; --panel2:#13161d; --border:#262b36; --border2:#2c3342;
    --text:#e8eaf0; --muted:#9aa3b2; --accent:#3e63dd; --accent2:#3358c4; --mine:#27407c; --mineb:#35539c;
    --other:#1d212b; --otherb:#2a3040; --chip:#1f2531; --ok:#46a758; --bad:#e5484d;
  }
  body.light {
    --bg:#f4f6fb; --panel:#ffffff; --panel2:#eef1f8; --border:#dde3ef; --border2:#c9d2e4;
    --text:#1a1e28; --muted:#667085; --accent:#3e63dd; --accent2:#3358c4; --mine:#dbe7ff; --mineb:#b3c8f5;
    --other:#ffffff; --otherb:#dde3ef; --chip:#eef1f8;
  }
  * { box-sizing:border-box; margin:0; padding:0; }
  html, body { height:100%; }
  body { font-family:Tahoma,"Segoe UI",Arial,sans-serif; background:var(--bg); color:var(--text); display:flex; flex-direction:column; height:100dvh; }
  header { background:var(--panel); border-bottom:1px solid var(--border); padding:8px 12px; display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
  header h1 { font-size:16px; }
  .dot { width:10px; height:10px; border-radius:50%; background:var(--bad); flex:none; }
  .dot.on { background:var(--ok); }
  .meta { font-size:12px; color:var(--muted); }
  .spacer { flex:1; }
  input, button, select { font-family:inherit; font-size:14px; border-radius:10px; border:1px solid var(--border2); background:var(--panel2); color:var(--text); padding:8px 11px; }
  input:focus { outline:2px solid var(--accent); border-color:transparent; }
  button { background:var(--accent); border-color:var(--accent); color:#fff; cursor:pointer; font-weight:bold; }
  button:hover { background:var(--accent2); }
  button.ghost { background:transparent; color:var(--text); font-weight:normal; }
  button.icon { padding:7px 9px; font-size:15px; }
  #pinned { display:none; background:var(--panel2); border-bottom:1px solid var(--border); padding:7px 12px; font-size:13px; align-items:center; gap:8px; }
  #pinned.show { display:flex; }
  #pinned .txt { flex:1; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
  #searchbar { display:none; background:var(--panel2); border-bottom:1px solid var(--border); padding:8px 12px; gap:8px; }
  #searchbar.show { display:flex; }
  #searchbar input { flex:1; }
  #searchresults { display:none; max-height:180px; overflow-y:auto; background:var(--panel2); border-bottom:1px solid var(--border); padding:6px 12px; font-size:13px; }
  #searchresults.show { display:block; }
  .sres { padding:6px 4px; border-bottom:1px solid var(--border); cursor:pointer; }
  .sres:hover { background:var(--chip); }
  #users { padding:6px 12px; background:var(--panel2); border-bottom:1px solid var(--border); font-size:12px; color:var(--muted); display:flex; gap:6px; flex-wrap:wrap; align-items:center; }
  .chip { background:var(--chip); border:1px solid var(--border2); border-radius:999px; padding:2px 10px; color:var(--text); display:inline-flex; align-items:center; gap:5px; }
  .cdot { width:8px; height:8px; border-radius:50%; display:inline-block; }
  .admin-badge { color:#ffb224; font-size:11px; }
  #log { flex:1; overflow-y:auto; padding:14px; display:flex; flex-direction:column; gap:8px; }
  .row { display:flex; gap:8px; align-items:flex-end; max-width:88%; }
  .row.mine { align-self:flex-end; flex-direction:row-reverse; }
  .row.other { align-self:flex-start; }
  .avatar { width:32px; height:32px; border-radius:50%; flex:none; display:flex; align-items:center; justify-content:center; color:#fff; font-weight:bold; font-size:14px; }
  .msg { padding:8px 12px; border-radius:14px; line-height:1.7; font-size:14.5px; word-wrap:break-word; white-space:pre-wrap; position:relative; min-width:60px; }
  .msg.other { background:var(--other); border:1px solid var(--otherb); border-bottom-right-radius:4px; }
  .msg.mine { background:var(--mine); border:1px solid var(--mineb); border-bottom-left-radius:4px; }
  .msg .who { font-size:11.5px; margin-bottom:2px; font-weight:bold; }
  .msg .when { font-size:10.5px; color:var(--muted); margin-top:3px; display:flex; gap:6px; justify-content:flex-end; align-items:center; }
  .msg .edited { font-size:10px; color:var(--muted); }
  .msg.deleted { color:var(--muted); font-style:italic; }
  .quote { border-right:3px solid var(--accent); background:rgba(127,127,127,.12); border-radius:6px; padding:4px 8px; margin-bottom:5px; font-size:12.5px; color:var(--muted); cursor:pointer; }
  .quote b { color:var(--text); }
  .msg img.pic { max-width:260px; max-height:260px; border-radius:10px; display:block; cursor:pointer; }
  .msg audio { width:230px; height:36px; }
  .filelink { color:#8fb0ff; text-decoration:none; font-weight:bold; }
  .reactions { display:flex; gap:4px; flex-wrap:wrap; margin-top:5px; }
  .react { background:var(--chip); border:1px solid var(--border2); border-radius:999px; padding:1px 8px; font-size:12.5px; cursor:pointer; }
  .react.mine { border-color:var(--accent); background:rgba(62,99,221,.25); }
  .actions { display:none; position:absolute; top:-14px; left:6px; background:var(--panel); border:1px solid var(--border2); border-radius:999px; padding:2px 4px; gap:2px; box-shadow:0 2px 10px rgba(0,0,0,.35); z-index:5; }
  .msg:hover .actions, .msg:focus-within .actions { display:flex; }
  .actions button { background:transparent; border:none; color:var(--text); font-size:13px; padding:3px 6px; font-weight:normal; }
  .actions button:hover { background:var(--chip); border-radius:999px; }
  .sys { align-self:center; font-size:12px; color:var(--muted); background:var(--panel); border:1px solid var(--border); border-radius:999px; padding:3px 12px; }
  .datesep { align-self:center; font-size:11.5px; color:var(--muted); margin:6px 0 2px; }
  .unreadsep { align-self:center; font-size:11.5px; color:#ffb224; border:1px solid #ffb22455; border-radius:999px; padding:2px 12px; }
  .highlight { outline:2px solid #ffb224; }
  #typing { padding:4px 14px; font-size:12px; color:var(--muted); min-height:20px; background:var(--bg); }
  #replybar { display:none; background:var(--panel2); border-top:1px solid var(--border); padding:7px 12px; font-size:13px; align-items:center; gap:8px; }
  #replybar.show { display:flex; }
  #replybar .txt { flex:1; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; color:var(--muted); }
  #emojipanel { display:none; background:var(--panel); border-top:1px solid var(--border); padding:10px; flex-wrap:wrap; gap:4px; max-height:170px; overflow-y:auto; }
  #emojipanel.show { display:flex; }
  #emojipanel button { background:transparent; border:none; font-size:21px; padding:4px; cursor:pointer; }
  footer { padding:10px 12px; background:var(--panel); border-top:1px solid var(--border); display:flex; gap:6px; align-items:center; }
  #text { flex:1; }
  #members { position:fixed; top:0; left:0; bottom:0; width:270px; background:var(--panel); border-left:1px solid var(--border); z-index:50; padding:14px; display:none; flex-direction:column; gap:8px; overflow-y:auto; }
  #members.show { display:flex; }
  #members h3 { font-size:15px; }
  .memrow { display:flex; align-items:center; gap:8px; padding:6px 4px; border-bottom:1px solid var(--border); font-size:14px; }
  .memrow .sp { flex:1; }
  .kickbtn { background:transparent; border:1px solid var(--bad); color:var(--bad); font-size:12px; padding:3px 8px; }
  #toast { position:fixed; bottom:84px; right:50%; transform:translateX(50%); background:var(--panel); border:1px solid var(--border2); color:var(--text); border-radius:10px; padding:9px 16px; font-size:13.5px; display:none; z-index:99; box-shadow:0 4px 18px rgba(0,0,0,.4); }
  .rec { background:var(--bad) !important; border-color:var(--bad) !important; }
  @media (max-width:560px) { .row { max-width:96%; } header h1 { font-size:14px; } .msg img.pic { max-width:200px; } }
</style>
</head>
<body>
<header>
  <span class="dot" id="dot"></span>
  <h1>چت پارسا</h1>
  <span class="meta" id="status">قطع</span>
  <span class="spacer"></span>
  <button class="ghost icon" id="btn-search" title="جستجو">🔍</button>
  <button class="ghost icon" id="btn-members" title="اعضا">👥</button>
  <button class="ghost icon" id="btn-theme" title="تم">🌓</button>
  <button class="ghost icon" id="btn-sound" title="صدا">🔔</button>
  <input id="name" placeholder="اسمت" maxlength="24" style="width:110px">
  <input id="room" placeholder="اسم روم" maxlength="32" style="width:110px">
  <input id="pass" type="password" placeholder="رمز روم" maxlength="64" style="width:100px">
  <button id="join">ورود</button>
</header>
<div id="searchbar">
  <input id="searchq" placeholder="جستجو در پیام‌ها…">
  <button id="dosearch">جستجو</button>
  <button class="ghost" id="closesearch">بستن</button>
</div>
<div id="searchresults"></div>
<div id="pinned"><span>📌</span><span class="txt" id="pinnedtxt"></span><button class="ghost icon" id="unpinbtn" title="برداشتن سنجاق">✖</button></div>
<div id="users"><span>آنلاین‌ها:</span><span id="chips"></span></div>
<div id="log"></div>
<div id="typing"></div>
<div id="replybar"><span id="replylabel">↩️ پاسخ به:</span><span class="txt" id="replytxt"></span><button class="ghost icon" id="cancelreply">✖</button></div>
<div id="emojipanel"></div>
<footer>
  <button class="ghost icon" id="btn-emoji" title="ایموجی">😀</button>
  <button class="ghost icon" id="btn-img" title="عکس">🖼️</button>
  <button class="ghost icon" id="btn-file" title="فایل">📎</button>
  <button class="ghost icon" id="btn-voice" title="پیام صوتی">🎤</button>
  <input id="text" placeholder="پیامت رو بنویس…" maxlength="2000" autocomplete="off">
  <button id="send">ارسال</button>
  <input type="file" id="imginput" accept="image/*" style="display:none">
  <input type="file" id="fileinput" style="display:none">
</footer>
<div id="members">
  <h3>اعضای روم</h3>
  <div id="memlist"></div>
  <div id="adminbox" style="display:none; flex-direction:column; gap:8px; margin-top:10px;">
    <h3>مدیریت (ادمین)</h3>
    <input id="newpass" type="password" placeholder="رمز جدید روم (خالی = حذف رمز)" maxlength="64">
    <button id="setpass">تنظیم رمز</button>
  </div>
  <button class="ghost" id="closemembers">بستن</button>
</div>
<div id="toast"></div>
<script>
(function () {
  var $ = function (id) { return document.getElementById(id); };
  var log = $("log"), dot = $("dot"), statusEl = $("status"), chips = $("chips");
  var nameInput = $("name"), roomInput = $("room"), passInput = $("pass"), textInput = $("text");
  var typingEl = $("typing"), toastEl = $("toast");
  var ws = null, myName = "", myRole = "member", retryMs = 1000, reconnectTimer = null;
  var messagesById = {}, membersByName = {}, lastMsgId = 0, lastSeenSent = 0;
  var replyToId = null, editId = null, typingUsers = {}, typingTimer = null, lastTypingSent = 0;
  var soundOn = localStorage.getItem("pc_sound") !== "0";
  var oldestId = null, loadingOlder = false, lastDateKey = "";
  var roomKey = "";

  if (localStorage.getItem("pc_theme") === "light") document.body.classList.add("light");
  nameInput.value = localStorage.getItem("pc_name") || "";
  roomInput.value = localStorage.getItem("pc_room") || "عمومی";
  $("btn-sound").textContent = soundOn ? "🔔" : "🔕";

  function toast(t) {
    toastEl.textContent = t; toastEl.style.display = "block";
    setTimeout(function () { toastEl.style.display = "none"; }, 2600);
  }
  function beep() {
    if (!soundOn) return;
    try {
      var ctx = new (window.AudioContext || window.webkitAudioContext)();
      var o = ctx.createOscillator(), g = ctx.createGain();
      o.connect(g); g.connect(ctx.destination);
      o.frequency.value = 880; g.gain.value = 0.08;
      o.start(); o.stop(ctx.currentTime + 0.12);
    } catch (e) {}
  }
  function fmtTime(ts) {
    var d = new Date(ts);
    return ("0" + d.getHours()).slice(-2) + ":" + ("0" + d.getMinutes()).slice(-2);
  }
  function dateKey(ts) {
    var d = new Date(ts);
    return d.getFullYear() + "-" + (d.getMonth() + 1) + "-" + d.getDate();
  }
  function fmtDate(ts) {
    try { return new Date(ts).toLocaleDateString("fa-IR", { weekday: "long", day: "numeric", month: "long" }); }
    catch (e) { return dateKey(ts); }
  }
  function colorOf(name) {
    var m = membersByName[name];
    if (m && m.color) return m.color;
    var pal = ["#e5484d","#f76b15","#ffb224","#46a758","#12a594","#0090ff","#6e56cf","#d6409f","#8e4ec6","#30a46c"];
    var h = 0; for (var i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) >>> 0;
    return pal[h % pal.length];
  }
  function scrollDown() { log.scrollTop = log.scrollHeight; }
  function nearBottom() { return log.scrollHeight - log.scrollTop - log.clientHeight < 160; }

  function maybeDateSep(ts, prepend) {
    var k = dateKey(ts);
    if (k === lastDateKey) return;
    lastDateKey = k;
    var div = document.createElement("div");
    div.className = "datesep"; div.textContent = fmtDate(ts);
    if (prepend) log.insertBefore(div, log.firstChild); else log.appendChild(div);
  }

  function reactionHtml(m) {
    if (!m.reactions || !m.reactions.length) return "";
    var html = '<div class="reactions">';
    m.reactions.forEach(function (r) {
      var mine = r.users.indexOf(myName) >= 0;
      html += '<span class="react' + (mine ? " mine" : "") + '" data-react="' + r.emoji + '" title="' + r.users.join("، ") + '">' + r.emoji + " " + r.count + "</span>";
    });
    return html + "</div>";
  }

  function msgBodyHtml(m) {
    if (m.deleted) return '<div class="msg deleted">این پیام حذف شده</div>';
    var html = "";
    if (m.replyTo && messagesById[m.replyTo]) {
      var q = messagesById[m.replyTo];
      var qt = q.deleted ? "پیام حذف‌شده" : (q.kind === "text" ? q.text : (q.kind === "image" ? "🖼️ عکس" : q.kind === "voice" ? "🎤 پیام صوتی" : "📎 " + q.text));
      html += '<div class="quote" data-jump="' + q.id + '"><b>' + esc(q.name) + "</b>: " + esc(qt.slice(0, 90)) + "</div>";
    }
    if (m.kind === "image") html += '<img class="pic" src="data:image/jpeg;base64,' + m.data + '" alt="عکس">';
    else if (m.kind === "voice") html += '<audio controls src="data:audio/webm;base64,' + m.data + '"></audio>';
    else if (m.kind === "file") html += '<a class="filelink" download="' + esc(m.text || "file") + '" href="data:application/octet-stream;base64,' + m.data + '">📎 ' + esc(m.text || "فایل") + "</a>";
    else html += "<div>" + esc(m.text) + "</div>";
    if (m.kind !== "text" && m.text && m.kind !== "file") html += "<div>" + esc(m.text) + "</div>";
    return html;
  }
  function esc(s) {
    return String(s == null ? "" : s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }

  function seenTicks(m) {
    if (m.name !== myName) return "";
    var others = Object.keys(membersByName).filter(function (n) { return n !== myName; });
    if (!others.length) return "✓";
    var allSeen = others.every(function (n) { return (membersByName[n].last_seen_id || 0) >= m.id; });
    return allSeen ? "✓✓" : "✓";
  }

  function renderMsg(m, prepend) {
    messagesById[m.id] = m;
    if (m.id > lastMsgId) lastMsgId = m.id;
    if (oldestId === null || m.id < oldestId) oldestId = m.id;
    var existing = document.querySelector('[data-mid="' + m.id + '"]');
    if (existing) existing.remove();
    var mine = m.name === myName;
    var row = document.createElement("div");
    row.className = "row " + (mine ? "mine" : "other");
    row.setAttribute("data-mid", m.id);
    var av = document.createElement("div");
    av.className = "avatar"; av.style.background = colorOf(m.name);
    av.textContent = (m.name || "?").charAt(0);
    var msg = document.createElement("div");
    msg.className = "msg " + (mine ? "mine" : "other") + (m.deleted ? " deleted" : "");
    var who = '<div class="who" style="color:' + colorOf(m.name) + '">' + esc(m.name) + (membersByName[m.name] && membersByName[m.name].role === "admin" ? ' <span class="admin-badge">⭐</span>' : "") + "</div>";
    var acts = '<div class="actions">' +
      '<button data-act="reply" title="پاسخ">↩️</button>' +
      '<button data-act="react" title="واکنش">😀</button>' +
      (mine && m.kind === "text" && !m.deleted ? '<button data-act="edit" title="ویرایش">✏️</button>' : "") +
      ((mine || myRole === "admin") && !m.deleted ? '<button data-act="delete" title="حذف">🗑️</button>' : "") +
      (myRole === "admin" && !m.deleted ? '<button data-act="pin" title="سنجاق">📌</button>' : "") +
      "</div>";
    var when = '<div class="when">' + (m.edited ? '<span class="edited">ویرایش‌شده</span>' : "") + "<span>" + fmtTime(m.ts) + "</span>" + (mine ? "<span>" + seenTicks(m) + "</span>" : "") + "</div>";
    msg.innerHTML = acts + who + msgBodyHtml(m) + reactionHtml(m) + when;
    row.appendChild(av); row.appendChild(msg);
    if (prepend) {
      var firstRow = log.querySelector(".row");
      if (firstRow) log.insertBefore(row, firstRow); else log.appendChild(row);
    } else {
      maybeDateSep(m.ts, false);
      log.appendChild(row);
    }
    return row;
  }

  function addSys(text) {
    var div = document.createElement("div");
    div.className = "sys"; div.textContent = text;
    log.appendChild(div); if (nearBottom()) scrollDown();
  }
  function setPresence(users) {
    chips.innerHTML = "";
    users.forEach(function (u) {
      if (!membersByName[u.name]) membersByName[u.name] = u;
      else { membersByName[u.name].color = u.color; membersByName[u.name].role = u.role; }
      var c = document.createElement("span"); c.className = "chip";
      c.innerHTML = '<span class="cdot" style="background:' + u.color + '"></span>' + esc(u.name) + (u.role === "admin" ? ' <span class="admin-badge">⭐</span>' : "");
      chips.appendChild(c);
    });
    if (!users.length) chips.textContent = "—";
    renderMembers();
  }
  function renderMembers() {
    var box = $("memlist"); box.innerHTML = "";
    Object.keys(membersByName).sort().forEach(function (n) {
      var m = membersByName[n];
      var row = document.createElement("div"); row.className = "memrow";
      var html = '<span class="cdot" style="background:' + m.color + '"></span><span>' + esc(n) + "</span>" +
        (m.role === "admin" ? '<span class="admin-badge">⭐ ادمین</span>' : "") + '<span class="sp"></span>';
      if (myRole === "admin" && n !== myName) html += '<button class="kickbtn" data-kick="' + esc(n) + '">اخراج</button>';
      row.innerHTML = html;
      box.appendChild(row);
    });
    $("adminbox").style.display = myRole === "admin" ? "flex" : "none";
  }
  function setPinned(m) {
    var bar = $("pinned");
    if (!m) { bar.classList.remove("show"); return; }
    var t = m.kind === "text" ? m.text : (m.kind === "image" ? "🖼️ عکس" : m.kind === "voice" ? "🎤 پیام صوتی" : "📎 " + m.text);
    $("pinnedtxt").textContent = m.name + ": " + t;
    bar.classList.add("show");
    bar.onclick = function (e) { if (e.target.id !== "unpinbtn") jumpTo(m.id); };
  }
  function jumpTo(id) {
    var el = document.querySelector('[data-mid="' + id + '"]');
    if (!el) { toast("این پیام توی تاریخچه بارگذاری‌شده نیست"); return; }
    el.scrollIntoView({ block: "center", behavior: "smooth" });
    var msg = el.querySelector(".msg");
    msg.classList.add("highlight");
    setTimeout(function () { msg.classList.remove("highlight"); }, 1600);
  }
  function updateTyping() {
    var names = Object.keys(typingUsers).filter(function (n) { return typingUsers[n]; });
    typingEl.textContent = names.length ? names.join("، ") + " داره می‌نویسه…" : "";
  }
  function setStatus(on, label) {
    dot.className = "dot" + (on ? " on" : "");
    statusEl.textContent = label;
  }
  function sendSeen() {
    if (!ws || ws.readyState !== 1 || lastMsgId <= lastSeenSent) return;
    lastSeenSent = lastMsgId;
    ws.send(JSON.stringify({ type: "seen", upToId: lastMsgId }));
    if (roomKey) localStorage.setItem("pc_seen_" + roomKey, String(lastMsgId));
  }

  function handle(d) {
    if (d.type === "history") {
      membersByName = {};
      (d.members || []).forEach(function (m) { membersByName[m.name] = m; });
      myRole = d.you ? d.you.role : "member";
      var lastRead = roomKey ? parseInt(localStorage.getItem("pc_seen_" + roomKey) || "0", 10) : 0;
      var unreadShown = false;
      d.messages.forEach(function (m) {
        if (!unreadShown && lastRead && m.id > lastRead && m.name !== myName) {
          var sep = document.createElement("div"); sep.className = "unreadsep"; sep.textContent = "پیام‌های جدید";
          log.appendChild(sep); unreadShown = true;
        }
        renderMsg(m, false);
      });
      setPinned(d.pinned || null);
      setPresence(Object.keys(membersByName).map(function (n) { return membersByName[n]; }));
      scrollDown(); sendSeen();
    }
    else if (d.type === "older") {
      loadingOlder = false;
      var prevHeight = log.scrollHeight;
      var list = d.messages.slice().reverse();
      list.forEach(function (m) { renderMsg(m, true); });
      log.scrollTop = log.scrollHeight - prevHeight;
    }
    else if (d.type === "msg") {
      var stick = nearBottom();
      renderMsg(d.message, false);
      if (d.message.name !== myName) {
        beep();
        if (document.hidden && window.Notification && Notification.permission === "granted") {
          try { new Notification("چت پارسا — " + d.message.name, { body: d.message.kind === "text" ? d.message.text : "پیام جدید" }); } catch (e) {}
        }
      }
      if (stick) scrollDown();
      sendSeen();
    }
    else if (d.type === "edited") {
      var m1 = messagesById[d.id]; if (m1) { m1.text = d.text; m1.edited = true; renderMsg(m1, false); }
    }
    else if (d.type === "deleted") {
      var m2 = messagesById[d.id]; if (m2) { m2.deleted = true; m2.text = ""; m2.data = null; renderMsg(m2, false); }
    }
    else if (d.type === "reaction") {
      var m3 = messagesById[d.id]; if (m3) { m3.reactions = d.reactions; renderMsg(m3, false); }
    }
    else if (d.type === "typing") { typingUsers[d.name] = d.on; updateTyping(); }
    else if (d.type === "seen") {
      if (membersByName[d.name]) membersByName[d.name].last_seen_id = Math.max(membersByName[d.name].last_seen_id || 0, d.upToId);
      Object.keys(messagesById).forEach(function (id) {
        var m = messagesById[id];
        if (m.name === myName) renderMsg(m, false);
      });
    }
    else if (d.type === "pinned") { setPinned(d.message); }
    else if (d.type === "presence") { setPresence(d.users); }
    else if (d.type === "system") { addSys(d.text); }
    else if (d.type === "error") { toast(d.text); }
    else if (d.type === "kicked") { toast("از روم اخراج شدی"); if (ws) ws.close(); }
    else if (d.type === "search_results") {
      var box = $("searchresults"); box.innerHTML = "";
      if (!d.messages.length) box.innerHTML = '<div class="sres">چیزی پیدا نشد</div>';
      d.messages.forEach(function (m) {
        if (!messagesById[m.id]) messagesById[m.id] = m;
        var div = document.createElement("div"); div.className = "sres";
        div.textContent = m.name + " (" + fmtTime(m.ts) + "): " + m.text;
        div.onclick = function () { jumpTo(m.id); };
        box.appendChild(div);
      });
      box.classList.add("show");
    }
  }

  function connect() {
    myName = nameInput.value.trim() || "مهمان";
    var room = roomInput.value.trim() || "عمومی";
    roomKey = room;
    localStorage.setItem("pc_name", myName);
    localStorage.setItem("pc_room", room);
    if (ws) { try { ws.close(); } catch (e) {} ws = null; }
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
    log.innerHTML = ""; messagesById = {}; oldestId = null; lastMsgId = 0; lastSeenSent = 0; lastDateKey = ""; typingUsers = {}; updateTyping();
    var proto = location.protocol === "https:" ? "wss://" : "ws://";
    var url = proto + location.host + "/ws?room=" + encodeURIComponent(room) + "&name=" + encodeURIComponent(myName);
    if (passInput.value) url += "&pass=" + encodeURIComponent(passInput.value);
    setStatus(false, "در حال اتصال…");
    ws = new WebSocket(url);
    ws.onopen = function () { retryMs = 1000; setStatus(true, "متصل — روم «" + room + "»"); textInput.focus(); };
    ws.onmessage = function (ev) { var d; try { d = JSON.parse(ev.data); } catch (e) { return; } handle(d); };
    ws.onclose = function (ev) {
      setStatus(false, ev.code === 4403 ? "دسترسی رد شد" : "قطع — تلاش دوباره…");
      if (ev.code !== 4403) {
        reconnectTimer = setTimeout(connect, retryMs);
        retryMs = Math.min(retryMs * 2, 15000);
      }
    };
    ws.onerror = function () { try { ws.close(); } catch (e) {} };
  }

  function sendText() {
    var t = textInput.value.trim();
    if (!t || !ws || ws.readyState !== 1) return;
    if (editId) {
      ws.send(JSON.stringify({ type: "edit", id: editId, text: t }));
      editId = null; $("replybar").classList.remove("show");
    } else {
      var payload = { type: "msg", text: t };
      if (replyToId) payload.replyTo = replyToId;
      ws.send(JSON.stringify(payload));
      cancelReply();
    }
    textInput.value = "";
    sendTyping(false);
    textInput.focus();
  }
  function sendTyping(on) {
    if (!ws || ws.readyState !== 1) return;
    var now = Date.now();
    if (on && now - lastTypingSent < 2500) return;
    lastTypingSent = now;
    ws.send(JSON.stringify({ type: "typing", on: on }));
  }
  function startReply(id) {
    var m = messagesById[id]; if (!m) return;
    replyToId = id; editId = null;
    $("replylabel").textContent = "↩️ پاسخ به " + m.name + ":";
    $("replytxt").textContent = m.kind === "text" ? m.text : "پیام";
    $("replybar").classList.add("show");
    textInput.focus();
  }
  function cancelReply() { replyToId = null; $("replybar").classList.remove("show"); }
  function startEdit(id) {
    var m = messagesById[id]; if (!m) return;
    editId = id; replyToId = null;
    $("replylabel").textContent = "✏️ ویرایش پیام:";
    $("replytxt").textContent = m.text;
    $("replybar").classList.add("show");
    textInput.value = m.text; textInput.focus();
  }

  log.addEventListener("click", function (e) {
    var row = e.target.closest(".row");
    var jump = e.target.closest("[data-jump]");
    if (jump) { jumpTo(parseInt(jump.getAttribute("data-jump"), 10)); return; }
    var react = e.target.closest("[data-react]");
    if (react && row) {
      ws.send(JSON.stringify({ type: "react", id: parseInt(row.getAttribute("data-mid"), 10), emoji: react.getAttribute("data-react") }));
      return;
    }
    var btn = e.target.closest("[data-act]");
    if (!btn || !row) return;
    var id = parseInt(row.getAttribute("data-mid"), 10);
    var act = btn.getAttribute("data-act");
    if (act === "reply") startReply(id);
    else if (act === "edit") startEdit(id);
    else if (act === "delete") { if (confirm("این پیام حذف بشه؟")) ws.send(JSON.stringify({ type: "delete", id: id })); }
    else if (act === "pin") ws.send(JSON.stringify({ type: "pin", id: id }));
    else if (act === "react") {
      var emojis = ["❤️","😂","👍","🔥","😮","😢","🎉","👏"];
      var pick = prompt("ایموجی رو انتخاب کن: " + emojis.join("  "));
      if (pick) ws.send(JSON.stringify({ type: "react", id: id, emoji: pick.trim().slice(0, 8) }));
    }
  });
  log.addEventListener("scroll", function () {
    if (log.scrollTop < 60 && !loadingOlder && oldestId && ws && ws.readyState === 1) {
      loadingOlder = true;
      ws.send(JSON.stringify({ type: "history_before", beforeId: oldestId }));
    }
    if (nearBottom()) sendSeen();
  });

  textInput.addEventListener("input", function () {
    sendTyping(true);
    clearTimeout(typingTimer);
    typingTimer = setTimeout(function () { sendTyping(false); }, 2500);
  });
  textInput.addEventListener("keydown", function (e) { if (e.key === "Enter") sendText(); });
  $("send").addEventListener("click", sendText);
  $("join").addEventListener("click", connect);
  $("cancelreply").addEventListener("click", function () { cancelReply(); editId = null; textInput.value = ""; });
  $("unpinbtn").addEventListener("click", function (e) { e.stopPropagation(); if (ws) ws.send(JSON.stringify({ type: "unpin" })); });

  var EMOJIS = ["😀","😂","😍","😎","😭","😡","👍","👎","❤️","🔥","🎉","👏","🙏","💯","🤔","😴","🤯","🥳","😇","🤝","👋","💪","🌹","⭐","⚽","🏆","☕","🍕","🎵","🚗"];
  var panel = $("emojipanel");
  EMOJIS.forEach(function (em) {
    var b = document.createElement("button"); b.textContent = em;
    b.onclick = function () { textInput.value += em; textInput.focus(); };
    panel.appendChild(b);
  });
  $("btn-emoji").addEventListener("click", function () { panel.classList.toggle("show"); });
  $("btn-theme").addEventListener("click", function () {
    document.body.classList.toggle("light");
    localStorage.setItem("pc_theme", document.body.classList.contains("light") ? "light" : "dark");
  });
  $("btn-sound").addEventListener("click", function () {
    soundOn = !soundOn;
    localStorage.setItem("pc_sound", soundOn ? "1" : "0");
    $("btn-sound").textContent = soundOn ? "🔔" : "🔕";
    if (soundOn && window.Notification && Notification.permission === "default") Notification.requestPermission();
  });
  $("btn-search").addEventListener("click", function () { $("searchbar").classList.toggle("show"); $("searchq").focus(); });
  $("closesearch").addEventListener("click", function () { $("searchbar").classList.remove("show"); $("searchresults").classList.remove("show"); });
  $("dosearch").addEventListener("click", function () {
    var q = $("searchq").value.trim();
    if (q && ws) ws.send(JSON.stringify({ type: "search", q: q }));
  });
  $("searchq").addEventListener("keydown", function (e) { if (e.key === "Enter") $("dosearch").click(); });
  $("btn-members").addEventListener("click", function () { renderMembers(); $("members").classList.add("show"); });
  $("closemembers").addEventListener("click", function () { $("members").classList.remove("show"); });
  $("memlist").addEventListener("click", function (e) {
    var k = e.target.closest("[data-kick]");
    if (k && confirm("«" + k.getAttribute("data-kick") + "» اخراج بشه؟")) {
      ws.send(JSON.stringify({ type: "kick", name: k.getAttribute("data-kick") }));
    }
  });
  $("setpass").addEventListener("click", function () {
    if (ws) { ws.send(JSON.stringify({ type: "set_password", password: $("newpass").value })); toast("رمز روم تنظیم شد"); }
  });

  function fileToB64(file, cb) {
    var r = new FileReader();
    r.onload = function () { cb(String(r.result).split(",")[1] || ""); };
    r.readAsDataURL(file);
  }
  $("btn-img").addEventListener("click", function () { $("imginput").click(); });
  $("imginput").addEventListener("change", function () {
    var f = $("imginput").files[0]; if (!f) return;
    var img = new Image();
    img.onload = function () {
      var scale = Math.min(1, 1024 / Math.max(img.width, img.height));
      var c = document.createElement("canvas");
      c.width = Math.round(img.width * scale); c.height = Math.round(img.height * scale);
      c.getContext("2d").drawImage(img, 0, 0, c.width, c.height);
      var b64 = c.toDataURL("image/jpeg", 0.72).split(",")[1];
      if (ws && ws.readyState === 1) ws.send(JSON.stringify({ type: "media", kind: "image", data: b64, text: "" }));
    };
    img.src = URL.createObjectURL(f);
    $("imginput").value = "";
  });
  $("btn-file").addEventListener("click", function () { $("fileinput").click(); });
  $("fileinput").addEventListener("change", function () {
    var f = $("fileinput").files[0]; if (!f) return;
    if (f.size > 1000000) { toast("فایل باید کمتر از ۱ مگ باشه"); $("fileinput").value = ""; return; }
    fileToB64(f, function (b64) {
      if (ws && ws.readyState === 1) ws.send(JSON.stringify({ type: "media", kind: "file", data: b64, text: f.name }));
    });
    $("fileinput").value = "";
  });

  var recorder = null, recChunks = [];
  $("btn-voice").addEventListener("click", function () {
    if (recorder && recorder.state === "recording") { recorder.stop(); return; }
    if (!navigator.mediaDevices || !window.MediaRecorder) { toast("مرورگرت ضبط صدا رو پشتیبانی نمی‌کنه"); return; }
    navigator.mediaDevices.getUserMedia({ audio: true }).then(function (stream) {
      recChunks = [];
      recorder = new MediaRecorder(stream);
      recorder.ondataavailable = function (e) { recChunks.push(e.data); };
      recorder.onstop = function () {
        stream.getTracks().forEach(function (t) { t.stop(); });
        $("btn-voice").classList.remove("rec"); $("btn-voice").textContent = "🎤";
        var blob = new Blob(recChunks, { type: "audio/webm" });
        if (blob.size > 1000000) { toast("پیام صوتی خیلی طولانیه"); return; }
        var r = new FileReader();
        r.onload = function () {
          var b64 = String(r.result).split(",")[1] || "";
          if (ws && ws.readyState === 1) ws.send(JSON.stringify({ type: "media", kind: "voice", data: b64, text: "" }));
        };
        r.readAsDataURL(blob);
      };
      recorder.start();
      $("btn-voice").classList.add("rec"); $("btn-voice").textContent = "⏹️";
      setTimeout(function () { if (recorder && recorder.state === "recording") recorder.stop(); }, 60000);
    }).catch(function () { toast("اجازه میکروفون داده نشد"); });
  });

  connect();
})();
</script>
</body>
</html>
`;
