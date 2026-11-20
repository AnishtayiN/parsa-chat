// Parsa Chat v3.0 — Telegram-style realtime chat room server
// Cloudflare Worker + Durable Objects (SQLite) + WebSocket Hibernation API
// v3 adds: polls, forwarding, multiple pinned messages, admin promote/demote,
// unban, bio + avatar color, room topic, last-seen, lite connections for a
// live chat list, longer messages (4096).

const VERSION = "3.0.0";
const MAX_HISTORY_SEND = 50;
const MAX_HISTORY_KEEP = 1000;
const MAX_TEXT_LEN = 4096;
const MAX_NAME_LEN = 24;
const MAX_ROOM_LEN = 32;
const MAX_MEDIA_B64 = 1500000; // ~1.1 MB binary
const MAX_PINS = 20;
const MAX_POLL_OPTS = 10;
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
    const cols = this.sql.exec("PRAGMA table_info(messages)").toArray().map((r) => r.name);
    const add = (col, def) => { if (!cols.includes(col)) this.sql.exec("ALTER TABLE messages ADD COLUMN " + col + " " + def); };
    add("kind", "TEXT NOT NULL DEFAULT 'text'");
    add("data", "TEXT");
    add("reply_to", "INTEGER");
    add("edited", "INTEGER NOT NULL DEFAULT 0");
    add("deleted", "INTEGER NOT NULL DEFAULT 0");
    add("pinned", "INTEGER NOT NULL DEFAULT 0");
    add("fwd_from", "TEXT"); // v3
    this.sql.exec("CREATE TABLE IF NOT EXISTS reactions (msg_id INTEGER NOT NULL, name TEXT NOT NULL, emoji TEXT NOT NULL, PRIMARY KEY (msg_id, name, emoji))");
    this.sql.exec("CREATE TABLE IF NOT EXISTS members (name TEXT PRIMARY KEY, color TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'member', last_seen_id INTEGER NOT NULL DEFAULT 0, banned INTEGER NOT NULL DEFAULT 0, joined_ts INTEGER NOT NULL)");
    const mcols = this.sql.exec("PRAGMA table_info(members)").toArray().map((r) => r.name);
    const madd = (col, def) => { if (!mcols.includes(col)) this.sql.exec("ALTER TABLE members ADD COLUMN " + col + " " + def); };
    madd("bio", "TEXT"); // v3
    madd("last_seen_ts", "INTEGER"); // v3
    this.sql.exec("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)");
  }

  // ---------- helpers ----------
  metaGet(key) {
    const r = this.sql.exec("SELECT value FROM meta WHERE key = ?", key).toArray();
    return r.length ? r[0].value : null;
  }
  metaSet(key, value) {
    if (value === null || value === "") this.sql.exec("DELETE FROM meta WHERE key = ?", key);
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
    const o = {
      id: row.id, name: row.name, kind: row.kind,
      text: row.deleted ? "" : row.text,
      data: row.deleted || row.kind === "poll" ? null : row.data,
      replyTo: row.reply_to, ts: row.ts,
      edited: !!row.edited, deleted: !!row.deleted,
      reactions: this.reactionsFor(row.id),
    };
    if (row.fwd_from) o.fwdFrom = row.fwd_from;
    if (row.kind === "poll" && !row.deleted) {
      try { o.poll = JSON.parse(row.data); } catch { o.poll = null; }
    }
    return o;
  }
  getMsg(id) {
    const r = this.sql.exec("SELECT * FROM messages WHERE id = ?", id).toArray();
    return r.length ? r[0] : null;
  }
  pinnedMsgs() {
    const rows = this.sql.exec(
      "SELECT * FROM messages WHERE pinned = 1 AND deleted = 0 ORDER BY id DESC LIMIT ?", MAX_PINS
    ).toArray().reverse();
    return rows.map((r) => this.msgObj(r));
  }
  onlineUsers() {
    const seen = new Map();
    for (const ws of this.ctx.getWebSockets()) {
      const att = ws.deserializeAttachment() || {};
      if (att.name && !att.lite) seen.set(att.name, att);
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
  broadcastMembers() {
    const all = this.sql.exec("SELECT name, color, role, last_seen_id, bio, last_seen_ts, banned FROM members").toArray();
    for (const ws of this.ctx.getWebSockets()) {
      const att = ws.deserializeAttachment() || {};
      const list = att.role === "admin" ? all : all.filter((m) => !m.banned);
      this.sendTo(ws, { type: "members", members: list });
    }
  }
  updateAttachments(name, patch) {
    for (const ws of this.ctx.getWebSockets()) {
      const att = ws.deserializeAttachment() || {};
      if (att.name === name) ws.serializeAttachment({ ...att, ...patch });
    }
  }
  sendTo(ws, obj) { try { ws.send(JSON.stringify(obj)); } catch { /* gone */ } }

  // ---------- connection ----------
  async fetch(request) {
    const url = new URL(request.url);
    const room = cleanText(url.searchParams.get("room"), MAX_ROOM_LEN, "عمومی");
    const name = cleanText(url.searchParams.get("name"), MAX_NAME_LEN, "مهمان");
    const lite = url.searchParams.get("lite") === "1";

    const passHash = this.metaGet("pass_hash");
    if (passHash) {
      const given = url.searchParams.get("pass") || "";
      const givenHash = await sha256hex(given + "::" + room);
      if (givenHash !== passHash) {
        const pair = new WebSocketPair();
        this.ctx.acceptWebSocket(pair[1]);
        pair[1].send(JSON.stringify({ type: "error", code: "bad_password", text: "رمز روم اشتباهه" }));
        pair[1].close(4403, "bad password");
        return new Response(null, { status: 101, webSocket: pair[0] });
      }
    }

    let mem = this.member(name);
    if (mem && mem.banned) {
      const pair = new WebSocketPair();
      this.ctx.acceptWebSocket(pair[1]);
      pair[1].send(JSON.stringify({ type: "error", code: "banned", text: "از این روم اخراج شدی" }));
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
    server.serializeAttachment({ name, color: mem.color, role: mem.role, room, lite });

    const rows = this.sql.exec(
      "SELECT * FROM messages ORDER BY id DESC LIMIT ?", MAX_HISTORY_SEND
    ).toArray().reverse();
    const isAdmin = mem.role === "admin";
    const members = this.sql.exec("SELECT name, color, role, last_seen_id, bio, last_seen_ts, banned FROM members").toArray()
      .filter((m) => isAdmin || !m.banned);
    this.sendTo(server, {
      type: "history",
      messages: rows.map((r) => this.msgObj(r)),
      pinned: this.pinnedMsgs(),
      members,
      you: { name, color: mem.color, role: mem.role, bio: mem.bio || "" },
      hasPassword: !!passHash,
      topic: this.metaGet("topic") || "",
      version: VERSION,
    });
    if (!lite) {
      this.broadcastPresence();
      this.broadcast({ type: "system", text: name + " وارد شد" }, server);
    }
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
    const fwdFrom = cleanText(d.fwdFrom, MAX_NAME_LEN, "") || null;

    switch (d.type) {
      case "msg": {
        const text = typeof d.text === "string" ? d.text.trim().slice(0, MAX_TEXT_LEN) : "";
        if (!text) return;
        const replyTo = Number.isInteger(d.replyTo) ? d.replyTo : null;
        const ts = Date.now();
        this.sql.exec("INSERT INTO messages (name, kind, text, reply_to, ts, fwd_from) VALUES (?, 'text', ?, ?, ?, ?)", name, text, replyTo, ts, fwdFrom);
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
        const caption = typeof d.text === "string" ? d.text.trim().slice(0, 1024) : "";
        const replyTo = Number.isInteger(d.replyTo) ? d.replyTo : null;
        const ts = Date.now();
        this.sql.exec("INSERT INTO messages (name, kind, text, data, reply_to, ts, fwd_from) VALUES (?, ?, ?, ?, ?, ?, ?)", name, kind, caption, d.data, replyTo, ts, fwdFrom);
        const id = this.sql.exec("SELECT last_insert_rowid() AS id").toArray()[0].id;
        this.prune();
        this.broadcast({ type: "msg", message: this.msgObj(this.getMsg(id)) });
        break;
      }
      case "poll": {
        const q = cleanText(d.question, 200, "");
        const opts = Array.isArray(d.options)
          ? d.options.map((o) => cleanText(o, 100, "")).filter(Boolean).slice(0, MAX_POLL_OPTS)
          : [];
        if (!q || opts.length < 2) { this.sendTo(ws, { type: "error", text: "نظرسنجی حداقل ۲ گزینه می‌خواد" }); return; }
        const poll = { q, opts: opts.map((t) => ({ t, v: [] })), multi: !!d.multi, closed: false };
        const replyTo = Number.isInteger(d.replyTo) ? d.replyTo : null;
        this.sql.exec("INSERT INTO messages (name, kind, text, data, reply_to, ts) VALUES (?, 'poll', ?, ?, ?, ?)", name, q, JSON.stringify(poll), replyTo, Date.now());
        const id = this.sql.exec("SELECT last_insert_rowid() AS id").toArray()[0].id;
        this.prune();
        this.broadcast({ type: "msg", message: this.msgObj(this.getMsg(id)) });
        break;
      }
      case "vote": {
        const row = this.getMsg(d.id);
        if (!row || row.deleted || row.kind !== "poll") return;
        let poll;
        try { poll = JSON.parse(row.data); } catch { return; }
        if (!poll || poll.closed) return;
        let idx = Array.isArray(d.idx) ? d.idx : [d.idx];
        idx = [...new Set(idx.filter((i) => Number.isInteger(i) && i >= 0 && i < poll.opts.length))];
        if (!poll.multi) idx = idx.slice(0, 1);
        for (const o of poll.opts) o.v = o.v.filter((n) => n !== name);
        for (const i of idx) poll.opts[i].v.push(name);
        this.sql.exec("UPDATE messages SET data = ? WHERE id = ?", JSON.stringify(poll), row.id);
        this.broadcast({ type: "poll", id: row.id, poll });
        break;
      }
      case "close_poll": {
        const row = this.getMsg(d.id);
        if (!row || row.deleted || row.kind !== "poll") return;
        if (row.name !== name && !isAdmin) return;
        let poll;
        try { poll = JSON.parse(row.data); } catch { return; }
        poll.closed = true;
        this.sql.exec("UPDATE messages SET data = ? WHERE id = ?", JSON.stringify(poll), row.id);
        this.broadcast({ type: "poll", id: row.id, poll });
        break;
      }
      case "edit": {
        const row = this.getMsg(d.id);
        if (!row || row.deleted || row.name !== name || row.kind !== "text") return;
        const text = typeof d.text === "string" ? d.text.trim().slice(0, MAX_TEXT_LEN) : "";
        if (!text) return;
        this.sql.exec("UPDATE messages SET text = ?, edited = 1 WHERE id = ?", text, row.id);
        this.broadcast({ type: "edited", id: row.id, text });
        if (row.pinned) this.broadcast({ type: "pinned", messages: this.pinnedMsgs() });
        break;
      }
      case "delete": {
        const row = this.getMsg(d.id);
        if (!row || row.deleted) return;
        if (row.name !== name && !isAdmin) return;
        this.sql.exec("UPDATE messages SET deleted = 1, text = '', data = NULL WHERE id = ?", row.id);
        this.broadcast({ type: "deleted", id: row.id });
        if (row.pinned) this.broadcast({ type: "pinned", messages: this.pinnedMsgs() });
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
        if (att.lite) return;
        this.broadcast({ type: "typing", name, on: !!d.on }, ws);
        break;
      }
      case "seen": {
        if (att.lite) return;
        const upTo = Number.isInteger(d.upToId) ? d.upToId : 0;
        if (!upTo) return;
        this.sql.exec("UPDATE members SET last_seen_id = MAX(last_seen_id, ?), last_seen_ts = ? WHERE name = ?", upTo, Date.now(), name);
        this.broadcast({ type: "seen", name, upToId: upTo }, ws);
        break;
      }
      case "pin": {
        if (!isAdmin) return;
        const row = this.getMsg(d.id);
        if (!row || row.deleted || row.pinned) return;
        const count = this.sql.exec("SELECT COUNT(*) AS c FROM messages WHERE pinned = 1 AND deleted = 0").toArray()[0].c;
        if (count >= MAX_PINS) {
          this.sql.exec("UPDATE messages SET pinned = 0 WHERE id = (SELECT id FROM messages WHERE pinned = 1 AND deleted = 0 ORDER BY id ASC LIMIT 1)");
        }
        this.sql.exec("UPDATE messages SET pinned = 1 WHERE id = ?", row.id);
        this.broadcast({ type: "pinned", messages: this.pinnedMsgs() });
        break;
      }
      case "unpin": {
        if (!isAdmin) return;
        if (Number.isInteger(d.id)) this.sql.exec("UPDATE messages SET pinned = 0 WHERE id = ?", d.id);
        else this.sql.exec("UPDATE messages SET pinned = 0");
        this.broadcast({ type: "pinned", messages: this.pinnedMsgs() });
        break;
      }
      case "search": {
        const q = typeof d.q === "string" ? d.q.trim().slice(0, 100) : "";
        if (!q) return;
        const rows = this.sql.exec(
          "SELECT * FROM messages WHERE deleted = 0 AND text LIKE ? ORDER BY id DESC LIMIT 30",
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
        if (!pw) { this.metaSet("pass_hash", null); this.broadcast({ type: "system", text: "رمز روم برداشته شد" }); this.broadcast({ type: "password", hasPassword: false }); }
        else {
          const hash = await sha256hex(pw + "::" + (att.room || "عمومی"));
          this.metaSet("pass_hash", hash);
          this.broadcast({ type: "system", text: "رمز روم تنظیم شد" });
          this.broadcast({ type: "password", hasPassword: true });
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
        this.broadcastMembers();
        this.broadcastPresence();
        break;
      }
      case "unban": {
        if (!isAdmin) return;
        const target = cleanText(d.name, MAX_NAME_LEN, "");
        if (!target) return;
        this.sql.exec("UPDATE members SET banned = 0 WHERE name = ?", target);
        this.broadcast({ type: "system", text: target + " از بن خارج شد" });
        this.broadcastMembers();
        break;
      }
      case "promote": {
        if (!isAdmin) return;
        const target = cleanText(d.name, MAX_NAME_LEN, "");
        const mem = target ? this.member(target) : null;
        if (!mem || mem.banned || mem.role === "admin") return;
        this.sql.exec("UPDATE members SET role = 'admin' WHERE name = ?", target);
        this.updateAttachments(target, { role: "admin" });
        this.broadcast({ type: "system", text: target + " ادمین شد" });
        this.broadcastMembers();
        this.broadcastPresence();
        break;
      }
      case "demote": {
        if (!isAdmin) return;
        const target = cleanText(d.name, MAX_NAME_LEN, "");
        if (!target || target === name) return;
        const mem = this.member(target);
        if (!mem || mem.role !== "admin") return;
        this.sql.exec("UPDATE members SET role = 'member' WHERE name = ?", target);
        this.updateAttachments(target, { role: "member" });
        this.broadcast({ type: "system", text: target + " دیگه ادمین نیست" });
        this.broadcastMembers();
        this.broadcastPresence();
        break;
      }
      case "set_topic": {
        if (!isAdmin) return;
        const topic = cleanText(d.text, 140, "");
        this.metaSet("topic", topic || null);
        this.broadcast({ type: "topic", text: topic });
        break;
      }
      case "set_bio": {
        const bio = cleanText(d.bio, 100, "");
        this.sql.exec("UPDATE members SET bio = ? WHERE name = ?", bio || null, name);
        this.broadcastMembers();
        break;
      }
      case "set_color": {
        const color = typeof d.color === "string" && PALETTE.includes(d.color) ? d.color : null;
        if (!color) return;
        this.sql.exec("UPDATE members SET color = ? WHERE name = ?", color, name);
        this.updateAttachments(name, { color });
        this.broadcastMembers();
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
      this.sql.exec("UPDATE members SET last_seen_ts = ? WHERE name = ?", Date.now(), att.name);
      if (!att.lite) {
        this.broadcast({ type: "typing", name: att.name, on: false }, ws);
        this.broadcast({ type: "system", text: att.name + " خارج شد" }, ws);
      }
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

// ---------------- Web client v3 (served at /) ----------------
// The client below is generated from worker/client.html by tools/sync-client.mjs.
// __WEB_HTML_START__
const WEB_HTML = `<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, viewport-fit=cover">
<meta name="theme-color" content="#212121">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<title>چت پارسا</title>
<style>
:root {
  --bg:#212121; --bg2:#181818; --panel:#212121; --panel2:#181818; --hover:#2c2c2c;
  --border:#303030; --text:#ffffff; --muted:#aaaaaa; --faint:#7a7a7a;
  --accent:#8774e1; --accent2:#6c5dd3; --accent-text:#fff;
  --mine:#8774e1; --mine-text:#fff; --mine-meta:rgba(255,255,255,.7); --mine-link:#fff;
  --other:#212121; --other-text:#fff; --other-meta:#8a8a8a; --link:#8774e1;
  --chatbg:#0f0f0f; --chip:#2c2c2c; --ok:#4fae4e; --bad:#ff595a; --warn:#ffb224;
  --unread:#8774e1; --shadow:0 4px 18px rgba(0,0,0,.45); --radius:12px;
  --wall-opacity:.05;
}
body.light {
  --bg:#ffffff; --bg2:#f4f4f5; --panel:#ffffff; --panel2:#f4f4f5; --hover:#f0f0f0;
  --border:#e4e4e6; --text:#000000; --muted:#707579; --faint:#9e9e9e;
  --accent:#3390ec; --accent2:#2b7fd4; --accent-text:#fff;
  --mine:#eeffde; --mine-text:#000; --mine-meta:#5fae4e; --mine-link:#006c3f;
  --other:#ffffff; --other-text:#000; --other-meta:#a0acb6; --link:#3390ec;
  --chatbg:#d9e2ec; --chip:#f0f0f0; --ok:#4fae4e; --bad:#df3f40; --warn:#e8a13c;
  --unread:#3390ec; --shadow:0 4px 18px rgba(0,0,0,.12);
  --wall-opacity:.07;
}
* { box-sizing:border-box; margin:0; padding:0; -webkit-tap-highlight-color:transparent; }
html, body { height:100%; }
body { font-family:Tahoma,"Segoe UI",Arial,sans-serif; color:var(--text); background:var(--bg2); height:100dvh; overflow:hidden; }
svg.ic { width:21px; height:21px; flex:none; stroke:currentColor; fill:none; stroke-width:2; stroke-linecap:round; stroke-linejoin:round; vertical-align:middle; }
button { font-family:inherit; cursor:pointer; border:none; background:none; color:inherit; }
.iconbtn { width:40px; height:40px; border-radius:50%; color:var(--muted); display:inline-flex; align-items:center; justify-content:center; transition:background .15s; flex:none; font-size:16px; }
.iconbtn:hover { background:var(--hover); }
.iconbtn.small { width:28px; height:28px; font-size:13px; }
input, textarea { font-family:inherit; font-size:14.5px; border-radius:10px; border:1px solid var(--border); background:var(--panel2); color:var(--text); padding:10px 12px; outline:none; transition:border-color .15s; }
input:focus, textarea:focus { border-color:var(--accent); }
.primary { background:var(--accent); color:var(--accent-text); font-weight:bold; border-radius:10px; padding:10px 18px; font-size:14px; }
.primary:hover { background:var(--accent2); }
.ghostbtn { border:1px solid var(--border); color:var(--text); border-radius:10px; padding:9px 14px; font-size:13.5px; }
.danger { color:var(--bad) !important; }
.spacer { flex:1; }
::-webkit-scrollbar { width:6px; height:6px; }
::-webkit-scrollbar-thumb { background:rgba(128,128,128,.4); border-radius:99px; }

#app { display:flex; height:100%; width:100%; max-width:1400px; margin:0 auto; background:var(--bg); }

/* ---------- sidebar (chat list) ---------- */
#sidebar { width:340px; flex:none; display:flex; flex-direction:column; background:var(--bg); border-left:1px solid var(--border); min-height:0; }
.sb-head { display:flex; align-items:center; gap:6px; padding:8px 10px; }
.sb-title { font-size:17px; font-weight:bold; flex:1; padding-right:4px; }
.sb-search { padding:0 10px 8px; }
.sb-search input { width:100%; border-radius:999px; background:var(--panel2); padding:8px 16px; }
#roomlist { flex:1; overflow-y:auto; padding:0 6px; }
.ritem { display:flex; gap:10px; align-items:center; padding:9px 8px; border-radius:12px; cursor:pointer; position:relative; }
.ritem:hover { background:var(--hover); }
.ritem.active { background:var(--accent); color:#fff; }
.ritem.active .rprev, .ritem.active .rtime { color:rgba(255,255,255,.8); }
.ritem.active .rbadge { background:#fff; color:var(--accent); }
.avatar { width:48px; height:48px; border-radius:50%; flex:none; display:flex; align-items:center; justify-content:center; color:#fff; font-weight:bold; font-size:19px; user-select:none; }
.avatar.sm { width:36px; height:36px; font-size:15px; }
.avatar.xs { width:30px; height:30px; font-size:13px; }
.rbody { flex:1; min-width:0; }
.rtop { display:flex; align-items:baseline; gap:6px; }
.rname { font-weight:bold; font-size:14.5px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; flex:1; }
.rtime { font-size:11.5px; color:var(--muted); flex:none; }
.rbottom { display:flex; align-items:center; gap:6px; margin-top:2px; }
.rprev { font-size:13px; color:var(--muted); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; flex:1; }
.rprev .draft { color:var(--bad); }
.rbadge { background:var(--unread); color:#fff; font-size:11.5px; font-weight:bold; border-radius:999px; min-width:20px; height:20px; padding:0 6px; display:flex; align-items:center; justify-content:center; flex:none; }
.newroom { margin:8px 12px 12px; padding:11px; border-radius:12px; background:var(--accent); color:#fff; font-weight:bold; font-size:14px; }
.newroom:hover { background:var(--accent2); }
.sb-empty { text-align:center; color:var(--muted); font-size:13px; padding:30px 16px; line-height:2; }
#sbbackdrop { display:none; }

/* ---------- main column ---------- */
#main { flex:1; display:flex; flex-direction:column; min-width:0; min-height:0; position:relative; }
#hdr { display:flex; align-items:center; gap:10px; padding:7px 12px; background:var(--panel); border-bottom:1px solid var(--border); cursor:pointer; z-index:5; }
#hdr .avatar { width:42px; height:42px; font-size:17px; }
#hdr-info { min-width:0; }
#hdr-title { font-size:15.5px; font-weight:bold; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
#hdr-sub { font-size:12.5px; color:var(--muted); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
#hdr-sub .on { color:var(--accent); }
#hdr-sub.typing { color:var(--accent); }
.only-mobile { display:none; }

#connbanner { display:none; background:var(--warn); color:#000; font-size:12.5px; padding:5px 12px; align-items:center; justify-content:center; gap:8px; }
#connbanner.show { display:flex; }
.spinner { width:13px; height:13px; border:2px solid rgba(0,0,0,.25); border-top-color:#000; border-radius:50%; animation:spin .7s linear infinite; flex:none; }
@keyframes spin { to { transform:rotate(360deg); } }

#pinbar { display:none; background:var(--panel); border-bottom:1px solid var(--border); padding:6px 12px; align-items:center; gap:10px; cursor:pointer; }
#pinbar.show { display:flex; }
.pin-ic { font-size:16px; }
.pin-txt { flex:1; min-width:0; border-right:2px solid var(--accent); padding-right:8px; }
.pin-txt b { display:block; font-size:12px; color:var(--accent); }
.pin-txt span { display:block; font-size:13px; color:var(--muted); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }

#searchbar { display:none; background:var(--panel); border-bottom:1px solid var(--border); padding:8px 12px; gap:8px; }
#searchbar.show { display:flex; }
#searchbar input { flex:1; }
#searchresults { display:none; max-height:200px; overflow-y:auto; background:var(--panel); border-bottom:1px solid var(--border); padding:4px 10px; font-size:13px; }
#searchresults.show { display:block; }
.sres { padding:8px 6px; border-bottom:1px solid var(--border); cursor:pointer; border-radius:8px; }
.sres:hover { background:var(--hover); }
.sres b { color:var(--accent); }

/* ---------- chat area + wallpaper ---------- */
#logwrap { flex:1; position:relative; display:flex; flex-direction:column; min-height:0; background:var(--chatbg); }
#logwrap::before { content:""; position:absolute; inset:0; pointer-events:none; opacity:var(--wall-opacity);
  background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='140' height='140' viewBox='0 0 140 140'%3E%3Cg fill='%23888' fill-rule='evenodd'%3E%3Ccircle cx='20' cy='22' r='7'/%3E%3Cpath d='M96 14l6 12 13 2-9 9 2 13-12-6-12 6 2-13-9-9 13-2z'/%3E%3Cpath d='M30 92c-6-8 2-18 10-12 8-6 16 4 10 12l-10 10z'/%3E%3Crect x='90' y='84' width='16' height='16' rx='4'/%3E%3Ccircle cx='64' cy='58' r='4'/%3E%3Cpath d='M118 118l8 14h-16z'/%3E%3Ccircle cx='128' cy='52' r='5'/%3E%3C/g%3E%3C/svg%3E");
}
body.nowall #logwrap::before { display:none; }
#log { flex:1; overflow-y:auto; padding:12px 14px 10px; display:flex; flex-direction:column; gap:2px; position:relative; z-index:1; }
#welcome { position:absolute; inset:0; display:none; align-items:center; justify-content:center; flex-direction:column; text-align:center; color:var(--muted); z-index:1; }
#welcome.show { display:flex; }
#welcome .big { font-size:56px; margin-bottom:12px; }
#welcome p { font-size:14px; line-height:2; background:rgba(0,0,0,.25); color:#fff; padding:10px 18px; border-radius:14px; }
body.light #welcome p { background:rgba(0,0,0,.45); }

.datechip { align-self:center; background:rgba(0,0,0,.35); color:#fff; font-size:12px; padding:3px 12px; border-radius:999px; margin:8px 0 6px; position:sticky; top:4px; z-index:2; }
body.light .datechip { background:rgba(90,120,150,.55); }
.unreaddv { align-self:stretch; text-align:center; background:rgba(0,0,0,.25); color:#fff; font-size:12px; padding:4px; margin:6px -14px; }
body.light .unreaddv { background:rgba(90,120,150,.35); }
.sysmsg { align-self:center; color:var(--muted); font-size:12px; background:rgba(0,0,0,.2); padding:2px 12px; border-radius:999px; margin:3px 0; }
body.light .sysmsg { background:rgba(255,255,255,.55); }

/* ---------- bubbles ---------- */
.row { display:flex; gap:6px; align-items:flex-end; max-width:82%; animation:msgIn .18s ease both; position:relative; }
@keyframes msgIn { from { opacity:0; transform:translateY(6px); } to { opacity:1; transform:none; } }
.row.mine { align-self:flex-end; flex-direction:row-reverse; }
.row.other { align-self:flex-start; }
.row.grouped { margin-top:-1px; }
.row.grouped .avatar { visibility:hidden; }
.row .avatar { width:32px; height:32px; font-size:13px; margin-bottom:2px; }
.msg { padding:6px 10px 5px; border-radius:var(--radius); line-height:1.7; font-size:14.5px; word-wrap:break-word; white-space:pre-wrap; position:relative; min-width:70px; max-width:100%; box-shadow:0 1px 2px rgba(0,0,0,.18); }
.msg.other { background:var(--other); color:var(--other-text); border-bottom-right-radius:4px; }
.msg.mine { background:var(--mine); color:var(--mine-text); border-bottom-left-radius:4px; }
.row.tail .msg.other { border-bottom-right-radius:0; }
.row.tail .msg.mine { border-bottom-left-radius:0; }
.row.tail .msg.other::after { content:""; position:absolute; bottom:0; right:-7px; border:8px solid transparent; border-bottom-color:var(--other); border-left-color:var(--other); border-top:0; border-right:0; }
.row.tail .msg.mine::after { content:""; position:absolute; bottom:0; left:-7px; border:8px solid transparent; border-bottom-color:var(--mine); border-right-color:var(--mine); border-top:0; border-left:0; }
.msg .who { font-size:12.5px; font-weight:bold; margin-bottom:1px; cursor:pointer; }
.msg a { color:var(--link); }
.msg.mine a { color:var(--mine-link); text-decoration:underline; }
.msg .mention { color:var(--accent); font-weight:bold; background:rgba(135,116,225,.12); border-radius:4px; padding:0 2px; }
.msg.mine .mention { color:inherit; background:rgba(255,255,255,.18); }
body.light .msg.mine .mention { background:rgba(51,144,236,.14); color:#006c3f; }
.meta { font-size:11px; color:var(--other-meta); float:left; margin:6px 0 -2px 8px; display:inline-flex; gap:3px; align-items:center; user-select:none; direction:ltr; }
.msg.mine .meta { color:var(--mine-meta); }
.meta .ticks { letter-spacing:-2px; font-size:12px; }
.meta .ticks.read { color:#5fd068; }
.msg.mine .meta .ticks.read { color:#fff; }
body.light .msg.mine .meta .ticks.read { color:#4fae4e; }
.msg .fwd { font-size:12px; color:var(--accent); margin-bottom:2px; }
.msg.mine .fwd { color:inherit; opacity:.85; }
.msg .fwd b { font-weight:bold; }
.replyq { border-right:2px solid var(--accent); background:rgba(135,116,225,.1); padding:2px 8px; border-radius:6px; margin-bottom:3px; font-size:12.5px; cursor:pointer; }
.msg.mine .replyq { border-color:currentColor; background:rgba(255,255,255,.14); }
body.light .msg.mine .replyq { background:rgba(79,174,78,.12); border-color:#4fae4e; }
.replyq b { display:block; color:var(--accent); font-size:12px; }
.msg.mine .replyq b { color:inherit; }
body.light .msg.mine .replyq b { color:#006c3f; }
.replyq span { color:var(--muted); display:block; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:260px; }
.msg.mine .replyq span { color:inherit; opacity:.8; }
.msg.jumbo { background:none !important; box-shadow:none; font-size:46px; line-height:1.3; padding:2px 6px; }
.msg.jumbo::after { display:none; }
.msg.jumbo .meta { float:none; display:flex; justify-content:flex-end; background:rgba(0,0,0,.3); border-radius:999px; padding:1px 8px; width:max-content; margin:2px 0 0 auto; color:#fff; }
.msg.deleted { font-style:italic; color:var(--muted); background:var(--other); opacity:.7; }
.msg img.photo { max-width:280px; max-height:320px; border-radius:8px; display:block; cursor:pointer; margin:2px 0; }
.msg .caption { margin-top:2px; }
.flash { animation:flash 1.2s ease; }
@keyframes flash { 0%,60% { background:rgba(135,116,225,.35); } 100% { background:transparent; } }

/* voice */
.voice { display:flex; align-items:center; gap:8px; min-width:220px; padding:2px 0; }
.vplay { width:40px; height:40px; border-radius:50%; background:var(--accent); color:#fff; display:flex; align-items:center; justify-content:center; flex:none; font-size:15px; }
.msg.mine .vplay { background:rgba(255,255,255,.25); }
body.light .msg.mine .vplay { background:#4fae4e; }
.vwave { flex:1; display:flex; align-items:center; gap:2px; height:28px; }
.vwave i { flex:1; background:var(--accent); opacity:.35; border-radius:2px; min-width:2px; }
.vwave i.on { opacity:1; }
.msg.mine .vwave i { background:#fff; }
body.light .msg.mine .vwave i { background:#4fae4e; }
.vinfo { display:flex; gap:6px; font-size:11.5px; color:var(--other-meta); margin-top:1px; }
.msg.mine .vinfo { color:var(--mine-meta); }
.vspeed { font-weight:bold; cursor:pointer; background:rgba(128,128,128,.2); border-radius:6px; padding:0 5px; }

/* file */
.filecard { display:flex; align-items:center; gap:10px; text-decoration:none !important; color:inherit !important; padding:4px 0; cursor:pointer; }
.filecard .fic { width:42px; height:42px; border-radius:50%; background:var(--accent); color:#fff; display:flex; align-items:center; justify-content:center; font-size:19px; flex:none; }
.msg.mine .filecard .fic { background:rgba(255,255,255,.25); }
body.light .msg.mine .filecard .fic { background:#4fae4e; }
.fname { font-weight:bold; font-size:13.5px; max-width:220px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.fsub { font-size:11.5px; color:var(--other-meta); }
.msg.mine .fsub { color:var(--mine-meta); }

/* poll */
.poll { min-width:250px; max-width:320px; }
.poll .pq { font-weight:bold; font-size:14.5px; margin-bottom:2px; }
.poll .ptype { font-size:11.5px; color:var(--other-meta); margin-bottom:6px; }
.msg.mine .poll .ptype { color:var(--mine-meta); }
.popt { display:block; width:100%; text-align:right; padding:5px 0; position:relative; }
.popt .ptxt { display:flex; justify-content:space-between; gap:8px; font-size:13.5px; margin-bottom:3px; }
.popt .pct { font-weight:bold; font-size:12px; flex:none; }
.popt .pbar { height:5px; border-radius:3px; background:rgba(128,128,128,.25); overflow:hidden; }
.popt .pbar i { display:block; height:100%; background:var(--accent); border-radius:3px; transition:width .3s; }
.msg.mine .popt .pbar i { background:#fff; }
body.light .msg.mine .popt .pbar i { background:#4fae4e; }
.popt.voted .ptxt::before { content:"✓ "; color:var(--accent); font-weight:bold; }
.msg.mine .popt.voted .ptxt::before { color:#fff; }
body.light .msg.mine .popt.voted .ptxt::before { color:#4fae4e; }
.pfoot { font-size:11.5px; color:var(--other-meta); margin-top:5px; display:flex; justify-content:space-between; align-items:center; }
.msg.mine .pfoot { color:var(--mine-meta); }
.pclose { font-size:11.5px; text-decoration:underline; cursor:pointer; }

/* reactions */
.reacts { display:flex; gap:4px; flex-wrap:wrap; margin-top:3px; }
.rchip { background:rgba(135,116,225,.16); border-radius:999px; padding:1px 8px; font-size:12.5px; cursor:pointer; display:inline-flex; gap:4px; align-items:center; transition:transform .1s; }
.rchip:active { transform:scale(.9); }
.rchip.mine { background:var(--accent); color:#fff; }
body.light .msg.mine .rchip { background:rgba(79,174,78,.2); color:#006c3f; }
body.light .msg.mine .rchip.mine { background:#4fae4e; color:#fff; }
.msg.mine .rchip { background:rgba(255,255,255,.2); }
.msg.mine .rchip.mine { background:rgba(255,255,255,.45); }
.heartpop { position:absolute; left:50%; top:40%; font-size:40px; pointer-events:none; animation:heartpop .7s ease both; z-index:3; }
@keyframes heartpop { 0% { transform:translate(-50%,-50%) scale(.3); opacity:0; } 30% { opacity:1; transform:translate(-50%,-50%) scale(1.25); } 100% { transform:translate(-50%,-90%) scale(1); opacity:0; } }

#fab { position:absolute; bottom:14px; left:14px; width:46px; height:46px; border-radius:50%; background:var(--panel); color:var(--muted); box-shadow:var(--shadow); display:none; align-items:center; justify-content:center; z-index:4; }
#fab.show { display:flex; }
#fab-badge { position:absolute; top:-5px; right:-3px; background:var(--unread); color:#fff; font-size:11px; font-weight:bold; border-radius:999px; min-width:19px; height:19px; padding:0 5px; display:none; align-items:center; justify-content:center; }
#fab-badge.show { display:flex; }

/* ---------- input ---------- */
#replybar { display:none; background:var(--panel); padding:6px 14px; align-items:center; gap:8px; border-top:1px solid var(--border); }
#replybar.show { display:flex; }
.rb-txt { flex:1; min-width:0; border-right:2px solid var(--accent); padding-right:8px; }
.rb-txt b { display:block; font-size:12.5px; color:var(--accent); }
.rb-txt span { display:block; font-size:12.5px; color:var(--muted); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
#recbar { display:none; background:var(--panel); padding:8px 14px; align-items:center; gap:10px; border-top:1px solid var(--border); }
#recbar.show { display:flex; }
.rec-dot { width:11px; height:11px; border-radius:50%; background:var(--bad); animation:pulse 1s infinite; flex:none; }
@keyframes pulse { 50% { opacity:.4; } }
#rec-time { font-size:14px; font-variant-numeric:tabular-nums; direction:ltr; }
#rec-wave { flex:1; display:flex; align-items:center; gap:2px; height:26px; }
#rec-wave i { flex:1; background:var(--accent); border-radius:2px; min-height:3px; transition:height .08s; }
#inputbar { display:flex; align-items:flex-end; gap:4px; padding:7px 8px; background:var(--panel); border-top:1px solid var(--border); }
#inputwrap { flex:1; }
#input { width:100%; resize:none; border:none; background:transparent; max-height:140px; padding:10px 6px; font-size:15px; }
#input:focus { border:none; }
.sendbtn { width:46px; height:46px; border-radius:50%; background:var(--accent); color:#fff; display:flex; align-items:center; justify-content:center; flex:none; transition:transform .12s, background .15s; }
.sendbtn:hover { background:var(--accent2); }
.sendbtn:active { transform:scale(.9); }
.sendbtn.rec { background:var(--bad); animation:pulse 1s infinite; }

/* ---------- menus / overlays ---------- */
.menu { position:fixed; display:none; background:var(--panel); border:1px solid var(--border); border-radius:12px; box-shadow:var(--shadow); padding:5px; z-index:60; min-width:190px; }
.menu.show { display:block; }
.menu button { display:block; width:100%; text-align:right; padding:9px 12px; border-radius:8px; font-size:13.5px; color:var(--text); }
.menu button:hover { background:var(--hover); }
#ctxmenu { position:fixed; display:none; background:var(--panel); border:1px solid var(--border); border-radius:14px; box-shadow:var(--shadow); z-index:70; min-width:200px; overflow:hidden; }
#ctxmenu.show { display:block; }
#ctx-reacts { display:flex; gap:2px; padding:6px 8px; border-bottom:1px solid var(--border); }
#ctx-reacts button { font-size:21px; padding:3px 5px; border-radius:8px; transition:transform .1s; }
#ctx-reacts button:hover { background:var(--hover); transform:scale(1.2); }
#ctx-items { padding:5px; }
#ctx-items button { display:block; width:100%; text-align:right; padding:8px 12px; border-radius:8px; font-size:13.5px; color:var(--text); }
#ctx-items button:hover { background:var(--hover); }
#emojipanel { position:absolute; bottom:64px; right:10px; width:330px; max-width:calc(100vw - 20px); height:300px; background:var(--panel); border:1px solid var(--border); border-radius:14px; box-shadow:var(--shadow); display:none; flex-direction:column; z-index:50; }
#emojipanel.show { display:flex; }
#emoji-tabs { display:flex; gap:2px; padding:6px; border-bottom:1px solid var(--border); overflow-x:auto; }
#emoji-tabs button { font-size:17px; padding:4px 8px; border-radius:8px; flex:none; }
#emoji-tabs button.active { background:var(--hover); }
#emoji-grid { flex:1; overflow-y:auto; display:grid; grid-template-columns:repeat(8,1fr); gap:2px; padding:8px; align-content:start; }
#emoji-grid button { font-size:22px; padding:4px; border-radius:8px; }
#emoji-grid button:hover { background:var(--hover); }
#lightbox { position:fixed; inset:0; background:rgba(0,0,0,.93); display:none; flex-direction:column; z-index:90; }
#lightbox.show { display:flex; }
.lb-bar { display:flex; justify-content:space-between; padding:10px 14px; color:#fff; }
.lb-bar .iconbtn { color:#fff; }
#lb-stage { flex:1; display:flex; align-items:center; justify-content:center; overflow:hidden; }
#lb-img { max-width:96%; max-height:100%; transition:transform .15s; cursor:zoom-in; }
#lb-cap { color:#ccc; text-align:center; padding:10px; font-size:13.5px; max-height:70px; overflow-y:auto; }
#dlgbackdrop { position:fixed; inset:0; background:rgba(0,0,0,.5); display:none; align-items:center; justify-content:center; z-index:80; padding:16px; }
#dlgbackdrop.show { display:flex; }
#dialog { background:var(--panel); border-radius:16px; box-shadow:var(--shadow); width:400px; max-width:100%; max-height:86dvh; overflow-y:auto; padding:18px; animation:dlgIn .16s ease both; }
@keyframes dlgIn { from { opacity:0; transform:scale(.95); } to { opacity:1; transform:none; } }
#dialog h3 { font-size:16px; margin-bottom:14px; }
#dialog .frow { margin-bottom:10px; }
#dialog label { display:block; font-size:12.5px; color:var(--muted); margin-bottom:4px; }
#dialog input[type=text], #dialog input[type=password], #dialog textarea { width:100%; }
#dialog .btnrow { display:flex; gap:8px; justify-content:flex-start; margin-top:14px; }
.mrow { display:flex; align-items:center; gap:10px; padding:7px 4px; border-radius:10px; }
.mrow:hover { background:var(--hover); }
.mbody { flex:1; min-width:0; }
.mname { font-size:14px; font-weight:bold; }
.mname .role { font-size:11px; color:var(--warn); font-weight:normal; margin-right:4px; }
.msub { font-size:11.5px; color:var(--muted); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.msub .on { color:var(--ok); }
.mact { display:flex; gap:2px; }
.toggle { display:flex; align-items:center; justify-content:space-between; padding:9px 2px; font-size:14px; cursor:pointer; }
.toggle .sw { width:40px; height:22px; border-radius:999px; background:var(--border); position:relative; transition:background .2s; flex:none; }
.toggle .sw::after { content:""; position:absolute; top:2px; right:2px; width:18px; height:18px; border-radius:50%; background:#fff; transition:transform .2s; }
.toggle.on .sw { background:var(--accent); }
.toggle.on .sw::after { transform:translateX(-18px); }
.colors { display:flex; gap:8px; flex-wrap:wrap; padding:4px 0; }
.colors button { width:32px; height:32px; border-radius:50%; border:3px solid transparent; }
.colors button.sel { border-color:var(--text); }
.poll-opt-row { display:flex; gap:6px; margin-bottom:6px; }
.poll-opt-row input { flex:1; }
#toast { position:fixed; bottom:80px; left:50%; transform:translateX(-50%) translateY(20px); background:rgba(0,0,0,.85); color:#fff; padding:9px 18px; border-radius:999px; font-size:13px; opacity:0; pointer-events:none; transition:opacity .2s, transform .2s; z-index:100; max-width:90vw; text-align:center; }
#toast.show { opacity:1; transform:translateX(-50%) translateY(0); }

/* ---------- responsive ---------- */
@media (max-width:900px) {
  #sidebar { position:fixed; top:0; bottom:0; right:0; width:320px; max-width:88vw; z-index:75; transform:translateX(105%); transition:transform .22s ease; box-shadow:var(--shadow); }
  #sidebar.open { transform:none; }
  #sbbackdrop { display:none; position:fixed; inset:0; background:rgba(0,0,0,.45); z-index:74; }
  #sbbackdrop.show { display:block; }
  .only-mobile { display:inline-flex; }
  .row { max-width:92%; }
  .msg img.photo { max-width:230px; }
}
@media (min-width:901px) { #btn-drawer { display:none; } }
</style>
</head>
<body>
<div id="app">
  <aside id="sidebar">
    <div class="sb-head">
      <button id="btn-settings" class="iconbtn" title="تنظیمات"><svg class="ic" viewBox="0 0 24 24"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 1 1-4 0v-.09a1.65 1.65 0 0 0-1-1.51 1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 1 1 0-4h.09a1.65 1.65 0 0 0 1.51-1 1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33h.01a1.65 1.65 0 0 0 1-1.51V3a2 2 0 1 1 4 0v.09a1.65 1.65 0 0 0 1 1.51h.01a1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82v.01a1.65 1.65 0 0 0 1.51 1H21a2 2 0 1 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg></button>
      <div class="sb-title">چت پارسا</div>
      <button id="btn-theme" class="iconbtn" title="تغییر تم"><svg class="ic" viewBox="0 0 24 24"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg></button>
    </div>
    <div class="sb-search"><input id="roomfilter" placeholder="جست‌وجوی روم…"></div>
    <div id="roomlist"></div>
    <button id="btn-newroom" class="newroom">＋ روم جدید</button>
  </aside>
  <div id="sbbackdrop"></div>
  <div id="main">
    <header id="hdr">
      <button id="btn-drawer" class="iconbtn only-mobile"><svg class="ic" viewBox="0 0 24 24"><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="18" x2="21" y2="18"/></svg></button>
      <div id="hdr-avatar" class="avatar"></div>
      <div id="hdr-info"><div id="hdr-title">چت پارسا</div><div id="hdr-sub"></div></div>
      <div class="spacer"></div>
      <button id="btn-search" class="iconbtn"><svg class="ic" viewBox="0 0 24 24"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg></button>
      <button id="btn-more" class="iconbtn"><svg class="ic" viewBox="0 0 24 24"><circle cx="12" cy="5" r="1.6"/><circle cx="12" cy="12" r="1.6"/><circle cx="12" cy="19" r="1.6"/></svg></button>
    </header>
    <div id="connbanner"><span class="spinner"></span><span id="connmsg">در حال اتصال…</span></div>
    <div id="pinbar"><span class="pin-ic">📌</span><div class="pin-txt"><b id="pin-count"></b><span id="pin-text"></span></div><button id="pin-close" class="iconbtn small" title="برداشتن پین">✕</button></div>
    <div id="searchbar"><input id="searchinput" placeholder="جست‌وجو در پیام‌ها…"><button id="search-close" class="iconbtn">✕</button></div>
    <div id="searchresults"></div>
    <div id="logwrap">
      <div id="log"></div>
      <div id="welcome"><div class="big">💬</div><p>یک روم از لیست انتخاب کن<br>یا یک روم جدید بساز!</p></div>
      <button id="fab"><svg class="ic" viewBox="0 0 24 24"><polyline points="6 9 12 15 18 9"/></svg><span id="fab-badge"></span></button>
    </div>
    <div id="replybar"><div class="rb-txt"><b id="replylabel"></b><span id="replytxt"></span></div><button id="reply-cancel" class="iconbtn">✕</button></div>
    <div id="recbar"><span class="rec-dot"></span><span id="rec-time">0:00</span><div id="rec-wave"></div><button id="rec-cancel" class="ghostbtn">لغو</button><button id="rec-send" class="primary">ارسال</button></div>
    <div id="inputbar">
      <button id="btn-attach" class="iconbtn"><svg class="ic" viewBox="0 0 24 24"><path d="M21.44 11.05l-9.19 9.19a6 6 0 0 1-8.49-8.49l9.19-9.19a4 4 0 0 1 5.66 5.66l-9.2 9.19a2 2 0 0 1-2.83-2.83l8.49-8.48"/></svg></button>
      <div id="inputwrap"><textarea id="input" rows="1" placeholder="پیام…"></textarea></div>
      <button id="btn-emoji" class="iconbtn"><svg class="ic" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><path d="M8 14s1.5 2 4 2 4-2 4-2"/><line x1="9" y1="9" x2="9.01" y2="9"/><line x1="15" y1="9" x2="15.01" y2="9"/></svg></button>
      <button id="btn-send" class="sendbtn"><svg id="ic-mic" class="ic" viewBox="0 0 24 24"><path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2"/><line x1="12" y1="19" x2="12" y2="23"/></svg><svg id="ic-send" class="ic" viewBox="0 0 24 24" style="display:none"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg></button>
    </div>
  </div>
</div>
<div id="attachmenu" class="menu">
  <button data-act="photo">🖼️ عکس</button>
  <button data-act="file">📎 فایل</button>
  <button data-act="poll">📊 نظرسنجی</button>
</div>
<div id="moremenu" class="menu">
  <button data-act="info">👥 اطلاعات روم</button>
  <button data-act="pins">📌 پیام‌های پین‌شده</button>
  <button data-act="newroom">＋ روم جدید</button>
  <button data-act="settings">⚙️ تنظیمات</button>
  <button data-act="theme">🌓 تغییر تم</button>
  <button data-act="leave" class="danger">🚪 خروج از روم</button>
</div>
<div id="ctxmenu"><div id="ctx-reacts"></div><div id="ctx-items"></div></div>
<div id="emojipanel"><div id="emoji-tabs"></div><div id="emoji-grid"></div></div>
<div id="lightbox"><div class="lb-bar"><button id="lb-dl" class="iconbtn" title="دانلود"><svg class="ic" viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg></button><button id="lb-close" class="iconbtn">✕</button></div><div id="lb-stage"><img id="lb-img" alt=""></div><div id="lb-cap"></div></div>
<div id="dlgbackdrop"><div id="dialog"></div></div>
<div id="toast"></div>
<input type="file" id="imginput" accept="image/*" style="display:none">
<input type="file" id="fileinput" style="display:none">
<script>
(function () {
"use strict";
var $ = function (id) { return document.getElementById(id); };
var LS = localStorage;
var VERSION = "3.0.0";
var PALETTE = ["#e5484d","#f76b15","#ffb224","#46a758","#12a594","#0090ff","#6e56cf","#d6409f","#8e4ec6","#30a46c"];
var QUICK_REACTS = ["❤️","👍","🔥","😂","😮","😢","🙏","🎉"];

// ---------- settings / state ----------
var myName = LS.getItem("pc_name") || "";
var theme = LS.getItem("pc_theme") || "dark";
var soundOn = LS.getItem("pc_sound") !== "0";
var notifOn = LS.getItem("pc_notif") !== "0";
var wallOn = LS.getItem("pc_wall") !== "0";
var rooms = [];
try { rooms = JSON.parse(LS.getItem("pc_rooms") || "[]"); } catch (e) { rooms = []; }
var activeRoom = null, ws = null, wsRetry = 1000, wsClosedByUs = false, reconnectTimer = null;
var lites = {}; // room -> {ws, retry, timer, closedByUs}
var messagesById = {}, membersByName = {}, onlineSet = {}, typingUsers = {};
var pinned = [], pinIdx = 0, topic = "", hasPassword = false;
var me = { name: "", color: "#0090ff", role: "member", bio: "" };
var replyToId = null, editId = null, oldestId = null, loadingOlder = false, pendingJump = null;
var lastMsgId = 0, lastSeenSent = 0, unreadActive = 0, lastDateKey = "", lastRowSender = "", lastRowTs = 0, lastTailRow = null;
var ctxId = null, draftTimer = null, typingTimer = null, lastTypingSent = 0;
var audioCtx = null;

function saveRooms() { LS.setItem("pc_rooms", JSON.stringify(rooms.slice(0, 30))); }
function roomEntry(name) { for (var i = 0; i < rooms.length; i++) if (rooms[i].name === name) return rooms[i]; return null; }
function esc(s) { return String(s == null ? "" : s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;"); }
function colorFor(name) { var h = 0; for (var i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) >>> 0; return PALETTE[h % PALETTE.length]; }
function memberColor(name) { return membersByName[name] ? membersByName[name].color : colorFor(name); }
function initial(name) { return (name || "?").trim().charAt(0).toUpperCase(); }
function avatarHtml(name, cls, color) { return '<div class="avatar ' + (cls || "") + '" style="background:' + (color || memberColor(name)) + '">' + esc(initial(name)) + "</div>"; }
function bigAvatarHtml(name, px, color) { return '<div class="avatar" style="width:' + px + "px;height:" + px + "px;font-size:" + Math.round(px * 0.42) + 'px;margin:0 auto 10px;background:' + (color || memberColor(name)) + '">' + esc(initial(name)) + "</div>"; }
function fmtTime(ts) { try { return new Date(ts).toLocaleTimeString("fa-IR", { hour: "2-digit", minute: "2-digit" }); } catch (e) { return ""; } }
function dateKey(ts) { var d = new Date(ts); return d.getFullYear() + "/" + (d.getMonth() + 1) + "/" + d.getDate(); }
function fmtDate(ts) { try { return new Date(ts).toLocaleDateString("fa-IR", { day: "numeric", month: "long", year: "numeric" }); } catch (e) { return dateKey(ts); } }
function fmtListTime(ts) { if (!ts) return ""; var k = dateKey(ts), now = dateKey(Date.now()); if (k === now) return fmtTime(ts); try { return new Date(ts).toLocaleDateString("fa-IR", { day: "numeric", month: "short" }); } catch (e) { return k; } }
function fmtLastSeen(ts) {
  if (!ts) return "آفلاین";
  var diff = Date.now() - ts;
  if (diff < 60000) return "به‌تازگی آنلاین بود";
  if (diff < 3600000) return Math.floor(diff / 60000) + " دقیقه پیش";
  if (dateKey(ts) === dateKey(Date.now())) return "امروز " + fmtTime(ts);
  return "آخرین بازدید " + fmtListTime(ts);
}
function fmtSize(b64len) { var b = Math.floor(b64len * 3 / 4); if (b < 1024) return b + " B"; if (b < 1048576) return (b / 1024).toFixed(1) + " KB"; return (b / 1048576).toFixed(2) + " MB"; }
function fmtDur(sec) { sec = Math.max(0, Math.round(sec || 0)); return Math.floor(sec / 60) + ":" + ("0" + (sec % 60)).slice(-2); }
function kindLabel(m) {
  if (!m) return "پیام";
  if (m.deleted) return "پیام حذف‌شده";
  if (m.kind === "image") return "🖼️ عکس";
  if (m.kind === "voice") return "🎤 پیام صوتی";
  if (m.kind === "file") return "📎 " + (m.text || "فایل");
  if (m.kind === "poll") return "📊 " + (m.poll ? m.poll.q : m.text);
  return m.text || "";
}
function snippet(m, len) { var t = kindLabel(m).replace(/\\n/g, " "); return t.length > (len || 60) ? t.slice(0, len || 60) + "…" : t; }

function applyTheme() {
  document.body.classList.toggle("light", theme === "light");
  document.body.classList.toggle("nowall", !wallOn);
  var tc = document.querySelector('meta[name=theme-color]');
  if (tc) tc.content = theme === "light" ? "#ffffff" : "#212121";
}
function toggleTheme() { theme = theme === "light" ? "dark" : "light"; LS.setItem("pc_theme", theme); applyTheme(); }

var toastTimer = null;
function toast(t) { var el = $("toast"); el.textContent = t; el.classList.add("show"); clearTimeout(toastTimer); toastTimer = setTimeout(function () { el.classList.remove("show"); }, 2600); }

function beep(freq) {
  if (!soundOn) return;
  try {
    audioCtx = audioCtx || new (window.AudioContext || window.webkitAudioContext)();
    var o = audioCtx.createOscillator(), g = audioCtx.createGain();
    o.frequency.value = freq || 880; o.type = "sine";
    g.gain.setValueAtTime(0.08, audioCtx.currentTime);
    g.gain.exponentialRampToValueAtTime(0.0001, audioCtx.currentTime + 0.18);
    o.connect(g); g.connect(audioCtx.destination);
    o.start(); o.stop(audioCtx.currentTime + 0.2);
  } catch (e) {}
}

// ---------- generic dialog ----------
var dlgOpen = false;
function openDialog(html) { $("dialog").innerHTML = html; $("dlgbackdrop").classList.add("show"); dlgOpen = true; }
function closeDialog() { $("dlgbackdrop").classList.remove("show"); dlgOpen = false; $("dialog").innerHTML = ""; }
$("dlgbackdrop").addEventListener("click", function (e) { if (e.target === this) closeDialog(); });

function confirmDialog(title, text, onYes, yesLabel) {
  openDialog("<h3>" + esc(title) + "</h3><p style='font-size:13.5px;color:var(--muted);line-height:1.9'>" + esc(text) + "</p>" +
    "<div class='btnrow'><button class='primary' id='dlg-yes'>" + esc(yesLabel || "تأیید") + "</button><button class='ghostbtn' id='dlg-no'>انصراف</button></div>");
  $("dlg-yes").onclick = function () { closeDialog(); onYes(); };
  $("dlg-no").onclick = closeDialog;
}

function notify(room, m) {
  if (!notifOn || m.name === myName) return;
  if (!document.hidden && room === activeRoom) return;
  if (window.Notification && Notification.permission === "granted") {
    try { new Notification(room + " — " + m.name, { body: m.kind === "text" ? m.text.slice(0, 120) : kindLabel(m) }); } catch (e) {}
  }
}
function askNotifPermission() {
  if (notifOn && window.Notification && Notification.permission === "default") { try { Notification.requestPermission(); } catch (e) {} }
}

// ---------- emoji data ----------
var EMOJI = {
  "😀": ["😀","😃","😄","😁","😆","😅","🤣","😂","🙂","🙃","😉","😊","😇","🥰","😍","🤩","😘","😗","😚","😙","😋","😛","😜","🤪","😝","🤑","🤗","🤭","🤫","🤔","🤐","🤨","😐","😑","😶","😏","😒","🙄","😬","🤥","😌","😔","😪","🤤","😴","😷","🤒","🤕","🤢","🤮","🥵","🥶","😵","🤯","🤠","🥳","😎","🤓","🧐","😕","😟","🙁","😮","😯","😲","😳","🥺","😦","😧","😨","😰","😥","😢","😭","😱","😖","😣","😞","😓","😩","😫","🥱","😤","😡","😠","🤬","😈","👿","💀","💩","🤡","👻","👽","🤖"],
  "👋": ["👋","🤚","🖐️","✋","🖖","👌","🤌","🤏","✌️","🤞","🤟","🤘","🤙","👈","👉","👆","👇","☝️","👍","👎","✊","👊","🤛","🤜","👏","🙌","👐","🤲","🤝","🙏","💪","🦾","✍️","💅","🤳","👀","👁️","👅","👄","💋","🫶","🫰","🫵","🫱","🫲"],
  "❤️": ["❤️","🧡","💛","💚","💙","💜","🖤","🤍","🤎","💔","❣️","💕","💞","💓","💗","💖","💘","💝","💟","♥️","💯","💢","💥","💫","💦","💨","🕳️","💣","💬","💭","💤","✨","⭐","🌟","🔥","🎉","🎊","🎈","🎁","🏆","🥇","🎯","🎮","🎲"],
  "🐱": ["🐱","🐶","🐭","🐹","🐰","🦊","🐻","🐼","🐨","🐯","🦁","🐮","🐷","🐸","🐵","🐔","🐧","🐦","🐤","🦆","🦅","🦉","🦇","🐺","🐗","🐴","🦄","🐝","🐛","🦋","🐌","🐞","🐜","🐢","🐍","🦎","🐙","🦑","🦐","🦀","🐡","🐠","🐟","🐬","🐳","🐋","🦈","🐊","🐅","🐆","🦓","🦍","🐘","🦛","🐪","🐫","🦒","🦘","🐃","🐂","🐄","🐎","🐖","🐏","🐑","🦙","🐐","🦌","🐕","🐩","🦮","🐈","🐓","🦃","🦚","🦜","🦢","🕊️","🐇","🦝","🦨","🦡","🦦","🦥","🐁","🐀","🐿️","🦔","🌵","🌲","🌳","🌴","🌱","🌿","☘️","🍀","🍁","🍂","🍃","🌸","💐","🌷","🌹","🥀","🌺","🌻","🌼"],
  "🍕": ["🍕","🍔","🍟","🌭","🍿","🧂","🥓","🥚","🍳","🧇","🥞","🧈","🍞","🥐","🥨","🥯","🥖","🧀","🥗","🥙","🥪","🌮","🌯","🥫","🍝","🍜","🍲","🍛","🍣","🍱","🥟","🍤","🍙","🍚","🍘","🍥","🥠","🍢","🍡","🍧","🍨","🍦","🥧","🧁","🍰","🎂","🍮","🍭","🍬","🍫","🍩","🍪","🌰","🥜","🍯","🥛","🍼","☕","🍵","🧃","🥤","🍶","🍺","🍻","🥂","🍷","🥃","🍸","🍹","🧉","🍾","🍎","🍏","🍐","🍊","🍋","🍌","🍉","🍇","🍓","🫐","🍈","🍒","🍑","🥭","🍍","🥥","🥝","🍅","🥑","🥦","🥬","🥒","🌶️","🌽","🥕","🧄","🧅","🥔"],
  "⚽": ["⚽","🏀","🏈","⚾","🥎","🎾","🏐","🏉","🥏","🎱","🪀","🏓","🏸","🏒","🏑","🥍","🏏","🥅","⛳","🪁","🏹","🎣","🤿","🥊","🥋","🎽","🛹","🛼","🛷","⛸️","🥌","🎿","⛷️","🏂","🪂","🏋️","🤼","🤸","🤺","⛹️","🤾","🏌️","🏇","🧘","🏄","🏊","🤽","🚣","🧗","🚵","🚴","🏆","🥇","🥈","🥉","🏅","🎖️","🏵️","🎗️","🎫","🎟️","🎪","🎭","🎨","🎬","🎤","🎧","🎼","🎹","🥁","🎷","🎺","🎸","🪕","🎻","🎲","♟️","🎯","🎳","🎮","🕹️","🎰"],
  "💡": ["💡","🔦","🕯️","📱","💻","⌨️","🖥️","🖨️","🖱️","💾","💿","📀","📼","📷","📸","📹","🎥","📽️","🎞️","📞","☎️","📟","📠","📺","📻","🎙️","⏰","⌚","⏳","⌛","📡","🔋","🔌","🪫","🧲","🔧","🔨","⚒️","🛠️","⛏️","🪓","🔩","⚙️","🧰","🧱","⛓️","🧲","🔫","💣","🧨","🔪","🗡️","⚔️","🛡️","🚬","⚰️","⚱️","🏺","🔮","📿","🧿","💈","⚗️","🔭","🔬","🕳️","🩹","🩺","💊","💉","🧬","🦠","🧫","🧪","🌡️","🧹","🧺","🧻","🚽","🚰","🚿","🛁","🛀","🧼","🪥","🪒","🧽","🧴","🛎️","🔑","🗝️","🚪","🪑","🛋️","🛏️","🧸","🖼️","🛍️","🛒","🎁","🎈","🎏","🎀","🪄","🪅","🎊","🎉","✉️","📩","📨","📧","💌","📥","📤","📦","🏷️","📪","📫","📬","📭","📮","📯","📜","📃","📄","📑","🧾","📊","📈","📉","🗒️","🗓️","📆","📅","📇","🗃️","🗳️","🗄️","📋","📁","📂","🗂️","🗞️","📰","📓","📔","📒","📕","📗","📘","📙","📚","📖","🔖","🧷","🔗","📎","🖇️","📐","📏","🧮","📌","📍","✂️","🖊️","🖋️","✒️","🖌️","🖍️","📝","✏️","🔍","🔎","🔏","🔐","🔒","🔓"],
  "🚗": ["🚗","🚕","🚙","🚌","🚎","🏎️","🚓","🚑","🚒","🚐","🛻","🚚","🚛","🚜","🦯","🦽","🦼","🛴","🚲","🛵","🏍️","🛺","🚨","🚔","🚍","🚘","🚖","🚡","🚠","🚟","🚃","🚋","🚞","🚝","🚄","🚅","🚈","🚂","🚆","🚇","🚊","🚉","✈️","🛫","🛬","🛩️","💺","🛰️","🚀","🛸","🚁","🛶","⛵","🚤","🛥️","🛳️","⛴️","🚢","⚓","🪝","⛽","🚧","🚦","🚥","🗺️","🗿","🗽","🗼","🏰","🏯","🏟️","🎡","🎢","🎠","⛲","⛱️","🏖️","🏝️","🏜️","🌋","⛰️","🏔️","🗻","🏕️","⛺","🏠","🏡","🏘️","🏚️","🏗️","🏭","🏢","🏬","🏣","🏤","🏥","🏦","🏨","🏪","🏫","🏩","💒","🏛️","⛪","🕌","🕍","🛕","🕋","⛩️","🌁","🌃","🏙️","🌄","🌅","🌆","🌇","🌉","🌌","🌠","🎇","🎆","🌈","☀️","🌤️","⛅","🌥️","☁️","🌦️","🌧️","⛈️","🌩️","🌨️","❄️","☃️","⛄","🌬️","💨","🌪️","🌫️","🌊","💧","💦","☔","⚡","🔥","🌙","🌛","🌜","🌚","🌝","🌞","⭐","🌟","💫","✨","☄️","🪐","🌍","🌎","🌏"]
};
var recentEmoji = [];
try { recentEmoji = JSON.parse(LS.getItem("pc_emoji_recent") || "[]"); } catch (e) {}
function pushRecent(e) {
  recentEmoji = [e].concat(recentEmoji.filter(function (x) { return x !== e; })).slice(0, 32);
  LS.setItem("pc_emoji_recent", JSON.stringify(recentEmoji));
}
// ---------- sidebar / chat list ----------
function renderRoomList() {
  var filter = $("roomfilter").value.trim();
  var list = rooms.slice().sort(function (a, b) { return (b.last ? b.last.ts : 0) - (a.last ? a.last.ts : 0); });
  var html = "";
  for (var i = 0; i < list.length; i++) {
    var r = list[i];
    if (filter && r.name.indexOf(filter) < 0) continue;
    var prev = "";
    if (r.draft && r.name !== activeRoom) prev = '<span class="draft">پیش‌نویس:</span> ' + esc(r.draft.slice(0, 40));
    else if (r.last) prev = (r.last.name === myName ? "شما: " : esc(r.last.name || "") + ": ") + esc(snippet(r.last, 40));
    else prev = "بدون پیام";
    html += '<div class="ritem' + (r.name === activeRoom ? " active" : "") + '" data-room="' + esc(r.name) + '">' +
      avatarHtml(r.name, "", colorFor(r.name)) +
      '<div class="rbody"><div class="rtop"><span class="rname">' + esc(r.name) + '</span><span class="rtime">' + fmtListTime(r.last ? r.last.ts : 0) + "</span></div>" +
      '<div class="rbottom"><span class="rprev">' + prev + "</span>" +
      (r.unread ? '<span class="rbadge">' + (r.unread > 99 ? "99+" : r.unread) + "</span>" : "") +
      "</div></div></div>";
  }
  $("roomlist").innerHTML = html || '<div class="sb-empty">هنوز رومی نداری.<br>با «روم جدید» شروع کن!</div>';
  var items = $("roomlist").querySelectorAll(".ritem");
  for (var j = 0; j < items.length; j++) {
    items[j].onclick = function () { switchRoom(this.getAttribute("data-room")); closeDrawer(); };
    items[j].oncontextmenu = function (e) { e.preventDefault(); roomContextMenu(this.getAttribute("data-room"), e.clientX, e.clientY); };
  }
}
function roomContextMenu(name, x, y) {
  var menu = $("moremenu");
  menu.innerHTML = '<button data-act="open">💬 باز کردن</button><button data-act="leave" class="danger">🗑️ حذف از لیست</button>';
  showMenuAt(menu, x, y);
  menu.onclick = function (e) {
    var act = e.target.getAttribute && e.target.getAttribute("data-act");
    hideMenus();
    if (act === "open") switchRoom(name);
    if (act === "leave") confirmDialog("حذف روم", "«" + name + "» از لیست چت‌های تو حذف می‌شه. تاریخچه روی سرور می‌مونه.", function () { leaveRoom(name); }, "حذف کن");
  };
}
function updateRoomLast(name, m) {
  var r = roomEntry(name);
  if (!r) return;
  r.last = { name: m.name, text: m.text, kind: m.kind, ts: m.ts, deleted: m.deleted, poll: m.poll ? { q: m.poll.q } : null };
  saveRooms(); renderRoomList();
}
function openDrawer() { $("sidebar").classList.add("open"); $("sbbackdrop").classList.add("show"); }
function closeDrawer() { $("sidebar").classList.remove("open"); $("sbbackdrop").classList.remove("show"); }
$("btn-drawer").onclick = openDrawer;
$("sbbackdrop").onclick = closeDrawer;
$("roomfilter").oninput = renderRoomList;

// ---------- lite connections (live chat list) ----------
function wsUrl(room, name, pass, lite) {
  var proto = location.protocol === "https:" ? "wss://" : "ws://";
  var url = proto + location.host + "/ws?room=" + encodeURIComponent(room) + "&name=" + encodeURIComponent(name);
  if (pass) url += "&pass=" + encodeURIComponent(pass);
  if (lite) url += "&lite=1";
  return url;
}
function closeLite(name) {
  var L = lites[name];
  if (!L) return;
  L.closedByUs = true;
  clearTimeout(L.timer);
  try { L.ws.close(); } catch (e) {}
  delete lites[name];
}
function ensureLites() {
  if (!myName) return;
  var count = 0;
  for (var i = 0; i < rooms.length && count < 8; i++) {
    var name = rooms[i].name;
    if (name === activeRoom || lites[name]) continue;
    openLite(name); count++;
  }
}
function openLite(name) {
  var r = roomEntry(name);
  if (!r) return;
  var L = { ws: null, retry: 2000, timer: null, closedByUs: false };
  lites[name] = L;
  var sock;
  try { sock = new WebSocket(wsUrl(name, myName, r.pass, true)); } catch (e) { delete lites[name]; return; }
  L.ws = sock;
  sock.onmessage = function (ev) {
    var d; try { d = JSON.parse(ev.data); } catch (e) { return; }
    if (d.type === "history") {
      var last = d.messages[d.messages.length - 1];
      if (last && (!r.last || last.ts > r.last.ts)) { r.last = { name: last.name, text: last.text, kind: last.kind, ts: last.ts, deleted: last.deleted }; saveRooms(); renderRoomList(); }
      L.retry = 2000;
    } else if (d.type === "msg") {
      updateRoomLast(name, d.message);
      if (d.message.name !== myName) { r.unread = (r.unread || 0) + 1; saveRooms(); renderRoomList(); notify(name, d.message); beep(700); }
    } else if (d.type === "error" && (d.code === "bad_password" || d.code === "banned")) {
      closeLite(name);
    } else if (d.type === "kicked") { closeLite(name); }
  };
  sock.onclose = function () {
    if (L.closedByUs || !roomEntry(name) || name === activeRoom) { delete lites[name]; return; }
    L.timer = setTimeout(function () { if (lites[name] === L && roomEntry(name)) openLite(name); }, L.retry);
    L.retry = Math.min(L.retry * 2, 60000);
  };
  sock.onerror = function () { try { sock.close(); } catch (e) {} };
}

// ---------- room switching ----------
function saveDraft() {
  if (!activeRoom) return;
  var r = roomEntry(activeRoom);
  if (r) { r.draft = $("input").value; saveRooms(); }
}
function switchRoom(name) {
  if (name === activeRoom && ws && ws.readyState === 1) { closeDrawer(); return; }
  saveDraft();
  closeLite(name);
  wsClosedByUs = true;
  clearTimeout(reconnectTimer);
  if (ws) { try { ws.close(); } catch (e) {} ws = null; }
  activeRoom = name;
  LS.setItem("pc_active", name);
  var r = roomEntry(name);
  if (r) { r.unread = 0; saveRooms(); }
  messagesById = {}; membersByName = {}; onlineSet = {}; typingUsers = {};
  pinned = []; pinIdx = 0; topic = ""; oldestId = null; lastMsgId = 0; lastSeenSent = 0;
  unreadActive = 0; lastDateKey = ""; lastRowSender = ""; lastRowTs = 0; lastTailRow = null;
  replyToId = null; editId = null; pendingJump = null;
  $("replybar").classList.remove("show");
  $("searchbar").classList.remove("show"); $("searchresults").classList.remove("show");
  $("log").innerHTML = "";
  $("welcome").classList.remove("show");
  $("input").value = r && r.draft ? r.draft : "";
  autoGrow(); updateSendBtn();
  $("hdr-title").textContent = name;
  $("hdr-avatar").style.background = colorFor(name);
  $("hdr-avatar").textContent = initial(name);
  $("hdr-sub").textContent = "در حال اتصال…";
  renderRoomList(); renderPinbar();
  wsClosedByUs = false;
  connect();
  ensureLites();
}
function leaveRoom(name) {
  rooms = rooms.filter(function (r) { return r.name !== name; });
  saveRooms();
  closeLite(name);
  if (name === activeRoom) {
    wsClosedByUs = true;
    clearTimeout(reconnectTimer);
    if (ws) { try { ws.close(); } catch (e) {} ws = null; }
    activeRoom = null;
    $("hdr-title").textContent = "چت پارسا";
    $("hdr-sub").textContent = "";
    $("hdr-avatar").textContent = "";
    $("log").innerHTML = "";
    $("welcome").classList.add("show");
    renderPinbar();
    if (rooms.length) switchRoom(rooms[0].name);
  }
  renderRoomList();
}
function addRoomAndSwitch(name, pass) {
  name = name.trim().slice(0, 32);
  if (!name) { toast("اسم روم رو بنویس"); return; }
  var r = roomEntry(name);
  if (!r) { r = { name: name, pass: pass || "", unread: 0 }; rooms.push(r); saveRooms(); }
  else if (pass) { r.pass = pass; saveRooms(); }
  closeDialog();
  switchRoom(name);
}

// ---------- new room / welcome / password dialogs ----------
function newRoomDialog() {
  openDialog("<h3>روم جدید</h3>" +
    "<div class='frow'><label>اسم روم</label><input type='text' id='nr-name' maxlength='32' placeholder='مثلاً دوستان'></div>" +
    "<div class='frow'><label>رمز (اختیاری — اگر روم رمز داره)</label><input type='password' id='nr-pass' maxlength='64'></div>" +
    "<p style='font-size:12px;color:var(--muted);line-height:1.9'>هر اسمی بنویسی روم ساخته می‌شه. اولین نفری که وارد بشه ادمین رومه.</p>" +
    "<div class='btnrow'><button class='primary' id='nr-go'>ورود</button><button class='ghostbtn' id='nr-cancel'>انصراف</button></div>");
  $("nr-go").onclick = function () { addRoomAndSwitch($("nr-name").value, $("nr-pass").value); };
  $("nr-cancel").onclick = closeDialog;
  $("nr-name").focus();
  $("nr-name").onkeydown = function (e) { if (e.key === "Enter") $("nr-go").click(); };
}
function welcomeDialog() {
  openDialog("<h3>به چت پارسا خوش اومدی! 💬</h3>" +
    "<div class='frow'><label>اسم تو</label><input type='text' id='w-name' maxlength='24' placeholder='اسمت رو بنویس' value='" + esc(myName) + "'></div>" +
    "<div class='frow'><label>اسم روم</label><input type='text' id='w-room' maxlength='32' value='عمومی'></div>" +
    "<div class='btnrow'><button class='primary' id='w-go'>شروع چت</button></div>");
  $("w-go").onclick = function () {
    var n = $("w-name").value.trim();
    if (!n) { toast("اسمت رو بنویس"); return; }
    myName = n.slice(0, 24); LS.setItem("pc_name", myName);
    addRoomAndSwitch($("w-room").value || "عمومی", "");
  };
  $("w-name").focus();
}
function passwordDialog(room) {
  openDialog("<h3>🔒 روم «" + esc(room) + "» رمز داره</h3>" +
    "<div class='frow'><label>رمز روم</label><input type='password' id='pw-input' maxlength='64'></div>" +
    "<div class='btnrow'><button class='primary' id='pw-go'>ورود</button><button class='ghostbtn' id='pw-cancel'>انصراف</button></div>");
  $("pw-go").onclick = function () {
    var r = roomEntry(room);
    if (r) { r.pass = $("pw-input").value; saveRooms(); }
    closeDialog();
    if (room === activeRoom) { wsClosedByUs = false; connect(); }
  };
  $("pw-cancel").onclick = closeDialog;
  $("pw-input").focus();
  $("pw-input").onkeydown = function (e) { if (e.key === "Enter") $("pw-go").click(); };
}
// ---------- active connection ----------
function setConn(on, msg) {
  $("connbanner").classList.toggle("show", !on);
  $("connmsg").textContent = msg || "";
}
function connect() {
  if (!activeRoom || !myName) return;
  var r = roomEntry(activeRoom) || { pass: "" };
  setConn(false, "در حال اتصال…");
  var sock;
  try { sock = new WebSocket(wsUrl(activeRoom, myName, r.pass, false)); } catch (e) { scheduleReconnect(); return; }
  ws = sock;
  sock.onopen = function () { wsRetry = 1000; setConn(true); updateSubtitle(); $("input").focus(); };
  sock.onmessage = function (ev) { var d; try { d = JSON.parse(ev.data); } catch (e) { return; } handle(d); };
  sock.onclose = function (ev) {
    if (ws !== sock) return;
    if (wsClosedByUs) return;
    if (ev.code === 4403) { setConn(false, "دسترسی رد شد"); return; }
    setConn(false, "قطع شد — تلاش دوباره…");
    scheduleReconnect();
  };
  sock.onerror = function () { try { sock.close(); } catch (e) {} };
}
function scheduleReconnect() {
  clearTimeout(reconnectTimer);
  reconnectTimer = setTimeout(function () { if (!wsClosedByUs) connect(); }, wsRetry);
  wsRetry = Math.min(wsRetry * 2, 15000);
}
function send(obj) { if (ws && ws.readyState === 1) { ws.send(JSON.stringify(obj)); return true; } toast("اتصال برقرار نیست"); return false; }

function handle(d) {
  if (d.type === "history") {
    me = d.you; myName = d.you.name;
    hasPassword = d.hasPassword; topic = d.topic || "";
    setMembers(d.members);
    $("log").innerHTML = "";
    lastDateKey = ""; lastRowSender = ""; lastRowTs = 0; lastTailRow = null; messagesById = {};
    var myMem = membersByName[myName];
    var myLastSeen = myMem ? myMem.last_seen_id || 0 : 0;
    var dividerPlaced = false;
    for (var i = 0; i < d.messages.length; i++) {
      var m = d.messages[i];
      if (!dividerPlaced && myLastSeen && m.id > myLastSeen && m.name !== myName) {
        var dv = document.createElement("div"); dv.className = "unreaddv"; dv.textContent = "پیام‌های نخوانده";
        $("log").appendChild(dv); dividerPlaced = true;
      }
      renderMsg(m, false);
    }
    if (d.messages.length) oldestId = d.messages[0].id;
    pinned = d.pinned || []; pinIdx = pinned.length - 1; renderPinbar();
    updateSubtitle();
    scrollDown(true); sendSeen();
    setConn(true);
    var r = roomEntry(activeRoom);
    if (r && d.messages.length) { r.last = { name: d.messages[d.messages.length - 1].name, text: d.messages[d.messages.length - 1].text, kind: d.messages[d.messages.length - 1].kind, ts: d.messages[d.messages.length - 1].ts }; saveRooms(); renderRoomList(); }
  }
  else if (d.type === "msg") {
    var stick = nearBottom();
    renderMsg(d.message, false);
    updateRoomLast(activeRoom, d.message);
    if (d.message.name !== myName) { beep(880); notify(activeRoom, d.message); }
    if (!stick && d.message.name !== myName) { unreadActive++; showFab(); }
    if (stick) { scrollDown(); sendSeen(); }
    updateSubtitle();
  }
  else if (d.type === "older") {
    loadingOlder = false;
    var prevHeight = $("log").scrollHeight;
    var msgs = d.messages.slice().reverse();
    for (var j = 0; j < msgs.length; j++) renderMsg(msgs[j], true);
    if (d.messages.length) oldestId = d.messages[0].id;
    $("log").scrollTop = $("log").scrollHeight - prevHeight;
    if (pendingJump) tryJump();
  }
  else if (d.type === "edited") {
    var m1 = messagesById[d.id];
    if (m1) { m1.text = d.text; m1.edited = true; rerender(m1); }
    updatePinTexts();
  }
  else if (d.type === "deleted") {
    var m2 = messagesById[d.id];
    if (m2) { m2.deleted = true; m2.text = ""; m2.data = null; m2.poll = null; rerender(m2); }
  }
  else if (d.type === "reaction") {
    var m3 = messagesById[d.id];
    if (m3) { m3.reactions = d.reactions; renderReactions(m3); }
  }
  else if (d.type === "poll") {
    var m4 = messagesById[d.id];
    if (m4) { m4.poll = d.poll; rerender(m4); }
  }
  else if (d.type === "typing") { typingUsers[d.name] = d.on; updateSubtitle(); }
  else if (d.type === "seen") {
    if (membersByName[d.name]) membersByName[d.name].last_seen_id = Math.max(membersByName[d.name].last_seen_id || 0, d.upToId);
    refreshTicks();
  }
  else if (d.type === "pinned") { pinned = d.messages || []; pinIdx = pinned.length - 1; renderPinbar(); }
  else if (d.type === "presence") {
    onlineSet = {};
    for (var k = 0; k < d.users.length; k++) onlineSet[d.users[k].name] = d.users[k];
    updateSubtitle();
  }
  else if (d.type === "members") { setMembers(d.members); updateSubtitle(); }
  else if (d.type === "topic") { topic = d.text || ""; }
  else if (d.type === "password") { hasPassword = d.hasPassword; }
  else if (d.type === "system") { addSys(d.text); }
  else if (d.type === "error") {
    toast(d.text);
    if (d.code === "bad_password" && activeRoom) passwordDialog(activeRoom);
  }
  else if (d.type === "kicked") {
    toast("از این روم اخراج شدی");
    wsClosedByUs = true;
    if (ws) { try { ws.close(); } catch (e) {} }
    leaveRoom(activeRoom);
  }
  else if (d.type === "search_results") { showSearchResults(d); }
}
function setMembers(list) {
  membersByName = {};
  for (var i = 0; i < list.length; i++) membersByName[list[i].name] = list[i];
  if (membersByName[myName]) { me.role = membersByName[myName].role; me.color = membersByName[myName].color; me.bio = membersByName[myName].bio || ""; }
}
function updateSubtitle() {
  var typing = Object.keys(typingUsers).filter(function (n) { return typingUsers[n] && n !== myName; });
  var sub = $("hdr-sub");
  if (typing.length) {
    sub.className = "typing";
    sub.textContent = typing.slice(0, 2).join(" و ") + (typing.length > 2 ? " و دیگران" : "") + " در حال نوشتن…";
    return;
  }
  sub.className = "";
  if (!activeRoom) { sub.textContent = ""; return; }
  var total = Object.keys(membersByName).filter(function (n) { return !membersByName[n].banned; }).length;
  var on = Object.keys(onlineSet).length;
  sub.innerHTML = esc(total + " عضو") + (on ? '، <span class="on">' + esc(on + " آنلاین") + "</span>" : "");
}
function addSys(text) {
  var div = document.createElement("div");
  div.className = "sysmsg"; div.textContent = text;
  $("log").appendChild(div);
  if (nearBottom()) scrollDown();
}

// ---------- pinned bar ----------
function pinSnippet(m) { return m ? snippet(m, 70) : ""; }
function renderPinbar() {
  var bar = $("pinbar");
  if (!pinned.length) { bar.classList.remove("show"); return; }
  if (pinIdx < 0 || pinIdx >= pinned.length) pinIdx = pinned.length - 1;
  bar.classList.add("show");
  $("pin-count").textContent = "پیام پین‌شده " + (pinned.length > 1 ? (pinIdx + 1) + "/" + pinned.length : "");
  $("pin-text").textContent = pinSnippet(pinned[pinIdx]);
  $("pin-close").style.display = me.role === "admin" ? "" : "none";
}
function updatePinTexts() {
  for (var i = 0; i < pinned.length; i++) { var m = messagesById[pinned[i].id]; if (m) pinned[i] = m; }
  renderPinbar();
}
$("pinbar").onclick = function (e) {
  if (e.target.closest("#pin-close")) return;
  if (!pinned.length) return;
  jumpTo(pinned[pinIdx].id);
  pinIdx = (pinIdx + 1) % pinned.length;
  renderPinbar();
};
$("pin-close").onclick = function () { send({ type: "unpin", id: pinned[pinIdx] ? pinned[pinIdx].id : undefined }); };

// ---------- search ----------
var searchTimer = null;
$("btn-search").onclick = function (e) {
  e.stopPropagation();
  var bar = $("searchbar");
  bar.classList.toggle("show");
  if (bar.classList.contains("show")) $("searchinput").focus();
  else $("searchresults").classList.remove("show");
};
$("search-close").onclick = function () { $("searchbar").classList.remove("show"); $("searchresults").classList.remove("show"); };
$("searchinput").oninput = function () {
  clearTimeout(searchTimer);
  var q = this.value.trim();
  searchTimer = setTimeout(function () { if (q) send({ type: "search", q: q }); else $("searchresults").classList.remove("show"); }, 300);
};
function showSearchResults(d) {
  var box = $("searchresults");
  box.innerHTML = "";
  if (!d.messages.length) box.innerHTML = '<div class="sres">چیزی پیدا نشد</div>';
  for (var i = d.messages.length - 1; i >= 0; i--) {
    (function (m) {
      if (!messagesById[m.id]) messagesById[m.id] = m;
      var div = document.createElement("div");
      div.className = "sres";
      div.innerHTML = "<b>" + esc(m.name) + "</b> (" + esc(fmtTime(m.ts)) + "): " + esc(snippet(m, 80));
      div.onclick = function () { jumpTo(m.id); };
      box.appendChild(div);
    })(d.messages[i]);
  }
  box.classList.add("show");
}
// ---------- rendering ----------
function linkify(text) {
  var out = esc(text);
  out = out.replace(/(https?:\\/\\/[^\\s<]+)/g, '<a href="$1" target="_blank" rel="noopener noreferrer">$1</a>');
  if (myName) {
    var pat = new RegExp("@" + myName.replace(/[.*+?^\${}()|[\\]\\\\]/g, "\\\\$&"), "g");
    out = out.replace(pat, '<span class="mention">@' + esc(myName) + "</span>");
  }
  return out;
}
function isJumbo(text) {
  var t = text.trim();
  if (!t || t.length > 24) return false;
  try {
    var stripped = t.replace(/[\\s\\u200d\\ufe0f]/g, "").replace(/\\p{Extended_Pictographic}/gu, "");
    if (stripped.length) return false;
    var count = (t.match(/\\p{Extended_Pictographic}/gu) || []).length;
    return count >= 1 && count <= 3;
  } catch (e) { return false; }
}
function metaHtml(m) {
  var h = '<span class="meta">';
  if (m.edited) h += "<span>ویرایش</span>";
  h += "<span>" + esc(fmtTime(m.ts)) + "</span>";
  if (m.name === myName && !m.deleted) {
    var read = false;
    var names = Object.keys(membersByName);
    for (var i = 0; i < names.length; i++) {
      if (names[i] !== myName && (membersByName[names[i]].last_seen_id || 0) >= m.id) { read = true; break; }
    }
    h += '<span class="ticks' + (read ? " read" : "") + '">' + (read ? "✓✓" : "✓") + "</span>";
  }
  return h + "</span>";
}
function replyHtml(m) {
  if (!m.replyTo) return "";
  var q = messagesById[m.replyTo];
  var label = q ? q.name : "پیام";
  var txt = q ? snippet(q, 60) : "برای دیدن، تاریخچه رو بالا بکش";
  return '<div class="replyq" data-jump="' + m.replyTo + '"><b>' + esc(label) + "</b><span>" + esc(txt) + "</span></div>";
}
function fwdHtml(m) { return m.fwdFrom ? '<div class="fwd">فوروارد شده از <b>' + esc(m.fwdFrom) + "</b></div>" : ""; }
function bodyHtml(m) {
  if (m.deleted) return "پیام حذف شد";
  if (m.kind === "text") return linkify(m.text);
  if (m.kind === "image") {
    return '<img class="photo" src="data:image/jpeg;base64,' + m.data + '" alt="عکس" loading="lazy">' +
      (m.text ? '<div class="caption">' + linkify(m.text) + "</div>" : "");
  }
  if (m.kind === "voice") return voiceHtml(m);
  if (m.kind === "file") {
    return '<a class="filecard" download="' + esc(m.text || "file") + '" href="data:application/octet-stream;base64,' + m.data + '">' +
      '<span class="fic">📎</span><span><span class="fname">' + esc(m.text || "فایل") + '</span><br><span class="fsub">' + fmtSize((m.data || "").length) + " — برای دانلود بزن</span></span></a>";
  }
  if (m.kind === "poll") return pollHtml(m);
  return esc(m.text);
}
function voiceHtml(m) {
  var bars = "", seed = m.id * 2654435761 % 2147483647;
  for (var i = 0; i < 26; i++) { seed = (seed * 48271) % 2147483647; bars += '<i style="height:' + (20 + seed % 80) + '%"></i>'; }
  var dur = parseFloat(m.text);
  return '<div class="voice" data-vid="' + m.id + '"><button class="vplay">▶</button><div style="flex:1"><div class="vwave">' + bars + "</div>" +
    '<div class="vinfo"><span class="vdur">' + (isFinite(dur) && dur > 0 ? fmtDur(dur) : "0:00") + '</span><span class="vspeed">1x</span></div></div></div>';
}
function pollHtml(m) {
  var p = m.poll;
  if (!p) return "📊 نظرسنجی";
  var total = 0, voters = {};
  for (var i = 0; i < p.opts.length; i++) { total += p.opts[i].v.length; for (var j = 0; j < p.opts[i].v.length; j++) voters[p.opts[i].v[j]] = 1; }
  var nv = Object.keys(voters).length;
  var h = '<div class="poll"><div class="pq">' + esc(p.q) + '</div><div class="ptype">' + (p.closed ? "نظرسنجی بسته‌شده" : p.multi ? "نظرسنجی — چند انتخابی" : "نظرسنجی") + "</div>";
  for (var k = 0; k < p.opts.length; k++) {
    var o = p.opts[k];
    var pct = nv ? Math.round(o.v.length / nv * 100) : 0;
    var voted = o.v.indexOf(myName) >= 0;
    h += '<button class="popt' + (voted ? " voted" : "") + '" data-idx="' + k + '"' + (p.closed ? " disabled" : "") + ">" +
      '<span class="ptxt"><span>' + esc(o.t) + "</span>" + (o.v.length ? '<span class="pct">' + pct + "٪ (" + o.v.length + ")</span>" : "") + "</span>" +
      '<span class="pbar"><i style="width:' + pct + '%"></i></span></button>';
  }
  h += '<div class="pfoot"><span>' + esc(nv + " رأی") + "</span>";
  if (!p.closed && (m.name === myName || me.role === "admin")) h += '<span class="pclose">بستن نظرسنجی</span>';
  h += "</div></div>";
  return h;
}
function renderReactions(m) {
  var row = $("log").querySelector('.row[data-id="' + m.id + '"]');
  if (!row) return;
  var box = row.querySelector(".reacts");
  var html = "";
  var rs = m.reactions || [];
  for (var i = 0; i < rs.length; i++) {
    var mine = rs[i].users.indexOf(myName) >= 0;
    html += '<span class="rchip' + (mine ? " mine" : "") + '" data-emoji="' + esc(rs[i].emoji) + '" title="' + esc(rs[i].users.join("، ")) + '">' + esc(rs[i].emoji) + " " + rs[i].count + "</span>";
  }
  if (box) box.innerHTML = html;
  else if (html) {
    box = document.createElement("div"); box.className = "reacts"; box.innerHTML = html;
    row.querySelector(".msg").appendChild(box);
  }
}
function rowHtml(m) {
  var mine = m.name === myName;
  var jumbo = m.kind === "text" && !m.deleted && isJumbo(m.text);
  var cls = "msg " + (mine ? "mine" : "other") + (jumbo ? " jumbo" : "") + (m.deleted ? " deleted" : "");
  var inner = "";
  if (!mine && !jumbo) inner += '<div class="who" style="color:' + memberColor(m.name) + '">' + esc(m.name) + "</div>";
  inner += fwdHtml(m) + replyHtml(m);
  inner += bodyHtml(m);
  inner += metaHtml(m);
  var rs = m.reactions || [];
  if (rs.length && !m.deleted) {
    inner += '<div class="reacts">';
    for (var i = 0; i < rs.length; i++) {
      var isMine = rs[i].users.indexOf(myName) >= 0;
      inner += '<span class="rchip' + (isMine ? " mine" : "") + '" data-emoji="' + esc(rs[i].emoji) + '" title="' + esc(rs[i].users.join("، ")) + '">' + esc(rs[i].emoji) + " " + rs[i].count + "</span>";
    }
    inner += "</div>";
  }
  var av = mine ? "" : avatarHtml(m.name, "xs");
  return av + '<div class="' + cls + '">' + inner + "</div>";
}
function renderMsg(m, prepend) {
  messagesById[m.id] = m;
  if (m.id > lastMsgId) lastMsgId = m.id;
  var log = $("log");
  var row = document.createElement("div");
  var mine = m.name === myName;
  row.className = "row " + (mine ? "mine" : "other");
  row.setAttribute("data-id", m.id);
  row.innerHTML = rowHtml(m);
  if (prepend) {
    var first = log.querySelector(".row");
    if (first) log.insertBefore(row, first); else log.appendChild(row);
    bindRow(row, m);
    return;
  }
  var dk = dateKey(m.ts);
  if (dk !== lastDateKey) {
    var chip = document.createElement("div");
    chip.className = "datechip"; chip.textContent = fmtDate(m.ts);
    log.appendChild(chip);
    lastDateKey = dk; lastRowSender = "";
  }
  var grouped = m.name === lastRowSender && m.ts - lastRowTs < 60000 && !m.deleted;
  if (grouped) row.classList.add("grouped");
  if (grouped && lastTailRow && lastTailRow.getAttribute("data-sender") === m.name) lastTailRow.classList.remove("tail");
  row.classList.add("tail");
  row.setAttribute("data-sender", m.name);
  log.appendChild(row);
  lastTailRow = row;
  lastRowSender = m.name; lastRowTs = m.ts;
  bindRow(row, m);
}
function rerender(m) {
  var row = $("log").querySelector('.row[data-id="' + m.id + '"]');
  if (!row) return;
  var stick = nearBottom();
  row.innerHTML = rowHtml(m);
  bindRow(row, m);
  if (stick) scrollDown();
}
function refreshTicks() {
  var rows = $("log").querySelectorAll(".row.mine");
  for (var i = 0; i < rows.length; i++) {
    var m = messagesById[rows[i].getAttribute("data-id")];
    if (!m || m.deleted) continue;
    var meta = rows[i].querySelector(".meta");
    if (meta) {
      var tmp = document.createElement("div"); tmp.innerHTML = metaHtml(m);
      rows[i].querySelector(".msg").replaceChild(tmp.firstChild, meta);
    }
  }
}
// ---------- per-message interactions ----------
var players = {}; // id -> {audio, speed}
function bindRow(row, m) {
  var msg = row.querySelector(".msg");
  var jump = row.querySelector(".replyq");
  if (jump) jump.onclick = function (e) { e.stopPropagation(); jumpTo(parseInt(this.getAttribute("data-jump"), 10)); };
  var who = row.querySelector(".who");
  if (who) who.onclick = function (e) { e.stopPropagation(); profileDialog(m.name); };
  var photo = row.querySelector("img.photo");
  if (photo) photo.onclick = function (e) { e.stopPropagation(); openLightbox(m); };
  var chips = row.querySelectorAll(".rchip");
  for (var i = 0; i < chips.length; i++) chips[i].onclick = function (e) { e.stopPropagation(); send({ type: "react", id: m.id, emoji: this.getAttribute("data-emoji") }); };
  var voice = row.querySelector(".voice");
  if (voice) bindVoice(voice, m);
  var opts = row.querySelectorAll(".popt");
  for (var j = 0; j < opts.length; j++) opts[j].onclick = function (e) { e.stopPropagation(); votePoll(m, parseInt(this.getAttribute("data-idx"), 10)); };
  var pclose = row.querySelector(".pclose");
  if (pclose) pclose.onclick = function (e) { e.stopPropagation(); confirmDialog("بستن نظرسنجی", "دیگه کسی نمی‌تونه رأی بده یا رأیش رو عوض کنه.", function () { send({ type: "close_poll", id: m.id }); }, "ببند"); };
  var filecard = row.querySelector(".filecard");
  if (filecard) filecard.onclick = function (e) { e.stopPropagation(); };

  // context menu: right-click / long-press
  msg.oncontextmenu = function (e) { e.preventDefault(); openCtx(m, e.clientX, e.clientY); };
  var lpTimer = null, touchStart = null, swiped = false, lastTap = 0;
  msg.addEventListener("touchstart", function (e) {
    if (e.touches.length !== 1) return;
    var t = e.touches[0];
    touchStart = { x: t.clientX, y: t.clientY }; swiped = false;
    lpTimer = setTimeout(function () { lpTimer = null; openCtx(m, t.clientX, t.clientY); }, 480);
  }, { passive: true });
  msg.addEventListener("touchmove", function (e) {
    if (!touchStart) return;
    var t = e.touches[0], dx = t.clientX - touchStart.x, dy = t.clientY - touchStart.y;
    if (lpTimer && (Math.abs(dx) > 10 || Math.abs(dy) > 10)) { clearTimeout(lpTimer); lpTimer = null; }
    if (!swiped && Math.abs(dx) > 60 && Math.abs(dy) < 40 && !m.deleted) {
      swiped = true; startReply(m.id);
      if (navigator.vibrate) try { navigator.vibrate(30); } catch (err) {}
    }
    if (Math.abs(dx) > 8 || Math.abs(dy) > 8) msg.style.transform = "translateX(" + Math.max(-70, Math.min(70, dx * 0.4)) + "px)";
  }, { passive: true });
  msg.addEventListener("touchend", function (e) {
    if (lpTimer) { clearTimeout(lpTimer); lpTimer = null; }
    msg.style.transform = "";
    if (!swiped && touchStart) {
      var now = Date.now();
      if (now - lastTap < 320 && !m.deleted) { quickReact(m, "❤️"); lastTap = 0; }
      else lastTap = now;
    }
    touchStart = null;
  }, { passive: true });
  msg.ondblclick = function (e) {
    if (e.target.closest("a,button,img.photo,.popt,.replyq")) return;
    if (!m.deleted) quickReact(m, "❤️");
  };
}
function quickReact(m, emoji) {
  send({ type: "react", id: m.id, emoji: emoji });
  var row = $("log").querySelector('.row[data-id="' + m.id + '"]');
  if (row) {
    var pop = document.createElement("span");
    pop.className = "heartpop"; pop.textContent = emoji;
    row.appendChild(pop);
    setTimeout(function () { pop.remove(); }, 750);
  }
}
function votePoll(m, idx) {
  if (!m.poll || m.poll.closed) return;
  var voted = m.poll.opts[idx].v.indexOf(myName) >= 0;
  var sel = [];
  if (m.poll.multi) {
    for (var i = 0; i < m.poll.opts.length; i++) if (m.poll.opts[i].v.indexOf(myName) >= 0) sel.push(i);
    if (voted) sel = sel.filter(function (x) { return x !== idx; }); else sel.push(idx);
  } else if (!voted) sel = [idx];
  send({ type: "vote", id: m.id, idx: sel });
}

// ---------- voice player ----------
function bindVoice(box, m) {
  var btn = box.querySelector(".vplay"), bars = box.querySelectorAll(".vwave i");
  var durEl = box.querySelector(".vdur"), speedEl = box.querySelector(".vspeed");
  var P = players[m.id];
  if (!P) { P = players[m.id] = { audio: null, speed: 1 }; }
  function ensureAudio() {
    if (!P.audio) {
      P.audio = new Audio("data:audio/webm;base64," + m.data);
      P.audio.addEventListener("loadedmetadata", function () {
        if (isFinite(P.audio.duration) && P.audio.duration > 0) durEl.textContent = fmtDur(P.audio.duration);
      });
      P.audio.addEventListener("timeupdate", function () {
        var d = P.audio.duration;
        if (!isFinite(d) || !d) return;
        var frac = P.audio.currentTime / d;
        for (var i = 0; i < bars.length; i++) bars[i].classList.toggle("on", i / bars.length <= frac);
        durEl.textContent = fmtDur(P.audio.currentTime) + " / " + fmtDur(d);
      });
      P.audio.addEventListener("ended", function () {
        btn.textContent = "▶";
        for (var i = 0; i < bars.length; i++) bars[i].classList.remove("on");
        durEl.textContent = fmtDur(P.audio.duration || 0);
      });
    }
    return P.audio;
  }
  btn.onclick = function (e) {
    e.stopPropagation();
    var a = ensureAudio();
    if (a.paused) {
      Object.keys(players).forEach(function (id) { if (id != m.id && players[id].audio && !players[id].audio.paused) players[id].audio.pause(); });
      a.playbackRate = P.speed;
      a.play().catch(function () { toast("پخش صدا ممکن نشد"); });
      btn.textContent = "⏸";
    } else { a.pause(); btn.textContent = "▶"; }
  };
  speedEl.onclick = function (e) {
    e.stopPropagation();
    P.speed = P.speed === 1 ? 1.5 : P.speed === 1.5 ? 2 : 1;
    speedEl.textContent = P.speed + "x";
    if (P.audio) P.audio.playbackRate = P.speed;
  };
  speedEl.textContent = P.speed + "x";
}

// ---------- lightbox ----------
var lbMsg = null, lbZoom = 1;
function openLightbox(m) {
  lbMsg = m; lbZoom = 1;
  $("lb-img").src = "data:image/jpeg;base64," + m.data;
  $("lb-img").style.transform = "";
  $("lb-cap").textContent = m.text || "";
  $("lightbox").classList.add("show");
}
function closeLightbox() { $("lightbox").classList.remove("show"); lbMsg = null; }
$("lb-close").onclick = closeLightbox;
$("lightbox").onclick = function (e) { if (e.target === this || e.target === $("lb-stage")) closeLightbox(); };
$("lb-img").onclick = function () { lbZoom = lbZoom === 1 ? 2.2 : 1; this.style.transform = "scale(" + lbZoom + ")"; this.style.cursor = lbZoom === 1 ? "zoom-in" : "zoom-out"; };
$("lb-img").addEventListener("wheel", function (e) {
  e.preventDefault();
  lbZoom = Math.max(0.5, Math.min(5, lbZoom + (e.deltaY < 0 ? 0.25 : -0.25)));
  this.style.transform = "scale(" + lbZoom + ")";
}, { passive: false });
$("lb-dl").onclick = function () {
  if (!lbMsg) return;
  var a = document.createElement("a");
  a.href = "data:image/jpeg;base64," + lbMsg.data;
  a.download = "parsa-chat-" + lbMsg.id + ".jpg";
  document.body.appendChild(a); a.click(); a.remove();
};

// ---------- scroll / jump / seen ----------
function nearBottom() { var l = $("log"); return l.scrollHeight - l.scrollTop - l.clientHeight < 130; }
function scrollDown(force) { var l = $("log"); if (force || nearBottom()) { l.scrollTop = l.scrollHeight; unreadActive = 0; showFab(); } }
function showFab() {
  $("fab").classList.toggle("show", !nearBottom());
  var b = $("fab-badge");
  b.textContent = unreadActive > 99 ? "99+" : unreadActive;
  b.classList.toggle("show", unreadActive > 0);
}
$("fab").onclick = function () { scrollDown(true); sendSeen(); };
$("log").addEventListener("scroll", function () {
  showFab();
  var l = $("log");
  if (l.scrollTop < 220 && oldestId && !loadingOlder && ws && ws.readyState === 1) {
    loadingOlder = true;
    ws.send(JSON.stringify({ type: "history_before", beforeId: oldestId }));
  }
  if (nearBottom() && document.hasFocus()) sendSeen();
});
function jumpTo(id) { pendingJump = { id: id, tries: 0 }; tryJump(); }
function tryJump() {
  if (!pendingJump) return;
  var row = $("log").querySelector('.row[data-id="' + pendingJump.id + '"]');
  if (row) {
    row.scrollIntoView({ block: "center" });
    row.classList.remove("flash"); void row.offsetWidth; row.classList.add("flash");
    pendingJump = null;
    return;
  }
  if (pendingJump.tries++ > 8 || !oldestId) { pendingJump = null; toast("پیام خیلی قدیمیه — دیگه نگهداری نمی‌شه"); return; }
  if (!loadingOlder && ws && ws.readyState === 1) {
    loadingOlder = true;
    ws.send(JSON.stringify({ type: "history_before", beforeId: pendingJump.id + 1 }));
  }
}
function sendSeen() {
  if (!lastMsgId || lastMsgId <= lastSeenSent || !ws || ws.readyState !== 1) return;
  lastSeenSent = lastMsgId;
  ws.send(JSON.stringify({ type: "seen", upToId: lastMsgId }));
  if (membersByName[myName]) membersByName[myName].last_seen_id = lastMsgId;
}
window.addEventListener("focus", function () { if (nearBottom()) sendSeen(); });
// ---------- input bar ----------
var input = $("input");
function autoGrow() { input.style.height = "auto"; input.style.height = Math.min(input.scrollHeight, 140) + "px"; }
function updateSendBtn() {
  var has = input.value.trim().length > 0;
  $("ic-mic").style.display = has ? "none" : "";
  $("ic-send").style.display = has ? "" : "none";
}
function startReply(id) {
  var m = messagesById[id];
  if (!m) return;
  replyToId = id; editId = null;
  $("replylabel").textContent = "↩️ پاسخ به " + m.name;
  $("replytxt").textContent = snippet(m, 80);
  $("replybar").classList.add("show");
  input.focus();
}
function startEdit(id) {
  var m = messagesById[id];
  if (!m || m.name !== myName || m.kind !== "text" || m.deleted) return;
  editId = id; replyToId = null;
  $("replylabel").textContent = "✏️ ویرایش پیام";
  $("replytxt").textContent = snippet(m, 80);
  $("replybar").classList.add("show");
  input.value = m.text; autoGrow(); updateSendBtn(); input.focus();
}
function cancelReplyEdit() { replyToId = null; editId = null; $("replybar").classList.remove("show"); }
$("reply-cancel").onclick = function () { cancelReplyEdit(); input.value = ""; autoGrow(); updateSendBtn(); };
function sendText() {
  var t = input.value.trim();
  if (!t) return;
  if (editId) {
    if (send({ type: "edit", id: editId, text: t })) { cancelReplyEdit(); }
  } else {
    var payload = { type: "msg", text: t };
    if (replyToId) payload.replyTo = replyToId;
    if (send(payload)) { cancelReplyEdit(); beep(660); askNotifPermission(); }
  }
  input.value = ""; autoGrow(); updateSendBtn(); saveDraft();
  sendTyping(false);
  input.focus();
}
function sendTyping(on) {
  if (!ws || ws.readyState !== 1) return;
  var now = Date.now();
  if (on && now - lastTypingSent < 2500) return;
  lastTypingSent = now;
  ws.send(JSON.stringify({ type: "typing", on: on }));
}
input.addEventListener("input", function () {
  autoGrow(); updateSendBtn();
  sendTyping(true);
  clearTimeout(typingTimer);
  typingTimer = setTimeout(function () { sendTyping(false); }, 3000);
  clearTimeout(draftTimer);
  draftTimer = setTimeout(saveDraft, 400);
});
input.addEventListener("keydown", function (e) {
  if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); if (input.value.trim()) sendText(); }
  if (e.key === "Escape") cancelReplyEdit();
  if (e.key === "ArrowUp" && !input.value) {
    var ids = Object.keys(messagesById).map(Number).sort(function (a, b) { return b - a; });
    for (var i = 0; i < ids.length; i++) {
      var m = messagesById[ids[i]];
      if (m.name === myName && m.kind === "text" && !m.deleted) { startEdit(m.id); break; }
    }
  }
});
$("btn-send").onclick = function () { if (input.value.trim()) sendText(); else startRecording(); };

// ---------- attach menu / images / files ----------
function showMenuAt(menu, x, y) {
  hideMenus();
  menu.classList.add("show");
  var w = menu.offsetWidth, h = menu.offsetHeight;
  menu.style.left = Math.max(6, Math.min(window.innerWidth - w - 6, x - w / 2)) + "px";
  menu.style.top = Math.max(6, Math.min(window.innerHeight - h - 6, y)) + "px";
}
function hideMenus() {
  $("attachmenu").classList.remove("show");
  $("moremenu").classList.remove("show");
  $("ctxmenu").classList.remove("show");
}
document.addEventListener("click", function (e) {
  if (!e.target.closest(".menu,#ctxmenu,#btn-attach,#btn-more")) hideMenus();
});
document.addEventListener("keydown", function (e) { if (e.key === "Escape") { hideMenus(); closeEmoji(); } });
$("log").addEventListener("scroll", hideMenus);
$("btn-attach").onclick = function (e) {
  e.stopPropagation();
  if (!activeRoom) { toast("اول یک روم انتخاب کن"); return; }
  var r = this.getBoundingClientRect();
  showMenuAt($("attachmenu"), r.left + r.width / 2, r.top - 140);
};
$("attachmenu").onclick = function (e) {
  var act = e.target.getAttribute && e.target.getAttribute("data-act");
  hideMenus();
  if (act === "photo") $("imginput").click();
  if (act === "file") $("fileinput").click();
  if (act === "poll") pollDialog();
};
function b64FromFile(file, cb) {
  var r = new FileReader();
  r.onload = function () { cb(String(r.result).split(",")[1] || ""); };
  r.readAsDataURL(file);
}
$("imginput").addEventListener("change", function () {
  var f = this.files[0];
  this.value = "";
  if (!f) return;
  var img = new Image();
  var url = URL.createObjectURL(f);
  img.onload = function () {
    URL.revokeObjectURL(url);
    var max = 1280, w = img.width, h = img.height;
    if (w > max || h > max) { var s = max / Math.max(w, h); w = Math.round(w * s); h = Math.round(h * s); }
    var cv = document.createElement("canvas");
    cv.width = w; cv.height = h;
    cv.getContext("2d").drawImage(img, 0, 0, w, h);
    var b64 = cv.toDataURL("image/jpeg", 0.82).split(",")[1] || "";
    if (b64.length > 1400000) { toast("عکس خیلی بزرگه"); return; }
    var payload = { type: "media", kind: "image", data: b64, text: input.value.trim() };
    if (replyToId) payload.replyTo = replyToId;
    if (send(payload)) { input.value = ""; autoGrow(); updateSendBtn(); cancelReplyEdit(); saveDraft(); beep(660); }
  };
  img.onerror = function () { URL.revokeObjectURL(url); toast("عکس خونده نشد"); };
  img.src = url;
});
$("fileinput").addEventListener("change", function () {
  var f = this.files[0];
  this.value = "";
  if (!f) return;
  if (f.size > 1000000) { toast("حداکثر حجم فایل ۱ مگابایته"); return; }
  b64FromFile(f, function (b64) {
    var payload = { type: "media", kind: "file", data: b64, text: f.name };
    if (replyToId) payload.replyTo = replyToId;
    if (send(payload)) { cancelReplyEdit(); beep(660); }
  });
});

// ---------- voice recording ----------
var rec = null; // {recorder, stream, chunks, start, timer, raf, analyser, ctx, cancelled}
function startRecording() {
  if (!activeRoom) { toast("اول یک روم انتخاب کن"); return; }
  if (rec) return;
  if (!navigator.mediaDevices || !window.MediaRecorder) { toast("مرورگرت ضبط صدا رو پشتیبانی نمی‌کنه"); return; }
  navigator.mediaDevices.getUserMedia({ audio: true }).then(function (stream) {
    var chunks = [];
    var mr = new MediaRecorder(stream);
    mr.ondataavailable = function (e) { chunks.push(e.data); };
    mr.onstop = function () {
      stream.getTracks().forEach(function (t) { t.stop(); });
      var cancelled = rec && rec.cancelled;
      var dur = rec ? (Date.now() - rec.start) / 1000 : 0;
      if (rec) { clearInterval(rec.timer); cancelAnimationFrame(rec.raf); if (rec.ctx) try { rec.ctx.close(); } catch (e) {} }
      rec = null;
      $("recbar").classList.remove("show");
      $("btn-send").classList.remove("rec");
      if (cancelled) { toast("ضبط لغو شد"); return; }
      var blob = new Blob(chunks, { type: "audio/webm" });
      if (blob.size > 1000000) { toast("پیام صوتی خیلی طولانیه (حداکثر ~۱ دقیقه)"); return; }
      if (blob.size < 500) { toast("خیلی کوتاه بود"); return; }
      var r = new FileReader();
      r.onload = function () {
        var b64 = String(r.result).split(",")[1] || "";
        var payload = { type: "media", kind: "voice", data: b64, text: String(Math.round(dur)) };
        if (replyToId) payload.replyTo = replyToId;
        if (send(payload)) { cancelReplyEdit(); beep(660); }
      };
      r.readAsDataURL(blob);
    };
    rec = { recorder: mr, stream: stream, chunks: chunks, start: Date.now(), cancelled: false };
    mr.start();
    $("recbar").classList.add("show");
    $("btn-send").classList.add("rec");
    $("rec-time").textContent = "0:00";
    rec.timer = setInterval(function () {
      if (!rec) return;
      var s = (Date.now() - rec.start) / 1000;
      $("rec-time").textContent = fmtDur(s);
      if (s >= 60) stopRecording(false);
    }, 250);
    // live waveform
    var wave = $("rec-wave");
    wave.innerHTML = "";
    for (var i = 0; i < 24; i++) wave.appendChild(document.createElement("i"));
    try {
      var actx = new (window.AudioContext || window.webkitAudioContext)();
      var srcNode = actx.createMediaStreamSource(stream);
      var an = actx.createAnalyser();
      an.fftSize = 128;
      srcNode.connect(an);
      rec.ctx = actx; rec.analyser = an;
      var data = new Uint8Array(an.frequencyBinCount);
      var bars = wave.querySelectorAll("i");
      var draw = function () {
        if (!rec || !rec.analyser) return;
        rec.analyser.getByteFrequencyData(data);
        for (var b = 0; b < bars.length; b++) {
          var v = data[Math.floor(b * data.length / bars.length)] / 255;
          bars[b].style.height = Math.max(8, v * 100) + "%";
        }
        rec.raf = requestAnimationFrame(draw);
      };
      draw();
    } catch (e) {}
  }).catch(function () { toast("اجازه میکروفون داده نشد"); });
}
function stopRecording(cancel) {
  if (!rec) return;
  rec.cancelled = !!cancel;
  try { rec.recorder.stop(); } catch (e) {}
}
$("rec-cancel").onclick = function () { stopRecording(true); };
$("rec-send").onclick = function () { stopRecording(false); };

// ---------- emoji panel ----------
var emojiCat = "😀";
function renderEmojiTabs() {
  var tabs = $("emoji-tabs");
  tabs.innerHTML = "";
  var cats = ["🕐"].concat(Object.keys(EMOJI));
  for (var i = 0; i < cats.length; i++) {
    (function (c) {
      var b = document.createElement("button");
      b.textContent = c;
      if (c === emojiCat || (c === "🕐" && emojiCat === "recent")) b.classList.add("active");
      b.onclick = function () { emojiCat = c === "🕐" ? "recent" : c; renderEmojiTabs(); renderEmojiGrid(); };
      tabs.appendChild(b);
    })(cats[i]);
  }
}
function renderEmojiGrid() {
  var grid = $("emoji-grid");
  grid.innerHTML = "";
  var list = emojiCat === "recent" ? (recentEmoji.length ? recentEmoji : EMOJI["😀"]) : EMOJI[emojiCat];
  for (var i = 0; i < list.length; i++) {
    (function (e) {
      var b = document.createElement("button");
      b.textContent = e;
      b.onclick = function () {
        var s = input.selectionStart || input.value.length;
        input.value = input.value.slice(0, s) + e + input.value.slice(input.selectionEnd || s);
        input.selectionStart = input.selectionEnd = s + e.length;
        pushRecent(e);
        autoGrow(); updateSendBtn(); input.focus();
      };
      grid.appendChild(b);
    })(list[i]);
  }
}
function closeEmoji() { $("emojipanel").classList.remove("show"); }
$("btn-emoji").onclick = function (e) {
  e.stopPropagation();
  var p = $("emojipanel");
  if (p.classList.contains("show")) { closeEmoji(); return; }
  renderEmojiTabs(); renderEmojiGrid();
  p.classList.add("show");
};
document.addEventListener("click", function (e) {
  if (!e.target.closest("#emojipanel,#btn-emoji")) closeEmoji();
});
// ---------- message context menu ----------
function openCtx(m, x, y) {
  ctxId = m.id;
  var reacts = $("ctx-reacts");
  reacts.innerHTML = "";
  if (!m.deleted) {
    for (var i = 0; i < QUICK_REACTS.length; i++) {
      (function (e) {
        var b = document.createElement("button");
        b.textContent = e;
        b.onclick = function () { hideMenus(); quickReact(m, e); };
        reacts.appendChild(b);
      })(QUICK_REACTS[i]);
    }
    reacts.style.display = "";
  } else reacts.style.display = "none";
  var items = [];
  if (!m.deleted) {
    items.push(["↩️ پاسخ", function () { startReply(m.id); }]);
    if (m.kind === "text") items.push(["📋 کپی", function () {
      if (navigator.clipboard) navigator.clipboard.writeText(m.text).then(function () { toast("کپی شد"); });
      else toast("کپی پشتیبانی نمی‌شه");
    }]);
    items.push(["➡️ فوروارد", function () { forwardDialog(m); }]);
    if (me.role === "admin") items.push([isPinned(m.id) ? "📌 برداشتن پین" : "📌 پین کردن", function () {
      send(isPinned(m.id) ? { type: "unpin", id: m.id } : { type: "pin", id: m.id });
    }]);
    if (m.name === myName && m.kind === "text") items.push(["✏️ ویرایش", function () { startEdit(m.id); }]);
    if (m.name === myName || me.role === "admin") items.push(["🗑️ حذف", function () {
      confirmDialog("حذف پیام", "پیام برای همه حذف می‌شه.", function () { send({ type: "delete", id: m.id }); }, "حذف کن");
    }, true]);
  }
  var box = $("ctx-items");
  box.innerHTML = "";
  for (var j = 0; j < items.length; j++) {
    (function (it) {
      var b = document.createElement("button");
      b.textContent = it[0];
      if (it[2]) b.classList.add("danger");
      b.onclick = function () { hideMenus(); it[1](); };
      box.appendChild(b);
    })(items[j]);
  }
  var menu = $("ctxmenu");
  hideMenus();
  menu.classList.add("show");
  var w = menu.offsetWidth, h = menu.offsetHeight;
  menu.style.left = Math.max(6, Math.min(window.innerWidth - w - 6, x - w / 2)) + "px";
  menu.style.top = Math.max(6, Math.min(window.innerHeight - h - 6, y - h - 10)) + "px";
}
function isPinned(id) { for (var i = 0; i < pinned.length; i++) if (pinned[i].id === id) return true; return false; }

// ---------- forward ----------
function forwardDialog(m) {
  var html = "<h3>فوروارد به…</h3>";
  if (!rooms.length) html += "<p style='color:var(--muted);font-size:13px'>رومی نداری!</p>";
  for (var i = 0; i < rooms.length; i++) {
    html += '<div class="mrow fw-row" data-room="' + esc(rooms[i].name) + '" style="cursor:pointer">' + avatarHtml(rooms[i].name, "sm", colorFor(rooms[i].name)) +
      '<div class="mbody"><div class="mname">' + esc(rooms[i].name) + "</div><div class='msub'>" + (rooms[i].name === activeRoom ? "روم فعلی" : "") + "</div></div></div>";
  }
  html += "<div class='btnrow'><button class='ghostbtn' id='fw-cancel'>انصراف</button></div>";
  openDialog(html);
  $("fw-cancel").onclick = closeDialog;
  var rows = $("dialog").querySelectorAll(".fw-row");
  for (var j = 0; j < rows.length; j++) rows[j].onclick = function () {
    var target = this.getAttribute("data-room");
    closeDialog();
    doForward(m, target);
  };
}
function doForward(m, target) {
  var payload;
  if (m.kind === "text") payload = { type: "msg", text: m.text, fwdFrom: m.name };
  else if (m.kind === "poll" && m.poll) payload = { type: "poll", question: m.poll.q, options: m.poll.opts.map(function (o) { return o.t; }), multi: m.poll.multi };
  else payload = { type: "media", kind: m.kind, data: m.data, text: m.text, fwdFrom: m.name };
  if (target === activeRoom) { if (send(payload)) toast("فوروارد شد به «" + target + "»"); return; }
  var L = lites[target];
  if (L && L.ws && L.ws.readyState === 1) { L.ws.send(JSON.stringify(payload)); toast("فوروارد شد به «" + target + "»"); }
  else toast("اتصال به «" + target + "» برقرار نیست — اول بازش کن");
}

// ---------- header / more menu ----------
$("hdr").onclick = function (e) {
  if (e.target.closest("button")) return;
  if (activeRoom) infoDialog();
};
$("btn-more").onclick = function (e) {
  e.stopPropagation();
  var menu = $("moremenu");
  menu.innerHTML = '<button data-act="info">👥 اطلاعات روم</button><button data-act="pins">📌 پیام‌های پین‌شده</button>' +
    '<button data-act="newroom">＋ روم جدید</button><button data-act="settings">⚙️ تنظیمات</button>' +
    '<button data-act="theme">🌓 تغییر تم</button>' + (activeRoom ? '<button data-act="leave" class="danger">🚪 خروج از روم</button>' : "");
  var r = this.getBoundingClientRect();
  showMenuAt(menu, r.left, r.bottom + 6);
  menu.onclick = function (ev) {
    var act = ev.target.getAttribute && ev.target.getAttribute("data-act");
    hideMenus();
    if (act === "info" && activeRoom) infoDialog();
    if (act === "pins") pinsDialog();
    if (act === "newroom") newRoomDialog();
    if (act === "settings") settingsDialog();
    if (act === "theme") toggleTheme();
    if (act === "leave" && activeRoom) { var name = activeRoom; confirmDialog("خروج از روم", "«" + name + "» از لیست چت‌هات حذف می‌شه.", function () { leaveRoom(name); }, "خروج"); }
  };
};
$("btn-newroom").onclick = newRoomDialog;
$("btn-theme").onclick = toggleTheme;
$("btn-settings").onclick = function () { settingsDialog(); };

function pinsDialog() {
  var html = "<h3>📌 پیام‌های پین‌شده</h3>";
  if (!pinned.length) html += "<p style='color:var(--muted);font-size:13px'>پیامی پین نشده.</p>";
  for (var i = pinned.length - 1; i >= 0; i--) {
    html += '<div class="mrow pin-row" data-id="' + pinned[i].id + '" style="cursor:pointer"><div class="mbody"><div class="mname">' + esc(pinned[i].name) + "</div><div class='msub'>" + esc(snippet(pinned[i], 70)) + "</div></div></div>";
  }
  html += "<div class='btnrow'><button class='ghostbtn' id='pins-close'>بستن</button></div>";
  openDialog(html);
  $("pins-close").onclick = closeDialog;
  var rows = $("dialog").querySelectorAll(".pin-row");
  for (var j = 0; j < rows.length; j++) rows[j].onclick = function () { var id = parseInt(this.getAttribute("data-id"), 10); closeDialog(); jumpTo(id); };
}

// ---------- room info / members ----------
function infoDialog() {
  var isAdmin = me.role === "admin";
  var html = "<div style='text-align:center;margin-bottom:12px'>" + bigAvatarHtml(activeRoom, 64, colorFor(activeRoom)) +
    "<h3 style='margin-bottom:2px'>" + esc(activeRoom) + (hasPassword ? " 🔒" : "") + "</h3>" +
    "<div style='font-size:12.5px;color:var(--muted)'>" + (topic ? esc(topic) : "بدون موضوع") + "</div></div>";
  if (isAdmin) {
    html += "<div class='frow'><label>موضوع روم</label><input type='text' id='ri-topic' maxlength='140' value='" + esc(topic) + "' placeholder='موضوع روم…'></div>";
    html += "<div class='frow'><label>رمز روم (خالی = بدون رمز)</label><div style='display:flex;gap:6px'><input type='password' id='ri-pass' maxlength='64' placeholder='" + (hasPassword ? "رمز فعلی فعاله" : "رمز جدید") + "' style='flex:1'><button class='primary' id='ri-pass-set'>ثبت</button></div></div>";
  }
  html += "<h3 style='font-size:14px;margin:12px 0 4px'>اعضا</h3><div id='ri-members'></div>";
  html += "<div class='btnrow'><button class='ghostbtn' id='ri-close'>بستن</button><button class='ghostbtn danger' id='ri-leave'>خروج از روم</button></div>";
  openDialog(html);
  $("ri-close").onclick = closeDialog;
  $("ri-leave").onclick = function () { var n = activeRoom; closeDialog(); confirmDialog("خروج از روم", "«" + n + "» از لیستت حذف می‌شه.", function () { leaveRoom(n); }, "خروج"); };
  if (isAdmin) {
    $("ri-topic").onchange = function () { send({ type: "set_topic", text: this.value.trim() }); toast("موضوع ثبت شد"); };
    $("ri-pass-set").onclick = function () {
      var pw = $("ri-pass").value;
      send({ type: "set_password", password: pw });
      var r = roomEntry(activeRoom);
      if (r) { r.pass = pw; saveRooms(); }
      toast(pw ? "رمز تنظیم شد" : "رمز برداشته شد");
      $("ri-pass").value = "";
    };
  }
  renderInfoMembers();
}
function renderInfoMembers() {
  var box = $("ri-members");
  if (!box) return;
  var isAdmin = me.role === "admin";
  var names = Object.keys(membersByName).sort(function (a, b) {
    var ao = onlineSet[a] ? 1 : 0, bo = onlineSet[b] ? 1 : 0;
    if (ao !== bo) return bo - ao;
    return a.localeCompare(b, "fa");
  });
  var html = "";
  for (var i = 0; i < names.length; i++) {
    var n = names[i], mem = membersByName[n];
    if (mem.banned) continue;
    var on = !!onlineSet[n];
    var sub = on ? '<span class="on">آنلاین</span>' : esc(fmtLastSeen(mem.last_seen_ts));
    if (mem.bio) sub = esc(mem.bio) + " · " + sub;
    html += '<div class="mrow">' + avatarHtml(n, "sm", mem.color) +
      '<div class="mbody"><div class="mname">' + esc(n) + (n === myName ? " (شما)" : "") + (mem.role === "admin" ? '<span class="role">ادمین</span>' : "") + "</div><div class='msub'>" + sub + "</div></div>";
    if (isAdmin && n !== myName) {
      html += '<div class="mact">' +
        '<button class="iconbtn small" data-act="' + (mem.role === "admin" ? "demote" : "promote") + '" data-name="' + esc(n) + '" title="' + (mem.role === "admin" ? "عزل ادمین" : "ارتقا به ادمین") + '">' + (mem.role === "admin" ? "⬇️" : "⬆️") + "</button>" +
        '<button class="iconbtn small danger" data-act="kick" data-name="' + esc(n) + '" title="اخراج">🚫</button></div>';
    } else if (!isAdmin) {
      html += '<div class="mact"><button class="iconbtn small" data-act="profile" data-name="' + esc(n) + '">👤</button></div>';
    }
    html += "</div>";
  }
  var banned = Object.keys(membersByName).filter(function (n) { return membersByName[n].banned; });
  if (isAdmin && banned.length) {
    html += "<h3 style='font-size:13px;margin:10px 0 4px;color:var(--bad)'>بن‌شده‌ها</h3>";
    for (var j = 0; j < banned.length; j++) {
      html += '<div class="mrow">' + avatarHtml(banned[j], "sm") + '<div class="mbody"><div class="mname">' + esc(banned[j]) + "</div></div>" +
        '<div class="mact"><button class="iconbtn small" data-act="unban" data-name="' + esc(banned[j]) + '" title="آن‌بن">♻️</button></div></div>';
    }
  }
  box.innerHTML = html;
  var btns = box.querySelectorAll("button[data-act]");
  for (var k = 0; k < btns.length; k++) btns[k].onclick = function () {
    var act = this.getAttribute("data-act"), n = this.getAttribute("data-name");
    if (act === "kick") confirmDialog("اخراج " + n, "از روم بن می‌شه و نمی‌تونه برگرده (مگر آن‌بن کنی).", function () { send({ type: "kick", name: n }); setTimeout(renderInfoMembers, 400); }, "اخراج");
    else if (act === "promote") { send({ type: "promote", name: n }); setTimeout(renderInfoMembers, 400); }
    else if (act === "demote") { send({ type: "demote", name: n }); setTimeout(renderInfoMembers, 400); }
    else if (act === "unban") { send({ type: "unban", name: n }); setTimeout(renderInfoMembers, 400); }
    else if (act === "profile") profileDialog(n);
  };
}
function profileDialog(n) {
  var mem = membersByName[n] || { color: colorFor(n) };
  var on = !!onlineSet[n];
  openDialog("<div style='text-align:center'>" + bigAvatarHtml(n, 72, mem.color) +
    "<h3 style='margin-bottom:2px'>" + esc(n) + (mem.role === "admin" ? " ⭐" : "") + "</h3>" +
    "<div style='font-size:12.5px;color:var(--muted);margin-bottom:8px'>" + (on ? "آنلاین" : esc(fmtLastSeen(mem.last_seen_ts))) + "</div>" +
    (mem.bio ? "<p style='font-size:13.5px;line-height:1.9'>" + esc(mem.bio) + "</p>" : "<p style='font-size:12.5px;color:var(--faint)'>بیو نداره</p>") +
    "<div class='btnrow' style='justify-content:center'><button class='ghostbtn' id='pf-close'>بستن</button>" +
    (n !== myName ? "<button class='primary' id='pf-mention'>منشن @</button>" : "") + "</div></div>");
  $("pf-close").onclick = closeDialog;
  if (n !== myName) $("pf-mention").onclick = function () { closeDialog(); input.value += "@" + n + " "; autoGrow(); updateSendBtn(); input.focus(); };
}

// ---------- settings ----------
function settingsDialog() {
  var html = "<h3>⚙️ تنظیمات</h3>" +
    "<div class='frow'><label>اسم تو</label><input type='text' id='st-name' maxlength='24' value='" + esc(myName) + "'></div>" +
    "<div class='frow'><label>بیو (در این روم)</label><input type='text' id='st-bio' maxlength='100' value='" + esc(me.bio) + "' placeholder='چند کلمه درباره خودت…'></div>" +
    "<div class='frow'><label>رنگ آواتار</label><div class='colors' id='st-colors'></div></div>" +
    '<div class="toggle' + (theme === "light" ? " on" : "") + '" id="st-theme"><span>حالت روشن</span><span class="sw"></span></div>' +
    '<div class="toggle' + (soundOn ? " on" : "") + '" id="st-sound"><span>صدای پیام</span><span class="sw"></span></div>' +
    '<div class="toggle' + (notifOn ? " on" : "") + '" id="st-notif"><span>اعلان پیام‌ها</span><span class="sw"></span></div>' +
    '<div class="toggle' + (wallOn ? " on" : "") + '" id="st-wall"><span>والپیپر چت</span><span class="sw"></span></div>' +
    "<div class='btnrow'><button class='primary' id='st-save'>ذخیره</button><button class='ghostbtn' id='st-close'>بستن</button></div>" +
    "<p style='font-size:11px;color:var(--faint);margin-top:12px;text-align:center'>چت پارسا v" + VERSION + "</p>";
  openDialog(html);
  var selColor = me.color;
  var cbox = $("st-colors");
  for (var i = 0; i < PALETTE.length; i++) {
    (function (c) {
      var b = document.createElement("button");
      b.style.background = c;
      if (c === selColor) b.classList.add("sel");
      b.onclick = function () {
        selColor = c;
        var all = cbox.querySelectorAll("button");
        for (var j = 0; j < all.length; j++) all[j].classList.remove("sel");
        b.classList.add("sel");
        send({ type: "set_color", color: c });
      };
      cbox.appendChild(b);
    })(PALETTE[i]);
  }
  function bindToggle(id, fn) { $(id).onclick = function () { this.classList.toggle("on"); fn(this.classList.contains("on")); }; }
  bindToggle("st-theme", function (on) { theme = on ? "light" : "dark"; LS.setItem("pc_theme", theme); applyTheme(); });
  bindToggle("st-sound", function (on) { soundOn = on; LS.setItem("pc_sound", on ? "1" : "0"); });
  bindToggle("st-notif", function (on) { notifOn = on; LS.setItem("pc_notif", on ? "1" : "0"); if (on) askNotifPermission(); });
  bindToggle("st-wall", function (on) { wallOn = on; LS.setItem("pc_wall", on ? "1" : "0"); applyTheme(); });
  $("st-save").onclick = function () {
    var n = $("st-name").value.trim().slice(0, 24);
    var bio = $("st-bio").value.trim();
    if (bio !== me.bio) send({ type: "set_bio", bio: bio });
    if (n && n !== myName) {
      myName = n; LS.setItem("pc_name", n);
      closeDialog(); toast("اسم عوض شد — اتصال دوباره…");
      Object.keys(lites).forEach(closeLite);
      wsClosedByUs = false;
      if (activeRoom) connect();
      ensureLites();
      return;
    }
    closeDialog(); toast("ذخیره شد");
  };
  $("st-close").onclick = closeDialog;
}

// ---------- poll creator ----------
function pollDialog() {
  var html = "<h3>📊 نظرسنجی جدید</h3>" +
    "<div class='frow'><label>سؤال</label><input type='text' id='pl-q' maxlength='200' placeholder='سؤال نظرسنجی…'></div>" +
    "<label>گزینه‌ها</label><div id='pl-opts'></div>" +
    "<button class='ghostbtn' id='pl-add' style='margin-top:2px'>＋ گزینه</button>" +
    '<div class="toggle" id="pl-multi"><span>چند انتخابی</span><span class="sw"></span></div>' +
    "<div class='btnrow'><button class='primary' id='pl-send'>ایجاد</button><button class='ghostbtn' id='pl-cancel'>انصراف</button></div>";
  openDialog(html);
  var multi = false;
  $("pl-multi").onclick = function () { this.classList.toggle("on"); multi = this.classList.contains("on"); };
  function addOpt(val) {
    var box = $("pl-opts");
    if (box.children.length >= 10) { toast("حداکثر ۱۰ گزینه"); return; }
    var row = document.createElement("div");
    row.className = "poll-opt-row";
    row.innerHTML = '<input type="text" maxlength="100" placeholder="گزینه ' + (box.children.length + 1) + '" value="' + esc(val || "") + '"><button class="iconbtn small">✕</button>';
    row.querySelector("button").onclick = function () { if (box.children.length > 2) row.remove(); else toast("حداقل ۲ گزینه لازمه"); };
    box.appendChild(row);
  }
  addOpt(); addOpt();
  $("pl-add").onclick = function () { addOpt(); };
  $("pl-cancel").onclick = closeDialog;
  $("pl-send").onclick = function () {
    var q = $("pl-q").value.trim();
    var opts = [];
    var inputs = $("pl-opts").querySelectorAll("input");
    for (var i = 0; i < inputs.length; i++) if (inputs[i].value.trim()) opts.push(inputs[i].value.trim());
    if (!q) { toast("سؤال رو بنویس"); return; }
    if (opts.length < 2) { toast("حداقل ۲ گزینه پر کن"); return; }
    var payload = { type: "poll", question: q, options: opts, multi: multi };
    if (replyToId) payload.replyTo = replyToId;
    if (send(payload)) { closeDialog(); cancelReplyEdit(); beep(660); }
  };
  $("pl-q").focus();
}

// ---------- Android back bridge ----------
window.__pcBack = function () {
  if ($("lightbox").classList.contains("show")) { closeLightbox(); return true; }
  if ($("ctxmenu").classList.contains("show") || $("attachmenu").classList.contains("show") || $("moremenu").classList.contains("show")) { hideMenus(); return true; }
  if ($("emojipanel").classList.contains("show")) { closeEmoji(); return true; }
  if (dlgOpen) { closeDialog(); return true; }
  if ($("searchbar").classList.contains("show")) { $("searchbar").classList.remove("show"); $("searchresults").classList.remove("show"); return true; }
  if ($("replybar").classList.contains("show")) { cancelReplyEdit(); return true; }
  if ($("sidebar").classList.contains("open")) { closeDrawer(); return true; }
  return false;
};

// ---------- init ----------
applyTheme();
renderRoomList();
$("welcome").classList.add("show");
if (!myName) { welcomeDialog(); }
else if (rooms.length) {
  var start = LS.getItem("pc_active");
  if (!roomEntry(start)) start = rooms[0].name;
  switchRoom(start);
} else { newRoomDialog(); }
window.addEventListener("beforeunload", saveDraft);
document.addEventListener("visibilitychange", function () { if (!document.hidden && nearBottom()) sendSeen(); });
})();
</script>
</body>
</html>`;
// __WEB_HTML_END__