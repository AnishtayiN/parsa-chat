// Extended live test for Parsa Chat v2.0 (via egress proxy).
const WebSocket = require("ws");
const { HttpsProxyAgent } = require("https-proxy-agent");

const BASE = "wss://parsa-chat.massagetoanishtayin.workers.dev/ws";
const proxy = process.env.HTTPS_PROXY || process.env.https_proxy;
const agent = proxy ? new HttpsProxyAgent(proxy) : undefined;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function connect(name, room, pass) {
  return new Promise((resolve, reject) => {
    let url = BASE + "?room=" + encodeURIComponent(room) + "&name=" + encodeURIComponent(name);
    if (pass) url += "&pass=" + encodeURIComponent(pass);
    const ws = new WebSocket(url, { agent });
    const inbox = [];
    const waiters = [];
    ws.on("message", (data) => {
      const d = JSON.parse(data.toString());
      inbox.push(d);
      for (let i = waiters.length - 1; i >= 0; i--) {
        if (waiters[i].pred(d)) { waiters[i].resolve(d); waiters.splice(i, 1); }
      }
    });
    ws.on("error", (e) => reject(new Error("ws error for " + name + ": " + e.message)));
    ws.on("open", () => resolve({
      ws, inbox,
      waitFor(pred, ms = 10000) {
        const found = inbox.find(pred);
        if (found) return Promise.resolve(found);
        return new Promise((res, rej) => {
          waiters.push({ pred, resolve: res });
          setTimeout(() => rej(new Error("timeout waiting for message")), ms);
        });
      },
    }));
  });
}

function assert(cond, label) {
  if (!cond) throw new Error("ASSERT FAILED: " + label);
  console.log("OK:", label);
}

(async () => {
  const room = "v2test-" + Date.now();
  const a = await connect("ادمین", room);   // first member -> admin
  const b = await connect("کاربر", room);

  const histA = await a.waitFor((d) => d.type === "history");
  assert(histA.you.role === "admin", "first member is admin");
  const histB = await b.waitFor((d) => d.type === "history");
  assert(histB.you.role === "member", "second member is member");

  // basic message + reply
  a.ws.send(JSON.stringify({ type: "msg", text: "سلام دنیا" }));
  const m1 = await b.waitFor((d) => d.type === "msg" && d.message.text === "سلام دنیا");
  b.ws.send(JSON.stringify({ type: "msg", text: "پاسخ به سلام", replyTo: m1.message.id }));
  const m2 = await a.waitFor((d) => d.type === "msg" && d.message.replyTo === m1.message.id);
  assert(m2.message.text === "پاسخ به سلام", "reply delivered with replyTo");

  // edit (own only)
  a.ws.send(JSON.stringify({ type: "edit", id: m1.message.id, text: "سلام دنیا (ویرایش)" }));
  const ed = await b.waitFor((d) => d.type === "edited" && d.id === m1.message.id);
  assert(ed.text.includes("ویرایش"), "edit broadcast");

  // reaction toggle
  b.ws.send(JSON.stringify({ type: "react", id: m1.message.id, emoji: "❤️" }));
  const r1 = await a.waitFor((d) => d.type === "reaction" && d.id === m1.message.id && d.reactions.length === 1);
  assert(r1.reactions[0].count === 1, "reaction added");
  b.ws.send(JSON.stringify({ type: "react", id: m1.message.id, emoji: "❤️" }));
  const r2 = await a.waitFor((d) => d.type === "reaction" && d.id === m1.message.id && d.reactions.length === 0);
  assert(r2.reactions.length === 0, "reaction toggled off");

  // typing
  a.ws.send(JSON.stringify({ type: "typing", on: true }));
  const ty = await b.waitFor((d) => d.type === "typing" && d.name === "ادمین" && d.on === true);
  assert(!!ty, "typing indicator relayed");

  // seen
  b.ws.send(JSON.stringify({ type: "seen", upToId: m2.message.id }));
  const sn = await a.waitFor((d) => d.type === "seen" && d.name === "کاربر");
  assert(sn.upToId === m2.message.id, "seen receipt relayed");

  // pin (admin) + non-admin pin must fail silently
  b.ws.send(JSON.stringify({ type: "pin", id: m2.message.id }));
  await sleep(600);
  assert(!a.inbox.some((d) => d.type === "pinned"), "non-admin pin ignored");
  a.ws.send(JSON.stringify({ type: "pin", id: m1.message.id }));
  const pn = await b.waitFor((d) => d.type === "pinned" && d.message && d.message.id === m1.message.id);
  assert(!!pn, "admin pin broadcast");

  // search
  b.ws.send(JSON.stringify({ type: "search", q: "پاسخ" }));
  const sr = await b.waitFor((d) => d.type === "search_results");
  assert(sr.messages.length === 1 && sr.messages[0].text === "پاسخ به سلام", "search found message");

  // media (tiny fake image payload)
  const tinyB64 = Buffer.from("fakejpegdata").toString("base64");
  a.ws.send(JSON.stringify({ type: "media", kind: "image", data: tinyB64, text: "" }));
  const md = await b.waitFor((d) => d.type === "msg" && d.message.kind === "image");
  assert(md.message.data === tinyB64, "image message delivered");

  // delete (admin deletes other's message)
  a.ws.send(JSON.stringify({ type: "delete", id: m2.message.id }));
  const del = await b.waitFor((d) => d.type === "deleted" && d.id === m2.message.id);
  assert(!!del, "admin delete broadcast");

  // history_before
  b.ws.send(JSON.stringify({ type: "history_before", beforeId: md.message.id }));
  const older = await b.waitFor((d) => d.type === "older");
  assert(older.messages.length >= 2, "older history loaded (" + older.messages.length + ")");

  // password: admin sets, wrong pass rejected, right pass accepted
  a.ws.send(JSON.stringify({ type: "set_password", password: "secret123" }));
  await sleep(500);
  const bad = await connect("مهمان", room, "wrongpass");
  const err = await bad.waitFor((d) => d.type === "error");
  assert(err.text.includes("رمز"), "wrong password rejected");
  bad.ws.close();
  const good = await connect("مهمان", room, "secret123");
  const hg = await good.waitFor((d) => d.type === "history");
  assert(hg.hasPassword === true, "correct password accepted");

  // kick: admin kicks member, banned rejoin rejected
  const kickedP = b.waitFor((d) => d.type === "kicked");
  a.ws.send(JSON.stringify({ type: "kick", name: "کاربر" }));
  await kickedP;
  assert(true, "kick delivered");
  await sleep(400);
  const rejoin = await connect("کاربر", room, "secret123");
  const err2 = await rejoin.waitFor((d) => d.type === "error");
  assert(err2.text.includes("اخراج"), "banned rejoin rejected");
  rejoin.ws.close();

  a.ws.close(); b.ws.close(); good.ws.close();
  await sleep(300);
  console.log("ALL V2 TESTS PASSED");
  process.exit(0);
})().catch((e) => { console.error("TEST FAILED:", e.message); process.exit(1); });
