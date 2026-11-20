// Live v3 protocol tests: polls, forward, multi-pin, roles, bio/color, topic,
// lite connections, seen, kick/unban, password, search, media.
// Usage: node test-v3.cjs   (override host: PC_HOST=... node test-v3.cjs)
const WebSocket = require("ws");
const { HttpsProxyAgent } = require("https-proxy-agent");

const HOST = process.env.PC_HOST || "parsa-chat.massagetoanishtayin.workers.dev";
const BASE = "wss://" + HOST + "/ws";
const room = "test-v3-" + Date.now();
const proxy = process.env.HTTPS_PROXY || process.env.https_proxy;
const agent = proxy ? new HttpsProxyAgent(proxy) : undefined;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function connect(name, opts = {}) {
  return new Promise((resolve, reject) => {
    let url = BASE + "?room=" + encodeURIComponent(room) + "&name=" + encodeURIComponent(name);
    if (opts.pass) url += "&pass=" + encodeURIComponent(opts.pass);
    if (opts.lite) url += "&lite=1";
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
      send: (o) => ws.send(JSON.stringify(o)),
      waitFor(pred, ms = 10000) {
        const found = inbox.find(pred);
        if (found) return Promise.resolve(found);
        return new Promise((res, rej) => {
          waiters.push({ pred, resolve: res });
          setTimeout(() => rej(new Error("timeout: " + name)), ms);
        });
      },
    }));
  });
}
const ok = (label) => console.log("PASS:", label);

(async () => {
  const a = await connect("ادمین");           // first member -> admin
  const b = await connect("سارا");
  const ha = await a.waitFor((d) => d.type === "history");
  if (ha.version !== "3.0.0") throw new Error("expected v3, got " + ha.version);
  if (ha.you.role !== "admin") throw new Error("first member should be admin");
  ok("connect + v3 history + first-member admin");

  // forward field
  a.send({ type: "msg", text: "سلام", fwdFrom: "رضا" });
  const fwd = await b.waitFor((d) => d.type === "msg" && d.message.fwdFrom === "رضا");
  ok("forward field (fwdFrom=" + fwd.message.fwdFrom + ")");

  // poll: create, vote, multi, close
  a.send({ type: "poll", question: "ناهار چی؟", options: ["پیتزا", "کباب", "سالاد"], multi: false });
  const pm = await b.waitFor((d) => d.type === "msg" && d.message.kind === "poll");
  const pollId = pm.message.id;
  b.send({ type: "vote", id: pollId, idx: [1] });
  const pv = await a.waitFor((d) => d.type === "poll" && d.id === pollId && d.poll.opts[1].v.includes("سارا"));
  b.send({ type: "vote", id: pollId, idx: [0] }); // single-choice switches
  await a.waitFor((d) => d.type === "poll" && d.id === pollId && d.poll.opts[0].v.includes("سارا") && !d.poll.opts[1].v.includes("سارا"));
  a.send({ type: "close_poll", id: pollId });
  await b.waitFor((d) => d.type === "poll" && d.id === pollId && d.poll.closed);
  ok("poll create/vote/switch/close");

  // multi-pin
  a.send({ type: "msg", text: "پین ۱" });
  const p1 = await b.waitFor((d) => d.type === "msg" && d.message.text === "پین ۱");
  a.send({ type: "msg", text: "پین ۲" });
  const p2 = await b.waitFor((d) => d.type === "msg" && d.message.text === "پین ۲");
  a.send({ type: "pin", id: p1.message.id });
  await b.waitFor((d) => d.type === "pinned" && d.messages.length === 1);
  a.send({ type: "pin", id: p2.message.id });
  await b.waitFor((d) => d.type === "pinned" && d.messages.length === 2);
  a.send({ type: "unpin", id: p1.message.id });
  await b.waitFor((d) => d.type === "pinned" && d.messages.length === 1 && d.messages[0].id === p2.message.id);
  ok("multiple pins + selective unpin");

  // promote / demote + member admin powers
  a.send({ type: "promote", name: "سارا" });
  await b.waitFor((d) => d.type === "members" && d.members.find((m) => m.name === "سارا" && m.role === "admin"));
  b.send({ type: "msg", text: "از ادمین جدید" });
  const bm = await a.waitFor((d) => d.type === "msg" && d.message.text === "از ادمین جدید");
  b.send({ type: "pin", id: bm.message.id });
  await a.waitFor((d) => d.type === "pinned" && d.messages.some((m) => m.id === bm.message.id));
  a.send({ type: "demote", name: "سارا" });
  await b.waitFor((d) => d.type === "members" && d.members.find((m) => m.name === "سارا" && m.role === "member"));
  ok("promote/demote + promoted admin can pin");

  // topic / bio / color
  a.send({ type: "set_topic", text: "روم تست v3" });
  await b.waitFor((d) => d.type === "topic" && d.text === "روم تست v3");
  b.send({ type: "set_bio", bio: "عاشق کباب" });
  await a.waitFor((d) => d.type === "members" && d.members.find((m) => m.name === "سارا" && m.bio === "عاشق کباب"));
  b.send({ type: "set_color", color: "#e5484d" });
  await a.waitFor((d) => d.type === "members" && d.members.find((m) => m.name === "سارا" && m.color === "#e5484d"));
  ok("topic + bio + avatar color");

  // lite connection: invisible in presence, receives messages, no join spam
  const lite = await connect("ناظر", { lite: true });
  await lite.waitFor((d) => d.type === "history");
  await sleep(400);
  const pres = a.inbox.filter((d) => d.type === "presence").pop();
  if (pres.users.some((u) => u.name === "ناظر")) throw new Error("lite user visible in presence");
  if (a.inbox.some((d) => d.type === "system" && d.text.includes("ناظر"))) throw new Error("lite join broadcast leaked");
  a.send({ type: "msg", text: "برای ناظر" });
  await lite.waitFor((d) => d.type === "msg" && d.message.text === "برای ناظر");
  ok("lite connection (hidden presence, live messages)");

  // seen receipts
  b.send({ type: "seen", upToId: p2.message.id });
  await a.waitFor((d) => d.type === "seen" && d.name === "سارا" && d.upToId === p2.message.id);
  ok("read receipts");

  // search + media
  a.send({ type: "search", q: "ناهار" });
  const sr = await a.waitFor((d) => d.type === "search_results");
  if (!sr.messages.length) throw new Error("search found nothing");
  a.send({ type: "media", kind: "file", data: "aGVsbG8=", text: "test.txt" });
  await b.waitFor((d) => d.type === "msg" && d.message.kind === "file" && d.message.data === "aGVsbG8=");
  ok("search + media");

  // kick + unban
  const c = await connect("قربی");
  await c.waitFor((d) => d.type === "history");
  a.send({ type: "kick", name: "قربی" });
  await c.waitFor((d) => d.type === "kicked");
  let bannedErr = null;
  try { const c2 = await connect("قربی"); bannedErr = await c2.waitFor((d) => d.type === "error" && d.code === "banned"); c2.ws.close(); } catch (e) {}
  if (!bannedErr) throw new Error("banned user could reconnect");
  a.send({ type: "unban", name: "قربی" });
  await a.waitFor((d) => d.type === "system" && d.text.includes("از بن خارج شد"));
  const c3 = await connect("قربی");
  await c3.waitFor((d) => d.type === "history");
  c3.ws.close();
  ok("kick + ban enforcement + unban");

  // password
  a.send({ type: "set_password", password: "raz123" });
  await b.waitFor((d) => d.type === "password" && d.hasPassword === true);
  const noPw = await connect("بدون‌رمز");
  const pwErr = await noPw.waitFor((d) => d.type === "error" && d.code === "bad_password");
  noPw.ws.close();
  const withPw = await connect("با‌رمز", { pass: "raz123" });
  await withPw.waitFor((d) => d.type === "history");
  withPw.ws.close();
  a.send({ type: "set_password", password: "" });
  await b.waitFor((d) => d.type === "password" && d.hasPassword === false);
  ok("room password set/enforce/remove");

  a.ws.close(); b.ws.close(); lite.ws.close();
  await sleep(300);
  console.log("ALL V3 TESTS PASSED");
  process.exit(0);
})().catch((e) => { console.error("TEST FAILED:", e.message); process.exit(1); });