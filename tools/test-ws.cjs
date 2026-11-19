// Live WebSocket test for Parsa Chat (via egress proxy):
// two clients in one room exchange messages, presence is checked,
// then a third client verifies history replay.
const WebSocket = require("ws");
const { HttpsProxyAgent } = require("https-proxy-agent");

const BASE = "wss://parsa-chat.massagetoanishtayin.workers.dev/ws";
const room = "test-" + Date.now();
const proxy = process.env.HTTPS_PROXY || process.env.https_proxy;
const agent = proxy ? new HttpsProxyAgent(proxy) : undefined;

function connect(name) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(BASE + "?room=" + encodeURIComponent(room) + "&name=" + encodeURIComponent(name), { agent });
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

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  const a = await connect("علی");
  const b = await connect("سارا");
  console.log("both connected to room:", room);

  const pres = await a.waitFor((d) => d.type === "presence" && d.users.length === 2);
  console.log("presence OK:", pres.users.join(", "));

  a.ws.send(JSON.stringify({ type: "msg", text: "سلام سارا! 👋" }));
  const got1 = await b.waitFor((d) => d.type === "msg" && d.name === "علی");
  console.log("B received from A:", got1.text);

  b.ws.send(JSON.stringify({ type: "msg", text: "سلام علی، رسید!" }));
  const got2 = await a.waitFor((d) => d.type === "msg" && d.name === "سارا");
  console.log("A received from B:", got2.text);

  const c = await connect("رضا");
  const hist = await c.waitFor((d) => d.type === "history");
  console.log("history replay count:", hist.messages.length);
  if (hist.messages.length !== 2) throw new Error("expected 2 history messages, got " + hist.messages.length);

  a.ws.close(); b.ws.close(); c.ws.close();
  await sleep(300);
  console.log("ALL WS TESTS PASSED");
  process.exit(0);
})().catch((e) => { console.error("TEST FAILED:", e.message); process.exit(1); });
