#!/usr/bin/env node
// Sync the single-source client (worker/client.html) into:
//   1. worker/worker.js  (embedded WEB_HTML, dynamic host)
//   2. windows/index.html (hardcoded WebSocket host)
//   3. android/app/src/main/assets/index.html (hardcoded WebSocket host)
// Run from anywhere: node tools/sync-client.mjs
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const HOST = "parsa-chat.massagetoanishtayin.workers.dev";
const client = readFileSync(join(root, "worker", "client.html"), "utf8");

// 1. embed into worker.js between markers (escape for template literal)
const workerPath = join(root, "worker", "worker.js");
let worker = readFileSync(workerPath, "utf8");
const escaped = client.replace(/\\/g, "\\\\").replace(/`/g, "\\`").replace(/\$\{/g, "\\${");
const start = worker.indexOf("// __WEB_HTML_START__");
const end = worker.indexOf("// __WEB_HTML_END__");
if (start < 0 || end < 0) throw new Error("WEB_HTML markers not found in worker.js");
worker = worker.slice(0, start) + "// __WEB_HTML_START__\nconst WEB_HTML = `" + escaped + "`;\n" + worker.slice(end);
writeFileSync(workerPath, worker);

// 2+3. app builds with hardcoded host
const appClient = client.replace("proto + location.host", 'proto + "' + HOST + '"');
if (appClient === client) throw new Error("host marker not found in client.html");
writeFileSync(join(root, "windows", "index.html"), appClient);
writeFileSync(join(root, "android", "app", "src", "main", "assets", "index.html"), appClient);

console.log("synced client.html -> worker.js, windows/index.html, android assets (" + client.length + " bytes)");