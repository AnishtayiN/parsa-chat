// Re-apply the "NSIS on Linux without wine" patch to electron-builder.
// Run after `npm install`:  node apply-nsis-patch.js
// See patches/nsis-linux-without-wine.md for background.
const fs = require("fs");
const path = require("path");

const target = path.join(
  __dirname,
  "node_modules/app-builder-lib/out/targets/nsis/NsisTarget.js"
);
const needle = "        if ((0, macosVersion_1.isMacOsCatalina)()) {";
const replacement = [
  '        if (process.platform === "linux") {',
  "            // Parsa Chat build patch: extract the uninstaller by parsing the NSIS exe",
  "            // (electron-builder's own UninstallerReader) instead of running the bootstrap",
  "            // installer under wine, which is not available on this build VM.",
  "            await nsisUtil_1.UninstallerReader.exec(installerPath, uninstallerPath);",
  "        }",
  "        else if ((0, macosVersion_1.isMacOsCatalina)()) {",
].join("\n");

let src = fs.readFileSync(target, "utf8");
if (src.includes("Parsa Chat build patch")) {
  console.log("patch already applied");
  process.exit(0);
}
if (!src.includes(needle)) {
  console.error("needle not found — electron-builder version may have changed");
  process.exit(1);
}
src = src.replace(needle, replacement);
fs.writeFileSync(target, src);
console.log("patch applied to", target);
