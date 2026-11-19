# electron-builder 24.13.3 — build NSIS on Linux without wine
#
# Problem: electron-builder generates the NSIS uninstaller by compiling a
# bootstrap installer and RUNNING it. On Linux it runs it under wine
# (NsisTarget.js -> execWine), so `electron-builder --win nsis` fails with
# "wine is required" even when win.signAndEditExecutable=false (that flag only
# skips rcedit/signing of the app exe, not the uninstaller bootstrap).
#
# Fix: on Linux, extract the uninstaller by parsing the bootstrap NSIS exe
# with electron-builder's own UninstallerReader (pure JS, used in production
# on macOS Catalina+ where wine is also unavailable).
#
# Apply after `npm install` (node_modules is not committed):
#   node apply-nsis-patch.js
# or manually edit node_modules/app-builder-lib/out/targets/nsis/NsisTarget.js
# in computeScriptAndSignUninstaller(), changing:
#
#     if ((0, macosVersion_1.isMacOsCatalina)()) {
#
# to:
#
#     if (process.platform === "linux") {
#         await nsisUtil_1.UninstallerReader.exec(installerPath, uninstallerPath);
#     }
#     else if ((0, macosVersion_1.isMacOsCatalina)()) {
