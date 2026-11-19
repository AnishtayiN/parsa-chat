// Parsa Chat — Windows desktop app (Electron 22, runs on Windows 7/8/10/11)
// Loads the bundled web client; the chat server is the Cloudflare Worker at
// parsa-chat.massagetoanishtayin.workers.dev (WebSocket host is hardcoded in index.html).

const { app, BrowserWindow, Menu, shell } = require("electron");
const path = require("path");

app.setName("Parsa Chat");

// Single instance: focus the existing window instead of opening a second copy.
const gotLock = app.requestSingleInstanceLock();
if (!gotLock) {
  app.quit();
} else {
  let mainWindow = null;

  function createWindow() {
    mainWindow = new BrowserWindow({
      width: 1100,
      height: 750,
      minWidth: 800,
      minHeight: 550,
      title: "Parsa Chat",
      autoHideMenuBar: true,
      backgroundColor: "#0f1115",
      webPreferences: {
        nodeIntegration: false,
        contextIsolation: true,
      },
    });

    mainWindow.loadFile(path.join(__dirname, "index.html"));

    // Open external links in the default browser, not inside the app.
    mainWindow.webContents.setWindowOpenHandler(({ url }) => {
      if (url.startsWith("http://") || url.startsWith("https://")) {
        shell.openExternal(url);
      }
      return { action: "deny" };
    });

    mainWindow.on("closed", () => {
      mainWindow = null;
    });
  }

  app.on("second-instance", () => {
    if (mainWindow) {
      if (mainWindow.isMinimized()) mainWindow.restore();
      mainWindow.focus();
    }
  });

  app.whenReady().then(() => {
    Menu.setApplicationMenu(null);
    createWindow();

    app.on("activate", () => {
      if (BrowserWindow.getAllWindows().length === 0) createWindow();
    });
  });

  app.on("window-all-closed", () => {
    app.quit();
  });
}
