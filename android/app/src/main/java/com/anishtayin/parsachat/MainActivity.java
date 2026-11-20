// Parsa Chat — Android app (WebView shell around the same chat client as web/Windows).
// Runs on Android 5.0+ (API 21). The bundled client is served from a secure
// https://appassets.androidplatform.net origin so voice messages (getUserMedia)
// work exactly like in the browser. Native bridges add: file downloads,
// notifications, microphone permission and the file picker.
package com.anishtayin.parsachat;

import android.app.Activity;
import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.content.ContentValues;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.os.Environment;
import android.provider.MediaStore;
import android.util.Base64;
import android.webkit.JavascriptInterface;
import android.webkit.PermissionRequest;
import android.webkit.ValueCallback;
import android.webkit.WebChromeClient;
import android.webkit.WebResourceRequest;
import android.webkit.WebResourceResponse;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.widget.Toast;

import androidx.webkit.WebViewAssetLoader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

public class MainActivity extends Activity {

    private static final int FILE_CHOOSER_REQUEST = 1001;
    private static final int MIC_PERMISSION_REQUEST = 1002;
    private static final int NOTIF_PERMISSION_REQUEST = 1003;
    private static final String APP_HOST = "appassets.androidplatform.net";
    private static final String START_URL = "https://" + APP_HOST + "/assets/index.html";
    private static final String CHANNEL_ID = "messages";

    private WebView webView;
    private ValueCallback<Uri[]> filePathCallback;
    private PermissionRequest pendingAudioRequest;
    private long lastBackPress;
    private int notificationId = 1;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        webView = new WebView(this);
        setContentView(webView);

        WebSettings s = webView.getSettings();
        s.setJavaScriptEnabled(true);
        s.setDomStorageEnabled(true);
        s.setMediaPlaybackRequiresUserGesture(false); // autoplay voice messages
        s.setAllowFileAccess(false);
        s.setAllowContentAccess(true);
        s.setSupportMultipleWindows(false);
        s.setUserAgentString(s.getUserAgentString() + " ParsaChatApp/3.0.0");
        if (Build.VERSION.SDK_INT >= 29) s.setForceDark(WebSettings.FORCE_DARK_OFF); // app has its own themes

        final WebViewAssetLoader assetLoader = new WebViewAssetLoader.Builder()
                .addPathHandler("/assets/", new WebViewAssetLoader.AssetsPathHandler(this))
                .build();

        webView.setWebViewClient(new WebViewClient() {
            @Override
            public boolean shouldOverrideUrlLoading(WebView view, WebResourceRequest request) {
                return handleUrl(request.getUrl());
            }

            @Override
            public boolean shouldOverrideUrlLoading(WebView view, String url) {
                return handleUrl(Uri.parse(url));
            }

            @Override
            public WebResourceResponse shouldInterceptRequest(WebView view, WebResourceRequest request) {
                return assetLoader.shouldInterceptRequest(request.getUrl());
            }

            @Override
            public WebResourceResponse shouldInterceptRequest(WebView view, String url) {
                return assetLoader.shouldInterceptRequest(Uri.parse(url));
            }

            @Override
            public void onPageFinished(WebView view, String url) {
                view.evaluateJavascript(BRIDGE_JS, null);
            }
        });

        webView.setWebChromeClient(new WebChromeClient() {
            @Override
            public void onPermissionRequest(final PermissionRequest request) {
                runOnUiThread(new Runnable() {
                    @Override
                    public void run() {
                        boolean wantsAudio = false;
                        for (String r : request.getResources()) {
                            if (PermissionRequest.RESOURCE_AUDIO_CAPTURE.equals(r)) wantsAudio = true;
                        }
                        if (!wantsAudio) { request.deny(); return; }
                        if (Build.VERSION.SDK_INT < 23
                                || checkSelfPermission(android.Manifest.permission.RECORD_AUDIO) == PackageManager.PERMISSION_GRANTED) {
                            request.grant(new String[]{PermissionRequest.RESOURCE_AUDIO_CAPTURE});
                        } else {
                            pendingAudioRequest = request;
                            requestPermissions(new String[]{android.Manifest.permission.RECORD_AUDIO}, MIC_PERMISSION_REQUEST);
                        }
                    }
                });
            }

            @Override
            public boolean onShowFileChooser(WebView view, ValueCallback<Uri[]> callback, FileChooserParams params) {
                if (filePathCallback != null) filePathCallback.onReceiveValue(null);
                filePathCallback = callback;
                try {
                    startActivityForResult(params.createIntent(), FILE_CHOOSER_REQUEST);
                } catch (Exception e) {
                    filePathCallback = null;
                    return false;
                }
                return true;
            }
        });

        webView.addJavascriptInterface(new Bridge(), "Android");

        createNotificationChannel();
        if (Build.VERSION.SDK_INT >= 33
                && checkSelfPermission(android.Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) {
            requestPermissions(new String[]{android.Manifest.permission.POST_NOTIFICATIONS}, NOTIF_PERMISSION_REQUEST);
        }

        webView.loadUrl(START_URL);
    }

    // Keep app-internal URLs in the WebView; open external links in the browser.
    private boolean handleUrl(Uri uri) {
        String scheme = uri.getScheme();
        if ("https".equals(scheme) && APP_HOST.equals(uri.getHost())) return false;
        if ("data".equals(scheme) || "blob".equals(scheme) || "about".equals(scheme)) return false;
        try {
            startActivity(new Intent(Intent.ACTION_VIEW, uri));
        } catch (Exception ignored) {
            // no handler for this scheme — ignore
        }
        return true;
    }

    private void createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= 26) {
            NotificationManager nm = getSystemService(NotificationManager.class);
            NotificationChannel channel = new NotificationChannel(
                    CHANNEL_ID, getString(R.string.notif_channel), NotificationManager.IMPORTANCE_HIGH);
            nm.createNotificationChannel(channel);
        }
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, String[] permissions, int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        if (requestCode == MIC_PERMISSION_REQUEST && pendingAudioRequest != null) {
            if (grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
                pendingAudioRequest.grant(new String[]{PermissionRequest.RESOURCE_AUDIO_CAPTURE});
            } else {
                pendingAudioRequest.deny();
            }
            pendingAudioRequest = null;
        }
    }

    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
        if (requestCode == FILE_CHOOSER_REQUEST && filePathCallback != null) {
            filePathCallback.onReceiveValue(WebChromeClient.FileChooserParams.parseResult(resultCode, data));
            filePathCallback = null;
        }
    }

    @Override
    public void onBackPressed() {
        if (webView != null) {
            // Let the client close its own overlays first (drawer, lightbox, dialogs…).
            webView.evaluateJavascript("(window.__pcBack && window.__pcBack())", value -> {
                if (!"true".equals(value)) defaultBack();
            });
            return;
        }
        super.onBackPressed();
    }

    private void defaultBack() {
        if (webView != null && webView.canGoBack()) {
            webView.goBack();
            return;
        }
        long now = System.currentTimeMillis();
        if (now - lastBackPress < 2000) {
            finish();
        } else {
            lastBackPress = now;
            Toast.makeText(this, R.string.back_to_exit, Toast.LENGTH_SHORT).show();
        }
    }

    @Override
    protected void onDestroy() {
        if (webView != null) webView.destroy();
        super.onDestroy();
    }

    private void toastOnUiThread(final String text) {
        runOnUiThread(new Runnable() {
            @Override
            public void run() {
                Toast.makeText(MainActivity.this, text, Toast.LENGTH_LONG).show();
            }
        });
    }

    // ---------- JS <-> native bridge ----------
    private class Bridge {

        // Saves a file sent in chat (data: URL from an <a download> click).
        @JavascriptInterface
        public void saveFile(String name, String mime, String base64) {
            try {
                byte[] bytes = Base64.decode(base64, Base64.DEFAULT);
                String safe = name == null ? "" : name.replaceAll("[\\\\/:*?\"<>|\\x00-\\x1F]", "_").trim();
                if (safe.isEmpty()) safe = "file";
                if (safe.length() > 100) safe = safe.substring(safe.length() - 100);
                if (mime == null || mime.isEmpty()) mime = "application/octet-stream";

                if (Build.VERSION.SDK_INT >= 29) {
                    ContentValues values = new ContentValues();
                    values.put(MediaStore.Downloads.DISPLAY_NAME, safe);
                    values.put(MediaStore.Downloads.MIME_TYPE, mime);
                    values.put(MediaStore.Downloads.RELATIVE_PATH, Environment.DIRECTORY_DOWNLOADS);
                    Uri uri = getContentResolver().insert(MediaStore.Downloads.EXTERNAL_CONTENT_URI, values);
                    if (uri == null) throw new Exception("insert failed");
                    OutputStream out = getContentResolver().openOutputStream(uri);
                    if (out == null) throw new Exception("open failed");
                    try { out.write(bytes); } finally { out.close(); }
                    toastOnUiThread(getString(R.string.saved_downloads, safe));
                } else {
                    File dir = getExternalFilesDir(Environment.DIRECTORY_DOWNLOADS);
                    if (dir == null) throw new Exception("no storage");
                    if (!dir.exists()) dir.mkdirs();
                    File file = new File(dir, safe);
                    int i = 1;
                    while (file.exists()) {
                        int dot = safe.lastIndexOf('.');
                        String base = dot > 0 ? safe.substring(0, dot) : safe;
                        String ext = dot > 0 ? safe.substring(dot) : "";
                        file = new File(dir, base + " (" + (i++) + ")" + ext);
                    }
                    FileOutputStream out = new FileOutputStream(file);
                    try { out.write(bytes); } finally { out.close(); }
                    toastOnUiThread(getString(R.string.saved_path, file.getAbsolutePath()));
                }
            } catch (Exception e) {
                toastOnUiThread(getString(R.string.save_failed));
            }
        }

        // Native notification for new messages while the app is in the background.
        @JavascriptInterface
        public void notify(String title, String body) {
            try {
                NotificationManager nm = getSystemService(NotificationManager.class);
                if (Build.VERSION.SDK_INT >= 24 && !nm.areNotificationsEnabled()) return;

                Intent intent = new Intent(MainActivity.this, MainActivity.class);
                int flags = PendingIntent.FLAG_UPDATE_CURRENT;
                if (Build.VERSION.SDK_INT >= 23) flags |= PendingIntent.FLAG_IMMUTABLE;
                PendingIntent pi = PendingIntent.getActivity(MainActivity.this, 0, intent, flags);

                Notification.Builder b = Build.VERSION.SDK_INT >= 26
                        ? new Notification.Builder(MainActivity.this, CHANNEL_ID)
                        : new Notification.Builder(MainActivity.this);
                b.setSmallIcon(R.drawable.ic_stat_chat)
                        .setContentTitle(title)
                        .setContentText(body)
                        .setAutoCancel(true)
                        .setContentIntent(pi)
                        .setColor(0xFF4F7CFF);
                if (Build.VERSION.SDK_INT < 26) b.setPriority(Notification.PRIORITY_HIGH);
                nm.notify(notificationId++, b.build());
            } catch (Exception ignored) {
                // notifications are best-effort
            }
        }
    }

    // Injected after page load: Notification shim + data:-URL download handler.
    private static final String BRIDGE_JS =
            "(function(){" +
            "if (window.__pcAndroid) return; window.__pcAndroid = true;" +
            "if (!window.Notification) {" +
            "  var N = function(title, opts){ try { Android.notify(String(title), (opts && opts.body) ? String(opts.body) : ''); } catch(e){} };" +
            "  N.permission = 'granted';" +
            "  N.requestPermission = function(cb){ if (cb) { try { cb('granted'); } catch(e){} } return Promise.resolve('granted'); };" +
            "  window.Notification = N;" +
            "}" +
            "document.addEventListener('click', function(e){" +
            "  var t = e.target; var a = (t && t.closest) ? t.closest('a[download]') : null;" +
            "  if (a && a.href && a.href.slice(0,5) === 'data:') {" +
            "    e.preventDefault(); e.stopPropagation();" +
            "    var i = a.href.indexOf(',');" +
            "    var mime = a.href.slice(5, i).split(';')[0] || 'application/octet-stream';" +
            "    try { Android.saveFile(a.getAttribute('download') || 'file', mime, a.href.slice(i+1)); } catch(err) {}" +
            "  }" +
            "}, true);" +
            "})();";
}