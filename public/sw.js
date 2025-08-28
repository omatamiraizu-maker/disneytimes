const CACHE = 'tdr-app-v1';
const APP_SHELL = ['/', '/app.html', '/manifest.json', '/.netlify/functions/env'];

self.addEventListener('install', (e) => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(APP_SHELL)));
  self.skipWaiting();
});
self.addEventListener('activate', (e) => {
  e.waitUntil(
    caches.keys().then(keys => Promise.all(keys.filter(k=>k!==CACHE).map(k=>caches.delete(k))))
  );
  self.clients.claim();
});
// network-first（データは最新を優先／オフライン時はキャッシュ）
self.addEventListener('fetch', (e) => {
  const url = new URL(e.request.url);
  if (url.origin === location.origin && (url.pathname === '/' || url.pathname.endsWith('/app.html') || url.pathname.startsWith('/.netlify/functions/env'))) {
    e.respondWith(
      fetch(e.request).then(r => {
        const copy = r.clone();
        caches.open(CACHE).then(c => c.put(e.request, copy));
        return r;
      }).catch(()=>caches.match(e.request))
    );
  }
});

// 新しいSWを即有効化して古いのを置き換える
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));

// ページ側から「SKIP_WAITING」メッセージを受けたら即適用
self.addEventListener('message', (event) => {
  if (event.data && event.data.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});

// ADD: Push 受信 → 通知表示（iOS PWA 必須）
self.addEventListener('push', (event) => {
  let data = {};
  try { data = event.data.json(); } catch(e){}
  const title = data.title || '通知';
  const options = {
    body: data.body || '',
    data: data.meta || {},
    icon: '/icons/icon-192.png',
    badge: '/icons/icon-192.png'
  };
  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const url = '/app.html';
  event.waitUntil(clients.matchAll({ type: 'window', includeUncontrolled: true }).then(windowClients => {
    for (const client of windowClients) {
      if (client.url.includes(url) && 'focus' in client) return client.focus();
    }
    if (clients.openWindow) return clients.openWindow(url);
  }));
});
