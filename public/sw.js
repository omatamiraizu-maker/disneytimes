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
