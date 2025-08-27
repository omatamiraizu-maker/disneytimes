// public/sw.js - service worker for Web Push
self.addEventListener('install', (event) => {
  self.skipWaiting();
});
self.addEventListener('activate', (event) => {
  self.clients.claim();
});
self.addEventListener('push', (event) => {
  let data = {};
  try { data = event.data.json(); } catch {}
  const title = data.title || '通知';
  const options = {
    body: data.body || '',
    data,
    icon: '/icon.png',
    badge: '/icon.png',
  };
  event.waitUntil(self.registration.showNotification(title, options));
});
self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  event.waitUntil(clients.openWindow('/app.html'));
});