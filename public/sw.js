self.addEventListener('push', (event) => {
  let data = {};
  try { data = event.data?.json() || {}; } catch(_) {}
  const title = data.title || '通知';
  const body  = data.body  || '';
  const url   = data.url   || '/';
  event.waitUntil(
    self.registration.showNotification(title, { body, data: { url } })
  );
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const url = event.notification.data?.url || '/';
  event.waitUntil(
    clients.matchAll({ type:'window', includeUncontrolled:true }).then(list => {
      for (const c of list) { if ('focus' in c) { c.navigate(url); return c.focus(); } }
      if (clients.openWindow) return clients.openWindow(url);
    })
  );
});
