// sw.js
const CACHE='tdr-app-v2';
const APP_SHELL=['/','/app.html','/manifest.json','/.netlify/functions/env','/icons/icon-192.png','/icons/icon-512.png','/icons/maskable-512.png'];
self.addEventListener('install',e=>{e.waitUntil(caches.open(CACHE).then(c=>c.addAll(APP_SHELL)));self.skipWaiting();});
self.addEventListener('activate',e=>{e.waitUntil(caches.keys().then(keys=>Promise.all(keys.filter(k=>k!==CACHE).map(k=>caches.delete(k)))));self.clients.claim();});
self.addEventListener('fetch',e=>{const u=new URL(e.request.url);const shell=u.origin===location.origin&&(u.pathname==='/'||u.pathname.endsWith('/app.html')||u.pathname.startsWith('/.netlify/functions/env')||u.pathname.startsWith('/icons/')||u.pathname==='/manifest.json');if(!shell)return;e.respondWith(fetch(e.request).then(r=>{caches.open(CACHE).then(c=>c.put(e.request,r.clone()));return r;}).catch(()=>caches.match(e.request)));});
self.addEventListener('message',ev=>{if(ev.data?.type==='SKIP_WAITING') self.skipWaiting();});
self.addEventListener('push',ev=>{let d={};try{d=ev.data.json()}catch{};ev.waitUntil(self.registration.showNotification(d.title||'通知',{body:d.body||'',data:d.meta||{},icon:'/icons/icon-192.png',badge:'/icons/icon-192.png'}));});
self.addEventListener('notificationclick',ev=>{ev.notification.close();const url='/app.html';ev.waitUntil(clients.matchAll({type:'window',includeUncontrolled:true}).then(cs=>{for(const c of cs){if(c.url.includes(url)&&'focus'in c) return c.focus();} if(clients.openWindow) return clients.openWindow(url);}));});
