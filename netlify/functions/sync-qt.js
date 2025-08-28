// netlify/functions/sync-qt.js
import { createClient } from '@supabase/supabase-js';
import webpush from 'web-push';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
const VAPID_PUBLIC_KEY = process.env.VAPID_PUBLIC_KEY;
const VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY;

webpush.setVapidDetails('mailto:admin@example.com', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

const QT_PARKS = (process.env.QT_PARKS || '274,275')
  .split(',').map(s=>parseInt(s.trim(),10)).filter(Boolean);

function errStr(e){
  if (!e) return 'unknown';
  if (typeof e === 'string') return e;
  const o = { name:e.name, message:e.message, status:e.status, statusText:e.statusText };
  return JSON.stringify(o).slice(0, 500);
}

async function fetchQT(parkId){
  const headers = {
    'User-Agent': 'Mozilla/5.0 (Netlify Function; QueueTimes)',
    'Accept': 'application/json',
    'Cache-Control': 'no-cache', 'Pragma': 'no-cache'
  };
  const urls = [
    `https://queue-times.com/parks/${parkId}/queue_times.json?nocache=${Date.now()}`,
    `https://queue-times.com/en-US/parks/${parkId}/queue_times.json?nocache=${Date.now()}`
  ];
  let lastErr;
  for (const url of urls){
    try{
      const r = await fetch(url, { headers });
      const body = await r.text();
      if (!r.ok) throw new Error(`HTTP ${r.status} ${r.statusText} body=${body.slice(0,180)}`);
      return JSON.parse(body);
    }catch(e){ lastErr=e; }
  }
  throw lastErr;
}

async function sendPushToUsers(sb, userIds, title, body, meta = {}){
  if (!userIds?.length) return { sent:0 };
  await sb.from('notifications').insert(userIds.map(uid => ({ user_id:uid, kind:meta.kind||'info', title, body, meta })));
  const { data: subs } = await sb.from('push_subscriptions').select('*').in('user_id', userIds);
  let sent=0; const payload = JSON.stringify({ title, body, meta });
  for (const s of subs||[]){
    const sub = { endpoint:s.endpoint, keys:{ p256dh:s.p256dh, auth:s.auth } };
    try{ await webpush.sendNotification(sub, payload); sent++; } catch(e){ /* 無効は無視 */ }
  }
  return { sent };
}

export const handler = async () => {
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth:{ persistSession:false } });

  // ★関数ハートビート（起動）
  try { await sb.from('function_heartbeats').insert({ name:'sync-qt', ok:true }); } catch {}

  const { data: parks } = await sb.from('parks').select('id, code, qt_park_id');
  const parksByQt = Object.fromEntries((parks||[]).map(p=>[p.qt_park_id, p]));
  const results = [];

  for (const qtParkId of QT_PARKS){
    try{
      const park = parksByQt[qtParkId];
      if (!park) throw new Error(`parks not found for qt_park_id=${qtParkId}`);

      const data = await fetchQT(qtParkId);

      // 直近の状態（施設ごと最新1件）
      const { data: prevRows } = await sb
        .from('queue_times')
        .select('attraction_name, is_open, wait_time, fetched_at')
        .eq('park_id', qtParkId)
        .order('fetched_at', { ascending:false })
        .limit(1000);

      const latestByName = new Map();
      for (const row of prevRows||[]){
        if (!latestByName.has(row.attraction_name)) latestByName.set(row.attraction_name, row);
      }

      // rides 抽出
      const rides = [];
      if (Array.isArray(data?.lands)) for (const land of data.lands) for (const ride of land.rides||[]) rides.push(ride);
      if (Array.isArray(data?.rides)) for (const r of data.rides) rides.push(r);

      // INSERT（※ fetched_at は DB の default now() で良いが、明示するなら nowIso を入れる）
      const nowIso = new Date().toISOString();
      const rows = rides.map(ride => ({
        park_id: qtParkId,
        attraction_name: ride.name,
        is_open: !!ride.is_open,
        wait_time: (typeof ride.wait_time==='number') ? ride.wait_time : null,
        fetched_at: nowIso              // ★ テーブルに合わせてこれだけ
      }));
      if (rows.length){
        const { error } = await sb.from('queue_times').insert(rows);
        if (error) throw error;
      }

      // お気に入り通知
      const { data: favs } = await sb
        .from('user_favorites')
        .select('user_id, attraction_name')
        .eq('park_id', qtParkId);

      const usersByAttr = new Map();
      for (const f of favs||[]){
        if (!usersByAttr.has(f.attraction_name)) usersByAttr.set(f.attraction_name, new Set());
        usersByAttr.get(f.attraction_name).add(f.user_id);
      }

      let notified=0;
      for (const cur of rows){
        const prev = latestByName.get(cur.attraction_name);
        const watchers = Array.from(usersByAttr.get(cur.attraction_name) || []);
        if (!prev) continue;

        const openChanged = prev.is_open !== cur.is_open;
        const waitPrev = (typeof prev.wait_time==='number') ? prev.wait_time : null;
        const waitCur  = (typeof cur.wait_time==='number') ? cur.wait_time : null;
        const waitDelta = (waitPrev!=null && waitCur!=null) ? (waitCur - waitPrev) : 0;
        const spike = Math.abs(waitDelta) >= 20;

        if (openChanged && watchers.length){
          const title = cur.is_open ? `【運営再開】${cur.attraction_name}` : `【運営中止】${cur.attraction_name}`;
          const body  = cur.is_open ? `現在の待ち時間: ${waitCur ?? '-'}分` : '現在、運営が中止されています。';
          await sendPushToUsers(sb, watchers, title, body, {
            kind: cur.is_open ? 'ride-reopen':'ride-closed',
            park_qt_id: qtParkId, name: cur.attraction_name, wait: waitCur
          });
          notified++;
        }else if (spike && watchers.length){
          const title = `【待ち時間急変】${cur.attraction_name}`;
          const body  = `待ち時間が ${waitPrev ?? '-'}→${waitCur ?? '-'} 分（${waitDelta>0?'+':''}${waitDelta}分）`;
          await sendPushToUsers(sb, watchers, title, body, {
            kind:'qt-spike', park_qt_id:qtParkId, name:cur.attraction_name, prev:waitPrev, cur:waitCur, delta:waitDelta
          });
          notified++;
        }
      }

      results.push({ parkId: qtParkId, ok:true, count: rows.length, notified });
    }catch(e){
      // ★失敗も心拍に記録
      try {
        await sb.from('function_heartbeats').insert({ name:'sync-qt', ok:false, note: errStr(e) });
      } catch {}
      results.push({ parkId: qtParkId, ok:false, error: errStr(e) });
    }
  }

  return { statusCode:200, headers:{'Content-Type':'application/json'}, body: JSON.stringify({ results }) };
};
