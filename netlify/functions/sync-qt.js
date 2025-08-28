// netlify/functions/sync-qt.js
// Queue-Times を取得して Supabase に保存（毎分実行）
// 追加: 直近スナップショットと比較して、運営中止/再開/待ち時間急変を自動通知（お気に入り登録者向け）
// 注意: VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY をNetlifyの環境変数に設定しておくこと
// 取り込み開始時に心拍を打つ
await supabase.from('function_heartbeats').insert({ name: 'sync-qt' });

import { createClient } from '@supabase/supabase-js';
import webpush from 'web-push';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
const VAPID_PUBLIC_KEY = process.env.VAPID_PUBLIC_KEY;
const VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY;

webpush.setVapidDetails('mailto:admin@example.com', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

const QT_PARKS = (process.env.QT_PARKS || '274,275')
  .split(',')
  .map((s) => parseInt(s.trim(), 10))
  .filter(Boolean);

function errStr(e) {
  if (!e) return 'unknown';
  if (typeof e === 'string') return e;
  const o = { name: e.name, message: e.message, stack: e.stack };
  if (e.status) o.status = e.status;
  if (e.statusText) o.statusText = e.statusText;
  if (e.cause) o.cause = String(e.cause);
  return JSON.stringify(o);
}

async function fetchQT(parkId) {
  const headers = {
    'User-Agent': 'Mozilla/5.0 (Netlify Function; QueueTimes)',
    Accept: 'application/json',
    'Cache-Control': 'no-cache',
    Pragma: 'no-cache',
  };
  const urls = [
    `https://queue-times.com/parks/${parkId}/queue_times.json?nocache=${Date.now()}`,
    `https://queue-times.com/en-US/parks/${parkId}/queue_times.json?nocache=${Date.now()}`,
  ];
  let lastErr;
  for (const url of urls) {
    try {
      const r = await fetch(url, { headers });
      const body = await r.text();
      if (!r.ok) throw new Error(`HTTP ${r.status} ${r.statusText} body=${body.slice(0, 180)}`);
      let data;
      try {
        data = JSON.parse(body);
      } catch {
        throw new Error(`Invalid JSON body=${body.slice(0, 180)}`);
      }
      return data;
    } catch (e) {
      lastErr = e;
    }
  }
  throw lastErr;
}

async function sendPushToUsers(sb, userIds, title, body, meta = {}) {
  if (!userIds?.length) return { sent: 0 };
  // 告知をDBにも残す（ユーザーごと）
  const inserts = userIds.map((uid) => ({ user_id: uid, kind: meta.kind || 'info', title, body, meta }));
  await sb.from('notifications').insert(inserts);

  // 該当ユーザーのPush購読を取得
  const { data: subs } = await sb.from('push_subscriptions').select('*').in('user_id', userIds);
  const payload = JSON.stringify({ title, body, meta });
  let sent = 0;

  for (const s of subs || []) {
    const sub = { endpoint: s.endpoint, keys: { p256dh: s.p256dh, auth: s.auth } };
    try {
      await webpush.sendNotification(sub, payload);
      sent++;
    } catch (e) {
      // 無効購読は放置（運用でGCすればOK）
      console.error('push error', e?.message || e);
    }
  }
  return { sent };
}

export const handler = async () => {
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });

  // parks マスタ（qt_park_id→parks.id の両方使えるようにする）
  const { data: parks } = await sb.from('parks').select('id, code, qt_park_id');
  const parksByQt = Object.fromEntries((parks || []).map((p) => [p.qt_park_id, p]));
  const results = [];

  for (const qtParkId of QT_PARKS) {
    try {
      const park = parksByQt[qtParkId];
      if (!park) throw new Error(`parks not found for qt_park_id=${qtParkId}`);

      const data = await fetchQT(qtParkId);

      // 直近の状態（施設ごとに最新1件）を事前にフェッチ
      const { data: prevRows } = await sb
        .from('queue_times')
        .select('attraction_name, is_open, wait_time, fetched_at')
        .eq('park_id', qtParkId) // ★FKは parks(qt_park_id) を参照する前提のスキーマに変更済み
        .order('fetched_at', { ascending: false })
        .limit(1000);

      const latestByName = new Map();
      for (const row of prevRows || []) {
        if (!latestByName.has(row.attraction_name)) latestByName.set(row.attraction_name, row);
      }

      // rides 取り出し
      const rides = [];
      if (Array.isArray(data?.lands)) for (const land of data.lands) for (const ride of land.rides || []) rides.push(ride);
      if (Array.isArray(data?.rides)) for (const r of data.rides) rides.push(r);

      // INSERT
      const nowIso = new Date().toISOString();
      const rows = rides.map((ride) => ({
        park_id: qtParkId, // ★274/275のままでOK（FKは parks.qt_park_id 参照）
        attraction_name: ride.name,
        is_open: !!ride.is_open,
        wait_time: typeof ride.wait_time === 'number' ? ride.wait_time : null,
        last_reported_at: ride.last_updated || nowIso,
      }));
      if (rows.length) {
        const { error } = await sb.from('queue_times').insert(rows);
        if (error) throw error;
      }

      // 変化検知（お気に入り登録者に通知）
      // 閾値: 待ち時間±20分 / is_open の true<->false
      const { data: favs } = await sb
        .from('user_favorites')
        .select('user_id, attraction_name')
        .eq('park_id', qtParkId);

      const usersByAttr = new Map(); // name -> Set(user_id)
      for (const f of favs || []) {
        if (!usersByAttr.has(f.attraction_name)) usersByAttr.set(f.attraction_name, new Set());
        usersByAttr.get(f.attraction_name).add(f.user_id);
      }

      let notified = 0;
      for (const cur of rows) {
        const prev = latestByName.get(cur.attraction_name);
        const watchers = Array.from(usersByAttr.get(cur.attraction_name) || []);
        if (!prev) continue; // 初回は比較不可

        const openChanged = prev.is_open !== cur.is_open;
        const waitPrev = typeof prev.wait_time === 'number' ? prev.wait_time : null;
        const waitCur = typeof cur.wait_time === 'number' ? cur.wait_time : null;
        const waitDelta = waitPrev != null && waitCur != null ? waitCur - waitPrev : 0;
        const spike = Math.abs(waitDelta) >= 20;

        if (openChanged && watchers.length) {
          if (!cur.is_open) {
            const title = `【運営中止】${cur.attraction_name}`;
            const body = `現在、運営が中止されています。`;
            await sendPushToUsers(sb, watchers, title, body, {
              kind: 'ride-closed',
              park_qt_id: qtParkId,
              name: cur.attraction_name,
            });
            notified++;
          } else {
            const title = `【運営再開】${cur.attraction_name}`;
            const body = `運営が再開されました。現在の待ち時間: ${waitCur ?? '-'}分`;
            await sendPushToUsers(sb, watchers, title, body, {
              kind: 'ride-reopen',
              park_qt_id: qtParkId,
              name: cur.attraction_name,
              wait: waitCur,
            });
            notified++;
          }
        } else if (spike && watchers.length) {
          const title = `【待ち時間急変】${cur.attraction_name}`;
          const body = `待ち時間が ${waitPrev ?? '-'}→${waitCur ?? '-'} 分（${waitDelta > 0 ? '+' : ''}${waitDelta}分）`;
          await sendPushToUsers(sb, watchers, title, body, {
            kind: 'qt-spike',
            park_qt_id: qtParkId,
            name: cur.attraction_name,
            prev: waitPrev,
            cur: waitCur,
            delta: waitDelta,
          });
          notified++;
        }
      }

      results.push({
        parkId: qtParkId,
        ok: true,
        count: rows.length,
        notified,
        summary: {
          lands: Array.isArray(data?.lands) ? data.lands.length : 0,
          ridesTopLevel: Array.isArray(data?.rides) ? data.rides.length : 0,
        },
      });
    } catch (e) {
      results.push({ parkId: qtParkId, ok: false, error: errStr(e) });
    }
  }

  return { statusCode: 200, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ results }) };
};
