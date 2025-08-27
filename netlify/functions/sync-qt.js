// netlify/functions/sync-qt.js
// Queue-Times を取得して Supabase に保存（毎分）
// 強化点:
//  - User-Agent/Accept ヘッダ付与
//  - /en-US/ 経路にフォールバック
//  - JSON揺れ (lands[].rides / rides[]) 両対応
//  - 失敗時の詳細エラー文字列化（[object Object] を撲滅）

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
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
    `https://queue-times.com/en-US/parks/${parkId}/queue_times.json?nocache=${Date.now()}`
  ];

  let lastErr;
  for (const url of urls) {
    try {
      const r = await fetch(url, { headers });
      const body = await r.text();
      if (!r.ok) throw new Error(`HTTP ${r.status} ${r.statusText} body=${body.slice(0, 180)}`);
      // JSONでない場合の保険
      let data;
      try { data = JSON.parse(body); }
      catch { throw new Error(`Invalid JSON body=${body.slice(0, 180)}`); }
      return data;
    } catch (e) {
      lastErr = e;
    }
  }
  throw lastErr;
}

export const handler = async () => {
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });

  const results = [];
  for (const parkId of QT_PARKS) {
    try {
      const data = await fetchQT(parkId);

      // rides の取り出し（どちらの形でもOKに）
      const rides = [];
      if (Array.isArray(data?.lands)) {
        for (const land of data.lands) for (const ride of land.rides || []) rides.push(ride);
      }
      if (Array.isArray(data?.rides)) for (const r of data.rides) rides.push(r);

      const rows = rides.map((ride) => ({
        park_id: parkId,
        attraction_name: ride.name,
        is_open: !!ride.is_open,
        wait_time: typeof ride.wait_time === 'number' ? ride.wait_time : null,
        last_reported_at: ride.last_updated || new Date().toISOString(),
      }));

      if (rows.length) {
        const { error } = await sb.from('queue_times').insert(rows);
        if (error) throw error;
      }

      results.push({
        parkId,
        ok: true,
        count: rows.length,
        summary: {
          lands: Array.isArray(data?.lands) ? data.lands.length : 0,
          ridesTopLevel: Array.isArray(data?.rides) ? data.rides.length : 0,
        },
      });
    } catch (e) {
      results.push({ parkId, ok: false, error: errStr(e) });
    }
  }

  return {
    statusCode: 200,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ results }),
  };
};
