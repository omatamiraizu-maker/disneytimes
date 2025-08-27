// netlify/functions/sync-qt.js
// Queue-Times を取得して Supabase に保存（毎分）
// 改良点: User-Agent/Accept を明示、/en-US/ 経路のフォールバック、
// JSON 構造の揺れ(rides直下)対応、0件時の raw 概要もレスポンスに出す

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
const QT_PARKS = (process.env.QT_PARKS || '274,275')
  .split(',')
  .map((s) => parseInt(s.trim(), 10))
  .filter(Boolean);

async function fetchQT(parkId) {
  const headers = {
    'User-Agent': 'Mozilla/5.0 (Netlify Function; +https://queue-times.com/en-US)',
    Accept: 'application/json',
  };

  // 正式API
  let r = await fetch(`https://queue-times.com/parks/${parkId}/queue_times.json`, { headers });
  if (!r.ok) {
    // ロケール経路のフォールバック
    r = await fetch(`https://queue-times.com/en-US/parks/${parkId}/queue_times.json`, { headers });
  }
  if (!r.ok) throw new Error(`queue-times HTTP ${r.status}`);
  return r.json();
}

export const handler = async () => {
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });

  const results = [];
  for (const parkId of QT_PARKS) {
    try {
      const data = await fetchQT(parkId);

      // 1) lands[].rides 形式 2) rides[] 直下 どちらも拾う
      const rides = [];
      if (Array.isArray(data?.lands)) {
        for (const land of data.lands) {
          for (const ride of land.rides || []) {
            rides.push(ride);
          }
        }
      }
      if (Array.isArray(data?.rides) && data.rides.length) {
        for (const ride of data.rides) rides.push(ride);
      }

      // INSERT
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
    } catch (err) {
      results.push({ parkId, ok: false, error: String(err) });
    }
  }

  return { statusCode: 200, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ results }) };
};
