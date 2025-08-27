// netlify/functions/sync-qt.js
// Scheduled every minute: fetch Queue-Times for each park, upsert into Supabase
import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
const QT_PARKS = (process.env.QT_PARKS || '274,275').split(',').map(s => parseInt(s.trim(), 10));

export const handler = async () => {
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });

  const results = [];
  for (const parkId of QT_PARKS) {
    try {
      const resp = await fetch(`https://queue-times.com/parks/${parkId}/queue_times.json`);
      if (!resp.ok) throw new Error(`queue-times HTTP ${resp.status}`);
      const data = await resp.json();
      // Normalize list of rides
      const rides = [];
      for (const land of data.lands || []) {
        for (const ride of land.rides || []) {
          rides.push({
            attraction_name: ride.name,
            is_open: ride.is_open,
            wait_time: ride.wait_time,
          });
        }
      }
      // Insert rows
      const rows = rides.map(r => ({
        park_id: parkId,
        attraction_name: r.attraction_name,
        is_open: !!r.is_open,
        wait_time: typeof r.wait_time === 'number' ? r.wait_time : null,
        last_reported_at: new Date().toISOString()
      }));
      const { error } = await sb.from('queue_times').insert(rows);
      if (error) throw error;
      results.push({ parkId, ok: true, count: rows.length });
    } catch (err) {
      results.push({ parkId, ok: false, error: String(err) });
    }
  }
  return {
    statusCode: 200,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ results }),
  };
};