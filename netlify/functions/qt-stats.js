import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;

export const handler = async (event) => {
  try {
    const url = new URL(event.rawUrl);
    const park = parseInt(url.searchParams.get('park') || '274', 10);
    const name = url.searchParams.get('name'); // raw名（英名）
    if (!name) return { statusCode: 400, body: 'missing name' };

    const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });

    const since = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000).toISOString();
    const { data, error } = await sb
      .from('queue_times')
      .select('wait_time,fetched_at')
      .eq('park_id', park)
      .eq('attraction_name', name)
      .gte('fetched_at', since);
    if (error) throw error;

    const now = new Date();
    const dow = now.getDay(); // 0=Sun
    const tgt = now.getHours() * 60 + now.getMinutes();

    const vals = [];
    for (const r of data || []) {
      if (typeof r.wait_time !== 'number') continue;
      const d = new Date(r.fetched_at);
      if (d.getDay() !== dow) continue;
      const mins = d.getHours() * 60 + d.getMinutes();
      if (Math.abs(mins - tgt) <= 30) vals.push(r.wait_time);
    }
    vals.sort((a,b)=>a-b);
    const median = vals.length ? (vals[Math.floor((vals.length-1)/2)] + vals[Math.ceil((vals.length-1)/2)]) / 2 : null;

    return { statusCode: 200, headers: { 'Content-Type':'application/json' }, body: JSON.stringify({ ok:true, median, n: vals.length }) };
  } catch (e) {
    return { statusCode: 500, body: JSON.stringify({ ok:false, error: String(e?.message || e) }) };
  }
};
