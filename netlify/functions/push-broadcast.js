// netlify/functions/push-broadcast.js
// Broadcast a notification to all subscribers (or a user) and insert into notifications table
import { createClient } from '@supabase/supabase-js';
import webpush from 'web-push';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
const VAPID_PUBLIC_KEY = process.env.VAPID_PUBLIC_KEY;
const VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY;

webpush.setVapidDetails(
  'mailto:admin@example.com',
  VAPID_PUBLIC_KEY,
  VAPID_PRIVATE_KEY
);

export const handler = async (event) => {
  if (event.httpMethod !== 'POST') return { statusCode: 405, body: 'Method not allowed' };
  const { kind = 'dpa-change', title = '更新', body = '', user_id = null, meta = {} } = JSON.parse(event.body || '{}');

  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });

  // Insert notification row
  const { data: inserted, error: nerr } = await sb.from('notifications')
    .insert({ user_id, kind, title, body, meta })
    .select('*')
    .single();
  if (nerr) return { statusCode: 500, body: JSON.stringify(nerr) };

  // Fetch subs
  let query = sb.from('push_subscriptions').select('*');
  if (user_id) query = query.eq('user_id', user_id);
  const { data: subs, error: serr } = await query;
  if (serr) return { statusCode: 500, body: JSON.stringify(serr) };

  // Send
  const payload = JSON.stringify({ title, body, meta, nid: inserted.id });
  const results = [];
  for (const s of subs) {
    const sub = { endpoint: s.endpoint, keys: { p256dh: s.p256dh, auth: s.auth } };
    try {
      await webpush.sendNotification(sub, payload);
      results.push({ endpoint: s.endpoint, ok: true });
    } catch (err) {
      results.push({ endpoint: s.endpoint, ok: false, error: String(err) });
    }
  }
  return { statusCode: 200, body: JSON.stringify({ ok: true, sent: results.length, results }) };
};