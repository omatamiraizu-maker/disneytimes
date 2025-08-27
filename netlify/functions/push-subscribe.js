// netlify/functions/push-subscribe.js
// Store web push subscription for the logged-in user
import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;

export const handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: 'Method not allowed' };
  }
  const authUserId = event.headers['x-user-id'] || null; // pass supabase auth uid from client after verifying
  const body = JSON.parse(event.body || '{}');
  const { endpoint, keys } = body || {};
  if (!endpoint || !keys?.p256dh || !keys?.auth || !authUserId) {
    return { statusCode: 400, body: 'Bad request' };
  }
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });
  const { error } = await sb.from('push_subscriptions').upsert({
    user_id: authUserId, endpoint, p256dh: keys.p256dh, auth: keys.auth
  }, { onConflict: 'endpoint' });
  if (error) return { statusCode: 500, body: JSON.stringify(error) };
  return { statusCode: 200, body: JSON.stringify({ ok: true }) };
};