import { createClient } from '@supabase/supabase-js';

const sb = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE,
  { auth: { persistSession: false } }
);

export async function handler(event) {
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: 'POST only' };
  }
  try {
    const body = JSON.parse(event.body || '{}');
    // ブラウザの PushSubscription そのままを {subscription: ...} で送る想定
    const sub = body.subscription || body; // どちらでも許容
    const endpoint = sub?.endpoint;
    const p256dh = sub?.keys?.p256dh;
    const auth = sub?.keys?.auth;
    if (!endpoint || !p256dh || !auth) {
      return { statusCode: 400, body: 'endpoint,p256dh,auth required' };
    }

    const { error } = await sb.from('push_subscriptions').upsert(
      { endpoint, p256dh, auth },
      { onConflict: 'endpoint' }
    );
    if (error) throw error;

    return { statusCode: 200, body: JSON.stringify({ ok: true }) };
  } catch (e) {
    return { statusCode: 500, body: String(e?.message || e) };
  }
}
