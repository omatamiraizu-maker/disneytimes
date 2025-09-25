import { createClient } from '@supabase/supabase-js';
import webpush from 'web-push';

webpush.setVapidDetails('mailto:notify@example.com',
  process.env.VAPID_PUBLIC_KEY, process.env.VAPID_PRIVATE_KEY);

export async function handler() {
  const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE,
    { auth: { persistSession: false }});

  const { data, error } = await sb
    .from('push_subscriptions')
    .select('endpoint,p256dh,auth')
    .order('created_at', { ascending: false })
    .limit(1);

  if (error) return { statusCode: 500, body: error.message };
  if (!data?.length) return { statusCode: 404, body: 'no subscription' };

  const s = data[0];
  try {
    await webpush.sendNotification(
      { endpoint: s.endpoint, keys: { p256dh: s.p256dh, auth: s.auth } },
      JSON.stringify({ title: 'テスト通知', body: 'DB最新購読に送信', url: '/' })
    );
    return { statusCode: 200, body: 'sent' };
  } catch (e) {
    return { statusCode: 500, body: String(e?.message || e) };
  }
}
