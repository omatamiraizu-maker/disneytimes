import { createClient } from '@supabase/supabase-js';
import webpush from 'web-push';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
const VAPID_PUBLIC_KEY = process.env.VAPID_PUBLIC_KEY;
const VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY;

webpush.setVapidDetails('mailto:admin@example.com', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

async function sendPush(sb, userIds, title, body, meta) {
  if (!userIds?.length) return 0;
  await sb.from('notifications').insert(userIds.map(uid => ({ user_id: uid, kind: meta.kind || 'info', title, body, meta })));
  const { data: subs } = await sb.from('push_subscriptions').select('*').in('user_id', userIds);
  const payload = JSON.stringify({ title, body, meta });
  let sent = 0;
  for (const s of subs || []) {
    try {
      await webpush.sendNotification({ endpoint: s.endpoint, keys: { p256dh: s.p256dh, auth: s.auth } }, payload);
      sent++;
    } catch (e) { /* 無効購読は握りつぶす */ }
  }
  return sent;
}

export const handler = async () => {
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });

  // ルール取得（ユーザーごとに window_minutes）
  const { data: rules } = await sb.from('user_alert_rules').select('user_id, park_id, window_minutes');
  const winByUserPark = new Map();
  for (const r of rules || []) {
    const k = `${r.user_id}:${r.park_id || -1}`;
    winByUserPark.set(k, r.window_minutes);
  }
  // デフォルト10分
  const getWindow = (uid, park) => winByUserPark.get(`${uid}:${park}`) ?? winByUserPark.get(`${uid}:-1`) ?? 10;

  // 直近30分のウィンドウ開始を対象（未通知のみ）
  const { data: rows, error } = await sb
    .from('dpa_purchases')
    .select('*')
    .eq('notified', false)
    .gte('slot_start', new Date(Date.now() - 30 * 60_000).toISOString())
    .lte('slot_start', new Date(Date.now() + 60 * 60_000).toISOString());
  if (error) return { statusCode: 500, body: JSON.stringify({ ok: false, error: String(error) }) };

  let touched = 0;
  for (const p of rows || []) {
    const minutesBefore = getWindow(p.user_id, p.park_id);
    const diffMin = Math.round((new Date(p.slot_start).getTime() - Date.now()) / 60000);
    if (diffMin === minutesBefore) {
      const title = `【もうすぐDPA】${p.attraction_name}`;
      const body = `${minutesBefore}分後に利用開始（${new Date(p.slot_start).toLocaleTimeString()}〜${new Date(p.slot_end).toLocaleTimeString()}）`;
      await sendPush(sb, [p.user_id], title, body, { kind: 'dpa-window', name: p.attraction_name, park_id: p.park_id, purchase_id: p.id });
      await sb.from('dpa_purchases').update({ notified: true }).eq('id', p.id);
      touched++;
    }
  }
  return { statusCode: 200, body: JSON.stringify({ ok: true, touched }) };
};
