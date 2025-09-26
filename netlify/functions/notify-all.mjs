// netlify/functions/notify-all.mjs  — full replacement (event-consumer)
import { createClient } from '@supabase/supabase-js';
import webpush from 'web-push';

const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE,
  VAPID_PUBLIC_KEY,
  VAPID_PRIVATE_KEY,
  PUSHOVER_TOKEN,
} = process.env;

webpush.setVapidDetails('mailto:notify@example.com', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

export default async function handler() {
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });

  try {
    // ---- 送信先の準備（まとめて） ----
    const [{ data: subsPush, error: ePush }, { data: poProfiles, error: ePo }] = await Promise.all([
      sb.from('push_subscriptions').select('endpoint,p256dh,auth'),
      sb.from('pushover_profiles').select('user_key'),
    ]);
    if (ePush) console.warn('push_subscriptions:', ePush.message);
    if (ePo)   console.warn('pushover_profiles:', ePo.message);

    const allPushSubs = (subsPush || []).map(s => ({
      endpoint: s.endpoint, keys: { p256dh: s.p256dh, auth: s.auth }
    }));
    const allPoKeys = [...new Set((poProfiles || []).map(p => p.user_key).filter(Boolean))];

    async function sendWebPush(title, body, url = '/') {
      if (!allPushSubs.length) return;
      // （任意）通知ログ
      await sb.from('notifications').insert({ kind: 'system', title, body }).throwOnError();
      for (const sub of allPushSubs) {
        try {
          await webpush.sendNotification(sub, JSON.stringify({ title, body, url }));
        } catch (err) {
          if (err?.statusCode === 410 || err?.statusCode === 404) {
            // 期限切れ・無効endpointは掃除
            await sb.from('push_subscriptions').delete().eq('endpoint', sub.endpoint);
          } else {
            console.warn('webpush error:', err?.statusCode, err?.message);
          }
        }
      }
    }

    async function sendPushover(title, message, url = '/') {
      if (!PUSHOVER_TOKEN || !allPoKeys.length) return;
      const body = (user) => new URLSearchParams({
        token: PUSHOVER_TOKEN, user,
        title, message, url, url_title: '開く', priority: '0'
      });
      await Promise.allSettled(allPoKeys.map(user =>
        fetch('https://api.pushover.net/1/messages.json', {
          method: 'POST',
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          body: body(user)
        })
      ));
    }

    // ---- 未送イベントを古い順でバッチ取得 ----
    const BATCH = 200;
    const { data: events, error: e1 } = await sb
      .from('event_queue')
      .select('id, kind, park_id, name_raw, event, changed_at, uniq_key, attraction_id')
      .is('sent_at', null)
      .order('changed_at', { ascending: true })
      .limit(BATCH);
    if (e1) throw new Error(e1.message);

    if (!events?.length) {
      return { statusCode: 202, body: JSON.stringify({ ok: true, sent: 0 }) };
    }

    // 宛先解決（user_favorites / device_favorites）
    async function resolveAudience(park_id, name_ja) {
      const [ufRes, dfRes] = await Promise.all([
        sb.from('user_favorites').select('user_id').eq('park_id', park_id).eq('attraction_name', name_ja),
        sb.from('device_favorites').select('device_id').eq('park_id', park_id).eq('attraction_name', name_ja)
      ]);
      const uf = ufRes.data || [], df = dfRes.data || [];
      return { users: uf.map(x => x.user_id), devices: df.map(x => x.device_id) };
    }

    function parseEventBody(ev) {
      try { return JSON.parse(ev.event); } catch { return {}; }
    }

    function formatTitle(ev, b) {
      // b: parsed body
      switch (ev.kind) {
        case 'reopen': return `${ev.name_raw} が再開`;
        case 'close':  return `${ev.name_raw} が休止`;
        case 'dpa_start': return `${ev.name_raw}：DPA販売開始`;
        case 'dpa_end':   return `${ev.name_raw}：DPA販売終了`;
        case 'pp_start':  return `${ev.name_raw}：PP発行開始`;
        case 'pp_end':    return `${ev.name_raw}：PP発行終了`;
        case 'wait_spike':return `${ev.name_raw}：待ち時間スパイク`;
        default:          return `${ev.name_raw}：${ev.kind}`;
      }
    }

    let sentCount = 0;

    for (const ev of events) {
      // 既送（uniq_key）保険
      const { data: exist, error: e2 } = await sb
        .from('notified_events').select('id').eq('uniq_key', ev.uniq_key).limit(1);
      if (e2) console.warn('notified_events check:', e2.message);
      if (exist?.length) {
        await sb.from('event_queue').update({ sent_at: new Date().toISOString() }).eq('id', ev.id);
        continue;
      }

      // 宛先がいない場合も sent_at を埋めて消化
      const aud = await resolveAudience(ev.park_id, ev.name_raw);
      if (!aud.users.length && !aud.devices.length) {
        await sb.from('event_queue').update({ sent_at: new Date().toISOString() }).eq('id', ev.id);
        continue;
      }

      const bodyJson = parseEventBody(ev);
      const title = formatTitle(ev, bodyJson);
      const body  = ev.event; // そのまま（必要なら整形）

      await sendWebPush(title, body, '/');
      await sendPushover(title, body, '/');

      await sb.from('notified_events').insert({
        kind: ev.kind,
        park_id: ev.park_id,
        name_raw: ev.name_raw,
        event: ev.event,
        changed_at: ev.changed_at,
        sent_at: new Date().toISOString(),
        uniq_key: ev.uniq_key,
      }).throwOnError();

      await sb.from('event_queue').update({ sent_at: new Date().toISOString() }).eq('id', ev.id);
      sentCount++;
    }

    // 履歴掃除（任意）：7日より前の通知を削除
    const cutoff = new Date(Date.now() - 7*24*60*60*1000).toISOString();
    await sb.from('notified_events').delete().lt('sent_at', cutoff);

    return { statusCode: 202, body: JSON.stringify({ ok: true, sent: sentCount }) };

  } catch (err) {
    console.error('notify-all error:', err);
    return { statusCode: 500, body: String(err?.message || err) };
  }
}
