// netlify/functions/notify-all.mjs
import { createClient } from '@supabase/supabase-js';
import webpush from 'web-push';

const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE,
  VAPID_PUBLIC_KEY,
  VAPID_PRIVATE_KEY,
  PUSHOVER_TOKEN,
} = process.env;

// JSTの通知時間帯
const START_HOUR_JST = 8;   // 08:00〜
const END_HOUR_JST   = 21;  // 21:59まで（22:00以降は抑止）

webpush.setVapidDetails('mailto:notify@example.com', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

// JSTヘルパ
const nowInJST = () => new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Tokyo' }));
const currentHourJST = () =>
  parseInt(new Date().toLocaleString('en-US', { timeZone: 'Asia/Tokyo', hour12: false, hour: '2-digit' }), 10);
const inQuietHours = () => {
  const h = currentHourJST();
  return !(h >= START_HOUR_JST && h <= END_HOUR_JST);
};

// 通知送信
async function sendWebPush(sb, subs, title, body, url = '/') {
  if (!subs.length) return;
  await sb.from('notifications').insert({ title, body }).catch(() => {});
  for (const s of subs) {
    const sub = { endpoint: s.endpoint, keys: { p256dh: s.p256dh, auth: s.auth } };
    try {
      await webpush.sendNotification(sub, JSON.stringify({ title, body, url }));
    } catch (err) {
      if (err?.statusCode === 410 || err?.statusCode === 404) {
        await sb.from('push_subscriptions').delete().eq('endpoint', s.endpoint).catch(()=>{});
      }
    }
  }
}
async function sendPushover(title, message, url = '/', token, users) {
  if (!token || !users.length) return;
  for (const user of users) {
    const body = new URLSearchParams({
      token, user, title, message, url, url_title: '開く', priority: '0'
    });
    try {
      await fetch('https://api.pushover.net/1/messages.json', {
        method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body
      });
    } catch (_) {}
  }
}

// メッセージ生成（nameは正規化済みの日本語名）
function buildMessage(ev) {
  const n = ev.name_raw;
  switch (`${ev.kind}:${ev.event}`) {
    case 'open:reopen':   return { title: `${n} が再開`,         body: `状態: 休止 → 運営中` };
    case 'open:close':    return { title: `${n} が休止`,         body: `状態: 運営中 → 休止` };
    case 'dpa:dpa_start': return { title: `${n}：DPA販売開始`,   body: `DPAが販売中になりました` };
    case 'dpa:dpa_end':   return { title: `${n}：DPA販売終了`,   body: `DPAが販売終了/非販売になりました` };
    case 'pp:pp_start':   return { title: `${n}：PP発行開始`,    body: `PPが発行中になりました` };
    case 'pp:pp_end':     return { title: `${n}：PP発行終了`,    body: `PPが発行終了/非発行になりました` };
    default: return null;
  }
}

export async function handler(event) {
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });

  try {
    const url = new URL(event?.rawUrl || 'http://local.test');
    const forced = url.searchParams.get('force') === '1';
    const jstNow = nowInJST();

    if (!forced && inQuietHours()) {
      return {
        statusCode: 200,
        body: JSON.stringify({
          ok: true, muted: true,
          message: 'Quiet hours (JST 22:00–07:59). Skipped notifications.',
          jst: jstNow.toISOString(),
        }),
      };
    }

    // 通知先
    const { data: subsPush } = await sb.from('push_subscriptions').select('endpoint,p256dh,auth');
    const pushSubs = subsPush || [];
    const { data: poProfiles } = await sb.from('pushover_profiles').select('user_key');
    const poUsers = [...new Set((poProfiles || []).map(p => p.user_key).filter(Boolean))];

    // === 変更点：イベントキューから未送だけ取得 ===
    const { data: events, error: eEv } = await sb
      .from('event_queue')
      .select('id, kind, park_id, name_raw, event, changed_at')
      .is('sent_at', null)
      .order('changed_at', { ascending: true });
    if (eEv) {
      console.warn('event_queue:', eEv.message);
      return { statusCode: 500, body: 'event query error' };
    }

    let openCnt = 0, dpaCnt = 0, ppCnt = 0;
    const sentIds = [];

    for (const ev of (events || [])) {
      const msg = buildMessage(ev);
      if (!msg) continue;

      await sendWebPush(sb, pushSubs, msg.title, msg.body, '/');
      await sendPushover(msg.title, msg.body, '/', PUSHOVER_TOKEN, poUsers);

      sentIds.push(ev.id);
      if (ev.kind === 'open') openCnt++;
      if (ev.kind === 'dpa')  dpaCnt++;
      if (ev.kind === 'pp')   ppCnt++;
    }

    // 送信済みを確定
    if (sentIds.length) {
      await sb.from('event_queue').update({ sent_at: new Date().toISOString() }).in('id', sentIds);
    }

    return {
      statusCode: 202,
      body: JSON.stringify({
        ok: true,
        window: { jst_now_iso: jstNow.toISOString(), allowed_hours: '08:00–21:59 JST', forced },
        notifications: {
          open_close: openCnt,
          dpa_pp: dpaCnt + ppCnt,
          total: openCnt + dpaCnt + ppCnt
        }
      })
    };

  } catch (err) {
    console.error('notify-all error:', err);
    return { statusCode: 500, body: String(err?.message || err) };
  }
}
