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

// 通知許可の時間帯（JST）
const START_HOUR_JST = 8;   // 8:00〜
const END_HOUR_JST   = 21;  // 21:59 まで

webpush.setVapidDetails('mailto:notify@example.com', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

// JSTヘルパ
const nowInJST = () => new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Tokyo' }));
const currentHourJST = () =>
  parseInt(new Date().toLocaleString('en-US', { timeZone: 'Asia/Tokyo', hour12: false, hour: '2-digit' }), 10);
const inQuietHours = () => {
  const h = currentHourJST();
  return !(h >= START_HOUR_JST && h <= END_HOUR_JST);
};

// ステータス正規化＆アクティブ判定
const norm = (s) => (s ?? '').trim();
const isDpaActive = (s) => norm(s) === '販売中';
const isPpActive  = (s) => norm(s) === '発行中';

// 送信処理
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
      } else {
        console.warn('webpush error:', err?.statusCode, err?.message);
      }
    }
  }
}
async function sendPushover(title, message, url = '/', token, users) {
  if (!token || !users.length) return;
  for (const user of users) {
    const body = new URLSearchParams({ token, user, title, message, url, url_title: '開く', priority: '0' });
    try {
      await fetch('https://api.pushover.net/1/messages.json', {
        method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body
      });
    } catch (_) {}
  }
}

export async function handler(event) {
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });

  try {
    const url = new URL(event?.rawUrl || 'http://local.test');
    const forced = url.searchParams.get('force') === '1';
    const jstNow = nowInJST();

    // JST時間帯外はスキップ（?force=1 なら実行）
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

    // 変化フラグのみ
    const { data: changed, error: eChg } = await sb
      .from('attraction_state')
      .select('id,park_id,name_ja,inopen_bef,inopen_now,dpastatus_bef,dpastatus_now,ppstatus_bef,ppstatus_now,has_changed')
      .eq('has_changed', true);
    if (eChg) {
      console.warn('attraction_state query error:', eChg.message);
      return { statusCode: 500, body: 'query error' };
    }

    let openNotificationCount = 0;
    let dpaNotificationCount  = 0;

    for (const r of (changed || [])) {
      // 休止/再開
      if ((r.inopen_bef ?? null) !== (r.inopen_now ?? null)) {
        const was = r.inopen_bef ? '運営中' : '休止';
        const now = r.inopen_now ? '運営中' : '休止';
        const title = `${r.name_ja} が${r.inopen_now ? '再開' : '休止'}`;
        const body  = `状態: ${was} → ${now}`;
        await sendWebPush(sb, pushSubs, title, body, '/');
        await sendPushover(title, body, '/', PUSHOVER_TOKEN, poUsers);
        openNotificationCount++;
      }

      // DPA：販売中の境界出入りのみ（販売開始/販売終了/再販）
      {
        const wasActive = isDpaActive(r.dpastatus_bef);
        const nowActive = isDpaActive(r.dpastatus_now);
        if (wasActive !== nowActive) {
          const title = `${r.name_ja}：DPA${nowActive ? '販売開始' : '販売終了'}`;
          const body  = `DPA: ${r.dpastatus_bef || '-'} → ${r.dpastatus_now || '-'}`;
          await sendWebPush(sb, pushSubs, title, body, '/');
          await sendPushover(title, body, '/', PUSHOVER_TOKEN, poUsers);
          dpaNotificationCount++;
        }
      }

      // PP：発行中の境界出入りのみ（発行開始/発行終了/再開）
      {
        const wasActive = isPpActive(r.ppstatus_bef);
        const nowActive = isPpActive(r.ppstatus_now);
        if (wasActive !== nowActive) {
          const title = `${r.name_ja}：PP${nowActive ? '発行開始' : '発行終了'}`;
          const body  = `PP: ${r.ppstatus_bef || '-'} → ${r.ppstatus_now || '-'}`;
          await sendWebPush(sb, pushSubs, title, body, '/');
          await sendPushover(title, body, '/', PUSHOVER_TOKEN, poUsers);
          dpaNotificationCount++;
        }
      }
    }

    // 同期（bef=now & フラグ解除）
    let finalized = false;
    try { await sb.rpc('notify_finalize_reset'); finalized = true; } catch (e) {
      console.warn('notify_finalize_reset RPC failed, fallback to per-row updates.', e?.message);
    }
    if (!finalized && (changed?.length || 0) > 0) {
      for (const r of changed) {
        await sb.from('attraction_state').update({
          inopen_bef: r.inopen_now,
          dpastatus_bef: r.dpastatus_now,
          ppstatus_bef: r.ppstatus_now,
          has_changed: false,
        }).eq('id', r.id).catch(()=>{});
      }
    }

    return {
      statusCode: 202,
      body: JSON.stringify({
        ok: true,
        window: { jst_now_iso: jstNow.toISOString(), allowed_hours: '08:00–21:59 JST', forced },
        notifications: {
          open_close: openNotificationCount,
          dpa_pp: dpaNotificationCount,
          total: openNotificationCount + dpaNotificationCount
        }
      })
    };

  } catch (err) {
    console.error('notify-all error:', err);
    return { statusCode: 500, body: String(err?.message || err) };
  }
}
