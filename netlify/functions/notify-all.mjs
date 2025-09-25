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
const START_HOUR_JST = 8;   // 08:00 から
const END_HOUR_JST   = 21;  // 21:59 まで

webpush.setVapidDetails('mailto:notify@example.com', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

// JST utils
const nowInJST = () => new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Tokyo' }));
const currentHourJST = () =>
  parseInt(new Date().toLocaleString('en-US', { timeZone: 'Asia/Tokyo', hour12: false, hour: '2-digit' }), 10);
const inQuietHours = () => {
  const h = currentHourJST();
  return !(h >= START_HOUR_JST && h <= END_HOUR_JST);
};

// テキスト整形・正規化
const norm = (s) => (s ?? '').toString().trim();

// 語彙マップ：DPA
const canonDpa = (s) => {
  const x = norm(s);
  if (['販売中'].includes(x)) return '販売中';
  if (['完売','売切','販売終了','在庫なし','売り切れ','SOLDOUT'].includes(x)) return '完売';
  if (['販売なし','取扱いなし','未販売','対象外'].includes(x)) return '販売なし';
  if (['一時停止','中止','停止','メンテ中','一時中断','準備中'].includes(x)) return '停止';
  return x || '-';
};
// 語彙マップ：PP
const canonPp = (s) => {
  const x = norm(s);
  if (['発行中'].includes(x)) return '発行中';
  if (['終了','配布終了','在庫なし','終了済','配布完了'].includes(x)) return '終了';
  if (['発行なし','取扱いなし','未配布','対象外'].includes(x)) return '発行なし';
  if (['一時停止','中止','停止','メンテ中','一時中断','準備中'].includes(x)) return '停止';
  return x || '-';
};

const isDpaActive = (s) => canonDpa(s) === '販売中';
const isPpActive  = (s) => canonPp(s) === '発行中';

// 送信処理
async function sendWebPush(sb, subs, title, body, url = '/') {
  if (!subs.length) return;
  // 任意：UI用の簡易ログ
  try { await sb.from('notifications').insert({ title, body }); } catch (_) {}
  for (const s of subs) {
    const sub = { endpoint: s.endpoint, keys: { p256dh: s.p256dh, auth: s.auth } };
    try {
      await webpush.sendNotification(sub, JSON.stringify({ title, body, url }));
    } catch (err) {
      // dead endpoint を掃除
      if (err?.statusCode === 410 || err?.statusCode === 404) {
        try { await sb.from('push_subscriptions').delete().eq('endpoint', s.endpoint); } catch (_) {}
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

    // 静音時間帯はスキップ（?force=1 で無視）
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

    // 宛先
    const { data: subsPush } = await sb.from('push_subscriptions').select('endpoint,p256dh,auth');
    const pushSubs = subsPush || [];
    const { data: poProfiles } = await sb.from('pushover_profiles').select('user_key');
    const poUsers = [...new Set((poProfiles || []).map(p => p.user_key).filter(Boolean))];

    // has_changed 行のみ
    const { data: changed, error: eChg } = await sb
      .from('attraction_state')
      .select('id,park_id,name_ja,inopen_bef,inopen_now,dpastatus_bef,dpastatus_now,ppstatus_bef,ppstatus_now,has_changed')
      .eq('has_changed', true);
    if (eChg) {
      console.warn('attraction_state query error:', eChg.message);
      return { statusCode: 500, body: 'query error' };
    }

    let countOpen = 0;
    let countDpa  = 0;
    let countPp   = 0;

    for (const r of (changed || [])) {
      // 1) 運営（休止/再開）
      if ((r.inopen_bef ?? null) !== (r.inopen_now ?? null)) {
        const was = r.inopen_bef ? '運営中' : '休止';
        const now = r.inopen_now ? '運営中' : '休止';
        const title = `${r.name_ja} が${r.inopen_now ? '再開' : '休止'}`;
        const body  = `状態: ${was} → ${now}`;
        await sendWebPush(sb, pushSubs, title, body, '/');
        await sendPushover(title, body, '/', PUSHOVER_TOKEN, poUsers);
        countOpen++;
      }

      // 2) DPA：境界も非境界も整然と通知（ただし語彙正規化で“意味の無い表記差”はまとめる）
      {
        const wasRaw = r.dpastatus_bef, nowRaw = r.dpastatus_now;
        const was = canonDpa(wasRaw),   now = canonDpa(nowRaw);
        if (was !== now) {
          const wasActive = (was === '販売中');
          const nowActive = (now === '販売中');
          const isBoundary = wasActive !== nowActive;
          const title = isBoundary
            ? `${r.name_ja}：DPA${nowActive ? '販売開始' : (now === '完売' ? '完売' : '販売終了')}`
            : `${r.name_ja}：DPAステータス変更`;
          const body  = `DPA: ${wasRaw || '-'} → ${nowRaw || '-'}`;
          await sendWebPush(sb, pushSubs, title, body, '/');
          await sendPushover(title, body, '/', PUSHOVER_TOKEN, poUsers);
          countDpa++;
        }
      }

      // 3) PP：同様
      {
        const wasRaw = r.ppstatus_bef, nowRaw = r.ppstatus_now;
        const was = canonPp(wasRaw),   now = canonPp(nowRaw);
        if (was !== now) {
          const wasActive = (was === '発行中');
          const nowActive = (now === '発行中');
          const isBoundary = wasActive !== nowActive;
          const title = isBoundary
            ? `${r.name_ja}：PP${nowActive ? '発行開始' : (now === '終了' ? '配布終了' : '発行終了')}`
            : `${r.name_ja}：PPステータス変更`;
          const body  = `PP: ${wasRaw || '-'} → ${nowRaw || '-'}`;
          await sendWebPush(sb, pushSubs, title, body, '/');
          await sendPushover(title, body, '/', PUSHOVER_TOKEN, poUsers);
          countPp++;
        }
      }
    }

    // 4) 同期（bef ← now, has_changed=false）
    let finalized = false;
    try { await sb.rpc('notify_finalize_reset'); finalized = true; } catch (e) {
      console.warn('notify_finalize_reset RPC failed; fallback to per-row updates.', e?.message);
    }
    if (!finalized && (changed?.length || 0) > 0) {
      for (const r of changed) {
        try {
          await sb.from('attraction_state').update({
            inopen_bef: r.inopen_now,
            dpastatus_bef: r.dpastatus_now,
            ppstatus_bef: r.ppstatus_now,
            has_changed: false,
          }).eq('id', r.id);
        } catch (_) {}
      }
    }

    return {
      statusCode: 202,
      body: JSON.stringify({
        ok: true,
        window: { jst_now_iso: jstNow.toISOString(), allowed_hours: '08:00–21:59 JST', forced },
        notifications: {
          open_close: countOpen,
          dpa: countDpa,
          pp: countPp,
          total: countOpen + countDpa + countPp,
        }
      })
    };

  } catch (err) {
    console.error('notify-all error:', err);
    return { statusCode: 500, body: String(err?.message || err) };
  }
}
