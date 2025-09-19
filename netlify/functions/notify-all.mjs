// netlify/functions/notify-all.mjs
// 役割: Supabaseのフラグ付き行だけ通知し、送信後に bef=now へ同期。
// 通知時間帯: JST 08:00〜21:59 のみ（それ以外は通知せず終了）

import { createClient } from '@supabase/supabase-js';
import webpush from 'web-push';

// ==== 環境変数 ====
const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE,   // Service Role Key を設定
  VAPID_PUBLIC_KEY,
  VAPID_PRIVATE_KEY,
  PUSHOVER_TOKEN,          // 使わない場合は未設定でもOK
} = process.env;

// ==== 通知許可の時間帯（JST）====
const START_HOUR_JST = 8;   // 8:00 から
const END_HOUR_JST   = 21;  // 21:59 まで（※21時ちょうども許可）

// ==== WebPush 初期化 ====
webpush.setVapidDetails('mailto:notify@example.com', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

// JSTの現在時刻オブジェクトを作るユーティリティ
function nowInJST() {
  const s = new Date().toLocaleString('en-US', { timeZone: 'Asia/Tokyo' });
  return new Date(s);
}
function currentHourJST() {
  return parseInt(
    new Date().toLocaleString('en-US', {
      timeZone: 'Asia/Tokyo',
      hour12: false,
      hour: '2-digit',
    }),
    10
  );
}
function inQuietHours() {
  const h = currentHourJST();
  // 8 <= h <= 21 の間だけ通知を許可（22時〜翌7時はサイレント）
  return !(h >= START_HOUR_JST && h <= END_HOUR_JST);
}

// ---- 通知送信（WebPush）----
async function sendWebPush(sb, subs, title, body, url = '/') {
  if (!subs.length) return;
  // 任意: 通知ログテーブルがあれば保存（失敗しても続行）
  await sb.from('notifications').insert({ title, body }).catch(() => {});
  // シンプルに逐次送信（購読数が多い場合はバッチ/Promise.allSettledに変更可）
  for (const s of subs) {
    const sub = { endpoint: s.endpoint, keys: { p256dh: s.p256dh, auth: s.auth } };
    try {
      await webpush.sendNotification(sub, JSON.stringify({ title, body, url }));
    } catch (err) {
      // 410/404 は無効購読。掃除して継続
      if (err?.statusCode === 410 || err?.statusCode === 404) {
        await sb.from('push_subscriptions').delete().eq('endpoint', s.endpoint).catch(() => {});
      } else {
        console.warn('webpush error:', err?.statusCode, err?.message);
      }
    }
  }
}

// ---- 通知送信（Pushover）----
async function sendPushover(title, message, url = '/', token, users) {
  if (!token || !users.length) return;
  for (const user of users) {
    const body = new URLSearchParams({
      token, user, title, message, url, url_title: '開く', priority: '0'
    });
    try {
      await fetch('https://api.pushover.net/1/messages.json', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body
      });
    } catch (_) {}
  }
}

export async function handler(event) {
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
    auth: { persistSession: false }
  });

  try {
    // ====== サイレント時間帯の抑制（JST基準）======
    // ただし ?force=1 を付けたアクセスはテスト用として強制実行可能
    const url = new URL(event?.rawUrl || 'http://local.test');
    const forced = url.searchParams.get('force') === '1';
    const jstNow = nowInJST();

    if (!forced && inQuietHours()) {
      return {
        statusCode: 200,
        body: JSON.stringify({
          ok: true,
          muted: true,
          message: 'Quiet hours (JST 22:00–07:59). Skipped notifications.',
          jst: jstNow.toISOString(),
        }),
      };
    }

    // ====== 通知先の取得 ======
    const { data: subsPush } = await sb
      .from('push_subscriptions')
      .select('endpoint,p256dh,auth');
    const pushSubs = subsPush || [];

    const { data: poProfiles } = await sb
      .from('pushover_profiles')
      .select('user_key');
    const poUsers = [...new Set((poProfiles || []).map(p => p.user_key).filter(Boolean))];

    // ====== 変化フラグ付き行を取得 ======
    const { data: changed, error: eChg } = await sb
      .from('attraction_state')
      .select('id,park_id,name_ja,inopen_bef,inopen_now,dpastatus_bef,dpastatus_now,ppstatus_bef,ppstatus_now,has_changed')
      .eq('has_changed', true);

    if (eChg) {
      console.warn('attraction_state query error:', eChg.message);
      return { statusCode: 500, body: 'query error' };
    }

    let openNotificationCount = 0;
    let dpaNotificationCount = 0;

    // ====== 通知本体 ======
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
      // DPA
      if ((r.dpastatus_bef ?? null) !== (r.dpastatus_now ?? null)) {
        const title = `${r.name_ja}：DPA状態変化`;
        const body  = `DPA: ${r.dpastatus_bef || '-'} → ${r.dpastatus_now || '-'}`;
        await sendWebPush(sb, pushSubs, title, body, '/');
        await sendPushover(title, body, '/', PUSHOVER_TOKEN, poUsers);
        dpaNotificationCount++;
      }
      // PP
      if ((r.ppstatus_bef ?? null) !== (r.ppstatus_now ?? null)) {
        const title = `${r.name_ja}：PP状態変化`;
        const body  = `PP: ${r.ppstatus_bef || '-'} → ${r.ppstatus_now || '-'}`;
        await sendWebPush(sb, pushSubs, title, body, '/');
        await sendPushover(title, body, '/', PUSHOVER_TOKEN, poUsers);
        dpaNotificationCount++;
      }
    }

    // ====== 同期（bef=now & フラグ解除）=====
    // 推奨: 事前に作った RPC を呼ぶ
    let finalized = false;
    try {
      await sb.rpc('notify_finalize_reset');
      finalized = true;
    } catch (e) {
      console.warn('notify_finalize_reset RPC failed, fallback to per-row updates.', e?.message);
    }

    // RPC が無い環境のフォールバック: 変更行を1件ずつ更新して bef=now に揃える
    if (!finalized && (changed?.length || 0) > 0) {
      for (const r of changed) {
        try {
          await sb
            .from('attraction_state')
            .update({
              inopen_bef: r.inopen_now,
              dpastatus_bef: r.dpastatus_now,
              ppstatus_bef: r.ppstatus_now,
              has_changed: false,
            })
            .eq('id', r.id);
        } catch (_) { /* 続行 */ }
      }
    }

    // ====== 応答 ======
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
