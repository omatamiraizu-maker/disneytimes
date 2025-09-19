// netlify/functions/notify-all.mjs
import { createClient } from '@supabase/supabase-js';
import webpush from 'web-push';

// ==== 環境変数 ====
const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE,
  VAPID_PUBLIC_KEY,
  VAPID_PRIVATE_KEY,
  PUSHOVER_TOKEN,
} = process.env;

// ==== WebPush 初期化 ====
webpush.setVapidDetails('mailto:notify@example.com', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

// ==== 再通知抑制設定 ====
const SAME_STATE_WINDOW_MIN = 60;  // 同じ状態なら60分は再通知しない
const CHANGE_WINDOW_MIN = 10;      // 直近変化を検出する時間窓（分）

// ===========================
// メイン処理
// ===========================
export async function handler() {
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
    auth: { persistSession: false }
  });

  try {
    // ---------- 全購読者取得 ----------
    let { data: subsPush, error: ePush } = await sb
      .from('push_subscriptions')
      .select('endpoint,p256dh,auth');
    if (ePush) { console.error('push_subscriptions:', ePush.message); subsPush = []; }
    const allPushSubs = subsPush || [];

    let { data: poProfiles, error: ePo } = await sb
      .from('pushover_profiles')
      .select('user_key');
    if (ePo) { console.error('pushover_profiles:', ePo.message); poProfiles = []; }
    const allPoKeys = [...new Set((poProfiles || []).map(p => p.user_key).filter(Boolean))];

    // ---------- 通知送信関数 ----------
    async function sendWebPush(title, body, url = '/') {
      if (!allPushSubs.length) return;
      await sb.from('notifications').insert({ title, body });
      for (const s of allPushSubs) {
        const sub = { endpoint: s.endpoint, keys: { p256dh: s.p256dh, auth: s.auth } };
        try {
          await webpush.sendNotification(sub, JSON.stringify({ title, body, url }));
        } catch (err) {
          if (err?.statusCode === 410 || err?.statusCode === 404) {
            await sb.from('push_subscriptions').delete().eq('endpoint', s.endpoint);
          } else {
            console.warn('webpush error:', err?.statusCode, err?.message);
          }
        }
      }
    }

    async function sendPushover(title, message, url = '/') {
      if (!PUSHOVER_TOKEN || !allPoKeys.length) return;
      for (const user of allPoKeys) {
        const body = new URLSearchParams({
          token: PUSHOVER_TOKEN,
          user,
          title,
          message,
          url,
          url_title: '開く',
          priority: '0'
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

    // ---------- 重複判定 ----------
    async function shouldSendNotification(kind, park_id, name_raw, event) {
      const cutoffTime = new Date(Date.now() - SAME_STATE_WINDOW_MIN * 60 * 1000).toISOString();
      const { data: recent, error } = await sb
        .from('notified_events')
        .select('id, event, sent_at')
        .eq('kind', kind)
        .eq('park_id', park_id)
        .eq('name_raw', name_raw)
        .eq('event', event)
        .gte('sent_at', cutoffTime)
        .order('sent_at', { ascending: false })
        .limit(1);

      if (error) {
        console.warn('重複チェックエラー:', error.message);
        return true; // エラー時は通知を送る
      }
      if (recent && recent.length > 0) {
        console.log(`通知スキップ: ${name_raw} - ${event}`);
        return false;
      }
      return true;
    }

    // ---------- 日本語→英語キー変換 ----------
    let { data: qmap, error: eq } = await sb
      .from('v_queue_times_latest')
      .select('park_id,name_raw,name_ja');
    if (eq) { console.warn('v_queue_times_latest:', eq.message); qmap = []; }

    const ja2raw = new Map();
    (qmap || []).forEach(r => {
      ja2raw.set(`${r.park_id}::${r.name_ja}`, r.name_raw);
    });

    // ===================== A) 休止/再開 =====================
    let { data: openChanges, error: eOpen } =
      await sb.rpc('sp_recent_open_changes', { minutes: CHANGE_WINDOW_MIN });
    if (eOpen) { console.warn('sp_recent_open_changes:', eOpen.message); openChanges = []; }

    let openNotificationCount = 0;

    for (const ch of (openChanges || [])) {
      const was = ch.prev_open ? '運営中' : '休止';
      const now = ch.curr_open ? '運営中' : '休止';
      if (was === now) continue;
      const event = ch.curr_open ? 'reopen' : 'close';

      const shouldSend = await shouldSendNotification('open', ch.park_id, ch.name_raw, event);
      if (shouldSend) {
        const title = `${ch.name_ja} が${ch.curr_open ? '再開' : '休止'}`;
        const body  = `状態: ${was} → ${now}`;

        await sendWebPush(title, body, '/');
        await sendPushover(title, body, '/');

        await sb.from('notified_events').insert({
          kind: 'open',
          park_id: ch.park_id,
          name_raw: ch.name_raw,
          event,
          changed_at: ch.changed_at,
          sent_at: new Date().toISOString(),
        });

        openNotificationCount++;
        console.log(`通知送信: ${title}`);
      }
    }

    // ===================== B) DPA/PP =====================
    let { data: latest, error: eLatest } = await sb
      .from('v_attraction_dpa_latest')
      .select('park_id,name,dpa_status,pp40_status,fetched_at');
    if (eLatest) { console.warn('v_attraction_dpa_latest:', eLatest.message); latest = []; }

    const sinceISO = new Date(Date.now() - CHANGE_WINDOW_MIN * 60 * 1000).toISOString();
    let { data: recentHist, error: eHist } = await sb
      .from('attraction_status')
      .select('park_id,name_raw,dpa_status,pp40_status,fetched_at')
      .gte('fetched_at', sinceISO)
      .order('fetched_at', { ascending: false });
    if (eHist) { console.warn('attraction_status recent:', eHist.message); recentHist = []; }

    const prevMap = new Map();
    for (const r of (recentHist || [])) {
      const key = `${r.park_id}::${r.name_raw}`;
      if (!prevMap.has(key)) {
        prevMap.set(key, { dpa: r.dpa_status || null, pp: r.pp40_status || null, ts: r.fetched_at });
      }
    }

    let dpaNotificationCount = 0;

    for (const v of (latest || [])) {
      const park_id = v.park_id;
      const name_ja = v.name;
      const name_raw = ja2raw.get(`${park_id}::${name_ja}`);
      if (!name_raw) continue;

      const nowD = v.dpa_status || null;
      const nowP = v.pp40_status || null;
      const key = `${park_id}::${name_raw}`;
      const prev = prevMap.get(key) || { dpa: null, pp: null, ts: v.fetched_at };

      const notifications = [];
      if (prev.dpa !== '販売中' && nowD === '販売中') {
        notifications.push({ event: 'dpa_start', label: 'DPA販売開始' });
      } else if (prev.dpa === '販売中' && nowD !== '販売中') {
        notifications.push({ event: 'dpa_end', label: 'DPA販売終了' });
      }
      if (prev.pp !== '発行中' && nowP === '発行中') {
        notifications.push({ event: 'pp_start', label: 'PP発行開始' });
      } else if (prev.pp === '発行中' && nowP !== '発行中') {
        notifications.push({ event: 'pp_end', label: 'PP発行終了' });
      }

      for (const notif of notifications) {
        const shouldSend = await shouldSendNotification('dpa', park_id, name_raw, notif.event);
        if (shouldSend) {
          const title = `${name_ja}：${notif.label}`;
          const body  = `DPA: ${prev.dpa || '-'} → ${nowD || '-'} / PP: ${prev.pp || '-'} → ${nowP || '-'}`;

          await sendWebPush(title, body, '/');
          await sendPushover(title, body, '/');

          await sb.from('notified_events').insert({
            kind: 'dpa',
            park_id,
            name_raw,
            event: notif.event,
            changed_at: v.fetched_at,
            sent_at: new Date().toISOString(),
          });

          dpaNotificationCount++;
          console.log(`通知送信: ${title}`);
        }
      }
    }

    // 古い通知履歴を掃除（7日以上前）
    const cleanupDate = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
    await sb.from('notified_events').delete().lt('sent_at', cleanupDate);

    console.log(`通知完了: 休止/再開=${openNotificationCount}件, DPA/PP=${dpaNotificationCount}件`);
    return {
      statusCode: 202,
      body: JSON.stringify({
        ok: true,
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
