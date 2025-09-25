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

webpush.setVapidDetails('mailto:notify@example.com', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

// 同じイベントの再通知抑止（保険）。ただし今回の主抑止は uniq_key で行う
const SAME_STATE_WINDOW_MIN = 60;
// 「最近の変化」を拾う時間窓
const CHANGE_WINDOW_MIN = 10;

export async function handler() {
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
    auth: { persistSession: false }
  });

  try {
    // ===== 購読者取得 =====
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

    // ===== 送信関数 =====
    async function sendWebPush(title, body, url = '/') {
      if (!allPushSubs.length) return;
      await sb.from('notifications').insert({ title, body });
      // 逐次awaitで確実に（大量時はPromise.allSettledに切替可）
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

    // ====== 既送チェック（最優先：uniq_key で完全一致） ======
    async function alreadyNotifiedByKey(uniq_key) {
      const { data, error } = await sb
        .from('notified_events')
        .select('id')
        .eq('uniq_key', uniq_key)
        .limit(1);
      if (error) {
        console.warn('alreadyNotifiedByKey error:', error.message);
        return false; // エラー時は既送扱いにしない
      }
      return !!(data && data.length);
    }

    // ====== 二重送信の保険（同一イベントを時間窓で抑止） ======
    async function shouldSendByWindow(kind, park_id, name_raw, event) {
      const cutoffTime = new Date(Date.now() - SAME_STATE_WINDOW_MIN * 60 * 1000).toISOString();
      const { data, error } = await sb
        .from('notified_events')
        .select('sent_at')
        .eq('kind', kind)
        .eq('park_id', park_id)
        .eq('name_raw', name_raw)
        .eq('event', event)
        .gte('sent_at', cutoffTime)
        .order('sent_at', { ascending: false })
        .limit(1);
      if (error) {
        console.warn('shouldSendByWindow error:', error.message);
        return true; // エラー時は送る
      }
      if (data && data.length) {
        console.log(`通知スキップ(時間窓): ${name_raw} - ${event}`);
        return false;
      }
      return true;
    }

    // ===== 日本語→raw名マップ =====
    let { data: qmap, error: eq } = await sb
      .from('v_queue_times_latest')
      .select('park_id,name_raw,name_ja');
    if (eq) { console.warn('v_queue_times_latest:', eq.message); qmap = []; }

    const ja2raw = new Map();
    (qmap || []).forEach(r => {
      ja2raw.set(`${r.park_id}::${r.name_ja}`, r.name_raw);
    });

    // ===== A) 休止/再開 変化の検出 =====
    let { data: openChanges, error: eOpen } =
      await sb.rpc('sp_recent_open_changes', { minutes: CHANGE_WINDOW_MIN });
    if (eOpen) { console.warn('sp_recent_open_changes:', eOpen.message); openChanges = []; }

    let openNotificationCount = 0;

    for (const ch of (openChanges || [])) {
      // 念のため、不正行はスキップ
      if (typeof ch.curr_open === 'undefined' || typeof ch.prev_open === 'undefined') continue;

      const was = ch.prev_open ? '運営中' : '休止';
      const now = ch.curr_open ? '運営中' : '休止';
      if (was === now) continue; // 本当に変わった時だけ

      const event = ch.curr_open ? 'reopen' : 'close';

      // 一意キー（同じ変化は二度と送らない）
      const uniq_key = `open:${ch.park_id}:${ch.name_raw}:${event}:${new Date(ch.changed_at).toISOString()}`;
      if (await alreadyNotifiedByKey(uniq_key) === true) {
        console.log(`通知スキップ(既送): ${uniq_key}`);
        continue;
      }

      // 時間窓の保険
      const okByWindow = await shouldSendByWindow('open', ch.park_id, ch.name_raw, event);
      if (!okByWindow) continue;

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
        uniq_key, // ★ これで完全重複防止
      });

      openNotificationCount++;
      console.log(`通知送信: ${title}`);
    }

    // ===== B) DPA/PP 変化の検出 =====
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
        prevMap.set(key, {
          dpa: r.dpa_status || null,
          pp:  r.pp40_status || null,
          ts:  r.fetched_at
        });
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
        // DPA/PP側も uniq_key で完全重複防止
        const changed_at = v.fetched_at; // 最新スナップの取得時刻を「変化時刻」として扱う
        const uniq_key = `dpa:${park_id}:${name_raw}:${notif.event}:${new Date(changed_at).toISOString()}`;
        if (await alreadyNotifiedByKey(uniq_key) === true) {
          console.log(`通知スキップ(既送): ${uniq_key}`);
          continue;
        }

        const okByWindow = await shouldSendByWindow('dpa', park_id, name_raw, notif.event);
        if (!okByWindow) continue;

        const title = `${name_ja}：${notif.label}`;
        const body  = `DPA: ${prev.dpa || '-'} → ${nowD || '-'} / PP: ${prev.pp || '-'} → ${nowP || '-'}`;

        await sendWebPush(title, body, '/');
        await sendPushover(title, body, '/');

        await sb.from('notified_events').insert({
          kind: 'dpa',
          park_id,
          name_raw,
          event: notif.event,
          changed_at,
          sent_at: new Date().toISOString(),
          uniq_key, // ★ 重複防止
        });

        dpaNotificationCount++;
        console.log(`通知送信: ${title}`);
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
