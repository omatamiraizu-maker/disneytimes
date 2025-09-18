// netlify/functions/notify-all.mjs
import { createClient } from '@supabase/supabase-js';
import webpush from 'web-push';

const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE,
  VAPID_PUBLIC_KEY,
  VAPID_PRIVATE_KEY,
  PUSHOVER_TOKEN, // Pushover Application Token（Netlifyの環境変数に設定）
} = process.env;

webpush.setVapidDetails('mailto:notify@example.com', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

// 直近N分内の同一イベントは重複送信しない
const DEDUP_WINDOW_MIN = 15;
// DB取り込みのズレを吸収するための検出窓
const CHANGE_WINDOW_MIN = 10;

export async function handler() {
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
    auth: { persistSession: false }
  });

  try {
    // 1) 全購読者を取得（お気に入り・ログイン概念は排除して“全員に配信”）
    let { data: subsPush, error: e1 } = await sb
      .from('push_subscriptions')
      .select('endpoint,p256dh,auth');

    if (e1) { console.error('push_subscriptions:', e1.message); subsPush = []; }
    const allPushSubs = subsPush || [];

    let { data: poProfiles, error: e2 } = await sb
      .from('pushover_profiles')
      .select('user_key');

    if (e2) { console.error('pushover_profiles:', e2.message); poProfiles = []; }
    const allPoKeys = [...new Set((poProfiles || []).map(p => p.user_key).filter(Boolean))];

    // Web Push 送信
    async function sendWebPush(title, body, url = '/') {
      if (!allPushSubs.length) return;
      // フィードは失敗しても無視
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

    // Pushover 送信
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
        } catch (_) { /* noop */ }
      }
    }

    // 重複送信防止（notified_events が既に作成されている前提）
    async function shouldSendOnce(kind, park_id, name_raw, event, changed_atISO) {
      const uniq_key = `${kind}:${park_id}:${name_raw}:${event}:${changed_atISO}`;
      const cutoffISO = new Date(Date.now() - DEDUP_WINDOW_MIN * 60 * 1000).toISOString();

      // 既に近い時間に同一イベントがあればスキップ
      let { data: existed, error: exErr } = await sb
        .from('notified_events')
        .select('uniq_key,changed_at')
        .eq('uniq_key', uniq_key)
        .gte('changed_at', cutoffISO)
        .limit(1);

      if (exErr) {
        // テーブル未作成などの場合は重複チェックせず送る
        console.warn('notified_events check:', exErr.message);
      }
      if (existed && existed.length) return false;

      // 記録（ユニーク制約に引っかかったら重複と判断）
      const ins = await sb.from('notified_events').insert({
        kind, park_id, name_raw, event, changed_at: changed_atISO, uniq_key
      });

      if (ins.error && /duplicate key|unique/i.test(ins.error.message)) return false;
      return true;
    }

    // ===================== A) 休止/再開 =====================
    let { data: openChanges, error: eOpen } =
      await sb.rpc('sp_recent_open_changes', { minutes: CHANGE_WINDOW_MIN });

    if (eOpen) {
      console.warn('sp_recent_open_changes:', eOpen.message);
      openChanges = [];
    }

    for (const ch of (openChanges || [])) {
      const was = ch.prev_open ? '運営中' : '休止';
      const now = ch.curr_open ? '運営中' : '休止';
      if (was === now) continue;

      const event = ch.curr_open ? 'reopen' : 'close';
      const changedAt = new Date(ch.changed_at).toISOString();

      const ok = await shouldSendOnce('open', ch.park_id, ch.name_raw, event, changedAt);
      if (!ok) continue;

      const title = `${ch.name_ja} が${ch.curr_open ? '再開' : '休止'}`;
      const body  = `状態: ${was} → ${now}`;
      await sendWebPush(title, body, '/');
      await sendPushover(title, body, '/');
    }

    // ===================== B) DPA/PP =====================
    // 最新スナップショット（英語キーが無い環境でも name_raw を試みる）
    let { data: latest, error: eLatest } = await sb
      .from('v_attraction_dpa_latest')
      .select('park_id,name,name_raw,dpa_status,pp40_status,fetched_at');

    if (eLatest) { console.warn('v_attraction_dpa_latest:', eLatest.message); latest = []; }

    // 直近の履歴から直前値を作る（CHANGE_WINDOW_MIN分）
    const sinceISO = new Date(Date.now() - CHANGE_WINDOW_MIN * 60 * 1000).toISOString();
    let { data: recentHist, error: eHist } = await sb
      .from('attraction_status')
      .select('park_id,name_raw,dpa_status,pp40_status,fetched_at')
      .gte('fetched_at', sinceISO);

    if (eHist) { console.warn('attraction_status recent:', eHist.message); recentHist = []; }

    const prevMap = new Map(); // key = park_id::name_raw -> { dpa, pp, ts }
    for (const r of (recentHist || [])) {
      const key = `${r.park_id}::${r.name_raw}`;
      const cur = prevMap.get(key);
      if (!cur || new Date(r.fetched_at) > new Date(cur.ts)) {
        prevMap.set(key, { dpa: r.dpa_status || null, pp: r.pp40_status || null, ts: r.fetched_at });
      }
    }

    for (const v of (latest || [])) {
      const park_id = v.park_id;
      // name_raw がビューに無い環境向けのフォールバック（日本語名を便宜キーにする）
      const name_raw = v.name_raw || v.name;
      const name_ja  = v.name;
      const nowD = v.dpa_status || null;
      const nowP = v.pp40_status || null;
      const key = `${park_id}::${name_raw}`;

      const prev = prevMap.get(key) || { dpa: null, pp: null, ts: v.fetched_at };
      const changedDpa = (prev.dpa || null) !== (nowD || null);
      const changedPp  = (prev.pp  || null) !== (nowP || null);
      if (!changedDpa && !changedPp) continue;

      // イベント名のラベル（DPA優先）
      let event = 'dpa_update';
      let label = '販売状況が更新';
      if (changedDpa) {
        if (nowD === '販売中' && prev.dpa !== '販売中') { event = 'dpa_start'; label = 'DPA販売開始'; }
        else if (prev.dpa === '販売中' && nowD !== '販売中') { event = 'dpa_end'; label = 'DPA販売終了'; }
      } else if (changedPp) {
        if (nowP === '発行中' && prev.pp !== '発行中') { event = 'pp_start'; label = 'PP発行開始'; }
        else if (prev.pp === '発行中' && nowP !== '発行中') { event = 'pp_end'; label = 'PP発行終了'; }
      }

      const changedAt = new Date(v.fetched_at || Date.now()).toISOString();
      const ok = await shouldSendOnce('dpa', park_id, name_raw, event, changedAt);
      if (!ok) continue;

      const title = `${name_ja}：${label}`;
      const body  = `DPA: ${prev.dpa ?? '-'} → ${nowD ?? '-'} / PP: ${prev.pp ?? '-'} → ${nowP ?? '-'}`;
      await sendWebPush(title, body, '/');
      await sendPushover(title, body, '/');
    }

    return { statusCode: 202, body: 'ok' };
  } catch (err) {
    console.error(err);
    return { statusCode: 500, body: String(err?.message || err) };
  }
}
