// netlify/functions/notify-all.mjs
import { createClient } from '@supabase/supabase-js';
import webpush from 'web-push';

const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE,
  VAPID_PUBLIC_KEY,
  VAPID_PRIVATE_KEY,
  PUSHOVER_TOKEN, // Pushover Application Token
} = process.env;

webpush.setVapidDetails('mailto:notify@example.com', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

// 同一イベント重複を抑止する時間窓（分）
const DEDUP_WINDOW_MIN = 15;
// 直近変化を拾うための検出窓（分）
const CHANGE_WINDOW_MIN = 10;

export async function handler() {
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
    auth: { persistSession: false }
  });

  try {
    // ---------- 全購読者（Web Push / Pushover） ----------
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

    // ---------- 重複送信防止（notified_events を使用） ----------
    async function shouldSendOnce(kind, park_id, name_raw, event, changed_atISO) {
      const uniq_key = `${kind}:${park_id}:${name_raw}:${event}:${changed_atISO}`;
      const cutoffISO = new Date(Date.now() - DEDUP_WINDOW_MIN * 60 * 1000).toISOString();

      let { data: existed, error: exErr } = await sb
        .from('notified_events')
        .select('uniq_key,changed_at')
        .eq('uniq_key', uniq_key)
        .gte('changed_at', cutoffISO)
        .limit(1);

      if (exErr) {
        console.warn('notified_events check:', exErr.message);
      }
      if (existed && existed.length) return false;

      const ins = await sb.from('notified_events').insert({
        kind, park_id, name_raw, event, changed_at: changed_atISO, uniq_key
      });
      if (ins.error && /duplicate key|unique/i.test(ins.error.message)) return false;
      return true;
    }

    // ---------- 日本語→英語キー変換マップ（v_queue_times_latest から作成） ----------
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
    // 最新スナップショット（name_raw を要求しない）
    let { data: latest, error: eLatest } = await sb
      .from('v_attraction_dpa_latest')
      .select('park_id,name,dpa_status,pp40_status,fetched_at');
    if (eLatest) { console.warn('v_attraction_dpa_latest:', eLatest.message); latest = []; }

    // 直近履歴（CHANGE_WINDOW_MIN分）で直前値を作る
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
      const name_ja = v.name;

      // 日本語→英語キー（見つからないものはスキップ）
      const name_raw = ja2raw.get(`${park_id}::${name_ja}`);
      if (!name_raw) continue;

      const nowD = v.dpa_status || null;
      const nowP = v.pp40_status || null;

      const key = `${park_id}::${name_raw}`;
      const prev = prevMap.get(key) || { dpa: null, pp: null, ts: v.fetched_at };

      const changedDpa = (prev.dpa || null) !== (nowD || null);
      const changedPp  = (prev.pp  || null) !== (nowP || null);
      if (!changedDpa && !changedPp) continue;

      // イベント名（DPA優先）
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
