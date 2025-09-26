// netlify/functions/notify-all.mjs
// Next-Gen Functions (Response API) + favorites/all + device rules + wave mute + URLフォールバック

import { createClient } from '@supabase/supabase-js';
import webpush from 'web-push';

// ===== ENV =====
const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE,
  VAPID_PUBLIC_KEY,
  VAPID_PRIVATE_KEY,
  PUSHOVER_TOKEN,
} = process.env;

// VAPIDキーが無ければ webpush を無効運用に（例外回避）
if (VAPID_PUBLIC_KEY && VAPID_PRIVATE_KEY) {
  webpush.setVapidDetails('mailto:notify@example.com', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);
}

// 共通：JSON Responseヘルパ
const json = (obj, status = 200) =>
  new Response(JSON.stringify(obj), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8' },
  });

// ランタイム差吸収：URLを安全に構築
function buildUrlLike(request, context) {
  try {
    if (request?.url) return new URL(request.url);                   // Next-Gen
    if (request?.rawUrl) return new URL(request.rawUrl);             // 旧
    const host = request?.headers?.host || context?.headers?.host;   // Fallback
    const path = request?.path || context?.path || '/.netlify/functions/notify-all';
    const rawQuery = request?.rawQuery || context?.rawQuery || '';
    if (host) return new URL(`https://${host}${path}${rawQuery ? '?' + rawQuery : ''}`);
  } catch (_) { /* ignore */ }
  // 最終フォールバック（scope=favorites 既定）
  return new URL('https://local.invalid/.netlify/functions/notify-all?scope=favorites');
}

// 種別→ON/OFF ルールキー
const RULE_FOR_KIND = (kind) => {
  if (kind === 'reopen' || kind === 'close') return 'notify_close_reopen';
  if (kind === 'dpa_start' || kind === 'dpa_end' || kind === 'pp_start' || kind === 'pp_end') return 'notify_dpa_sale';
  if (kind === 'wait_spike') return null;
  return null;
};

// タイトル整形
const titleOf = (ev, body = {}) => {
  switch (ev.kind) {
    case 'reopen':    return `${ev.name_raw} が再開`;
    case 'close':     return `${ev.name_raw} が休止`;
    case 'dpa_start': return `${ev.name_raw}：DPA販売開始`;
    case 'dpa_end':   return `${ev.name_raw}：DPA販売終了`;
    case 'pp_start':  return `${ev.name_raw}：PP発行開始`;
    case 'pp_end':    return `${ev.name_raw}：PP発行終了`;
    case 'wait_spike':return `${ev.name_raw}：待ち時間スパイク`;
    default:          return `${ev.name_raw}：${ev.kind}`;
  }
};

// ===== Netlify handler =====
export const handler = async (request, context) => {
  console.log('[notify-all] start');

  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
    console.error('[notify-all] Missing Supabase envs');
    return json({ ok: false, error: 'Missing Supabase envs' }, 500);
  }

  // URL（クエリ）を安全に取得
  const url = buildUrlLike(request, context);
  const scopeParam = (url.searchParams.get('scope') || 'favorites').toLowerCase(); // 'favorites' | 'all'
  const BATCH = 200;

  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
    auth: { persistSession: false },
  });

  try {
    // 1) 未送イベント（minute_bucket も読む：ラッシュ判定に使用）
    const evRes = await sb
      .from('event_queue')
      .select('id, kind, park_id, name_raw, event, changed_at, minute_bucket, uniq_key, attraction_id')
      .is('sent_at', null)
      .order('changed_at', { ascending: true })
      .limit(BATCH);
    if (evRes.error) {
      console.error('[notify-all] event_queue fetch error:', evRes.error);
      return json({ ok: false, error: evRes.error.message || 'event_queue fetch failed' }, 500);
    }
    const events = evRes.data || [];
    if (!events.length) return json({ ok: true, sent: 0, scope: scopeParam }, 202);

    // 2) 購読とPushoverプロファイル
    const pushRes = await sb
      .from('push_subscriptions')
      .select('endpoint,p256dh,auth,user_id,device_id');
    if (pushRes.error) console.warn('[notify-all] push_subscriptions:', pushRes.error.message);
    const allPushSubs = (pushRes.data || []).map(s => ({
      endpoint: s.endpoint,
      keys: { p256dh: s.p256dh, auth: s.auth },
      user_id: s.user_id,
      device_id: s.device_id,
    }));

    const poRes = await sb.from('pushover_profiles').select('user_id,device_id,user_key');
    if (poRes.error) console.warn('[notify-all] pushover_profiles:', poRes.error.message);
    const allPoProfiles = (poRes.data || []).filter(p => p.user_key);

    // 3) favorites と ルールをまとめ読み
    const [uf, df, uar, dar] = await Promise.all([
      sb.from('user_favorites').select('user_id,park_id,attraction_name'),
      sb.from('device_favorites').select('device_id,park_id,attraction_name'),
      sb.from('user_alert_rules').select('user_id,park_id,notify_close_reopen,notify_dpa_sale'),
      sb.from('device_alert_rules').select('device_id,park_id,notify_close_reopen,notify_dpa_sale,notify_mode,mute_open_close_waves,wave_threshold'),
    ]);
    if (uf.error) console.warn('[notify-all] user_favorites:', uf.error.message);
    if (df.error) console.warn('[notify-all] device_favorites:', df.error.message);
    if (uar.error) console.warn('[notify-all] user_alert_rules:', uar.error.message);
    if (dar.error) console.warn('[notify-all] device_alert_rules:', dar.error.message);

    // favorites をキー毎(Set)へ
    const favUsersByKey = new Map();   // `${park_id}::${name}` -> Set(user_id)
    const favDevicesByKey = new Map(); // `${park_id}::${name}` -> Set(device_id)
    (uf.data || []).forEach(r => {
      const k = `${r.park_id}::${r.attraction_name}`;
      (favUsersByKey.get(k) || favUsersByKey.set(k, new Set()).get(k)).add(r.user_id);
    });
    (df.data || []).forEach(r => {
      const k = `${r.park_id}::${r.attraction_name}`;
      (favDevicesByKey.get(k) || favDevicesByKey.set(k, new Set()).get(k)).add(r.device_id);
    });

    // ルールをMap化
    const userRules = new Map();   // `${user_id}::${park_id}` -> row
    const deviceRules = new Map(); // `${device_id}::${park_id}` -> row
    (uar.data || []).forEach(r => userRules.set(`${r.user_id}::${r.park_id}`, r));
    (dar.data || []).forEach(r => deviceRules.set(`${r.device_id}::${r.park_id}`, r));

    // 4) “Open/Close一斉波” 検出のため minute_bucket 同時件数を集計
    const minuteCounts = new Map(); // `${park_id}::${minute_bucket}::${kind}` -> count
    for (const ev of events) {
      if (ev.kind !== 'reopen' && ev.kind !== 'close') continue;
      const mb = ev.minute_bucket ?? Math.floor(new Date(ev.changed_at).getTime() / 60000);
      const key = `${ev.park_id}::${mb}::${ev.kind}`;
      minuteCounts.set(key, (minuteCounts.get(key) || 0) + 1);
    }

    // 5) 送信ヘルパ
    const allowByRule = (ruleRow, kind) => {
      const flag = RULE_FOR_KIND(kind);
      if (!flag) return true;
      if (!ruleRow) return true;
      return !!ruleRow[flag];
    };

    async function sendWebPushTo(subs, title, body, urlPath = '/') {
      if (!subs.length || !VAPID_PUBLIC_KEY || !VAPID_PRIVATE_KEY) return;
      await sb.from('notifications').insert({ kind: 'system', title, body }).throwOnError();
      for (const s of subs) {
        try {
          await webpush.sendNotification(
            { endpoint: s.endpoint, keys: s.keys },
            JSON.stringify({ title, body, url: urlPath })
          );
        } catch (err) {
          if (err?.statusCode === 410 || err?.statusCode === 404) {
            await sb.from('push_subscriptions').delete().eq('endpoint', s.endpoint);
          } else {
            console.warn('webpush error:', err?.statusCode, err?.message);
          }
        }
      }
    }

    async function sendPushoverTo(profiles, title, message, urlPath = '/') {
      if (!PUSHOVER_TOKEN || !profiles.length) return;
      await Promise.allSettled(
        profiles.map(({ user_key }) => {
          const body = new URLSearchParams({
            token: PUSHOVER_TOKEN,
            user: user_key,
            title,
            message,
            url: urlPath,
            url_title: '開く',
            priority: '0',
          });
          return fetch('https://api.pushover.net/1/messages.json', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body,
          });
        })
      );
    }

    // 6) イベント配信ループ
    let sentCount = 0;

    for (const ev of events) {
      // 重複保険（uniq_key）
      const dup = await sb.from('notified_events').select('id').eq('uniq_key', ev.uniq_key).limit(1);
      if (dup.error) console.warn('[notify-all] notified check:', dup.error.message);
      if (dup.data?.length) {
        await sb.from('event_queue').update({ sent_at: new Date().toISOString() }).eq('id', ev.id);
        continue;
      }

      // 本文整形
      let bodyJson = {};
      try { bodyJson = JSON.parse(ev.event); } catch {}
      const title = titleOf(ev, bodyJson);
      const body  = ev.event;

      // 宛先抽出（scopeパラメータは“初期姿勢”。端末側ルールが favorites の場合はさらに絞る）
      const favKey = `${ev.park_id}::${ev.name_raw}`;
      const favUserSet = favUsersByKey.get(favKey) || new Set();
      const favDevSet  = favDevicesByKey.get(favKey) || new Set();

      // ラッシュ判定（端末ごとの wave_threshold で切る）
      const mb = ev.minute_bucket ?? Math.floor(new Date(ev.changed_at).getTime() / 60000);
      const sameMinuteCount = (ev.kind === 'reopen' || ev.kind === 'close')
        ? (minuteCounts.get(`${ev.park_id}::${mb}::${ev.kind}`) || 0)
        : 0;

      // push
      const pushTargets = allPushSubs.filter(s => {
        // 端末側ルール
        const dr = deviceRules.get(`${s.device_id}::${ev.park_id}`);
        const mode = (dr?.notify_mode || scopeParam); // device が未設定なら query の scope を採用
        const muteWave = !!dr?.mute_open_close_waves;
        const waveThresh = dr?.wave_threshold ?? 12;
        const allowWave = !(muteWave && (ev.kind === 'reopen' || ev.kind === 'close') && sameMinuteCount >= waveThresh);

        if (!allowWave) return false;

        // user側購読がある場合
        if (s.user_id) {
          const ur = userRules.get(`${s.user_id}::${ev.park_id}`);
          if (mode === 'all') return allowByRule(ur, ev.kind);
          // favorites
          return favUserSet.has(s.user_id) && allowByRule(ur, ev.kind);
        }

        // device側のみ
        if (mode === 'all') return allowByRule(dr, ev.kind);
        // favorites
        return favDevSet.has(s.device_id) && allowByRule(dr, ev.kind);
      });

      // pushover
      const poTargets = allPoProfiles.filter(p => {
        const dr = deviceRules.get(`${p.device_id}::${ev.park_id}`);
        const ur = userRules.get(`${p.user_id}::${ev.park_id}`);
        const mode = (dr?.notify_mode || scopeParam);
        const muteWave = !!dr?.mute_open_close_waves;
        const waveThresh = dr?.wave_threshold ?? 12;
        const allowWave = !(muteWave && (ev.kind === 'reopen' || ev.kind === 'close') && sameMinuteCount >= waveThresh);
        if (!allowWave) return false;

        if (p.user_id) {
          if (mode === 'all') return allowByRule(ur, ev.kind);
          return favUserSet.has(p.user_id) && allowByRule(ur, ev.kind);
        }
        if (p.device_id) {
          if (mode === 'all') return allowByRule(dr, ev.kind);
          return favDevSet.has(p.device_id) && allowByRule(dr, ev.kind);
        }
        return false;
      });

      // 宛先なしでも sent_at は埋めてキューを捌く
      if (!pushTargets.length && !poTargets.length) {
        await sb.from('event_queue').update({ sent_at: new Date().toISOString() }).eq('id', ev.id);
        continue;
      }

      await sendWebPushTo(pushTargets, title, body, '/');
      await sendPushoverTo(poTargets, title, body, '/');

      const ins = await sb.from('notified_events').insert({
        kind: ev.kind,
        park_id: ev.park_id,
        name_raw: ev.name_raw,
        event: ev.event,
        changed_at: ev.changed_at,
        sent_at: new Date().toISOString(),
        uniq_key: ev.uniq_key,
      });
      if (ins.error) console.error('[notify-all] notified_events insert error:', ins.error);

      await sb.from('event_queue').update({ sent_at: new Date().toISOString() }).eq('id', ev.id);
      sentCount++;
    }

    // 古い通知の掃除（任意）
    const cutoff = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
    await sb.from('notified_events').delete().lt('sent_at', cutoff);

    return json({ ok: true, sent: sentCount, scope: scopeParam }, 202);
  } catch (err) {
    console.error('[notify-all] error:', err);
    return json({ ok: false, error: String(err?.message || err) }, 500);
  }
};
