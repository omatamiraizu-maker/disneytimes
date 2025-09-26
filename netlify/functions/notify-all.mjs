// netlify/functions/notify-all.mjs — Next-Gen Functions (Response API) + favorites/all
import { createClient } from '@supabase/supabase-js';
import webpush from 'web-push';

// ---- ENV ----
const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE,
  VAPID_PUBLIC_KEY,
  VAPID_PRIVATE_KEY,
  PUSHOVER_TOKEN,
} = process.env;

// VAPID は無い場合は webpush を無効運用に（例外回避）
if (VAPID_PUBLIC_KEY && VAPID_PRIVATE_KEY) {
  webpush.setVapidDetails('mailto:notify@example.com', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);
}

// ---- helpers ----
const json = (obj, status = 200) =>
  new Response(JSON.stringify(obj), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8' },
  });

const RULE_FOR_KIND = (kind) => {
  if (kind === 'reopen' || kind === 'close') return 'notify_close_reopen';
  if (kind === 'dpa_start' || kind === 'dpa_end' || kind === 'pp_start' || kind === 'pp_end') return 'notify_dpa_sale';
  if (kind === 'wait_spike') return null; // optional
  return null;
};

const titleOf = (ev, b = {}) => {
  switch (ev.kind) {
    case 'reopen': return `${ev.name_raw} が再開`;
    case 'close': return `${ev.name_raw} が休止`;
    case 'dpa_start': return `${ev.name_raw}：DPA販売開始`;
    case 'dpa_end': return `${ev.name_raw}：DPA販売終了`;
    case 'pp_start': return `${ev.name_raw}：PP発行開始`;
    case 'pp_end': return `${ev.name_raw}：PP発行終了`;
    case 'wait_spike': return `${ev.name_raw}：待ち時間スパイク`;
    default: return `${ev.name_raw}：${ev.kind}`;
  }
};

// ==== Next-Gen Functions signature ====
// request: Web Fetch API Request, context: Netlify context
export const handler = async (request, context) => {
  console.log('[notify-all] start');

  // ENV チェック（欠落時は明示的に 500）
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
    console.error('[notify-all] Missing Supabase envs');
    return json({ ok: false, error: 'Missing Supabase envs' }, 500);
  }

  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
    auth: { persistSession: false },
  });

  // クエリ: scope=favorites|all（既定 favorites）
  const url = new URL(request.url);
  const scope = (url.searchParams.get('scope') || 'favorites').toLowerCase();

  try {
    // 1) 未送イベント取得
    const BATCH = 200;
    const evRes = await sb
      .from('event_queue')
      .select('id, kind, park_id, name_raw, event, changed_at, uniq_key, attraction_id')
      .is('sent_at', null)
      .order('changed_at', { ascending: true })
      .limit(BATCH);
    if (evRes.error) {
      console.error('[notify-all] event_queue fetch error:', evRes.error);
      return json({ ok: false, error: evRes.error.message || 'event_queue fetch failed' }, 500);
    }
    const events = evRes.data || [];
    if (!events.length) {
      return json({ ok: true, sent: 0, scope }, 202);
    }

    // 2) 購読 & プロファイル取得
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

    const poRes = await sb
      .from('pushover_profiles')
      .select('user_id,device_id,user_key');
    if (poRes.error) console.warn('[notify-all] pushover_profiles:', poRes.error.message);
    const allPoProfiles = (poRes.data || []).filter(p => p.user_key);

    // 3) favorites / ルール（scope=favorites の時に使う）
    let favUsersByKey = new Map();
    let favDevicesByKey = new Map();
    if (scope === 'favorites') {
      const [uf, df] = await Promise.all([
        sb.from('user_favorites').select('user_id,park_id,attraction_name'),
        sb.from('device_favorites').select('device_id,park_id,attraction_name'),
      ]);
      if (uf.error) console.warn('[notify-all] user_favorites:', uf.error.message);
      if (df.error) console.warn('[notify-all] device_favorites:', df.error.message);
      (uf.data || []).forEach(r => {
        const key = `${r.park_id}::${r.attraction_name}`;
        if (!favUsersByKey.has(key)) favUsersByKey.set(key, new Set());
        favUsersByKey.get(key).add(r.user_id);
      });
      (df.data || []).forEach(r => {
        const key = `${r.park_id}::${r.attraction_name}`;
        if (!favDevicesByKey.has(key)) favDevicesByKey.set(key, new Set());
        favDevicesByKey.get(key).add(r.device_id);
      });
    }

    const [uar, dar] = await Promise.all([
      sb.from('user_alert_rules')
        .select('user_id,park_id,notify_close_reopen,notify_dpa_sale'),
      sb.from('device_alert_rules')
        .select('device_id,park_id,notify_close_reopen,notify_dpa_sale'),
    ]);
    if (uar.error) console.warn('[notify-all] user_alert_rules:', uar.error.message);
    if (dar.error) console.warn('[notify-all] device_alert_rules:', dar.error.message);
    const userRules = new Map();
    const deviceRules = new Map();
    (uar.data || []).forEach(r => userRules.set(`${r.user_id}::${r.park_id}`, r));
    (dar.data || []).forEach(r => deviceRules.set(`${r.device_id}::${r.park_id}`, r));

    // 4) 送信ヘルパ
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
      await Promise.allSettled(profiles.map(({ user_key }) => {
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
      }));
    }

    // 5) イベント配信ループ
    let sentCount = 0;

    const allowByRule = (ruleRow, kind) => {
      const flag = RULE_FOR_KIND(kind);
      if (!flag) return true;
      if (!ruleRow) return true;
      return !!ruleRow[flag];
    };

    for (const ev of events) {
      // 重複保険（uniq_key）
      const dup = await sb.from('notified_events').select('id').eq('uniq_key', ev.uniq_key).limit(1);
      if (dup.error) console.warn('[notify-all] notified check:', dup.error.message);
      if (dup.data?.length) {
        await sb.from('event_queue').update({ sent_at: new Date().toISOString() }).eq('id', ev.id);
        continue;
      }

      // タイトル/本文
      let bodyJson = {};
      try { bodyJson = JSON.parse(ev.event); } catch {}
      const title = titleOf(ev, bodyJson);
      const body  = ev.event;

      // 宛先抽出
      let pushTargets = [];
      let poTargets = [];

      if (scope === 'all') {
        pushTargets = allPushSubs;
        poTargets = allPoProfiles;
      } else {
        const key = `${ev.park_id}::${ev.name_raw}`; // name_raw は日本語正規名
        const userSet = favUsersByKey.get(key) || new Set();
        const devSet  = favDevicesByKey.get(key) || new Set();

        pushTargets = allPushSubs.filter(s => {
          if (s.user_id && userSet.has(s.user_id)) {
            return allowByRule(userRules.get(`${s.user_id}::${ev.park_id}`), ev.kind);
          }
          if (s.device_id && devSet.has(s.device_id)) {
            return allowByRule(deviceRules.get(`${s.device_id}::${ev.park_id}`), ev.kind);
          }
          return false;
        });

        poTargets = allPoProfiles.filter(p => {
          if (p.user_id && userSet.has(p.user_id)) {
            return allowByRule(userRules.get(`${p.user_id}::${ev.park_id}`), ev.kind);
          }
          if (p.device_id && devSet.has(p.device_id)) {
            return allowByRule(deviceRules.get(`${p.device_id}::${ev.park_id}`), ev.kind);
          }
          return false;
        });
      }

      // 宛先なしでも sent_at を埋めてキューを捌く
      if (!pushTargets.length && !poTargets.length) {
        await sb.from('event_queue').update({ sent_at: new Date().toISOString() }).eq('id', ev.id);
        continue;
      }

      // 送信
      await sendWebPushTo(pushTargets, title, body, '/');
      await sendPushoverTo(poTargets, title, body, '/');

      // 記録 & 消し込み
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

    // 掃除（任意）
    const cutoff = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
    await sb.from('notified_events').delete().lt('sent_at', cutoff);

    return json({ ok: true, sent: sentCount, scope }, 202);
  } catch (err) {
    console.error('[notify-all] error:', err);
    return json({ ok: false, error: String(err?.message || err) }, 500);
  }
};
