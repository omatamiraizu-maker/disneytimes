// netlify/functions/notify-all.mjs
import { createClient } from '@supabase/supabase-js';
import webpush from 'web-push';

const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE,
  VAPID_PUBLIC_KEY,
  VAPID_PRIVATE_KEY,
  // 任意（設定したら使われる）
  PUSHOVER_TOKEN,
} = process.env;

webpush.setVapidDetails('mailto:notify@example.com', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

export async function handler() {
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });
  const MINUTES = 3; // 直近窓

  try {
    // ---------------------------
    // 事前ロード：購読・名前・ルール・★
    // ---------------------------

    // WebPush購読（user_id / device_id で分けて扱う）
    const { data: subsUser } = await sb
      .from('push_subscriptions')
      .select('endpoint,p256dh,auth,user_id')
      .not('user_id', 'is', null);
    const { data: subsDev } = await sb
      .from('push_subscriptions')
      .select('endpoint,p256dh,auth,device_id')
      .not('device_id', 'is', null);

    const subsByUser = new Map();
    (subsUser || []).forEach(s => {
      if (!s.user_id) return;
      if (!subsByUser.has(s.user_id)) subsByUser.set(s.user_id, []);
      subsByUser.get(s.user_id).push(s);
    });

    const subsByDev = new Map();
    (subsDev || []).forEach(s => {
      if (!s.device_id) return;
      if (!subsByDev.has(s.device_id)) subsByDev.set(s.device_id, []);
      subsByDev.get(s.device_id).push(s);
    });

    // parks: 内部id → 274/275 への変換（内製スキーマに合わせて）
    const { data: parks } = await sb.from('parks').select('id,code');
    const EXT_PARK = { TDL: 274, TDS: 275 };
    const internalToExt = new Map((parks || []).map(p => [p.id, EXT_PARK[p.code] ?? p.id]));

    // 最新の英日対応（name_ja → name_raw 変換用）
    const { data: qmap } = await sb
      .from('v_queue_times_latest')
      .select('park_id,name_raw,name_ja');
    const ja2raw = new Map(); // key: `${park_id}::${name_ja}` -> name_raw
    (qmap || []).forEach(r => {
      ja2raw.set(`${r.park_id}::${r.name_ja}`, r.name_raw);
    });

    // アラートルール & ★
    const watchParks = [274, 275];
    const { data: rulesUser } = await sb
      .from('user_alert_rules')
      .select('user_id,park_id,notify_close_reopen,notify_dpa_sale')
      .in('park_id', watchParks);
    const { data: rulesDev } = await sb
      .from('device_alert_rules')
      .select('device_id,park_id,notify_close_reopen,notify_dpa_sale')
      .in('park_id', watchParks);

    const ruleCloseUser = new Set((rulesUser || [])
      .filter(r => r.notify_close_reopen).map(r => `${r.user_id}::${r.park_id}`));
    const ruleDpaUser = new Set((rulesUser || [])
      .filter(r => r.notify_dpa_sale).map(r => `${r.user_id}::${r.park_id}`));
    const ruleCloseDev = new Set((rulesDev || [])
      .filter(r => r.notify_close_reopen).map(r => `${r.device_id}::${r.park_id}`));
    const ruleDpaDev = new Set((rulesDev || [])
      .filter(r => r.notify_dpa_sale).map(r => `${r.device_id}::${r.park_id}`));

    const { data: favUser } = await sb
      .from('user_favorites')
      .select('user_id,park_id,attraction_name')
      .in('park_id', watchParks);
    const { data: favDev } = await sb
      .from('device_favorites')
      .select('device_id,park_id,attraction_name')
      .in('park_id', watchParks);

    const favUserSet = new Set((favUser || [])
      .map(f => `${f.user_id}::${f.park_id}::${f.attraction_name}`)); // ★は英語キー
    const favDevSet = new Set((favDev || [])
      .map(f => `${f.device_id}::${f.park_id}::${f.attraction_name}`));

    // Pushoverプロファイル（ここでは全件取得せず、後で必要なIDだけ再取得）
    // → まずは枠だけ用意

    // 共通：Web Push送信
    const sendWebPush = async (targets, title, body, url = '/') => {
      if (!targets?.length) return;
      try { await sb.from('notifications').insert({ title, body }); } catch {}
      await Promise.all(targets.map(async s => {
        const sub = { endpoint: s.endpoint, keys: { p256dh: s.p256dh, auth: s.auth } };
        try {
          await webpush.sendNotification(sub, JSON.stringify({ title, body, url }));
        } catch (e) {
          if (e?.statusCode === 410 || e?.statusCode === 404) {
            await sb.from('push_subscriptions').delete().eq('endpoint', s.endpoint).catch(() => {});
          } else {
            console.warn('push fail', e?.statusCode, e?.message);
          }
        }
      }));
    };

    // 共通：Pushover送信（個別キー向け）
    const sendPushover = async (userKey, title, message, url = '/') => {
      try {
        if (!PUSHOVER_TOKEN || !userKey) return;
        const body = new URLSearchParams({
          token: PUSHOVER_TOKEN,
          user: userKey,
          title,
          message,
          url,
          url_title: '開く',
          priority: '0',
        });
        await fetch('https://api.pushover.net/1/messages.json', {
          method: 'POST',
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          body
        });
      } catch (_) {}
    };

    // 対象者のPushoverキーを取って一斉送信
    const broadcastPushover = async ({ userIds = [], deviceIds = [] }, title, body, url = '/') => {
      const keys = new Set();

      if (userIds.length) {
        const { data: poUsers } = await sb
          .from('pushover_profiles')
          .select('user_id,user_key')
          .in('user_id', userIds)
          .not('user_id', 'is', null);
        (poUsers || []).forEach(p => p.user_key && keys.add(p.user_key));
      }

      if (deviceIds.length) {
        const { data: poDevs } = await sb
          .from('pushover_profiles')
          .select('device_id,user_key')
          .in('device_id', deviceIds)
          .not('device_id', 'is', null);
        (poDevs || []).forEach(p => p.user_key && keys.add(p.user_key));
      }

      if (!keys.size) return;
      await Promise.all([...keys].map(k => sendPushover(k, title, body, url)));
    };

    // ---------------------------
    // A) 休止/再開（sp_recent_open_changes）
    // ---------------------------
    const { data: openChanges } = await sb.rpc('sp_recent_open_changes', { minutes: MINUTES });

    for (const ch of (openChanges || [])) {
      const was = ch.prev_open ? '運営中' : '休止';
      const now = ch.curr_open ? '運営中' : '休止';
      if (was === now) continue;

      const title = `${ch.name_ja} が${ch.curr_open ? '再開' : '休止'}`;
      const body = `状態: ${was} → ${now}`;
      const url = '/';

      // Web Push 対象抽出（user / device）
      const userTargets = [];
      for (const [uid, list] of subsByUser.entries()) {
        if (!ruleCloseUser.has(`${uid}::${ch.park_id}`)) continue;
        if (!favUserSet.has(`${uid}::${ch.park_id}::${ch.name_raw}`)) continue;
        list.forEach(s => userTargets.push(s));
      }
      const devTargets = [];
      for (const [did, list] of subsByDev.entries()) {
        if (!ruleCloseDev.has(`${did}::${ch.park_id}`)) continue;
        if (!favDevSet.has(`${did}::${ch.park_id}::${ch.name_raw}`)) continue;
        list.forEach(s => devTargets.push(s));
      }
      await sendWebPush([...userTargets, ...devTargets], title, body, url);

      // Pushover 対象抽出（WebPush購読の有無に関係なくキーがあれば送る）
      const okUserIds = [];
      const okDeviceIds = [];
      // user
      const userIdSet = new Set((favUser || []).filter(f =>
        f.park_id === ch.park_id && f.attraction_name === ch.name_raw
      ).map(f => f.user_id));
      userIdSet.forEach(uid => {
        if (ruleCloseUser.has(`${uid}::${ch.park_id}`)) okUserIds.push(uid);
      });
      // device
      const devIdSet = new Set((favDev || []).filter(f =>
        f.park_id === ch.park_id && f.attraction_name === ch.name_raw
      ).map(f => f.device_id));
      devIdSet.forEach(did => {
        if (ruleCloseDev.has(`${did}::${ch.park_id}`)) okDeviceIds.push(did);
      });

      await broadcastPushover({ userIds: okUserIds, deviceIds: okDeviceIds }, title, body, url);
    }

    // ---------------------------
    // B) DPA/PP（v_attraction_dpa_latest + dpa_status_cache）
    // ---------------------------
    const { data: rawDpa } = await sb
      .from('v_attraction_dpa_latest')
      .select('park_id,name,dpa_status,pp40_status,fetched_at');

    // 日本語→英語キー／内部→外部 park_id へ
    const dpaRows = [];
    for (const r of (rawDpa || [])) {
      const ext = internalToExt.get(r.park_id) ?? r.park_id;
      const raw = ja2raw.get(`${ext}::${r.name}`) || null;
      if (!raw) continue;
      dpaRows.push({
        park_id: ext,
        name_raw: raw,
        name_ja: r.name,
        dpa: r.dpa_status || null,
        pp: r.pp40_status || null,
      });
    }

    // キャッシュ読み
    const { data: cacheAll } = await sb
      .from('dpa_status_cache')
      .select('park_id,name_raw,last_dpa,last_pp')
      .in('park_id', watchParks);

    const cacheMap = new Map((cacheAll || []).map(c => [`${c.park_id}::${c.name_raw}`, c]));

    for (const row of dpaRows) {
      const key = `${row.park_id}::${row.name_raw}`;
      const prev = cacheMap.get(key) || { last_dpa: null, last_pp: null };
      const changedDpa = (row.dpa || null) !== (prev.last_dpa || null);
      const changedPp = (row.pp || null) !== (prev.last_pp || null);
      if (!changedDpa && !changedPp) continue;

      const title = `${row.name_ja} の販売状況が更新`;
      const body = `DPA: ${row.dpa ?? '-'} / PP: ${row.pp ?? '-'}`;
      const url = '/';

      // Web Push 対象抽出
      const userTargets = [];
      for (const [uid, list] of subsByUser.entries()) {
        if (!ruleDpaUser.has(`${uid}::${row.park_id}`)) continue;
        if (!favUserSet.has(`${uid}::${row.park_id}::${row.name_raw}`)) continue;
        list.forEach(s => userTargets.push(s));
      }
      const devTargets = [];
      for (const [did, list] of subsByDev.entries()) {
        if (!ruleDpaDev.has(`${did}::${row.park_id}`)) continue;
        if (!favDevSet.has(`${did}::${row.park_id}::${row.name_raw}`)) continue;
        list.forEach(s => devTargets.push(s));
      }
      await sendWebPush([...userTargets, ...devTargets], title, body, url);

      // Pushover 対象抽出
      const okUserIds = [];
      const okDeviceIds = [];
      // user
      const userIdSet = new Set((favUser || []).filter(f =>
        f.park_id === row.park_id && f.attraction_name === row.name_raw
      ).map(f => f.user_id));
      userIdSet.forEach(uid => {
        if (ruleDpaUser.has(`${uid}::${row.park_id}`)) okUserIds.push(uid);
      });
      // device
      const devIdSet = new Set((favDev || []).filter(f =>
        f.park_id === row.park_id && f.attraction_name === row.name_raw
      ).map(f => f.device_id));
      devIdSet.forEach(did => {
        if (ruleDpaDev.has(`${did}::${row.park_id}`)) okDeviceIds.push(did);
      });

      await broadcastPushover({ userIds: okUserIds, deviceIds: okDeviceIds }, title, body, url);

      // キャッシュ更新
      await sb.from('dpa_status_cache').upsert({
        park_id: row.park_id,
        name_raw: row.name_raw,
        last_dpa: row.dpa,
        last_pp: row.pp,
        updated_at: new Date().toISOString(),
      }, { onConflict: 'park_id,name_raw' });
    }

    return { statusCode: 202, body: 'ok' };
  } catch (e) {
    console.error(e);
    return { statusCode: 500, body: String(e?.message || e) };
  }
}
