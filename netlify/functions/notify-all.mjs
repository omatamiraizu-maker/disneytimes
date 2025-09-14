// netlify/functions/notify-all.mjs
import { createClient } from '@supabase/supabase-js';
import webpush from 'web-push';

const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE,
  VAPID_PUBLIC_KEY,
  VAPID_PRIVATE_KEY,
} = process.env;

webpush.setVapidDetails('mailto:notify@example.com', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

export async function handler() {
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });
  const MINUTES = 3;                 // 直近窓
  const EXT_PARK = { TDL: 274, TDS: 275 }; // 内部→外部ID変換

  try {
    // =============== 事前ロード：購読・ルール・★・名前マップ ===============
    // push購読
    const { data: subsUser } = await sb
      .from('push_subscriptions')
      .select('endpoint,p256dh,auth,user_id')
      .not('user_id', 'is', null);
    const { data: subsDev } = await sb
      .from('push_subscriptions')
      .select('endpoint,p256dh,auth,device_id')
      .not('device_id', 'is', null);

    // マップ
    const subsByUser = new Map();
    (subsUser||[]).forEach(s => { if(!s.user_id) return;
      (subsByUser.get(s.user_id) || subsByUser.set(s.user_id, []).get(s.user_id)).push(s);
    });
    const subsByDev = new Map();
    (subsDev||[]).forEach(s => { if(!s.device_id) return;
      (subsByDev.get(s.device_id) || subsByDev.set(s.device_id, []).get(s.device_id)).push(s);
    });

    // parks（DPA側 park_id 内部→ code 変換）
    const { data: parks } = await sb.from('parks').select('id,code');
    const parkIdMap = new Map((parks||[]).map(p => [p.id, EXT_PARK[p.code] || null])); // 内部id -> 274/275

    // 最新の英日対応（DPAが日本語名しか無くても英語キーに変換できるように）
    const { data: qmap } = await sb
      .from('v_queue_times_latest')
      .select('park_id,name_raw,name_ja');
    const ja2raw = new Map(); // key: `${park_id}::${name_ja}` -> raw
    (qmap||[]).forEach(r=>{
      ja2raw.set(`${r.park_id}::${r.name_ja}`, r.name_raw);
    });

    // ルール・★（両方）
    const allExtParks = [274, 275];
    const { data: rulesUser } = await sb
      .from('user_alert_rules')
      .select('user_id,park_id,notify_close_reopen,notify_dpa_sale')
      .in('park_id', allExtParks);
    const { data: rulesDev } = await sb
      .from('device_alert_rules')
      .select('device_id,park_id,notify_close_reopen,notify_dpa_sale')
      .in('park_id', allExtParks);

    const ruleCloseUser = new Set((rulesUser||[]).filter(r=>r.notify_close_reopen).map(r=>`${r.user_id}::${r.park_id}`));
    const ruleDpaUser   = new Set((rulesUser||[]).filter(r=>r.notify_dpa_sale).map(r=>`${r.user_id}::${r.park_id}`));
    const ruleCloseDev  = new Set((rulesDev ||[]).filter(r=>r.notify_close_reopen).map(r=>`${r.device_id}::${r.park_id}`));
    const ruleDpaDev    = new Set((rulesDev ||[]).filter(r=>r.notify_dpa_sale).map(r=>`${r.device_id}::${r.park_id}`));

    const { data: favUser } = await sb
      .from('user_favorites')
      .select('user_id,park_id,attraction_name')
      .in('park_id', allExtParks);
    const { data: favDev } = await sb
      .from('device_favorites')
      .select('device_id,park_id,attraction_name')
      .in('park_id', allExtParks);

    const favUserSet = new Set((favUser||[]).map(f=>`${f.user_id}::${f.park_id}::${f.attraction_name}`)); // ★は英語キー
    const favDevSet  = new Set((favDev ||[]).map(f=>`${f.device_id}::${f.park_id}::${f.attraction_name}`));

    // 共通送信ユーティリティ
    const sendPush = async (targets, title, body, url = '/') => {
      if (!targets?.length) return;
      try { await sb.from('notifications').insert({ title, body }); } catch {}
      await Promise.all(targets.map(async t=>{
        const sub = { endpoint:t.endpoint, keys:{ p256dh:t.p256dh, auth:t.auth } };
        try { await webpush.sendNotification(sub, JSON.stringify({ title, body, url })); }
        catch(e){
          if (e?.statusCode === 410 || e?.statusCode === 404) {
            await sb.from('push_subscriptions').delete().eq('endpoint', t.endpoint).catch(()=>{});
          } else {
            console.warn('push fail', e?.statusCode, e?.message);
          }
        }
      }));
    };

    // =============== A) 休止/再開 の通知（queue_timesベース） ===============
    const { data: openChanges, error: errOpen } = await sb.rpc('sp_recent_open_changes', { minutes: MINUTES });
    if (errOpen) console.error('sp_recent_open_changes:', errOpen.message);

    for (const ch of (openChanges||[])) {
      const was = ch.prev_open ? '運営中' : '休止';
      const now = ch.curr_open ? '運営中' : '休止';
      if (was === now) continue;

      const keyUserRule = uid => `${uid}::${ch.park_id}`;
      const keyDevRule  = did => `${did}::${ch.park_id}`;
      const keyUserFav  = uid => `${uid}::${ch.park_id}::${ch.name_raw}`;
      const keyDevFav   = did => `${did}::${ch.park_id}::${ch.name_raw}`;

      // user
      const userTargets = [];
      for (const [uid, list] of subsByUser.entries()) {
        if (!ruleCloseUser.has(keyUserRule(uid))) continue;
        if (!favUserSet.has(keyUserFav(uid))) continue;
        list.forEach(s=>userTargets.push(s));
      }
      // device
      const devTargets = [];
      for (const [did, list] of subsByDev.entries()) {
        if (!ruleCloseDev.has(keyDevRule(did))) continue;
        if (!favDevSet.has(keyDevFav(did))) continue;
        list.forEach(s=>devTargets.push(s));
      }
      const targets = [...userTargets, ...devTargets];
      if (!targets.length) continue;

      const kind = ch.curr_open ? '再開' : '休止';
      const title = `${ch.name_ja} が${kind}`;
      const body  = `状態: ${was} → ${now}`;
      await sendPush(targets, title, body, '/');
    }

    // =============== B) DPA/PP の通知（v_attraction_dpa_latest + キャッシュ） ===============
    // parks.id（内部）で返る場合があるので外部 park_id に変換する
    const { data: dpaLatest, error: errDpa } = await sb
      .from('v_attraction_dpa_latest')
      .select('park_id,name,dpa_status,pp40_status,fetched_at');
    if (errDpa) console.error('v_attraction_dpa_latest:', errDpa.message);

    // 日本語名から英語キーへ変換し、外部park_idへマッピング
    const dpaRows = [];
    for (const r of (dpaLatest||[])) {
      const extPark = parkIdMap.get(r.park_id) ?? r.park_id; // 内部id→274/275 に寄せる（不明ならそのまま）
      const raw = ja2raw.get(`${extPark}::${r.name}`) || null; // 日本語を英語キーへ
      if (!raw) continue; // マップに無いものはスキップ（一致しないと★判定できない）
      dpaRows.push({
        park_id: extPark,
        name_raw: raw,
        name_ja: r.name,
        dpa: r.dpa_status || null,
        pp:  r.pp40_status || null,
      });
    }

    // キャッシュをまとめて取得
    const uniqKeys = dpaRows.map(x => [x.park_id, x.name_raw]);
    const parkIdsIn = [...new Set(uniqKeys.map(k=>k[0]))];
    // まとめ読み（大量なら分割してください）
    const { data: cacheAll } = await sb
      .from('dpa_status_cache')
      .select('park_id,name_raw,last_dpa,last_pp')
      .in('park_id', parkIdsIn);

    const cacheMap = new Map((cacheAll||[]).map(c => [`${c.park_id}::${c.name_raw}`, c]));

    // 変化検知 → 通知 → キャッシュ更新
    for (const row of dpaRows) {
      const key = `${row.park_id}::${row.name_raw}`;
      const prev = cacheMap.get(key) || { last_dpa: null, last_pp: null };

      const changedDpa = (row.dpa || null) !== (prev.last_dpa || null);
      const changedPp  = (row.pp  || null) !== (prev.last_pp  || null);
      if (!changedDpa && !changedPp) continue;

      // 対象抽出（DPA/PP通知ON かつ ★一致）
      const keyUserRule = uid => `${uid}::${row.park_id}`;
      const keyDevRule  = did => `${did}::${row.park_id}`;
      const keyUserFav  = uid => `${uid}::${row.park_id}::${row.name_raw}`;
      const keyDevFav   = did => `${did}::${row.park_id}::${row.name_raw}`;

      const userTargets = [];
      for (const [uid, list] of subsByUser.entries()) {
        if (!ruleDpaUser.has(keyUserRule(uid))) continue;
        if (!favUserSet.has(keyUserFav(uid))) continue;
        list.forEach(s=>userTargets.push(s));
      }
      const devTargets = [];
      for (const [did, list] of subsByDev.entries()) {
        if (!ruleDpaDev.has(keyDevRule(did))) continue;
        if (!favDevSet.has(keyDevFav(did))) continue;
        list.forEach(s=>devTargets.push(s));
      }
      const targets = [...userTargets, ...devTargets];
      if (targets.length) {
        const title = `${row.name_ja} の販売状況が更新`;
        const body  = `DPA: ${row.dpa ?? '-'} / PP: ${row.pp ?? '-'}`;
        await sendPush(targets, title, body, '/');
      }

      // キャッシュ更新（upsert）
      await sb.from('dpa_status_cache').upsert({
        park_id: row.park_id,
        name_raw: row.name_raw,
        last_dpa: row.dpa,
        last_pp:  row.pp,
        updated_at: new Date().toISOString(),
      }, { onConflict: 'park_id,name_raw' });
    }

    return { statusCode: 202, body: 'ok' };
  } catch (e) {
    console.error(e);
    return { statusCode: 500, body: String(e?.message || e) };
  }
}
