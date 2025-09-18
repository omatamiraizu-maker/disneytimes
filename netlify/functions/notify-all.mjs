// netlify/functions/notify-all.mjs
import { createClient } from '@supabase/supabase-js';
import webpush from 'web-push';

const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE,
  VAPID_PUBLIC_KEY,
  VAPID_PRIVATE_KEY,
  PUSHOVER_TOKEN, // ← 必須（Pushover Application Token）
} = process.env;

webpush.setVapidDetails('mailto:notify@example.com', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

// 送信済み重複を防ぐ（直近N分同一イベントはスキップ）
const DEDUP_WINDOW_MIN = 15;
// 休止/再開を拾う時間窓（DBに取り込み直後のズレ吸収）
const CHANGE_WINDOW_MIN = 10;

export async function handler() {
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });

  try {
    // ---------- 事前ロード：全購読（お気に入り/ルールは無視して全送信） ----------
    const { data: subsPush } = await sb
      .from('push_subscriptions')
      .select('endpoint,p256dh,auth'); // 全員

    const allPushSubs = subsPush || [];

    const { data: poProfiles } = await sb
      .from('pushover_profiles')
      .select('user_key')
      .not('user_key', 'is', null);

    const allPoKeys = [...new Set((poProfiles || []).map(p => p.user_key).filter(Boolean))];

    const sendWebPush = async (title, body, url = '/') => {
      if (!allPushSubs.length) return;
      try { await sb.from('notifications').insert({ title, body }).catch(() => {}); } catch {}
      await Promise.all(allPushSubs.map(async s => {
        const sub = { endpoint: s.endpoint, keys: { p256dh: s.p256dh, auth: s.auth } };
        try { await webpush.sendNotification(sub, JSON.stringify({ title, body, url })); }
        catch (e) {
          if (e?.statusCode === 410 || e?.statusCode === 404) {
            await sb.from('push_subscriptions').delete().eq('endpoint', s.endpoint).catch(()=>{});
          }
        }
      }));
    };

    const sendPushover = async (title, message, url = '/') => {
      if (!PUSHOVER_TOKEN || !allPoKeys.length) return;
      const bodyCommon = (user) =>
        new URLSearchParams({ token: PUSHOVER_TOKEN, user, title, message, url, url_title: '開く', priority: '0' });
      await Promise.all(allPoKeys.map(async user => {
        try {
          await fetch('https://api.pushover.net/1/messages.json', {
            method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: bodyCommon(user)
          });
        } catch (_) {}
      }));
    };

    // 送信済み重複チェック（notified_events を使用）
    const ensureDedupTable = `
      create table if not exists public.notified_events(
        id bigserial primary key,
        kind text not null,           -- 'open' | 'dpa'
        park_id int not null,
        name_raw text not null,
        event text not null,          -- 'reopen'|'close'|'dpa_start'|'dpa_end'|'pp_start'|'pp_end'|'dpa_update'
        changed_at timestamptz not null,
        sent_at timestamptz default now(),
        uniq_key text not null unique
      );`;
    await sb.rpc('exec_sql', { sql: ensureDedupTable }).catch(async () => {
      // helpersが無ければ直接HTTPで流さずスルー（既存ならOK）
    });

    async function shouldSendOnce(kind, park_id, name_raw, event, changed_at) {
      const ts = new Date(changed_at).toISOString();
      const uniq_key = `${kind}:${park_id}:${name_raw}:${event}:${ts}`;
      // 直近 N 分内の重複はスキップ
      const cutoff = new Date(Date.now() - DEDUP_WINDOW_MIN * 60 * 1000).toISOString();
      const { data: existed } = await sb
        .from('notified_events')
        .select('uniq_key,changed_at')
        .eq('uniq_key', uniq_key)
        .gte('changed_at', cutoff)
        .limit(1);
      if (existed && existed.length) return false;

      await sb.from('notified_events').insert({ kind, park_id, name_raw, event, changed_at, uniq_key }).catch(()=>{});
      return true;
    }

    // ---------- A) 休止/再開（最近の変化を全て通知） ----------
    const { data: openChanges, error: errOpen } =
      await sb.rpc('sp_recent_open_changes', { minutes: CHANGE_WINDOW_MIN });
    if (errOpen) console.warn('sp_recent_open_changes:', errOpen.message);

    for (const ch of (openChanges || [])) {
      const was = ch.prev_open ? '運営中' : '休止';
      const now = ch.curr_open ? '運営中' : '休止';
      if (was === now) continue;

      const event = ch.curr_open ? 'reopen' : 'close';
      if (!(await shouldSendOnce('open', ch.park_id, ch.name_raw, event, ch.changed_at))) continue;

      const title = `${ch.name_ja} が${ch.curr_open ? '再開' : '休止'}`;
      const body  = `状態: ${was} → ${now}`;
      await sendWebPush(title, body, '/');
      await sendPushover(title, body, '/');
    }

    // ---------- B) DPA/PP（最新と直前を比較、全ての変化を通知） ----------
    // 最新状態
    const { data: rawDpa, error: errLatest } =
      await sb.from('v_attraction_dpa_latest').select('park_id,name,name_raw,dpa_status,pp40_status,fetched_at');
    if (errLatest) console.warn('v_attraction_dpa_latest:', errLatest.message);

    // 直近の履歴から直前値（CHANGE_WINDOW_MIN分）を拾う
    const since = new Date(Date.now() - CHANGE_WINDOW_MIN * 60 * 1000).toISOString();
    const { data: recentHist } = await sb
      .from('attraction_status')
      .select('park_id,name_raw,dpa_status,pp40_status,fetched_at')
      .gte('fetched_at', since);

    // name_raw が v 側に無い場合に備え、英日名の解決（vに name_raw が無い構成でもOK）
    const vHasRaw = rawDpa?.length && Object.prototype.hasOwnProperty.call(rawDpa[0], 'name_raw');

    // 直前値マップ：key=park_id::name_raw → {dpa,pp,ts}
    const prevMap = new Map();
    for (const r of (recentHist || [])) {
      const key = `${r.park_id}::${r.name_raw}`;
      const cur = prevMap.get(key);
      if (!cur || new Date(r.fetched_at) > new Date(cur.ts)) {
        prevMap.set(key, { dpa: r.dpa_status || null, pp: r.pp40_status || null, ts: r.fetched_at });
      }
    }

    for (const v of (rawDpa || [])) {
      const park_id = v.park_id;
      const name_ja = v.name;
      const name_raw = vHasRaw ? (v.name_raw || v.name) : v.name; // ない場合は日本語名をキーに（暫定）
      const nowD = v.dpa_status || null;
      const nowP = v.pp40_status || null;

      const key = `${park_id}::${name_raw}`;
      const prev = prevMap.get(key) || { dpa: null, pp: null, ts: v.fetched_at };

      const changedDpa = (prev.dpa || null) !== (nowD || null);
      const changedPp  = (prev.pp  || null) !== (nowP || null);
      if (!changedDpa && !changedPp) continue;

      // イベント名（DPA優先でラベル化）
      let event = 'dpa_update';
      let eventLabel = '販売状況が更新';
      if (changedDpa) {
        if (nowD === '販売中' && prev.dpa !== '販売中') { event = 'dpa_start'; eventLabel = 'DPA販売開始'; }
        else if (prev.dpa === '販売中' && nowD !== '販売中') { event = 'dpa_end'; eventLabel = 'DPA販売終了'; }
      } else if (changedPp) {
        if (nowP === '発行中' && prev.pp !== '発行中') { event = 'pp_start'; eventLabel = 'PP発行開始'; }
        else if (prev.pp === '発行中' && nowP !== '発行中') { event = 'pp_end'; eventLabel = 'PP発行終了'; }
      }

      // 直近送信済みならスキップ
      if (!(await shouldSendOnce('dpa', park_id, name_raw, event, v.fetched_at))) continue;

      const title = `${name_ja}：${eventLabel}`;
      const body  = `DPA: ${prev.dpa ?? '-'} → ${nowD ?? '-'} / PP: ${prev.pp ?? '-'} → ${nowP ?? '-'}`;

      await sendWebPush(title, body, '/');
      await sendPushover(title, body, '/');
    }

    return { statusCode: 202, body: 'ok' };
  } catch (e) {
    console.error(e);
    return { statusCode: 500, body: String(e?.message || e) };
  }
}
