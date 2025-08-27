// netlify/functions/ingest-tdr-dpa-background.js
// 背景関数：TDR公式の一覧を取得→DPA/PPラベル抽出→attractions/upsert + attraction_status/insert
// 追加: 直近スナップショットと比較して DPA/PP 変化をお気に入り登録者に自動通知

import { createClient } from '@supabase/supabase-js';
import * as cheerio from 'cheerio';
import webpush from 'web-push';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
const VAPID_PUBLIC_KEY = process.env.VAPID_PUBLIC_KEY;
const VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY;

webpush.setVapidDetails('mailto:admin@example.com', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

const PAGES = [
  { code: 'TDL', url: 'https://www.tokyodisneyresort.jp/tdl/attraction.html' },
  { code: 'TDS', url: 'https://www.tokyodisneyresort.jp/tds/attraction.html' },
];

function classify(labels) {
  const t = labels.join(' ');
  let dpa = '記載なし', pp = '記載なし';
  if (t.includes('ディズニー・プレミアアクセス')) {
    if (t.includes('販売中')) dpa = '販売中';
    else if (t.includes('販売なし') || t.includes('販売を行わない')) dpa = '販売なし';
    else dpa = '要確認（記載あり）';
  }
  if (t.includes('40周年記念プライオリティパス')) {
    if (t.includes('発行なし')) pp = '発行なし';
    else if (t.includes('発行中') || t.includes('対象')) pp = '発行中/対象';
    else pp = '要確認（記載あり）';
  }
  return { dpa, pp };
}

async function fetchHTML(url, ms = 20000) {
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), ms);
  try {
    const r = await fetch(url, {
      signal: ctrl.signal,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Netlify Background Function)',
        'Accept-Language': 'ja,en;q=0.8',
        'Cache-Control': 'no-cache',
      },
    });
    const body = await r.text();
    if (!r.ok) throw new Error(`HTTP ${r.status} ${r.statusText} body=${body.slice(0, 180)}`);
    return body;
  } finally {
    clearTimeout(id);
  }
}

async function sendPushToUsers(sb, userIds, title, body, meta = {}) {
  if (!userIds?.length) return { sent: 0 };
  await sb.from('notifications').insert(userIds.map((uid) => ({ user_id: uid, kind: meta.kind || 'info', title, body, meta })));
  const { data: subs } = await sb.from('push_subscriptions').select('*').in('user_id', userIds);
  const payload = JSON.stringify({ title, body, meta });
  let sent = 0;
  for (const s of subs || []) {
    const sub = { endpoint: s.endpoint, keys: { p256dh: s.p256dh, auth: s.auth } };
    try { await webpush.sendNotification(sub, payload); sent++; } catch (e) { console.error('push error', e?.message || e); }
  }
  return { sent };
}

export const handler = async () => {
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });

  const { data: parks } = await sb.from('parks').select('id, code, qt_park_id');
  const parkByCode = Object.fromEntries((parks || []).map((p) => [p.code, p]));
  for (const p of PAGES) {
    try {
      const html = await fetchHTML(p.url, 20000);
      const $ = cheerio.load(html);
      const anchors = $('a[href*="/attraction/detail/"]').toArray();

      // 解析
      const rows = anchors.map((a) => {
        const el = $(a);
        const name =
          el.find('.name, .ttl, .title, .headline').text().trim() ||
          el.attr('title') ||
          (el.text() || '').trim();
        const url = new URL(el.attr('href'), p.url).toString();
        const labels = [];
        const holder = el.closest('li, .postItem, .listItem, .col, .box');
        holder.find('.operation, .operation .warning, .label, .tag, .notes, .notice, .status').each((_, n) => {
          labels.push($(n).text().replace(/\s+/g, ' ').trim());
        });
        const { dpa, pp } = classify(labels);
        return { name, url, dpa, pp };
      });

      // attractions upsert
      const park = parkByCode[p.code];
      const upserts = rows.map((r) => ({ park_id: park.id, name: r.name, tdr_url: r.url }));
      const { data: attrs, error: upErr } = await sb
        .from('attractions')
        .upsert(upserts, { onConflict: 'park_id,name' })
        .select('id,name');
      if (upErr) throw upErr;
      const idByName = Object.fromEntries((attrs || []).map((a) => [a.name, a.id]));

      // 直近スナップショットを attraction_id ごとに取得（比較用）
      const attrIds = Object.values(idByName);
      const { data: latest } = await sb
        .from('attraction_status')
        .select('attraction_id, dpa_status, pp40_status, fetched_at')
        .in('attraction_id', attrIds)
        .order('fetched_at', { ascending: false })
        .limit(2000);

      const latestById = new Map();
      for (const row of latest || []) {
        if (!latestById.has(row.attraction_id)) latestById.set(row.attraction_id, row);
      }

      // 新スナップショットの作成
      const snaps = rows.map((r) => ({
        attraction_id: idByName[r.name],
        dpa_status: r.dpa,
        pp40_status: r.pp,
        status_operational: null,
        source: 'tdr-bg',
      }));

      // 変化検知（お気に入り登録ユーザー）
      const { data: favs } = await sb
        .from('user_favorites')
        .select('user_id, park_id, attraction_name')
        .eq('park_id', park.qt_park_id); // ★favoritesはpark_idにqt_park_id（274/275）を使う前提

      const usersByName = new Map();
      for (const f of favs || []) {
        if (!usersByName.has(f.attraction_name)) usersByName.set(f.attraction_name, new Set());
        usersByName.get(f.attraction_name).add(f.user_id);
      }

      let notified = 0;
      for (const snap of snaps) {
        const prev = latestById.get(snap.attraction_id);
        const name = rows.find((r) => idByName[r.name] === snap.attraction_id)?.name || 'アトラクション';
        const watchers = Array.from(usersByName.get(name) || []);
        if (!prev || !watchers.length) continue;

        // DPA変化
        if (prev.dpa_status !== snap.dpa_status) {
          const title =
            snap.dpa_status === '販売中'
              ? `【DPA販売中】${name}`
              : snap.dpa_status === '販売なし'
              ? `【DPA販売終了】${name}`
              : `【DPA変更】${name}`;
          const body = `DPA: ${prev.dpa_status ?? '-'} → ${snap.dpa_status ?? '-'}`;
          await sendPushToUsers(sb, watchers, title, body, {
            kind: 'dpa-change',
            park_code: p.code,
            name,
            prev: prev.dpa_status,
            cur: snap.dpa_status,
          });
          notified++;
        }

        // PP40変化（必要なら）
        if (prev.pp40_status !== snap.pp40_status) {
          const title = `【PP(40th)変更】${name}`;
          const body = `PP: ${prev.pp40_status ?? '-'} → ${snap.pp40_status ?? '-'}`;
          await sendPushToUsers(sb, watchers, title, body, {
            kind: 'pp40-change',
            park_code: p.code,
            name,
            prev: prev.pp40_status,
            cur: snap.pp40_status,
          });
          notified++;
        }
      }

      // スナップショット挿入
      const { error: snapErr } = await sb.from('attraction_status').insert(snaps);
      if (snapErr) throw snapErr;

      console.log(`ingest ${p.code}: rows=${rows.length}, notified=${notified}`);
    } catch (e) {
      console.error('ingest background error', p.code, e?.message || e);
    }
  }

  // 背景関数は 202 を即返す
};
