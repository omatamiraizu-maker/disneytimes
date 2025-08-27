// netlify/functions/ingest-tdr-dpa-background.js
// Background Function: 実行すると 202 で即時返答。処理は裏で継続。
// 本文は返りません（Netlify仕様）。スケジュール実行もこの挙動です。
import { createClient } from '@supabase/supabase-js';
import * as cheerio from 'cheerio';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;

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
        'Cache-Control': 'no-cache'
      },
    });
    const body = await r.text();
    if (!r.ok) throw new Error(`HTTP ${r.status} ${r.statusText} body=${body.slice(0,180)}`);
    return body;
  } finally {
    clearTimeout(id);
  }
}

export const handler = async () => {
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });
  const { data: parks } = await sb.from('parks').select('*');
  const parkByCode = Object.fromEntries((parks || []).map((p) => [p.code, p]));

  for (const p of PAGES) {
    try {
      const html = await fetchHTML(p.url, 20000);
      const $ = cheerio.load(html);
      const anchors = $('a[href*="/attraction/detail/"]').toArray();

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

      const park = parkByCode[p.code];
      const upserts = rows.map((r) => ({ park_id: park.id, name: r.name, tdr_url: r.url }));
      const { data: attrs, error: upErr } = await sb
        .from('attractions')
        .upsert(upserts, { onConflict: 'park_id,name' })
        .select('id,name');
      if (upErr) throw upErr;

      const idByName = Object.fromEntries((attrs || []).map((a) => [a.name, a.id]));
      const snaps = rows.map((r) => ({
        attraction_id: idByName[r.name],
        dpa_status: r.dpa,
        pp40_status: r.pp,
        status_operational: null,
        source: 'tdr-bg',
      }));
      const { error: snapErr } = await sb.from('attraction_status').insert(snaps);
      if (snapErr) throw snapErr;
    } catch (e) {
      // 背景関数なのでレスポンスは返せない。ログに出す。
      console.error('ingest background error', p.code, e);
    }
  }
  // 背景関数は 202 を即返す（Netlify仕様）
};
