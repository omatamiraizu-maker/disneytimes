// netlify/functions/ingest-tdr-dpa.js
// TDR公式一覧ページを短時間でスクレイピング → Supabase に保存
import { createClient } from '@supabase/supabase-js';
import * as cheerio from 'cheerio';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;

const PAGES = [
  { code: 'TDL', url: 'https://www.tokyodisneyresort.jp/tdl/attraction.html' },
  { code: 'TDS', url: 'https://www.tokyodisneyresort.jp/tds/attraction.html' },
];

// 8秒でタイムアウトする fetch
async function fetchWithTimeout(url, ms = 8000, opts = {}) {
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), ms);
  try {
    const r = await fetch(url, {
      ...opts,
      signal: ctrl.signal,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Netlify Function)',
        'Accept-Language': 'ja,en;q=0.8',
        ...(opts.headers || {}),
      },
    });
    return r;
  } finally {
    clearTimeout(id);
  }
}

function classify(labels) {
  const t = labels.join(' ');
  let dpa = '記載なし';
  let pp = '記載なし';
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

export const handler = async () => {
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });
  const { data: parks } = await sb.from('parks').select('*');
  const parkByCode = Object.fromEntries((parks || []).map((p) => [p.code, p]));

  const out = [];
  for (const p of PAGES) {
    try {
      const resp = await fetchWithTimeout(p.url);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const html = await resp.text();
      const $ = cheerio.load(html);

      // a[href*="/attraction/detail/"] のみ抽出して最小限の領域だけテキスト化（高速化）
      const anchors = $('a[href*="/attraction/detail/"]').toArray();
      const rows = anchors.map((a) => {
        const el = $(a);
        const name =
          el.find('.name, .ttl, .title, .headline').text().trim() ||
          el.attr('title') ||
          (el.text() || '').trim();
        const url = new URL(el.attr('href'), p.url).toString();

        // 近傍の注意ラベルから DPA/PP を判定
        const labels = [];
        const holder = el.closest('li, .postItem, .listItem, .col, .box');
        holder.find('.operation, .operation .warning, .label, .tag, .notes, .notice, .status').each((_, n) => {
          labels.push($(n).text().replace(/\s+/g, ' ').trim());
        });
        const { dpa, pp } = classify(labels);
        return { name, url, dpa, pp };
      });

      // Supabase へ upsert + snapshot insert（バルク）
      const park = parkByCode[p.code];
      // attractions upsert（重複を避けるため名前とparkでユニーク）
      const upserts = rows.map((r) => ({ park_id: park.id, name: r.name, tdr_url: r.url }));
      const { data: attrs, error: upErr } = await sb
        .from('attractions')
        .upsert(upserts, { onConflict: 'park_id,name' })
        .select('id,name');
      if (upErr) throw upErr;

      const idByName = Object.fromEntries((attrs || []).map((a) => [a.name, a.id]));
      const snapshots = rows.map((r) => ({
        attraction_id: idByName[r.name],
        dpa_status: r.dpa,
        pp40_status: r.pp,
        status_operational: null,
        source: 'tdr',
      }));
      const { error: snapErr } = await sb.from('attraction_status').insert(snapshots);
      if (snapErr) throw snapErr;

      out.push({ park: p.code, count: snapshots.length });
    } catch (e) {
      out.push({ park: p.code, error: String(e) });
    }
  }

  return { statusCode: 200, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ok: true, out }) };
};
