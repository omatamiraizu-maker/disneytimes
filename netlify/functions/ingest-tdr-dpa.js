// netlify/functions/ingest-tdr-dpa.js
// Scrape TDR attraction listing pages (no headless browser), parse DPA / PP40 labels, store to Supabase
import { createClient } from '@supabase/supabase-js';
import * as cheerio from 'cheerio';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
const PAGES = [
  { code: 'TDL', url: 'https://www.tokyodisneyresort.jp/tdl/attraction.html' },
  { code: 'TDS', url: 'https://www.tokyodisneyresort.jp/tds/attraction.html' },
];

function classify(labels) {
  const joined = labels.join(' ');
  let dpa = '記載なし';
  let pp = '記載なし';
  if (joined.includes('ディズニー・プレミアアクセス')) {
    if (joined.includes('販売中')) dpa = '販売中';
    else if (joined.includes('販売なし') || joined.includes('販売を行わない')) dpa = '販売なし';
    else dpa = '要確認（記載あり）';
  }
  if (joined.includes('40周年記念プライオリティパス')) {
    if (joined.includes('発行なし')) pp = '発行なし';
    else if (joined.includes('発行中') || joined.includes('対象')) pp = '発行中/対象';
    else pp = '要確認（記載あり）';
  }
  return { dpa, pp };
}

export const handler = async () => {
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });

  // Map parks
  const { data: parks, error: parksErr } = await sb.from('parks').select('*');
  if (parksErr) return { statusCode: 500, body: JSON.stringify({ error: String(parksErr) }) };

  const parkByCode = Object.fromEntries(parks.map(p => [p.code, p]));

  const out = [];
  for (const p of PAGES) {
    try {
      const resp = await fetch(p.url, { headers: { 'User-Agent': 'Mozilla/5.0 (Netlify Function)' }});
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const html = await resp.text();
      const $ = cheerio.load(html);

      // Each card in listing page often appears as ".postItem" etc; be resilient
      const cards = $('a[href*="/attraction/detail/"]').toArray();
      const rows = [];
      for (const a of cards) {
        const el = $(a);
        const name = el.find('.name, .ttl, .title, .headline').text().trim() || el.attr('title') || el.text().trim();
        const url = new URL(el.attr('href'), p.url).toString();
        // Collect labels near the anchor
        const labels = [];
        el.closest('li, .postItem, .listItem, .col, .box').find('.operation, .operation .warning, .label, .tag, .notes, .notice, .status').each((i, lab) => {
          labels.push($(lab).text().replace(/\s+/g, ' ').trim());
        });
        const { dpa, pp } = classify(labels);

        rows.push({ name, url, labels, dpa, pp });
      }

      // Upsert attractions
      for (const r of rows) {
        const park = parkByCode[p.code];
        // upsert attraction
        let { data: attr, error: aerr } = await sb.from('attractions')
          .upsert({ park_id: park.id, name: r.name, tdr_url: r.url }, { onConflict: 'park_id,name' })
          .select('id')
          .single();
        if (aerr) throw aerr;
        // insert status snapshot
        const { error: serr } = await sb.from('attraction_status').insert({
          attraction_id: attr.id,
          dpa_status: r.dpa,
          pp40_status: r.pp,
          status_operational: null,
          source: 'tdr'
        });
        if (serr) throw serr;
      }

      out.push({ park: p.code, count: rows.length });
    } catch (err) {
      out.push({ park: p.code, error: String(err) });
    }
  }

  return { statusCode: 200, body: JSON.stringify({ ok: true, out }) };
};