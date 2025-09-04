// Netlify Background Function: TDR DPA/PP ingest → Supabase
import { createClient } from '@supabase/supabase-js';
import * as cheerio from 'cheerio';

const { SUPABASE_URL, SUPABASE_SERVICE_ROLE } = process.env;

// 収集対象
const PAGES = [
  { code: 'TDL', url: 'https://www.tokyodisneyresort.jp/tdl/attraction.html' },
  { code: 'TDS', url: 'https://www.tokyodisneyresort.jp/tds/attraction.html' },
];

// ---- ユーティリティ／HTTP ----
const FETCH_TIMEOUT_MS = Number(process.env.TDR_FETCH_TIMEOUT_MS || 45000);
const RETRIES = Number(process.env.TDR_FETCH_RETRIES || 2);

const errStr = (e) => {
  if (!e) return 'unknown';
  if (typeof e === 'string') return e;
  if (e?.message) return e.message;
  try { return JSON.stringify(e); } catch { return String(e); }
};

async function fetchHTMLOnce(url, ms = FETCH_TIMEOUT_MS) {
  const ctl = new AbortController(); const id = setTimeout(() => ctl.abort(), ms);
  try {
    const r = await fetch(url, {
      signal: ctl.signal,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Netlify; TDR DPA)',
        'Accept-Language': 'ja,en;q=0.8',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'Referer': 'https://www.tokyodisneyresort.jp/',
      }
    });
    const body = await r.text();
    if (!r.ok) throw new Error(`HTTP ${r.status} ${r.statusText} body=${body.slice(0,180)}`);
    return body;
  } finally { clearTimeout(id); }
}

async function fetchHTML(url) {
  let lastErr;
  for (let i = 0; i <= RETRIES; i++) {
    try { return await fetchHTMLOnce(url); }
    catch (e) {
      lastErr = e;
      // AbortError（=タイムアウト等）は少し待って再試行
      await new Promise(r => setTimeout(r, 1500 * (i + 1)));
    }
  }
  throw new Error(`fetch failed after ${RETRIES+1} tries: ${errStr(lastErr)}`);
}

// ---- 解析（元実装に準拠） ----
function parsePage($, baseUrl) {
  const dateDiv = {};
  $('div[class*="str_id-"]').each((_, div) => {
    const cls = $(div).attr('class') || '';
    const m = cls.match(/str_id-(\d+)/);
    if (m) dateDiv[m[1]] = $(div);
  });

  const rows = [];
  $('li[data-categorize][data-area]').each((_, li) => {
    const $li = $(li);
    const $a = $li.find('a[href*="/attraction/detail/"]').first();
    if (!$a.length) return;

    const name = ($li.find('h3.heading3').first().text() || '').replace(/\s+/g, ' ').trim();
    if (!name) return;

    const href = $a.attr('href') || '';
    const url = new URL(href, baseUrl).toString();

    let blob = $li.html() || '';
    const mid = href.match(/\/(\d+)\/$/);
    if (mid && dateDiv[mid[1]]) {
      const $d = dateDiv[mid[1]];
      blob += ' ' + ($d.html() || '');
      $d.find('span.operation.warning').each((_, e) => {
        blob += ' ' + ($(e).text() || '').replace(/\s+/g, ' ').trim();
      });
    }
    $li.find('.realtimeInformation span.operation.warning').each((_, e) => {
      blob += ' ' + ($(e).text() || '').replace(/\s+/g, ' ').trim();
    });

    // DPA
    let dpa = '記載なし';
    if (blob.includes('ディズニー・プレミアアクセス販売中')) dpa = '販売中';
    else if (blob.includes('ディズニー・プレミアアクセス販売なし') || blob.includes('販売を行わない')) dpa = '販売なし';
    else if (blob.includes('販売終了')) dpa = '販売終了';
    else if (blob.includes('ディズニー・プレミアアクセス対象')) dpa = '要確認（記載あり）';

    // PP
    let pp = '記載なし';
    if (blob.includes('40周年記念プライオリティパス発行中') || blob.includes('プライオリティパス発行中')) pp = '発行中/対象';
    else if (blob.includes('40周年記念プライオリティパス発行なし') || blob.includes('プライオリティパス発行なし')) pp = '発行なし';
    else if (blob.includes('発行終了')) pp = '発行終了';
    else if (blob.includes('プライオリティパス対象') || blob.includes('40周年記念プライオリティパス対象')) pp = '対象';

    rows.push({ name, url, dpa, pp });
  });

  return rows;
}

const normalizeKey = (s='') =>
  s.replace(/[’＇`´‘]/g,"'")
   .replace(/[“”＂]/g,'"')
   .replace(/[‐‑‒–—―ーｰ]/g,'ー')
   .replace(/[&＆]/g,'&')
   .replace(/\s+/g,' ')
   .trim();

const rankDpa = (s) => ({ '販売中':4, '販売なし':3, '販売終了':3, '要確認（記載あり）':2, '記載なし':1, null:0, undefined:0 }[s] ?? 0);
const rankPp  = (s) => ({ '発行中/対象':4, 'PP発行中':4, 'PP対象':3, '対象':3, '発行なし':2, 'PP発行なし':2, '発行終了':2, 'PP発行終了':2, '記載なし':1, null:0, undefined:0 }[s] ?? 0);
const mergeRow = (a,b) => ({ name:a.name, url:a.url||b.url, dpa: rankDpa(a.dpa)>=rankDpa(b.dpa)?a.dpa:b.dpa, pp: rankPp(a.pp)>=rankPp(b.pp)?a.pp:b.pp });
const dedupeRows = (rows) => {
  const m = new Map();
  for (const r of rows) {
    if (!r?.name) continue;
    const k = normalizeKey(r.name);
    if (!m.has(k)) m.set(k, r); else m.set(k, mergeRow(m.get(k), r));
  }
  return [...m.values()];
};

// ---- Background Function 本体 ----
export default async (request) => {
  if (request.method !== 'POST') return new Response('Use POST', { status: 405 });

  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });

  try {
    const { data: parks, error: pErr } = await sb.from('parks').select('id,code,qt_park_id');
    if (pErr) throw new Error(`parks select: ${errStr(pErr)}`);
    const parkByCode = Object.fromEntries((parks || []).map(p => [p.code, p]));

    for (const p of PAGES) {
      try {
        const html = await fetchHTML(p.url);
        const $ = cheerio.load(html);

        const rows = dedupeRows(parsePage($, p.url));
        if (!rows.length) { console.warn(`ingest ${p.code}: rows=0`); continue; }

        const park = parkByCode[p.code];
        if (!park) throw new Error(`parks not found for code=${p.code}`);

        // attractions upsert
        const upserts = rows.map(r => ({ park_id: park.id, name: r.name, tdr_url: r.url }));
        const { data: attrs, error: upErr } = await sb
          .from('attractions')
          .upsert(upserts, { onConflict: 'park_id,name' })
          .select('id,name');
        if (upErr) throw new Error(`attractions upsert: ${errStr(upErr)}`);

        const idByName = Object.fromEntries((attrs || []).map(a => [a.name, a.id]));

        // 最新スナップショット
        const snaps = rows.map(r => ({
          attraction_id: idByName[r.name],
          dpa_status: r.dpa,
          pp40_status: r.pp,
          status_operational: null,
          source: 'netlify'
        })).filter(s => !!s.attraction_id);

        if (snaps.length) {
          const { error: sErr } = await sb.from('attraction_status').insert(snaps);
          if (sErr) throw new Error(`attraction_status insert: ${errStr(sErr)}`);
        }
        console.log(`ingest ${p.code}: inserted=${snaps.length}`);
      } catch (e) {
        console.error(`ingest error ${p.code}:`, errStr(e));
      }
    }

    // Background Function としては 202 を返すのが普通（戻り値は UI では使わない）
    return new Response(JSON.stringify({ ok: true }), { status: 202, headers: { 'Content-Type':'application/json' } });
  } catch (e) {
    console.error('fatal', errStr(e));
    return new Response(JSON.stringify({ ok:false, error: errStr(e) }), { status: 500, headers: { 'Content-Type':'application/json' } });
  }
};
