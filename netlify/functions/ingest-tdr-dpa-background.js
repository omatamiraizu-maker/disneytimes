// netlify/functions/ingest-tdr-dpa-background.mjs
import { createClient } from '@supabase/supabase-js';
import * as cheerio from 'cheerio';

const PAGES = [
  { code: 'TDL', url: 'https://www.tokyodisneyresort.jp/tdl/attraction.html' },
  { code: 'TDS', url: 'https://www.tokyodisneyresort.jp/tds/attraction.html' },
];

// ---- 小物（AWS版と同じ思想） ----
const errStr = (e) => e?.message || (()=>{ try{return JSON.stringify(e)}catch{return String(e) } })();

async function fetchHTML(url, ms = 20000) {
  const ctl = new AbortController(); const id = setTimeout(()=>ctl.abort(), ms);
  try {
    const r = await fetch(url, {
      signal: ctl.signal,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Netlify; TDR DPA)',
        'Accept-Language': 'ja,en;q=0.8',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'Referer': 'https://www.tokyodisneyresort.jp/',
      },
    });
    const body = await r.text();
    if (!r.ok) throw new Error(`HTTP ${r.status} ${r.statusText} body=${body.slice(0,160)}`);
    return body;
  } finally { clearTimeout(id); }
}

function normalizeKey(s=''){
  return s.replace(/[’＇`´‘]/g,"'")
          .replace(/[“”＂]/g,'"')
          .replace(/[‐‑‒–—―ーｰ]/g,'ー')
          .replace(/[&＆]/g,'&')
          .replace(/\s+/g,' ')
          .trim();
}
function parsePage($, baseUrl) {
  const dateDiv = {};
  $('div[class*="str_id-"]').each((_, div)=>{
    const cls = $(div).attr('class')||''; const m = cls.match(/str_id-(\d+)/); if (m) dateDiv[m[1]]=$(div);
  });
  const rows = [];
  $('li[data-categorize][data-area]').each((_, li)=>{
    const $li=$(li);
    const $a=$li.find('a[href*="/attraction/detail/"]').first(); if(!$a.length) return;
    const name = ($li.find('h3.heading3').first().text()||'').replace(/\s+/g,' ').trim(); if(!name) return;
    const href=$a.attr('href')||''; const url=new URL(href, baseUrl).toString();

    let blob=$li.html()||'';
    const mid=href.match(/\/(\d+)\/$/);
    if(mid && dateDiv[mid[1]]) {
      const $d=dateDiv[mid[1]];
      blob += ' '+($d.html()||'');
      $d.find('span.operation.warning').each((_,e)=>{ blob += ' '+($(e).text()||'').replace(/\s+/g,' ').trim(); });
    }
    $li.find('.realtimeInformation span.operation.warning').each((_,e)=>{ blob += ' '+($(e).text()||'').replace(/\s+/g,' ').trim(); });

    let dpa='記載なし';
    if (blob.includes('ディズニー・プレミアアクセス販売中')) dpa='販売中';
    else if (blob.includes('ディズニー・プレミアアクセス販売なし') || blob.includes('販売を行わない')) dpa='販売なし';
    else if (blob.includes('販売終了')) dpa='販売終了';
    else if (blob.includes('ディズニー・プレミアアクセス対象')) dpa='要確認（記載あり）';

    let pp='記載なし';
    if (blob.includes('プライオリティパス発行中')) pp='発行中/対象';
    else if (blob.includes('プライオリティパス発行なし')) pp='発行なし';
    else if (blob.includes('発行終了')) pp='発行終了';
    else if (blob.includes('プライオリティパス対象')) pp='対象';

    rows.push({ name, url, dpa, pp });
  });
  // park 内で重複名があれば“強い方”で統合
  const rankDpa = (s)=>({ '販売中':4,'販売なし':3,'販売終了':3,'要確認（記載あり）':2,'記載なし':1 }[s]||0);
  const rankPp  = (s)=>({ '発行中/対象':4,'対象':3,'発行なし':2,'発行終了':2,'記載なし':1 }[s]||0);
  const map = new Map();
  for (const r of rows){
    const k = normalizeKey(r.name);
    const ex = map.get(k);
    if (!ex) map.set(k,r);
    else map.set(k, {
      name:r.name, url:r.url||ex.url,
      dpa: rankDpa(r.dpa)>=rankDpa(ex.dpa)? r.dpa:ex.dpa,
      pp:  rankPp(r.pp)  >=rankPp(ex.pp) ? r.pp :ex.pp,
    });
  }
  return [...map.values()];
}

export default async (req, ctx) => {
  const SUPABASE_URL = process.env.SUPABASE_URL;
  const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth:{ persistSession:false } });

  try {
    const { data: parks, error: pErr } = await sb.from('parks').select('id,code');
    if (pErr) throw pErr;
    const parkByCode = Object.fromEntries((parks||[]).map(p=>[p.code, p]));

    for (const p of PAGES){
      try{
        const html = await fetchHTML(p.url, 20000);
        const $ = cheerio.load(html);
        const rows = parsePage($, p.url);
        const park = parkByCode[p.code];
        if (!park || !rows.length) continue;

        // attractions upsert
        const upserts = rows.map(r => ({ park_id: park.id, name: r.name, tdr_url: r.url }));
        const { data: attrs, error: upErr } = await sb
          .from('attractions')
          .upsert(upserts, { onConflict: 'park_id,name' })
          .select('id,name');
        if (upErr) throw upErr;
        const idByName = Object.fromEntries((attrs||[]).map(a=>[a.name,a.id]));

        // 最新スナップショット insert
        const snaps = rows.map(r=>({
          attraction_id: idByName[r.name],
          dpa_status: r.dpa,
          pp40_status: r.pp,
          status_operational: null,
          source: 'tdr-netlify'
        })).filter(s=>!!s.attraction_id);
        if (snaps.length){
          const { error: sErr } = await sb.from('attraction_status').insert(snaps);
          if (sErr) throw sErr;
        }
        console.log(`ingest ${p.code}: rows=${rows.length}, inserted=${snaps.length}`);
      }catch(e){
        console.error(`ingest error ${p.code}:`, errStr(e));
      }
    }
    return new Response(JSON.stringify({ ok:true }), { status:200 });
  } catch(e){
    console.error('fatal', errStr(e));
    return new Response(JSON.stringify({ ok:false, error:errStr(e) }), { status:500 });
  }
}
