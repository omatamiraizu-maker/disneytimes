// netlify/functions/push-subscribe.mjs
import { createClient } from '@supabase/supabase-js';

const { SUPABASE_URL, SUPABASE_SERVICE_ROLE } = process.env;

const json = (obj, status = 200) =>
  new Response(JSON.stringify(obj), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8' },
  });

export const handler = async (request) => {
  try {
    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
      return json({ ok: false, error: 'Missing Supabase envs' }, 500);
    }
    const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });

    const body = await request.json().catch(() => ({}));
    const sub = body?.subscription || {};
    const device_id = body?.device_id || null;

    if (!sub?.endpoint || !sub?.keys?.p256dh || !sub?.keys?.auth) {
      return json({ ok: false, error: 'invalid subscription' }, 400);
    }

    // （任意）Authorization ヘッダがあれば user を取得
    let user_id = null;
    try {
      const auth = request.headers.get('authorization') || '';
      const token = auth.startsWith('Bearer ') ? auth.slice(7) : null;
      if (token) {
        const userRes = await sb.auth.getUser(token);
        user_id = userRes?.data?.user?.id || null;
      }
    } catch {}

    // push_subscriptions を UPSERT（device_id を必ず保存）
    const row = {
      endpoint: sub.endpoint,
      p256dh: sub.keys.p256dh,
      auth: sub.keys.auth,
      device_id,
      user_id,          // null でもOK
    };

    // endpoint を一意と見なして upsert
    const up = await sb
      .from('push_subscriptions')
      .upsert(row, { onConflict: 'endpoint' })
      .select('id, device_id, user_id')
      .single();

    if (up.error) {
      // たまに onConflict 未対応の環境があるため保険
      const del = await sb.from('push_subscriptions').delete().eq('endpoint', row.endpoint);
      if (del.error) console.warn('cleanup error:', del.error);
      const ins = await sb.from('push_subscriptions').insert(row).select('id, device_id, user_id').single();
      if (ins.error) return json({ ok: false, error: ins.error.message }, 500);
    }

    // （任意）端末・お気に入り・ルールのスナップショットも保存しておく
    if (device_id && Number.isInteger(body?.park_id)) {
      const park_id = body.park_id;
      // device_favorites を簡易同期（挿し替え/追加は運用に合わせて）
      if (Array.isArray(body?.favs)) {
        // 既存を消して差し替えたいなら以下の1行を有効化
        // await sb.from('device_favorites').delete().match({ device_id, park_id });
        const rows = body.favs.map(name => ({ device_id, park_id, attraction_name: name }));
        if (rows.length) await sb.from('device_favorites').insert(rows).catch(()=>{});
      }
      if (body?.rules && typeof body.rules === 'object') {
        const r = body.rules;
        await sb.from('device_alert_rules').upsert({
          device_id, park_id,
          notify_close_reopen: !!r.notify_close_reopen,
          notify_dpa_sale:     !!r.notify_dpa_sale,
          spike_threshold:     Number.isFinite(r.spike_threshold) ? r.spike_threshold : 20,
          window_minutes:      Number.isFinite(r.window_minutes) ? r.window_minutes : 10,
        }, { onConflict: 'device_id,park_id' }).catch(()=>{});
      }
    }

    return json({ ok: true });
  } catch (err) {
    console.error('push-subscribe error:', err);
    return json({ ok: false, error: String(err?.message || err) }, 500);
  }
};
