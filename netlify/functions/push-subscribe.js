import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;

export const handler = async (event) => {
  if (event.httpMethod !== 'POST') return { statusCode: 405, body: 'Method Not Allowed' };
  try {
    const token = (event.headers.authorization || '').replace(/^Bearer\s+/i,'');
    if (!token) return { statusCode: 401, body: 'missing token' };
    const body = JSON.parse(event.body || '{}');
    const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });
    // Supabase 管理APIでトークンを検証し user.id を取得
    const { data: u, error: uerr } = await sb.auth.getUser(token);
    if (uerr || !u?.user?.id) return { statusCode: 401, body: 'invalid token' };
    const userId = u.user.id;
    
    const row = {
      user_id: userId,
      endpoint: body.endpoint,
      p256dh: body.keys?.p256dh || null,
      auth: body.keys?.auth || null,
    };
    const { error } = await sb.from('push_subscriptions')
      .upsert(row, { onConflict: 'user_id,endpoint' });
    if (error) throw error;
    return { statusCode: 200, body: JSON.stringify({ ok: true }) };
  } catch (e) {
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: String(e?.message || e) }) };
  }
};
