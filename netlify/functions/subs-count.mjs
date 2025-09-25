import { createClient } from '@supabase/supabase-js';

export async function handler() {
  const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE, { auth: { persistSession: false }});
  const { data, error } = await sb.from('push_subscriptions').select('endpoint, p256dh, auth').limit(5);
  return {
    statusCode: error ? 500 : 200,
    body: JSON.stringify({
      ok: !error,
      sample: (data || []).map(x => ({
        endpoint_ok: !!x.endpoint,
        p256dh_ok: !!x.p256dh,
        auth_ok: !!x.auth
      })),
      count_hint: (data || []).length
    })
  };
}
