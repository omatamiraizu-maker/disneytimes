export default async (req) => {
  try{
    const body = await req.json();
    const auth = req.headers.get('authorization') || '';
    const bearer = auth.startsWith('Bearer ') ? auth.slice(7) : null;

    const { device_id, subscription, park_id, favs, rules } = body || {};
    if (!subscription?.endpoint) return new Response('Bad subscription', { status: 400 });

    // 1) user_id の同定（あれば）— 任意
    let user_id = null;
    if (bearer) {
      // Supabase JWT を軽くデコード（検証は省略。必要に応じて /auth/v1/user 叩く）
      try { user_id = JSON.parse(atob(bearer.split('.')[1])).sub || null; } catch {}
    }

    // 2) Supabase へ保存（Device優先の匿名購読）
    const SUPABASE_URL = process.env.SUPABASE_URL;
    const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
    const upsert = async (table, row, conflictTarget) => {
      const res = await fetch(`${SUPABASE_URL}/rest/v1/${table}?on_conflict=${encodeURIComponent(conflictTarget)}`, {
        method: 'POST',
        headers: {
          'apikey': SUPABASE_SERVICE_ROLE,
          'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE}`,
          'Content-Type': 'application/json',
          'Prefer': 'resolution=merge-duplicates'
        },
        body: JSON.stringify([row])
      });
      if (!res.ok) throw new Error(`${table} upsert failed: ${await res.text()}`);
    };

    // push_subscriptions（endpoint一意）: user_idはnull可・device_idはnull可（今回はdeviceを主に）
    await upsert('push_subscriptions', {
      endpoint: subscription.endpoint,
      p256dh: subscription.keys?.p256dh || null,
      auth:   subscription.keys?.auth   || null,
      device_id: device_id || null,
      user_id
    }, 'endpoint');

    // device_profiles（なければ）
    if (device_id) await upsert('device_profiles', { device_id }, 'device_id');

    // device_rules/favs（匿名通知用のルール＆★を保存）
    if (device_id && park_id){
      if (Array.isArray(favs)){
        // 全消し→再挿入でもよいが、ここは上書きUpsertで簡易に
        // テーブル側のuniqueは (device_id, park_id, attraction_name)
        // バルクUpsert
        const rows = favs.map(a => ({ device_id, park_id, attraction_name: a }));
        if (rows.length){
          const res = await fetch(`${SUPABASE_URL}/rest/v1/device_favorites`, {
            method:'POST',
            headers:{ 'apikey':SUPABASE_SERVICE_ROLE, 'Authorization':`Bearer ${SUPABASE_SERVICE_ROLE}`, 'Content-Type':'application/json', 'Prefer':'resolution=merge-duplicates' },
            body: JSON.stringify(rows)
          });
          if (!res.ok) throw new Error('device_favorites upsert failed: ' + await res.text());
        }
      }
      if (rules){
        await upsert('device_alert_rules', {
          device_id, park_id,
          notify_close_reopen: !!rules.notify_close_reopen,
          notify_dpa_sale: !!rules.notify_dpa_sale,
          spike_threshold: rules.spike_threshold ?? 20,
          window_minutes: rules.window_minutes ?? 10
        }, 'device_id,park_id');
      }
    }

    return new Response('OK', { status: 200 });
  }catch(e){
    return new Response('ERR ' + e.message, { status: 500 });
  }
}
