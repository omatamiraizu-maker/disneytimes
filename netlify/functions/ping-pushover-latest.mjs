export async function handler() {
  const { SUPABASE_URL, SUPABASE_SERVICE_ROLE, PUSHOVER_TOKEN } = process.env;
  if (!PUSHOVER_TOKEN) return { statusCode: 500, body: 'PUSHOVER_TOKEN missing' };

  // 直近の宛先を1件取得
  const { createClient } = await import('@supabase/supabase-js');
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession:false }});
  const { data, error } = await sb.from('pushover_profiles').select('user_key,label').limit(1);
  if (error) return { statusCode: 500, body: error.message };
  if (!data?.length) return { statusCode: 404, body: 'no pushover_profiles' };

  const user = data[0].user_key;
  try {
    const res = await fetch('https://api.pushover.net/1/messages.json', {
      method: 'POST',
      headers: { 'Content-Type':'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        token: PUSHOVER_TOKEN,
        user,
        title: 'テスト通知',
        message: 'DB最新のuser_key宛に送信',
        priority: '1',          // ← DND回避テスト（重要）
        sound: 'magic'          // 任意：聞こえやすく
      })
    });
    const text = await res.text();
    return { statusCode: res.ok ? 200 : 500, body: text };
  } catch (e) {
    return { statusCode: 500, body: String(e?.message || e) };
  }
}
