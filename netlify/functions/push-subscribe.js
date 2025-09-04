// 保存だけ（認証ヘッダはフロント側で付けています）
import { createClient } from '@supabase/supabase-js';
export async function handler(event){
  const { SUPABASE_URL, SUPABASE_SERVICE_ROLE } = process.env;
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth:{ persistSession:false }});
  const sub = JSON.parse(event.body||'{}');

  try{
    // park_id は現状固定（両パークを対象にしたい場合はNULLで保存→送信側で park_id 無視でもOK）
    const row = {
      endpoint: sub.endpoint, p256dh: sub.keys?.p256dh, auth: sub.keys?.auth,
      park_id: null // 使うならリクエストに同梱
    };
    // 重複 upsert
    await sb.from('push_subscriptions').upsert(row, { onConflict:'endpoint' });
    return { statusCode:200, body:'ok' };
  }catch(e){
    return { statusCode:500, body:String(e?.message||e) };
  }
}
