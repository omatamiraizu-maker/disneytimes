// netlify/functions/notify-dpa-window-background.mjs
import { createClient } from '@supabase/supabase-js';
import webpush from 'web-push';

const { SUPABASE_URL, SUPABASE_SERVICE_ROLE, VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY } = process.env;
webpush.setVapidDetails('mailto:notify@example.com', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

export async function handler(){
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth:{ persistSession:false }});
  try{
    // 直近 3 分で DPA/PP に変化があったアトラクションを拾う（簡易）
    const { data:changes } = await sb.rpc('sp_recent_status_changes', { minutes: 3 }); // 既存RPCがなければ SQL 直書きでもOK

    if (!changes?.length) return { statusCode:202, body:'no changes' };

    // 通知対象ユーザーを引いて送る（簡易：全員 + park_id マッチ）
    for (const ch of changes){
      const { data:subs } = await sb.from('push_subscriptions').select('id,user_id,endpoint,p256dh,auth').eq('park_id', ch.park_id);
      if (!subs?.length) continue;

      const title = `${ch.name} の販売状況が更新`;
      const body  = `DPA:${ch.dpa_status||'-'} / PP:${ch.pp40_status||'-'}`;

      // DB の notifications にも記録（フロントのフィードで見えるように）
      await sb.from('notifications').insert({ title, body });

      // WebPush 送信（失敗は握りつぶし）
      for(const s of subs){
        const payload = JSON.stringify({ title, body, url: '/' });
        const pushSub = { endpoint:s.endpoint, keys:{ p256dh:s.p256dh, auth:s.auth } };
        webpush.sendNotification(pushSub, payload).catch(()=>{});
      }
    }
    return { statusCode:202, body:'ok' };
  }catch(e){
    console.error(e);
    return { statusCode:500, body:String(e?.message||e) };
  }
}
