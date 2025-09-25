// netlify/functions/notify-all.mjs
import { createClient } from '@supabase/supabase-js';
import webpush from 'web-push';

const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE,
  VAPID_PUBLIC_KEY,
  VAPID_PRIVATE_KEY,
  PUSHOVER_TOKEN,
} = process.env;

webpush.setVapidDetails('mailto:notify@example.com', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

async function sendWebPush(sb, subs, title, body, url = '/') {
  if (!subs.length) return;
  for (const s of subs) {
    const sub = { endpoint: s.endpoint, keys: { p256dh: s.p256dh, auth: s.auth } };
    try {
      await webpush.sendNotification(sub, JSON.stringify({ title, body, url }));
      await sb.from('notifications').insert({ kind:'webpush', title, body });
    } catch (err) {
      if (err?.statusCode === 410 || err?.statusCode === 404) {
        await sb.from('push_subscriptions').delete().eq('endpoint', s.endpoint).catch(()=>{});
      } else {
        await sb.from('notifications').insert({ kind:'webpush-error', title, body: String(err) });
      }
    }
  }
}

async function sendPushover(sb, title, message, url = '/', token, users) {
  if (!token || !users.length) return;
  for (const user of users) {
    const body = new URLSearchParams({ token, user, title, message, url, url_title: '開く', priority: '0' });
    try {
      const res = await fetch('https://api.pushover.net/1/messages.json', {
        method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body
      });
      const txt = await res.text();
      if (res.ok) {
        await sb.from('notifications').insert({ kind:'pushover', title, body: message });
      } else {
        await sb.from('notifications').insert({ kind:'pushover-error', title:`[${res.status}] ${title}`, body:txt });
      }
    } catch (e) {
      await sb.from('notifications').insert({ kind:'pushover-error', title, body: String(e) });
    }
  }
}

export async function handler() {
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });

  try {
    // 通知先
    const { data: subsPush } = await sb.from('push_subscriptions').select('endpoint,p256dh,auth');
    const pushSubs = subsPush || [];
    const { data: poProfiles } = await sb.from('pushover_profiles').select('user_key');
    const poUsers = [...new Set((poProfiles || []).map(p => p.user_key).filter(Boolean))];

    // 変化ありの行をすべて取得
    const { data: changed, error: eChg } = await sb
      .from('attraction_state')
      .select('id,park_id,name_ja,inopen_bef,inopen_now,dpastatus_bef,dpastatus_now,ppstatus_bef,ppstatus_now,has_changed')
      .eq('has_changed', true);

    if (eChg) {
      return { statusCode: 500, body: 'query error: ' + eChg.message };
    }

    let count = 0;

    for (const r of (changed || [])) {
      // inopen
      if (r.inopen_bef !== r.inopen_now) {
        const was = r.inopen_bef ? '運営中' : '休止';
        const now = r.inopen_now ? '運営中' : '休止';
        const title = `${r.name_ja}：運営状態変化`;
        const body  = `${was} → ${now}`;
        await sendWebPush(sb, pushSubs, title, body, '/');
        await sendPushover(sb, title, body, '/', PUSHOVER_TOKEN, poUsers);
        count++;
      }

      // DPA
      if (r.dpastatus_bef !== r.dpastatus_now) {
        const title = `${r.name_ja}：DPAステータス変化`;
        const body  = `${r.dpastatus_bef || '-'} → ${r.dpastatus_now || '-'}`;
        await sendWebPush(sb, pushSubs, title, body, '/');
        await sendPushover(sb, title, body, '/', PUSHOVER_TOKEN, poUsers);
        count++;
      }

      // PP
      if (r.ppstatus_bef !== r.ppstatus_now) {
        const title = `${r.name_ja}：PPステータス変化`;
        const body  = `${r.ppstatus_bef || '-'} → ${r.ppstatus_now || '-'}`;
        await sendWebPush(sb, pushSubs, title, body, '/');
        await sendPushover(sb, title, body, '/', PUSHOVER_TOKEN, poUsers);
        count++;
      }
    }

    // 同期（bef ← now & has_changed=false）
    if (changed?.length) {
      for (const r of changed) {
        await sb.from('attraction_state').update({
          inopen_bef: r.inopen_now,
          dpastatus_bef: r.dpastatus_now,
          ppstatus_bef: r.ppstatus_now,
          has_changed: false,
        }).eq('id', r.id);
      }
    }

    return { statusCode: 200, body: JSON.stringify({ ok:true, sent:count }) };

  } catch (err) {
    return { statusCode: 500, body: String(err?.message || err) };
  }
}
