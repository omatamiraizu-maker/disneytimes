import webpush from 'web-push';
webpush.setVapidDetails('mailto:notify@example.com', process.env.VAPID_PUBLIC_KEY, process.env.VAPID_PRIVATE_KEY);

export async function handler(event) {
  try {
    const { endpoint, p256dh, auth } = JSON.parse(event.body || '{}');
    if (!endpoint || !p256dh || !auth) return { statusCode: 400, body: 'endpoint,p256dh,auth required' };
    await webpush.sendNotification(
      { endpoint, keys: { p256dh, auth } },
      JSON.stringify({ title: 'テスト通知', body: '受信できればVAPID/経路OK', url: '/' })
    );
    return { statusCode: 200, body: 'sent' };
  } catch (e) {
    return { statusCode: 500, body: String(e?.message || e) };
  }
}
