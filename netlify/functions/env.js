export const handler = async () => {
  const payload = {
    SUPABASE_URL: process.env.SUPABASE_URL || '',
    SUPABASE_ANON_KEY: process.env.SUPABASE_ANON_KEY || '',
    VAPID_PUBLIC_KEY: process.env.VAPID_PUBLIC_KEY || ''
  };
  return {
    statusCode: 200,
    headers: { 'Content-Type': 'application/javascript' },
    body: `window.ENV = ${JSON.stringify(payload)};`
  };
};
