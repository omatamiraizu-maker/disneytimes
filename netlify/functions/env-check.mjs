// /.netlify/functions/env-check
export async function handler() {
  const env = process.env;
  const has = (k) => !!env[k] && env[k].trim() !== '';

  return {
    statusCode: 200,
    body: JSON.stringify({
      ok: true,
      node: process.version,
      // 関数ランタイムから見えているか（true/false）
      SUPABASE_URL: has('SUPABASE_URL'),
      SUPABASE_SERVICE_ROLE: has('SUPABASE_SERVICE_ROLE'),
      VAPID_PUBLIC_KEY: has('VAPID_PUBLIC_KEY'),
      VAPID_PRIVATE_KEY: has('VAPID_PRIVATE_KEY'),
      // Netlify 環境別に違う値を入れてないかの気付き用
      NETLIFY: {
        CONTEXT: env.CONTEXT, // 'production' | 'deploy-preview' | 'branch-deploy' など
        BRANCH: env.BRANCH,
      }
    })
  };
}
