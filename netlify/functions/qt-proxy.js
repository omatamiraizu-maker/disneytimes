// netlify/functions/qt-proxy.js
// Proxy to Queue-Times JSON (keeps your site origin clean, allows CORS)
export const handler = async (event) => {
  const parkId = event.queryStringParameters?.parkId;
  if (!parkId) {
    return {
      statusCode: 400,
      headers: { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: 'parkId is required' }),
    };
  }
  try {
    const url = `https://queue-times.com/parks/${parkId}/queue_times.json`;
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(data),
    };
  } catch (err) {
    return {
      statusCode: 500,
      headers: { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: 'Failed to fetch', details: String(err) }),
    };
  }
};