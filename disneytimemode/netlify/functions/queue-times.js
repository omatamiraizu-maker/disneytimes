// ✅ 正しい形式（CommonJS）
exports.handler = async (event) => {
  const { parkId } = event.queryStringParameters || {};

  if (!parkId) {
    return {
      statusCode: 400,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ error: 'parkId is required' })
    };
  }

  try {
    // node-fetchを使用
    const fetch = (await import('node-fetch')).default;

    const response = await fetch(
      `https://queue-times.com/parks/${parkId}/queue_times.json`
    );

    const data = await response.json();

    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(data),
    };
  } catch (error) {
    return {
      statusCode: 500,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        error: 'Failed to fetch data',
        message: error.message
      }),
    };
  }
};
