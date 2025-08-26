exports.handler = async (event) => {
  // クエリパラメータを安全に取得
  const parkId = event.queryStringParameters?.parkId;

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
    // fetchを使用（Node.js 18以降では標準で使用可能）
    const response = await fetch(
      `https://queue-times.com/parks/${parkId}/queue_times.json`
    );

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.json();

    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(data),
    };
  } catch (error) {
    console.error('Error fetching queue times:', error);
    return {
      statusCode: 500,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        error: 'Failed to fetch queue times',
        details: error.message
      }),
    };
  }
};
