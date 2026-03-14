export async function safeJson(response) {
  const text = await response.text();

  try {
    return JSON.parse(text);
  } catch {
    return {
      error: text || `HTTP ${response.status}`
    };
  }
}