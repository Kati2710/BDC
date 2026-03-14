import { safeJson } from "../utils/safeJson.js";

const HETZNER_API = process.env.HETZNER_API_BASE || "http://89.167.48.3:5010";
const HETZNER_KEY = process.env.HETZNER_API_KEY;

export async function runQuery(sql) {
  if (!HETZNER_KEY) {
    throw new Error("HETZNER_API_KEY ausente");
  }

  const response = await fetch(`${HETZNER_API}/query_unified`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-API-Key": HETZNER_KEY
    },
    body: JSON.stringify({ sql }),
    signal: AbortSignal.timeout(240000)
  });

  const data = await safeJson(response);

  if (!response.ok || data.error) {
    throw new Error(data.error || "Falha ao executar query");
  }

  return {
    rowCount: Number(data.row_count || 0),
    rows: Array.isArray(data.rows) ? data.rows : []
  };
}