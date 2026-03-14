import { maskSensitiveRows } from "../utils/maskSensitive.js";

function extractSources(rows = []) {
  const seen = new Set();
  const out = [];

  for (const row of rows) {
    const pairs = [
      {
        arquivo: row?.sancao_arquivo,
        linha: row?.sancao_linha,
        url: row?.sancao_url,
        data: row?.sancao_data_base
      },
      {
        arquivo: row?.despesa_arquivo,
        linha: row?.despesa_linha,
        url: row?.despesa_url,
        data: row?.despesa_data_base
      }
    ];

    for (const src of pairs) {
      const key = JSON.stringify(src);
      if (!seen.has(key) && (src.arquivo || src.url || src.data)) {
        seen.add(key);
        out.push(src);
      }
    }
  }

  return out;
}

export function formatAuditedAnswer({ query, sql, rows, rowCount }) {
  const safeRows = maskSensitiveRows(rows || []);
  const preview = safeRows.slice(0, 20);
  const sources = extractSources(rows || []);

  return {
    ok: true,
    query,
    sql,
    rows_returned: rowCount,
    preview,
    sources,
    message:
      rowCount === 0
        ? "Nenhum registro encontrado no cruzamento."
        : `${rowCount} registros encontrados no cruzamento auditável.`,
    confidence: "high",
    strategy: "cross_dataset"
  };
}