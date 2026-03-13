import { maskSensitiveRows } from "../utils/maskSensitive.js";

export function formatAnswer({ query, sql, rows, rowCount }) {

  const safeRows = maskSensitiveRows(rows);

  return {
    ok: true,

    query,

    sql,

    rows_returned: rowCount,

    preview: safeRows.slice(0, 20),

    message: rowCount === 0
      ? "Nenhum registro encontrado."
      : `${rowCount} registros encontrados.`,

    confidence: "high"
  };
}