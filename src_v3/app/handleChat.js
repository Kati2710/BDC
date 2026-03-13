import { normalizeQuery } from "../core/normalizeQuery.js";
import { detectIntent } from "../core/detectIntent.js";
import { extractEntities } from "../core/extractEntities.js";
import { detectDomain } from "../catalog/semanticMap.js";
import { buildSql } from "../core/buildSql.js";
import { validateSql } from "../core/validateSql.js";
import { runQuery } from "../core/runQuery.js";
import { formatAnswer } from "../core/formatAnswer.js";

export async function handleChat(query) {

  const { normalized } = normalizeQuery(query);

  const intent = detectIntent(normalized);

  const entities = extractEntities(query);

  const domain = detectDomain(normalized);

  const sql = buildSql({
    domain,
    entities,
    intent
  });

  if (!sql) {
    return {
      ok: false,
      error: "Não foi possível gerar SQL para essa pergunta."
    };
  }

  validateSql(sql);

  const result = await runQuery(sql);

  return formatAnswer({
    query,
    sql,
    rows: result.rows,
    rowCount: result.rowCount
  });
}