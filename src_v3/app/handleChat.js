import { normalizeQuery } from "../core/normalizeQuery.js";
import { detectIntent } from "../core/detectIntent.js";
import { extractEntities } from "../core/extractEntities.js";
import { detectDomain } from "../catalog/semanticMap.js";
import { buildSql } from "../core/buildSql.js";
import { validateSql } from "../core/validateSql.js";
import { runQuery } from "../core/runQuery.js";
import { formatAnswer } from "../core/formatAnswer.js";
import { detectCrossDatasetIntent } from "../core/detectCrossDatasetIntent.js";
import { buildCrossDatasetSql } from "../core/buildCrossDatasetSql.js";
import { formatAuditedAnswer } from "../core/formatAuditedAnswer.js";
import { buildExecutionPlan } from "../core/buildExecutionPlan.js";

export async function handleChat(query) {
  const { normalized } = normalizeQuery(query);

  const intent = detectIntent(normalized);
  const entities = extractEntities(query);
  const domain = detectDomain(normalized);

  const plan = buildExecutionPlan({
    normalizedQuery: normalized,
    intent,
    entities,
    domain
  });

  console.log("🧠 execution plan:", JSON.stringify(plan, null, 2));

  const crossPlan = detectCrossDatasetIntent(normalized, entities);

  if (crossPlan.type === "cross_dataset") {
    const sql = buildCrossDatasetSql({
      strategy: crossPlan.strategy,
      entities,
      intent,
      plan
    });

    if (!sql) {
      return {
        ok: false,
        error: "Não foi possível gerar SQL de cruzamento."
      };
    }

    validateSql(sql);

    const result = await runQuery(sql);

    return formatAuditedAnswer({
      query,
      sql,
      rows: result.rows,
      rowCount: result.rowCount
    });
  }

  const sql = buildSql({
    domain,
    entities,
    intent,
    plan
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