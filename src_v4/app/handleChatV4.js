import { normalizeQuery } from "../core/normalizeQuery.js";
import { extractEntities } from "../core/extractEntities.js";
import { buildQueryPlan } from "../planner/queryPlanner.js";
import { compilePlanToSql } from "../compiler/sqlCompiler.js";
import { runQuery } from "../core/runQuery.js";
import { formatAuditedAnswer } from "../core/formatAuditedAnswer.js";

export async function handleChatV4(query) {
  const { normalized } = normalizeQuery(query);
  const entities = extractEntities(query);

  const plan = buildQueryPlan({
    normalizedQuery: normalized,
    entities
  });

  console.log("🧠 v4 query plan:", JSON.stringify(plan, null, 2));

  if (plan.mode === "unsupported") {
    return {
      ok: false,
      error: "Pergunta ainda não suportada no v4."
    };
  }

  const sql = compilePlanToSql(plan);

  if (!sql) {
    return {
      ok: false,
      error: "Não foi possível compilar o plano em SQL."
    };
  }

  const result = await runQuery(sql);

  return formatAuditedAnswer({
    query,
    sql,
    rows: result.rows,
    rowCount: result.rowCount
  });
}