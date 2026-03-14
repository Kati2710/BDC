import { SEMANTIC_CATALOG } from "../catalog/semanticCatalog.js";

const AUDIT_LABEL = {
  "_audit_arquivo_csv_origem":        "arquivo",
  "_audit_linha_csv":                 "linha",
  "_audit_url_download":              "url",
  "_audit_data_disponibilizacao_gov": "data_base"
};

function esc(v) { return String(v || "").replace(/'/g, "''"); }
function getPrefix(plan, side) { return plan.auditPrefixes?.[side] || side; }
function entityValue(plan) { return esc(plan.filters.cnpj || plan.filters.cpf || ""); }

// ---------------------------------------------------------------------------
// FINDINGS — query simples na tabela pre-computada
// ---------------------------------------------------------------------------
function compileFindingsSql(plan) {
  const val   = entityValue(plan);
  const table = plan.baseDataset;
  const order = plan.projectionMode === "summary"
    ? "valor_recebido_total DESC NULLS LAST"
    : "ultimo_pagamento DESC NULLS LAST";
  return "SELECT * FROM " + table + " WHERE documento = '" + val + "' ORDER BY " + order + " LIMIT 100";
}

// ---------------------------------------------------------------------------
// CROSS DATASET — CTE base
// ---------------------------------------------------------------------------
function buildBaseCte(plan) {
  const meta   = SEMANTIC_CATALOG[plan.baseDataset];
  const prefix = getPrefix(plan, "base");
  const val    = entityValue(plan);
  const keyCol = plan.edge.leftKey.column;
  const lines  = [];
  for (const [alias, fm] of Object.entries(meta.fields)) {
    lines.push('"' + fm.column + '" AS ' + alias);
  }
  for (const af of meta.auditFields || []) {
    const label = AUDIT_LABEL[af] || af;
    lines.push(af + " AS " + prefix + "_" + label);
  }
  return "SELECT\n    " + lines.join(",\n    ") + "\n  FROM " + plan.baseDataset + "\n  WHERE \"" + keyCol + "\" = '" + val + "'";
}

// ---------------------------------------------------------------------------
// CROSS DATASET — CTE rel
// ---------------------------------------------------------------------------
function buildRelCte(plan) {
  const meta    = SEMANTIC_CATALOG[plan.relatedDataset];
  const prefix  = getPrefix(plan, "related");
  const val     = entityValue(plan);
  const keyCol  = plan.edge.rightKey.column;
  const lines   = [];
  for (const [alias, fm] of Object.entries(meta.fields)) {
    const col = '"' + fm.column + '"';
    if (fm.type === "money_string") {
      lines.push("CAST(REPLACE(REPLACE(" + col + ", '.', ''), ',', '.') AS DECIMAL(18,2)) AS " + alias + "_num");
    } else if (fm.type === "month_string") {
      lines.push(col + " AS " + alias);
      lines.push("TRY_STRPTIME('01/' || " + col + ", '%d/%m/%Y') AS " + alias + "_date");
    } else {
      lines.push(col + " AS " + alias);
    }
  }
  for (const af of meta.auditFields || []) {
    const label = AUDIT_LABEL[af] || af;
    lines.push(af + " AS " + prefix + "_" + label);
  }
  return "SELECT\n    " + lines.join(",\n    ") + "\n  FROM " + plan.relatedDataset + "\n  WHERE \"" + keyCol + "\" = '" + val + "'";
}

// ---------------------------------------------------------------------------
// JOIN ON
// ---------------------------------------------------------------------------
function buildJoinOn(plan) {
  const leftAlias  = plan.edge.leftKey.semanticField;
  const rightAlias = plan.edge.rightKey.semanticField;
  const relMeta    = SEMANTIC_CATALOG[plan.relatedDataset];
  const conditions = ["r." + rightAlias + " = b." + leftAlias];
  for (const rule of plan.edge.temporalRules || []) {
    if (rule.type === "after_start") {
      const timeEntry = Object.entries(relMeta.fields).find(([, fm]) => fm.type === "month_string");
      if (timeEntry) conditions.push("r." + timeEntry[0] + "_date >= b." + rule.baseField.replace(/ /g,"_").toLowerCase());
    }
  }
  return conditions.join("\n   AND ");
}

// ---------------------------------------------------------------------------
// SELECT summary
// ---------------------------------------------------------------------------
function buildSummaryQuery(plan, joinOn) {
  const baseMeta   = SEMANTIC_CATALOG[plan.baseDataset];
  const relMeta    = SEMANTIC_CATALOG[plan.relatedDataset];
  const basePrefix = getPrefix(plan, "base");
  const relPrefix  = getPrefix(plan, "related");
  const dims = [];
  const metrics = [];
  for (const alias of Object.keys(baseMeta.fields)) dims.push("b." + alias);
  const relKeyAlias = plan.edge.rightKey.semanticField;
  for (const [alias, fm] of Object.entries(relMeta.fields)) {
    if (alias === relKeyAlias || fm.type === "money_string" || fm.type === "month_string" || fm.type === "document") continue;
    dims.push("r." + alias);
  }
  metrics.push("COUNT(*) AS qtd_registros");
  let orderMetric = "qtd_registros";
  for (const [alias, fm] of Object.entries(relMeta.fields)) {
    if (fm.type === "money_string") {
      metrics.push("SUM(r." + alias + "_num) AS " + alias + "_total");
      orderMetric = alias + "_total";
    }
  }
  for (const af of baseMeta.auditFields || []) {
    const ca = basePrefix + "_" + (AUDIT_LABEL[af] || af);
    metrics.push("MAX(b." + ca + ") AS " + ca);
  }
  for (const af of relMeta.auditFields || []) {
    const ca = relPrefix + "_" + (AUDIT_LABEL[af] || af);
    metrics.push("MAX(r." + ca + ") AS " + ca);
  }
  const allCols  = dims.concat(metrics);
  const groupNums = dims.map((_, i) => i + 1).join(", ");
  return "SELECT\n  " + allCols.join(",\n  ") + "\nFROM base b\nLEFT JOIN rel r\n  ON " + joinOn + "\nGROUP BY " + groupNums + "\nORDER BY " + orderMetric + " DESC NULLS LAST\nLIMIT 100";
}

// ---------------------------------------------------------------------------
// SELECT detail
// ---------------------------------------------------------------------------
function buildDetailQuery(plan, joinOn) {
  const baseMeta   = SEMANTIC_CATALOG[plan.baseDataset];
  const relMeta    = SEMANTIC_CATALOG[plan.relatedDataset];
  const basePrefix = getPrefix(plan, "base");
  const relPrefix  = getPrefix(plan, "related");
  const cols = [];
  for (const alias of Object.keys(baseMeta.fields)) cols.push("b." + alias);
  for (const af of baseMeta.auditFields || []) cols.push("b." + basePrefix + "_" + (AUDIT_LABEL[af] || af));
  const relKeyAlias = plan.edge.rightKey.semanticField;
  for (const [alias, fm] of Object.entries(relMeta.fields)) {
    if (alias === relKeyAlias) continue;
    if (fm.type === "money_string") { cols.push("r." + alias + "_num"); }
    else if (fm.type === "month_string") { cols.push("r." + alias); cols.push("r." + alias + "_date"); }
    else { cols.push("r." + alias); }
  }
  for (const af of relMeta.auditFields || []) cols.push("r." + relPrefix + "_" + (AUDIT_LABEL[af] || af));
  const timeEntry = Object.entries(relMeta.fields).find(([, fm]) => fm.type === "month_string");
  const orderBy   = timeEntry ? "r." + timeEntry[0] + "_date DESC NULLS LAST" : "1";
  return "SELECT\n  " + cols.join(",\n  ") + "\nFROM base b\nLEFT JOIN rel r\n  ON " + joinOn + "\nORDER BY " + orderBy + "\nLIMIT 100";
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

function compileSingleTableSql(plan) {
  const table  = plan.baseDataset;
  const meta   = SEMANTIC_CATALOG[table];
  const topN   = plan.topN || 20;
  const filter = plan.tableFilter;
  let where = "";
  if (filter) {
    const col = filter.col;
    const val = String(filter.value || "").replace(/'/g, "''");
    if (filter.type === "exact") {
      where = "WHERE \"" + col + "\" = '" + val + "'";
    } else {
      where = "WHERE \"" + col + "\" ILIKE '%" + val + "%'";
    }
  }
  const dateField = Object.values(meta.fields || {}).find(f => f.type === "date" || f.type === "month_string");
  const orderBy = dateField ? "ORDER BY \"" + dateField.column + "\" DESC NULLS LAST" : "";
  return "SELECT * FROM " + table + " " + where + " " + orderBy + " LIMIT " + topN;
}
export function compilePlanToSql(plan) {
  if (!plan?.edge) return null;
  if (plan.mode === "single_table") return compileSingleTableSql(plan);
  if (plan.mode === "findings") return compileFindingsSql(plan);
  if (plan.mode !== "cross_dataset") return null;
  if (!SEMANTIC_CATALOG[plan.baseDataset] || !SEMANTIC_CATALOG[plan.relatedDataset]) return null;
  const baseCte  = buildBaseCte(plan);
  const relCte   = buildRelCte(plan);
  const joinOn   = buildJoinOn(plan);
  const finalSql = plan.projectionMode === "summary"
    ? buildSummaryQuery(plan, joinOn)
    : buildDetailQuery(plan, joinOn);
  return "WITH base AS (\n  " + baseCte + "\n),\nrel AS (\n  " + relCte + "\n)\n" + finalSql;
}

