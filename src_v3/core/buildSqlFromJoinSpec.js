function esc(value) {
  return String(value || "").replace(/'/g, "''");
}

export function buildSqlFromJoinSpec(spec) {
  if (!spec) return null;

  const {
    baseTable,
    relatedTable,
    baseAlias = "b",
    relatedAlias = "r",
    baseSelect = [],
    relatedSelect = [],
    joinLeftColumn,
    joinRightColumn,
    baseWhere = [],
    relatedWhere = [],
    extraJoinConditions = [],
    output = "detail",
    summary = null,
    detailOrderBy = null,
    limit = 100
  } = spec;

  if (!baseTable || !relatedTable || !joinLeftColumn || !joinRightColumn) {
    return null;
  }

  const baseSelectSql = baseSelect.join(",\n    ");
  const relSelectSql = relatedSelect.join(",\n    ");

  const baseWhereSql = baseWhere.length
    ? `WHERE ${baseWhere.join("\n    AND ")}`
    : "";

  const relatedWhereSql = relatedWhere.length
    ? `WHERE ${relatedWhere.join("\n    AND ")}`
    : "";

  const joinConditions = [
    `${relatedAlias}.${joinRightColumn} = ${baseAlias}.${joinLeftColumn}`,
    ...extraJoinConditions
  ];

  const joinSql = joinConditions.join("\n   AND ");

  if (output === "summary" && summary) {
    return `
WITH base AS (
  SELECT
    ${baseSelectSql}
  FROM ${baseTable}
  ${baseWhereSql}
),
rel AS (
  SELECT
    ${relSelectSql}
  FROM ${relatedTable}
  ${relatedWhereSql}
),
joined AS (
  SELECT
    *
  FROM base ${baseAlias}
  LEFT JOIN rel ${relatedAlias}
    ON ${joinSql}
)
SELECT
  ${summary.select.join(",\n  ")}
FROM joined
${summary.groupBy?.length ? `GROUP BY\n  ${summary.groupBy.join(",\n  ")}` : ""}
${summary.orderBy ? `ORDER BY ${summary.orderBy}` : ""}
LIMIT ${limit}
`.trim();
  }

  return `
WITH base AS (
  SELECT
    ${baseSelectSql}
  FROM ${baseTable}
  ${baseWhereSql}
),
rel AS (
  SELECT
    ${relSelectSql}
  FROM ${relatedTable}
  ${relatedWhereSql}
)
SELECT
  *
FROM base ${baseAlias}
LEFT JOIN rel ${relatedAlias}
  ON ${joinSql}
${detailOrderBy ? `ORDER BY ${detailOrderBy}` : ""}
LIMIT ${limit}
`.trim();
}