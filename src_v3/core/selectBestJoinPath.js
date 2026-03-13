export function selectBestJoinPath({ joins = [], baseTable, relatedTables = [] }) {
  if (!Array.isArray(joins) || joins.length === 0) return [];

  const wanted = new Set([baseTable, ...relatedTables].filter(Boolean));

  const filtered = joins.filter((join) => {
    return wanted.has(join.leftTable) || wanted.has(join.rightTable);
  });

  const exact = filtered.filter((join) => {
    return (
      join.leftTable === baseTable &&
      relatedTables.includes(join.rightTable)
    ) || (
      join.rightTable === baseTable &&
      relatedTables.includes(join.leftTable)
    );
  });

  if (exact.length > 0) {
    return exact;
  }

  if (filtered.length > 0) {
    return filtered.slice(0, 1);
  }

  return joins.slice(0, 1);
}