import { getGraphEdgesByKey } from "../catalog/datasetGraph.js";

export function resolveJoinPath({ entities, desiredTables = [] }) {
  const paths = [];

  if (entities?.cnpj) {
    const edges = getGraphEdgesByKey("cnpj");
    for (const edge of edges) {
      if (
        desiredTables.length === 0 ||
        desiredTables.includes(edge.leftTable) ||
        desiredTables.includes(edge.rightTable)
      ) {
        paths.push({
          joinKey: "cnpj",
          ...edge
        });
      }
    }
  }

  if (entities?.cpf) {
    const edges = getGraphEdgesByKey("cpf");
    for (const edge of edges) {
      if (
        desiredTables.length === 0 ||
        desiredTables.includes(edge.leftTable) ||
        desiredTables.includes(edge.rightTable)
      ) {
        paths.push({
          joinKey: "cpf",
          ...edge
        });
      }
    }
  }

  return paths;
}