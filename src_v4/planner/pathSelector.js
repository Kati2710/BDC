import { DATASET_GRAPH } from "../catalog/datasetGraph.js";

function edgeMatchesEntity(edge, entityType) {
  if (!entityType) return true;
  return Array.isArray(edge.allowedEntityTypes) && edge.allowedEntityTypes.includes(entityType);
}

function scoreEdge(edge) {
  let score = 0;

  if (edge.confidence === "high") score += 100;
  else if (edge.confidence === "medium") score += 50;
  else score += 10;

  if (edge.joinType === "direct") score += 40;
  if (edge.cardinality === "1:N") score += 20;
  if (Array.isArray(edge.temporalRules) && edge.temporalRules.length > 0) score += 15;

  return score;
}

export function findBestDirectEdge({ leftDataset, rightDataset, entityType }) {
  const candidates = DATASET_GRAPH.filter((edge) => {
    const samePair =
      (edge.leftDataset === leftDataset && edge.rightDataset === rightDataset) ||
      (edge.leftDataset === rightDataset && edge.rightDataset === leftDataset);

    return samePair && edgeMatchesEntity(edge, entityType);
  });

  if (candidates.length === 0) return null;

  candidates.sort((a, b) => scoreEdge(b) - scoreEdge(a));
  return candidates[0];
}