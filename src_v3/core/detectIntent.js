export function detectIntent(normalizedQuery) {
  const q = String(normalizedQuery || "");

  const hasAggregate =
    q.includes("quantos") ||
    q.includes("quantas") ||
    q.includes("total") ||
    q.includes("soma") ||
    q.includes("somar") ||
    q.includes("media") ||
    q.includes("média") ||
    q.includes("avg") ||
    q.includes("count");

  if (hasAggregate) {
    return "aggregate";
  }

  const hasTimeline =
    q.includes("ultimas") ||
    q.includes("ultimos") ||
    q.includes("recentes") ||
    q.includes("mais recentes") ||
    q.includes("historico") ||
    q.includes("histórico");

  if (hasTimeline) {
    return "timeline";
  }

  const hasListing =
    q.includes("mostre") ||
    q.includes("mostrar") ||
    q.includes("liste") ||
    q.includes("listar") ||
    q.includes("quais sao") ||
    q.includes("quais são") ||
    q.includes("dados completos") ||
    q.includes("detalhes");

  if (hasListing) {
    return "listing";
  }

  return "lookup";
}