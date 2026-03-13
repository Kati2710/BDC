export function classifyQuestionType(query) {

  const q = query.toLowerCase();

  if (
    q.includes("quantos") ||
    q.includes("total") ||
    q.includes("soma")
  ) {
    return "aggregate";
  }

  if (
    q.includes("mostre") ||
    q.includes("liste") ||
    q.includes("quais") ||
    q.includes("últimas") ||
    q.includes("ultimas")
  ) {
    return "listing";
  }

  if (
    q.includes("evolução") ||
    q.includes("evolucao")
  ) {
    return "timeline";
  }

  return "lookup";
}