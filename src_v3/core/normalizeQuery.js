export function normalizeQuery(input) {
  const original = String(input || "").trim();

  const noAccents = original.normalize("NFD").replace(/[\u0300-\u036f]/g, "");

  const lowered = noAccents.toLowerCase();

  const collapsed = lowered.replace(/\s+/g, " ").trim();

  return {
    original,
    normalized: collapsed
  };
}