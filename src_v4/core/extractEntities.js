function extractCnpj(query) {
  const digits = String(query || "").replace(/\D/g, "");
  if (digits.length >= 14) return digits.slice(0, 14);
  return null;
}

function extractCpf(query) {
  const digits = String(query || "").replace(/\D/g, "");
  if (digits.length === 11) return digits;
  return null;
}

function extractTopN(query, fallback = 10) {
  const match = String(query || "").match(/\b(\d{1,3})\b/);
  if (!match) return fallback;

  const n = Number(match[1]);
  if (!Number.isFinite(n) || n <= 0) return fallback;

  return Math.min(n, 100);
}

function extractPersonName(query) {
  const q = String(query || "").trim();

  let m = q.match(/viagens?\s+de\s+(.+?)(?:\s+com|\s+dos|\s+das|\s*$)/i);
  if (m?.[1]) return m[1].trim();

  m = q.match(/ultimas?\s+\d+\s+viagens?\s+de\s+(.+?)(?:\s+com|\s+dos|\s+das|\s*$)/i);
  if (m?.[1]) return m[1].trim();

  return null;
}

function extractCompanyName(query) {
  const q = String(query || "").trim();

  let m = q.match(/acordo\s+de\s+leniencia\s+da\s+(.+)$/i);
  if (m?.[1]) return m[1].trim();

  m = q.match(/dados\s+completos\s+do\s+acordo\s+de\s+leniencia\s+da\s+(.+)$/i);
  if (m?.[1]) return m[1].trim();

  return null;
}

function extractAgency(query) {
  const q = String(query || "").trim();

  let m = q.match(/servidores\s+do\s+(.+?)(?:\s+com|\s+e|\s*$)/i);
  if (m?.[1]) return m[1].trim();

  return null;
}

export function extractEntities(query) {
  return {
    cnpj: extractCnpj(query),
    cpf: extractCpf(query),
    topN: extractTopN(query, 10),
    personName: extractPersonName(query),
    companyName: extractCompanyName(query),
    agency: extractAgency(query)
  };
}