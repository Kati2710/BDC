export function validateSql(sql) {
  const text = String(sql || "").trim();
  const normalized = text.replace(/\s+/g, " ").toLowerCase();

  if (!text) {
    throw new Error("SQL vazio");
  }

  if (!(normalized.startsWith("select") || normalized.startsWith("with"))) {
    throw new Error("Somente SELECT/WITH é permitido");
  }

  const forbidden = [
    /\bdelete\b/i,
    /\binsert\b/i,
    /\bupdate\b/i,
    /\bdrop\b/i,
    /\balter\b/i,
    /\btruncate\b/i,
    /\bcreate\b/i,
    /\battach\b/i,
    /\bdetach\b/i,
    /\bcopy\b/i,
    /\bexport\b/i,
    /\bimport\b/i,
    /\bcall\b/i,
    /\bpragma\b/i,
    /\binstall\b/i,
    /\bload\b/i
  ];

  for (const pattern of forbidden) {
    if (pattern.test(text)) {
      throw new Error("SQL contém operação proibida");
    }
  }

  if (text.includes(";")) {
    throw new Error("SQL não pode conter múltiplas instruções");
  }

  if (/_empresas_[a-z]{2}\b/i.test(text)) {
    throw new Error("Tabela antiga _empresas_UF não permitida");
  }

  return true;
}