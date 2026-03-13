export const SEMANTIC_DOMAIN_MAP = {
  viagens: [
    "viagem",
    "viagens",
    "viajou",
    "viajar",
    "passagem",
    "diaria",
    "diárias"
  ],

  sancoes: [
    "ceis",
    "sanção",
    "sanções",
    "punido",
    "impedimento",
    "suspensão"
  ],

  acordos: [
    "acordo de leniencia",
    "leniência",
    "acordo leniencia",
    "acordos"
  ],

  rfb: [
    "cnpj",
    "empresa",
    "receita federal",
    "socios",
    "sócios"
  ],

  servidores: [
    "servidor",
    "servidores",
    "funcionario publico"
  ],

  despesas: [
    "despesa",
    "gasto",
    "pagamento",
    "favorecido"
  ]
};

export function detectDomain(queryNormalized) {
  const q = String(queryNormalized || "");

  for (const [domain, keywords] of Object.entries(SEMANTIC_DOMAIN_MAP)) {
    for (const k of keywords) {
      if (q.includes(k)) {
        return domain;
      }
    }
  }

  return "unknown";
}