export const SEMANTIC_DOMAIN_MAP = {
  viagens: [
    "viagem",
    "viagens",
    "viajou",
    "viajar",
    "passagem",
    "passagens",
    "diaria",
    "diarias"
  ],

  sancoes: [
    "ceis",
    "sancao",
    "sancoes",
    "punido",
    "impedimento",
    "suspensao",
    "inidonea",
    "inidoneo"
  ],

  acordos: [
    "acordo de leniencia",
    "leniencia",
    "acordo leniencia",
    "acordos"
  ],

  rfb: [
    "cnpj",
    "empresa",
    "receita federal",
    "socios",
    "socio",
    "razao social"
  ],

  servidores: [
    "servidor",
    "servidores",
    "funcionario publico",
    "funcionario federal"
  ],

  despesas: [
    "despesa",
    "despesas",
    "gasto",
    "gastos",
    "pagamento",
    "pagamentos",
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