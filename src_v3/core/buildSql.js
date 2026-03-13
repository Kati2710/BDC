function escapeSqlString(value) {
  return String(value || "").replace(/'/g, "''");
}

function buildViagensSql(entities) {
  if (!entities.personName) return null;

  const name = escapeSqlString(entities.personName);
  const limit = entities.topN || 10;

  return `
SELECT
  "Identificador do processo de viagem",
  "Número da Proposta (PCDP)",
  "Nome do órgão superior",
  "Nome do órgao pagador",
  "CPF viajante",
  "Nome",
  "Cargo",
  "Função",
  "Período - Data de início",
  "Período - Data de fim",
  "Destinos",
  "Motivo",
  "Valor diárias",
  "Valor passagens",
  "Valor devolução",
  "Valor outros gastos",
  "Situação",
  "Viagem Urgente",
  _audit_arquivo_csv_origem,
  _audit_linha_csv,
  _audit_url_download,
  _audit_data_disponibilizacao_gov
FROM _viagens
WHERE "Nome" ILIKE '%${name}%'
ORDER BY "Período - Data de início" DESC
LIMIT ${limit}
`.trim();
}

function buildSancoesSql(entities) {
  if (!entities.cnpj) return null;

  return `
SELECT
  "TIPO DE PESSOA",
  "CPF OU CNPJ DO SANCIONADO",
  "NOME DO SANCIONADO",
  "RAZÃO SOCIAL - CADASTRO RECEITA",
  "NOME FANTASIA - CADASTRO RECEITA",
  "CATEGORIA DA SANÇÃO",
  "DATA INÍCIO SANÇÃO",
  "DATA FINAL SANÇÃO",
  "ÓRGÃO SANCIONADOR",
  "UF ÓRGÃO SANCIONADOR",
  "ESFERA ÓRGÃO SANCIONADOR",
  "FUNDAMENTAÇÃO LEGAL",
  "NÚMERO DO PROCESSO",
  "OBSERVAÇÕES",
  _audit_arquivo_csv_origem,
  _audit_linha_csv,
  _audit_url_download,
  _audit_data_disponibilizacao_gov
FROM _ceis
WHERE "CPF OU CNPJ DO SANCIONADO" = '${entities.cnpj}'
ORDER BY "DATA INÍCIO SANÇÃO" DESC
LIMIT 100
`.trim();
}

function buildAcordosSql(entities) {
  if (!entities.companyName) return null;

  const company = escapeSqlString(entities.companyName);

  return `
SELECT
  "ID DO ACORDO",
  "CNPJ DO SANCIONADO",
  "RAZÃO SOCIAL – CADASTRO RECEITA",
  "NOME FANTASIA – CADASTRO RECEITA",
  "DATA DE INÍCIO DO ACORDO",
  "DATA DE FIM DO ACORDO",
  "SITUAÇÃO DO ACORDO DE LENIÊNICA",
  "DATA DA INFORMAÇÃO",
  "NÚMERO DO PROCESSO",
  "TERMOS DO ACORDO",
  "ÓRGÃO SANCIONADOR",
  "EFEITO DO ACORDO DE LENIENCIA",
  "COMPLEMENTO",
  _audit_arquivo_csv_origem,
  _audit_linha_csv,
  _audit_url_download,
  _audit_data_disponibilizacao_gov
FROM _acordos
WHERE "RAZÃO SOCIAL – CADASTRO RECEITA" ILIKE '%${company}%'
LIMIT 100
`.trim();
}

export function buildSql({ domain, entities }) {
  switch (domain) {
    case "viagens":
      return buildViagensSql(entities);
    case "sancoes":
      return buildSancoesSql(entities);
    case "acordos":
      return buildAcordosSql(entities);
    default:
      return null;
  }
}