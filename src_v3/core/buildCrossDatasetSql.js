function escapeSqlString(value) {
  return String(value || "").replace(/'/g, "''");
}

export function buildCrossDatasetSql({ strategy, entities }) {
  if (strategy === "empresa_sancionada_recebimentos") {
    if (!entities.cnpj) return null;

    const cnpj = escapeSqlString(entities.cnpj);

    return `
WITH sancoes AS (
  SELECT
    "CPF OU CNPJ DO SANCIONADO" AS cnpj,
    "NOME DO SANCIONADO" AS nome_sancionado,
    "CATEGORIA DA SANÇÃO" AS categoria_sancao,
    "DATA INÍCIO SANÇÃO" AS data_inicio_sancao,
    "DATA FINAL SANÇÃO" AS data_final_sancao,
    "ÓRGÃO SANCIONADOR" AS orgao_sancionador,
    "FUNDAMENTAÇÃO LEGAL" AS fundamentacao_legal,
    _audit_arquivo_csv_origem AS sancao_arquivo,
    _audit_linha_csv AS sancao_linha,
    _audit_url_download AS sancao_url,
    _audit_data_disponibilizacao_gov AS sancao_data_base
  FROM _ceis
  WHERE "CPF OU CNPJ DO SANCIONADO" = '${cnpj}'
),
despesas AS (
  SELECT
    "Código Favorecido" AS cnpj,
    "Nome Favorecido" AS nome_favorecido,
    "Nome Órgão Superior" AS orgao_superior,
    "Ano e mês do lançamento" AS ano_mes_lancamento,
    "Valor Recebido" AS valor_recebido,
    _audit_arquivo_csv_origem AS despesa_arquivo,
    _audit_linha_csv AS despesa_linha,
    _audit_url_download AS despesa_url,
    _audit_data_disponibilizacao_gov AS despesa_data_base
  FROM _despesas_favorecidos
  WHERE "Código Favorecido" = '${cnpj}'
),
despesas_convertidas AS (
  SELECT
    d.*,
    TRY_STRPTIME('01/' || d.ano_mes_lancamento, '%d/%m/%Y') AS data_lancamento_real,
    CAST(REPLACE(REPLACE(d.valor_recebido, '.', ''), ',', '.') AS DECIMAL(18,2)) AS valor_recebido_num
  FROM despesas d
),
cruzamento AS (
  SELECT
    s.cnpj,
    s.nome_sancionado,
    s.categoria_sancao,
    s.data_inicio_sancao,
    s.data_final_sancao,
    s.orgao_sancionador,
    s.fundamentacao_legal,
    dc.nome_favorecido,
    dc.orgao_superior,
    dc.ano_mes_lancamento,
    dc.data_lancamento_real,
    dc.valor_recebido,
    dc.valor_recebido_num,
    s.sancao_arquivo,
    s.sancao_linha,
    s.sancao_url,
    s.sancao_data_base,
    dc.despesa_arquivo,
    dc.despesa_linha,
    dc.despesa_url,
    dc.despesa_data_base
  FROM sancoes s
  LEFT JOIN despesas_convertidas dc
    ON dc.cnpj = s.cnpj
   AND dc.data_lancamento_real >= s.data_inicio_sancao
)
SELECT
  cnpj,
  nome_sancionado,
  categoria_sancao,
  data_inicio_sancao,
  data_final_sancao,
  orgao_sancionador,
  fundamentacao_legal,
  nome_favorecido,
  orgao_superior,
  ano_mes_lancamento,
  data_lancamento_real,
  valor_recebido,
  valor_recebido_num,
  sancao_arquivo,
  sancao_linha,
  sancao_url,
  sancao_data_base,
  despesa_arquivo,
  despesa_linha,
  despesa_url,
  despesa_data_base
FROM cruzamento
ORDER BY data_lancamento_real DESC NULLS LAST
LIMIT 100
`.trim();
  }

  return null;
}