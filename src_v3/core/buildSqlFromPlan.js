function esc(value) {
  return String(value || "").replace(/'/g, "''");
}

function buildCnpjSancoesRecebimentosSql(plan) {
  const cnpj = esc(plan.filters?.cnpj);
  const summary = plan.output === "summary";

  if (summary) {
    return `
WITH base AS (
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
rel AS (
  SELECT
    "Código Favorecido" AS cnpj,
    "Nome Favorecido" AS nome_favorecido,
    "Nome Órgão Superior" AS orgao_superior,
    "Ano e mês do lançamento" AS ano_mes_lancamento,
    TRY_STRPTIME('01/' || "Ano e mês do lançamento", '%d/%m/%Y') AS data_lancamento_real,
    CAST(REPLACE(REPLACE("Valor Recebido", '.', ''), ',', '.') AS DECIMAL(18,2)) AS valor_recebido_num,
    _audit_arquivo_csv_origem AS despesa_arquivo,
    _audit_linha_csv AS despesa_linha,
    _audit_url_download AS despesa_url,
    _audit_data_disponibilizacao_gov AS despesa_data_base
  FROM _despesas_favorecidos
  WHERE "Código Favorecido" = '${cnpj}'
),
joined AS (
  SELECT
    b.cnpj,
    b.nome_sancionado,
    b.categoria_sancao,
    b.data_inicio_sancao,
    b.data_final_sancao,
    b.orgao_sancionador,
    b.fundamentacao_legal,
    r.orgao_superior,
    r.ano_mes_lancamento,
    r.data_lancamento_real,
    r.valor_recebido_num,
    b.sancao_arquivo,
    b.sancao_linha,
    b.sancao_url,
    b.sancao_data_base,
    r.despesa_arquivo,
    r.despesa_linha,
    r.despesa_url,
    r.despesa_data_base
  FROM base b
  LEFT JOIN rel r
    ON r.cnpj = b.cnpj
   AND r.data_lancamento_real >= b.data_inicio_sancao
)
SELECT
  cnpj,
  nome_sancionado,
  categoria_sancao,
  data_inicio_sancao,
  data_final_sancao,
  orgao_sancionador,
  fundamentacao_legal,
  orgao_superior,
  COUNT(*) AS qtd_registros,
  SUM(valor_recebido_num) AS valor_total_recebido,
  MAX(sancao_arquivo) AS sancao_arquivo,
  MAX(sancao_linha) AS sancao_linha,
  MAX(sancao_url) AS sancao_url,
  MAX(sancao_data_base) AS sancao_data_base,
  MAX(despesa_arquivo) AS despesa_arquivo,
  MAX(despesa_linha) AS despesa_linha,
  MAX(despesa_url) AS despesa_url,
  MAX(despesa_data_base) AS despesa_data_base
FROM joined
GROUP BY
  cnpj,
  nome_sancionado,
  categoria_sancao,
  data_inicio_sancao,
  data_final_sancao,
  orgao_sancionador,
  fundamentacao_legal,
  orgao_superior
ORDER BY valor_total_recebido DESC NULLS LAST
LIMIT 100
`.trim();
  }

  return `
WITH base AS (
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
rel AS (
  SELECT
    "Código Favorecido" AS cnpj,
    "Nome Favorecido" AS nome_favorecido,
    "Nome Órgão Superior" AS orgao_superior,
    "Ano e mês do lançamento" AS ano_mes_lancamento,
    "Valor Recebido" AS valor_recebido,
    TRY_STRPTIME('01/' || "Ano e mês do lançamento", '%d/%m/%Y') AS data_lancamento_real,
    CAST(REPLACE(REPLACE("Valor Recebido", '.', ''), ',', '.') AS DECIMAL(18,2)) AS valor_recebido_num,
    _audit_arquivo_csv_origem AS despesa_arquivo,
    _audit_linha_csv AS despesa_linha,
    _audit_url_download AS despesa_url,
    _audit_data_disponibilizacao_gov AS despesa_data_base
  FROM _despesas_favorecidos
  WHERE "Código Favorecido" = '${cnpj}'
)
SELECT
  b.cnpj,
  b.nome_sancionado,
  b.categoria_sancao,
  b.data_inicio_sancao,
  b.data_final_sancao,
  b.orgao_sancionador,
  b.fundamentacao_legal,
  r.nome_favorecido,
  r.orgao_superior,
  r.ano_mes_lancamento,
  r.data_lancamento_real,
  r.valor_recebido,
  r.valor_recebido_num,
  b.sancao_arquivo,
  b.sancao_linha,
  b.sancao_url,
  b.sancao_data_base,
  r.despesa_arquivo,
  r.despesa_linha,
  r.despesa_url,
  r.despesa_data_base
FROM base b
LEFT JOIN rel r
  ON r.cnpj = b.cnpj
 AND r.data_lancamento_real >= b.data_inicio_sancao
ORDER BY r.data_lancamento_real DESC NULLS LAST, r.valor_recebido_num DESC NULLS LAST
LIMIT 100
`.trim();
}

function buildCpfServidoresImoveisSql(plan) {
  const cpf = esc(plan.filters?.cpf);
  const summary = plan.output === "summary";

  if (summary) {
    return `
WITH serv AS (
  SELECT
    CPF,
    NOME,
    ORGSUP_LOTACAO,
    ORG_LOTACAO,
    DESCRICAO_CARGO
  FROM _servidores
  WHERE CPF = '${cpf}'
),
imov AS (
  SELECT
    CPF,
    "NOME PERMISSIONÁRIO" AS nome_permissionario,
    "ÓRGÃO EXERCÍCIO DO PERMISSIONÁRIO" AS orgao_exercicio,
    "DATA INÍCIO OCUPAÇÃO" AS data_inicio_ocupacao,
    _audit_arquivo_csv_origem AS imovel_arquivo,
    _audit_linha_csv AS imovel_linha,
    _audit_url_download AS imovel_url,
    _audit_data_disponibilizacao_gov AS imovel_data_base
  FROM _imoveisfuncionais
  WHERE CPF = '${cpf}'
)
SELECT
  s.CPF,
  s.NOME,
  s.ORGSUP_LOTACAO,
  s.ORG_LOTACAO,
  s.DESCRICAO_CARGO,
  COUNT(i.CPF) AS qtd_imoveis_funcionais,
  MIN(i.data_inicio_ocupacao) AS primeira_ocupacao,
  MAX(i.data_inicio_ocupacao) AS ultima_ocupacao,
  MAX(i.imovel_arquivo) AS imovel_arquivo,
  MAX(i.imovel_linha) AS imovel_linha,
  MAX(i.imovel_url) AS imovel_url,
  MAX(i.imovel_data_base) AS imovel_data_base
FROM serv s
LEFT JOIN imov i
  ON i.CPF = s.CPF
GROUP BY
  s.CPF,
  s.NOME,
  s.ORGSUP_LOTACAO,
  s.ORG_LOTACAO,
  s.DESCRICAO_CARGO
LIMIT 100
`.trim();
  }

  return `
WITH serv AS (
  SELECT
    CPF,
    NOME,
    ORGSUP_LOTACAO,
    ORG_LOTACAO,
    DESCRICAO_CARGO
  FROM _servidores
  WHERE CPF = '${cpf}'
),
imov AS (
  SELECT
    CPF,
    "NOME PERMISSIONÁRIO" AS nome_permissionario,
    "ÓRGÃO EXERCÍCIO DO PERMISSIONÁRIO" AS orgao_exercicio,
    "DATA INÍCIO OCUPAÇÃO" AS data_inicio_ocupacao,
    _audit_arquivo_csv_origem AS imovel_arquivo,
    _audit_linha_csv AS imovel_linha,
    _audit_url_download AS imovel_url,
    _audit_data_disponibilizacao_gov AS imovel_data_base
  FROM _imoveisfuncionais
  WHERE CPF = '${cpf}'
)
SELECT
  s.CPF,
  s.NOME,
  s.ORGSUP_LOTACAO,
  s.ORG_LOTACAO,
  s.DESCRICAO_CARGO,
  i.nome_permissionario,
  i.orgao_exercicio,
  i.data_inicio_ocupacao,
  i.imovel_arquivo,
  i.imovel_linha,
  i.imovel_url,
  i.imovel_data_base
FROM serv s
LEFT JOIN imov i
  ON i.CPF = s.CPF
ORDER BY i.data_inicio_ocupacao DESC NULLS LAST
LIMIT 100
`.trim();
}

export function buildSqlFromPlan(plan) {
  if (!plan || !plan.mode) return null;

  if (
    plan.baseTable === "_ceis" &&
    plan.relatedTables?.includes("_despesas_favorecidos") &&
    plan.filters?.cnpj
  ) {
    return buildCnpjSancoesRecebimentosSql(plan);
  }

  if (
    plan.relatedTables?.includes("_imoveisfuncionais") &&
    plan.filters?.cpf
  ) {
    return buildCpfServidoresImoveisSql(plan);
  }

  return null;
}