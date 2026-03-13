import { buildSqlFromJoinSpec } from "./buildSqlFromJoinSpec.js";

function buildCnpjSancoesRecebimentos(plan) {
  const cnpj = esc(plan.filters?.cnpj);
  const summary = plan.output === "summary";

  return buildSqlFromJoinSpec({
    baseTable: "_ceis",
    relatedTable: "_despesas_favorecidos",
    baseAlias: "b",
    relatedAlias: "r",
    baseSelect: [
      `"CPF OU CNPJ DO SANCIONADO" AS cnpj`,
      `"NOME DO SANCIONADO" AS nome_sancionado`,
      `"CATEGORIA DA SANÇÃO" AS categoria_sancao`,
      `"DATA INÍCIO SANÇÃO" AS data_inicio_sancao`,
      `"DATA FINAL SANÇÃO" AS data_final_sancao`,
      `"ÓRGÃO SANCIONADOR" AS orgao_sancionador`,
      `"FUNDAMENTAÇÃO LEGAL" AS fundamentacao_legal`,
      `_audit_arquivo_csv_origem AS sancao_arquivo`,
      `_audit_linha_csv AS sancao_linha`,
      `_audit_url_download AS sancao_url`,
      `_audit_data_disponibilizacao_gov AS sancao_data_base`
    ],
    relatedSelect: [
      `"Código Favorecido" AS cnpj`,
      `"Nome Favorecido" AS nome_favorecido`,
      `"Nome Órgão Superior" AS orgao_superior`,
      `"Ano e mês do lançamento" AS ano_mes_lancamento`,
      `TRY_STRPTIME('01/' || "Ano e mês do lançamento", '%d/%m/%Y') AS data_lancamento_real`,
      `CAST(REPLACE(REPLACE("Valor Recebido", '.', ''), ',', '.') AS DECIMAL(18,2)) AS valor_recebido_num`,
      `_audit_arquivo_csv_origem AS despesa_arquivo`,
      `_audit_linha_csv AS despesa_linha`,
      `_audit_url_download AS despesa_url`,
      `_audit_data_disponibilizacao_gov AS despesa_data_base`
    ],
    joinLeftColumn: "cnpj",
    joinRightColumn: "cnpj",
    baseWhere: [
      `"CPF OU CNPJ DO SANCIONADO" = '${cnpj}'`
    ],
    relatedWhere: [
      `"Código Favorecido" = '${cnpj}'`
    ],
    extraJoinConditions: [
      `r.data_lancamento_real >= b.data_inicio_sancao`
    ],
    output: summary ? "summary" : "detail",
    summary: {
      select: [
        `cnpj`,
        `nome_sancionado`,
        `categoria_sancao`,
        `data_inicio_sancao`,
        `data_final_sancao`,
        `orgao_sancionador`,
        `fundamentacao_legal`,
        `orgao_superior`,
        `COUNT(*) AS qtd_registros`,
        `SUM(valor_recebido_num) AS valor_total_recebido`,
        `MAX(sancao_arquivo) AS sancao_arquivo`,
        `MAX(sancao_linha) AS sancao_linha`,
        `MAX(sancao_url) AS sancao_url`,
        `MAX(sancao_data_base) AS sancao_data_base`,
        `MAX(despesa_arquivo) AS despesa_arquivo`,
        `MAX(despesa_linha) AS despesa_linha`,
        `MAX(despesa_url) AS despesa_url`,
        `MAX(despesa_data_base) AS despesa_data_base`
      ],
      groupBy: [
        `cnpj`,
        `nome_sancionado`,
        `categoria_sancao`,
        `data_inicio_sancao`,
        `data_final_sancao`,
        `orgao_sancionador`,
        `fundamentacao_legal`,
        `orgao_superior`
      ],
      orderBy: `valor_total_recebido DESC NULLS LAST`
    },
    detailOrderBy: `r.data_lancamento_real DESC NULLS LAST, r.valor_recebido_num DESC NULLS LAST`,
    limit: 100
  });
}