function buildCpfServidoresImoveis(plan) {
  const cpf = esc(plan.filters?.cpf);
  const summary = plan.output === "summary";

  return buildSqlFromJoinSpec({
    baseTable: "_servidores",
    relatedTable: "_imoveisfuncionais",
    baseAlias: "b",
    relatedAlias: "r",
    baseSelect: [
      `CPF`,
      `NOME`,
      `ORGSUP_LOTACAO`,
      `ORG_LOTACAO`,
      `DESCRICAO_CARGO`
    ],
    relatedSelect: [
      `CPF`,
      `"NOME PERMISSIONÁRIO" AS nome_permissionario`,
      `"ÓRGÃO EXERCÍCIO DO PERMISSIONÁRIO" AS orgao_exercicio`,
      `"DATA INÍCIO OCUPAÇÃO" AS data_inicio_ocupacao`,
      `_audit_arquivo_csv_origem AS imovel_arquivo`,
      `_audit_linha_csv AS imovel_linha`,
      `_audit_url_download AS imovel_url`,
      `_audit_data_disponibilizacao_gov AS imovel_data_base`
    ],
    joinLeftColumn: "CPF",
    joinRightColumn: "CPF",
    baseWhere: [
      `CPF = '${cpf}'`
    ],
    relatedWhere: [
      `CPF = '${cpf}'`
    ],
    output: summary ? "summary" : "detail",
    summary: {
      select: [
        `CPF`,
        `NOME`,
        `ORGSUP_LOTACAO`,
        `ORG_LOTACAO`,
        `DESCRICAO_CARGO`,
        `COUNT(r.CPF) AS qtd_imoveis_funcionais`,
        `MIN(data_inicio_ocupacao) AS primeira_ocupacao`,
        `MAX(data_inicio_ocupacao) AS ultima_ocupacao`,
        `MAX(imovel_arquivo) AS imovel_arquivo`,
        `MAX(imovel_linha) AS imovel_linha`,
        `MAX(imovel_url) AS imovel_url`,
        `MAX(imovel_data_base) AS imovel_data_base`
      ],
      groupBy: [
        `CPF`,
        `NOME`,
        `ORGSUP_LOTACAO`,
        `ORG_LOTACAO`,
        `DESCRICAO_CARGO`
      ],
      orderBy: `ultima_ocupacao DESC NULLS LAST`
    },
    detailOrderBy: `r.data_inicio_ocupacao DESC NULLS LAST`,
    limit: 100
  });
}