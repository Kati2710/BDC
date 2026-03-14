// semanticCatalog.js
// Gerado com base nas colunas reais do brazildatacorp.duckdb (volume)
// 42 tabelas confirmadas em 14/03/2026

export const SEMANTIC_CATALOG = {

  // ─── SANÇÕES ────────────────────────────────────────────────────────────────

  _ceis: {
    dataset: "_ceis", domain: "sancoes", entityType: "cnpj_ou_cpf",
    description: "Cadastro de Empresas Inidôneas e Suspensas",
    keyCol: "CPF OU CNPJ DO SANCIONADO",
    dateStartCol: "DATA INÍCIO SANÇÃO",
    dateEndCol:   "DATA FINAL SANÇÃO",
    fields: {
      cnpj_cpf_sancionado: { column: "CPF OU CNPJ DO SANCIONADO", type: "document" },
      nome_sancionado:     { column: "NOME DO SANCIONADO",         type: "text" },
      categoria_sancao:    { column: "CATEGORIA DA SANÇÃO",        type: "text" },
      data_inicio_sancao:  { column: "DATA INÍCIO SANÇÃO",         type: "date" },
      data_final_sancao:   { column: "DATA FINAL SANÇÃO",          type: "date" },
      orgao_sancionador:   { column: "ÓRGÃO SANCIONADOR",          type: "text" },
      uf_orgao_sancionador:{ column: "UF ÓRGÃO SANCIONADOR",       type: "text" },
      fundamentacao_legal: { column: "FUNDAMENTAÇÃO LEGAL",        type: "text" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  _cnep: {
    dataset: "_cnep", domain: "sancoes", entityType: "cnpj_ou_cpf",
    description: "Cadastro Nacional de Empresas Punidas",
    keyCol: "CPF OU CNPJ DO SANCIONADO",
    dateStartCol: "DATA INÍCIO SANÇÃO",
    dateEndCol:   "DATA FINAL SANÇÃO",
    fields: {
      cnpj_cpf_sancionado: { column: "CPF OU CNPJ DO SANCIONADO", type: "document" },
      nome_sancionado:     { column: "NOME DO SANCIONADO",         type: "text" },
      categoria_sancao:    { column: "CATEGORIA DA SANÇÃO",        type: "text" },
      data_inicio_sancao:  { column: "DATA INÍCIO SANÇÃO",         type: "date" },
      data_final_sancao:   { column: "DATA FINAL SANÇÃO",          type: "date" },
      orgao_sancionador:   { column: "ÓRGÃO SANCIONADOR",          type: "text" },
      valor_multa:         { column: "VALOR DA MULTA",             type: "money_string" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  _ceaf: {
    dataset: "_ceaf", domain: "sancoes", entityType: "cnpj_ou_cpf",
    description: "Cadastro de Expulsões da Administração Federal",
    keyCol: "CPF OU CNPJ DO SANCIONADO",
    dateStartCol: "DATA INÍCIO SANÇÃO",
    dateEndCol:   "DATA FINAL SANÇÃO",
    fields: {
      cnpj_cpf_sancionado: { column: "CPF OU CNPJ DO SANCIONADO", type: "document" },
      nome_sancionado:     { column: "NOME DO SANCIONADO",         type: "text" },
      categoria_sancao:    { column: "CATEGORIA DA SANÇÃO",        type: "text" },
      data_inicio_sancao:  { column: "DATA INÍCIO SANÇÃO",         type: "date" },
      data_final_sancao:   { column: "DATA FINAL SANÇÃO",          type: "date" },
      orgao_sancionador:   { column: "ÓRGÃO SANCIONADOR",          type: "text" },
      cargo_efetivo:       { column: "CARGO EFETIVO",              type: "text" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  _cepim: {
    dataset: "_cepim", domain: "sancoes", entityType: "cnpj",
    description: "Entidades Privadas Sem Fins Lucrativos Impedidas",
    keyCol: "CNPJ ENTIDADE",
    dateStartCol: null, dateEndCol: null,
    fields: {
      cnpj_entidade:       { column: "CNPJ ENTIDADE",         type: "document" },
      nome_entidade:       { column: "NOME ENTIDADE",         type: "text" },
      numero_convenio:     { column: "NÚMERO CONVÊNIO",       type: "text" },
      orgao_concedente:    { column: "ÓRGÃO CONCEDENTE",      type: "text" },
      motivo_impedimento:  { column: "MOTIVO DO IMPEDIMENTO", type: "text" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  _acordos: {
    dataset: "_acordos", domain: "sancoes", entityType: "cnpj",
    description: "Acordos de Leniência",
    keyCol: "CNPJ DO SANCIONADO",
    dateStartCol: "DATA DE INÍCIO DO ACORDO",
    dateEndCol:   "DATA DE FIM DO ACORDO",
    fields: {
      cnpj_sancionado:  { column: "CNPJ DO SANCIONADO",            type: "document" },
      razao_social:     { column: "RAZÃO SOCIAL – CADASTRO RECEITA",type: "text" },
      data_inicio:      { column: "DATA DE INÍCIO DO ACORDO",       type: "date" },
      data_fim:         { column: "DATA DE FIM DO ACORDO",          type: "date" },
      situacao:         { column: "SITUAÇÃO DO ACORDO DE LENIENCA", type: "text" },
      orgao_sancionador:{ column: "ÓRGÃO SANCIONADOR",              type: "text" },
      termos:           { column: "TERMOS DO ACORDO",               type: "text" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  // ─── PAGAMENTOS / BENEFÍCIOS ─────────────────────────────────────────────────

  _despesas_favorecidos: {
    dataset: "_despesas_favorecidos", domain: "despesas", entityType: "cnpj_ou_cpf",
    description: "Recebimentos de recursos públicos por favorecido",
    keyCol: "Código Favorecido",
    fields: {
      codigo_favorecido:   { column: "Código Favorecido",        type: "document" },
      nome_favorecido:     { column: "Nome Favorecido",          type: "text" },
      orgao_superior:      { column: "Nome Órgão Superior",      type: "text" },
      orgao:               { column: "Nome Órgão",               type: "text" },
      uf:                  { column: "Sigla UF",                 type: "text" },
      ano_mes_lancamento:  { column: "Ano e mês do lançamento",  type: "month_string" },
      valor_recebido:      { column: "Valor Recebido",           type: "money_string" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  _bolsafamilia_pagamentos: {
    dataset: "_bolsafamilia_pagamentos", domain: "beneficios", entityType: "cpf",
    description: "Pagamentos do Bolsa Família",
    keyCol: "CPF FAVORECIDO",
    fields: {
      cpf_favorecido:  { column: "CPF FAVORECIDO",    type: "document" },
      nome_favorecido: { column: "NOME FAVORECIDO",   type: "text" },
      uf:              { column: "UF",                type: "text" },
      municipio:       { column: "NOME MUNICÍPIO",    type: "text" },
      mes_competencia: { column: "MÊS COMPETÊNCIA",   type: "month_string" },
      valor_parcela:   { column: "VALOR PARCELA",     type: "money_string" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  _novobolsafamilia: {
    dataset: "_novobolsafamilia", domain: "beneficios", entityType: "cpf",
    description: "Pagamentos do Novo Bolsa Família",
    keyCol: "CPF FAVORECIDO",
    fields: {
      cpf_favorecido:  { column: "CPF FAVORECIDO",    type: "document" },
      nome_favorecido: { column: "NOME FAVORECIDO",   type: "text" },
      uf:              { column: "UF",                type: "text" },
      municipio:       { column: "NOME MUNICÍPIO",    type: "text" },
      mes_competencia: { column: "MÊS COMPETÊNCIA",   type: "month_string" },
      valor_parcela:   { column: "VALOR PARCELA",     type: "money_string" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  _auxiliobrasil: {
    dataset: "_auxiliobrasil", domain: "beneficios", entityType: "cpf",
    description: "Pagamentos do Auxílio Brasil",
    keyCol: "CPF FAVORECIDO",
    fields: {
      cpf_favorecido:  { column: "CPF FAVORECIDO",    type: "document" },
      nome_favorecido: { column: "NOME FAVORECIDO",   type: "text" },
      uf:              { column: "UF",                type: "text" },
      municipio:       { column: "NOME MUNICÍPIO",    type: "text" },
      mes_competencia: { column: "MÊS COMPETÊNCIA",   type: "month_string" },
      valor_parcela:   { column: "VALOR PARCELA",     type: "money_string" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  _auxilioemergencial: {
    dataset: "_auxilioemergencial", domain: "beneficios", entityType: "cpf",
    description: "Pagamentos do Auxílio Emergencial",
    keyCol: "CPF BENEFICIÁRIO",
    fields: {
      cpf_beneficiario: { column: "CPF BENEFICIÁRIO",     type: "document" },
      nome_beneficiario:{ column: "NOME BENEFICIÁRIO",    type: "text" },
      uf:               { column: "UF",                   type: "text" },
      municipio:        { column: "NOME MUNICÍPIO",        type: "text" },
      enquadramento:    { column: "ENQUADRAMENTO",         type: "text" },
      mes_disponib:     { column: "MÊS DISPONIBILIZAÇÃO", type: "month_string" },
      valor_beneficio:  { column: "VALOR BENEFÍCIO",      type: "money_string" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  _auxilioreconstrucao: {
    dataset: "_auxilioreconstrucao", domain: "beneficios", entityType: "cpf",
    description: "Auxílio Reconstrução (desastres naturais)",
    keyCol: "CPF FAVORECIDO",
    fields: {
      cpf_favorecido:  { column: "CPF FAVORECIDO",  type: "document" },
      nome_favorecido: { column: "NOME FAVORECIDO", type: "text" },
      uf:              { column: "UF",              type: "text" },
      municipio:       { column: "NOME MUNICÍPIO",  type: "text" },
      mes_referencia:  { column: "MÊS REFERÊNCIA",  type: "month_string" },
      valor_parcela:   { column: "VALOR PARCELA",   type: "money_string" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  _bpc: {
    dataset: "_bpc", domain: "beneficios", entityType: "cpf",
    description: "Benefício de Prestação Continuada",
    keyCol: "CPF BENEFICIÁRIO",
    fields: {
      cpf_beneficiario: { column: "CPF BENEFICIÁRIO",  type: "document" },
      nome_beneficiario:{ column: "NOME BENEFICIÁRIO", type: "text" },
      uf:               { column: "UF",               type: "text" },
      municipio:        { column: "NOME MUNICÍPIO",    type: "text" },
      mes_competencia:  { column: "MÊS COMPETÊNCIA",  type: "month_string" },
      valor_parcela:    { column: "VALOR PARCELA",    type: "money_string" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  _segurodefeso: {
    dataset: "_segurodefeso", domain: "beneficios", entityType: "cpf",
    description: "Seguro Defeso (pescadores)",
    keyCol: "CPF FAVORECIDO",
    fields: {
      cpf_favorecido:  { column: "CPF FAVORECIDO",  type: "document" },
      nome_favorecido: { column: "NOME FAVORECIDO", type: "text" },
      uf:              { column: "UF",              type: "text" },
      municipio:       { column: "NOME MUNICÍPIO",  type: "text" },
      mes_referencia:  { column: "MÊS REFERÊNCIA",  type: "month_string" },
      valor_parcela:   { column: "VALOR PARCELA",   type: "money_string" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  _garantiasafra: {
    dataset: "_garantiasafra", domain: "beneficios", entityType: "cpf",
    description: "Garantia Safra (agricultores)",
    keyCol: "NIS FAVORECIDO",
    fields: {
      nis_favorecido:  { column: "NIS FAVORECIDO",  type: "document" },
      nome_favorecido: { column: "NOME FAVORECIDO", type: "text" },
      uf:              { column: "UF",              type: "text" },
      municipio:       { column: "NOME MUNICÍPIO",  type: "text" },
      mes_referencia:  { column: "MÊS REFERÊNCIA",  type: "month_string" },
      valor_parcela:   { column: "VALOR PARCELA",   type: "money_string" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  _pedemeia: {
    dataset: "_pedemeia", domain: "beneficios", entityType: "cpf",
    description: "Programa Pé-de-Meia (estudantes)",
    keyCol: "CPF BENEFICIÁRIO",
    fields: {
      cpf_beneficiario: { column: "CPF BENEFICIÁRIO",  type: "document" },
      nome_beneficiario:{ column: "NOME BENEFICIÁRIO", type: "text" },
      uf:               { column: "UF",               type: "text" },
      municipio:        { column: "NOME MUNICÍPIO",    type: "text" },
      mes_folha:        { column: "MÊS FOLHA",        type: "month_string" },
      valor_parcela:    { column: "VALOR PARCELA",    type: "money_string" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  _peti: {
    dataset: "_peti", domain: "beneficios", entityType: "cpf",
    description: "Programa de Erradicação do Trabalho Infantil",
    keyCol: "NIS FAVORECIDO",
    fields: {
      nis_favorecido:  { column: "NIS FAVORECIDO",  type: "document" },
      nome_favorecido: { column: "NOME FAVORECIDO", type: "text" },
      uf:              { column: "UF",              type: "text" },
      municipio:       { column: "NOME MUNICÍPIO",  type: "text" },
      mes_referencia:  { column: "MÊS REFERÊNCIA",  type: "month_string" },
      valor_parcela:   { column: "VALOR PARCELA",   type: "money_string" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  _cpgf: {
    dataset: "_cpgf", domain: "despesas", entityType: "cnpj_ou_cpf",
    description: "Cartão de Pagamento do Governo Federal",
    keyCol: "CNPJ OU CPF FAVORECIDO",
    fields: {
      cnpj_cpf_favorecido: { column: "CNPJ OU CPF FAVORECIDO", type: "document" },
      nome_favorecido:     { column: "NOME FAVORECIDO",         type: "text" },
      orgao_superior:      { column: "NOME ÓRGÃO SUPERIOR",    type: "text" },
      orgao:               { column: "NOME ÓRGÃO",              type: "text" },
      mes_extrato:         { column: "MÊS EXTRATO",             type: "text" },
      valor_transacao:     { column: "VALOR TRANSAÇÃO",         type: "money_string" },
      transacao:           { column: "TRANSAÇÃO",               type: "text" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  _cpcc: {
    dataset: "_cpcc", domain: "despesas", entityType: "cnpj_ou_cpf",
    description: "Cartão de Pagamento de Contratos Corporativos",
    keyCol: "CNPJ OU CPF FAVORECIDO",
    fields: {
      cnpj_cpf_favorecido: { column: "CNPJ OU CPF FAVORECIDO", type: "document" },
      nome_favorecido:     { column: "NOME FAVORECIDO",         type: "text" },
      orgao_superior:      { column: "NOME ÓRGÃO SUPERIOR",    type: "text" },
      mes_extrato:         { column: "MÊS EXTRATO",             type: "text" },
      valor_transacao:     { column: "VALOR TRANSAÇÃO",         type: "money_string" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  _transferencias: {
    dataset: "_transferencias", domain: "despesas", entityType: "cnpj_ou_cpf",
    description: "Transferências da União para estados, municípios e entidades",
    keyCol: "CÓDIGO FAVORECIDO",
    fields: {
      codigo_favorecido: { column: "CÓDIGO FAVORECIDO",  type: "document" },
      nome_favorecido:   { column: "NOME FAVORECIDO",    type: "text" },
      orgao:             { column: "NOME ÓRGÃO",         type: "text" },
      uf:                { column: "UF",                 type: "text" },
      municipio:         { column: "NOME MUNICÍPIO",     type: "text" },
      ano_mes:           { column: "ANO / MÊS",          type: "month_string" },
      valor_transferido: { column: "VALOR TRANSFERIDO",  type: "money_string" },
      tipo_transferencia:{ column: "TIPO TRANSFERÊNCIA", type: "text" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  _convenios: {
    dataset: "_convenios", domain: "despesas", entityType: "cnpj",
    description: "Convênios federais",
    keyCol: "CÓDIGO CONVENENTE",
    fields: {
      codigo_convenente: { column: "CÓDIGO CONVENENTE",     type: "document" },
      nome_convenente:   { column: "NOME CONVENENTE",       type: "text" },
      orgao_superior:    { column: "NOME ÓRGÃO SUPERIOR",   type: "text" },
      objeto:            { column: "OBJETO DO CONVÊNIO",    type: "text" },
      uf:                { column: "UF",                    type: "text" },
      valor_convenio:    { column: "VALOR CONVÊNIO",        type: "money_string" },
      data_inicio:       { column: "DATA INÍCIO VIGÊNCIA",  type: "date" },
      data_final:        { column: "DATA FINAL VIGÊNCIA",   type: "date" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  _compras: {
    dataset: "_compras", domain: "contratos", entityType: "cnpj",
    description: "Contratos e compras públicas",
    keyCol: "Código Contratado",
    fields: {
      codigo_contratado: { column: "Código Contratado",      type: "document" },
      nome_contratado:   { column: "Nome Contratado",        type: "text" },
      orgao_superior:    { column: "Nome Órgão Superior",    type: "text" },
      objeto:            { column: "Objeto",                 type: "text" },
      modalidade:        { column: "Modalidade Compra",      type: "text" },
      situacao:          { column: "Situação Contrato",      type: "text" },
      data_assinatura:   { column: "Data Assinatura Contrato",type: "date" },
      valor_inicial:     { column: "Valor Inicial Compra",   type: "money_string" },
      valor_final:       { column: "Valor Final Compra",     type: "money_string" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  _licitacoes: {
    dataset: "_licitacoes", domain: "contratos", entityType: "cnpj",
    description: "Licitações públicas",
    keyCol: "Código Vencedor",
    fields: {
      codigo_vencedor:   { column: "Código Vencedor",        type: "document" },
      nome_vencedor:     { column: "Nome Vencedor",          type: "text" },
      orgao_superior:    { column: "Nome Órgão Superior",    type: "text" },
      objeto:            { column: "Objeto",                 type: "text" },
      modalidade:        { column: "Modalidade Compra",      type: "text" },
      uf:                { column: "UF",                     type: "text" },
      valor_licitacao:   { column: "Valor Licitação",        type: "money_string" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  // ─── SERVIDORES / PESSOAS ────────────────────────────────────────────────────

  _servidores: {
    dataset: "_servidores", domain: "servidores", entityType: "cpf",
    description: "Servidores públicos federais — cadastro e remuneração",
    keyCol: "CPF",
    fields: {
      cpf:              { column: "CPF",               type: "document" },
      nome:             { column: "NOME",              type: "text" },
      orgao_lotacao:    { column: "ORG_LOTACAO",       type: "text" },
      orgsup_lotacao:   { column: "ORGSUP_LOTACAO",    type: "text" },
      cargo:            { column: "DESCRICAO_CARGO",   type: "text" },
      tipo_vinculo:     { column: "TIPO_VINCULO",      type: "text" },
      remuneracao:      { column: "REMUNERAÇÃO BÁSICA BRUTA (R$)", type: "money_string" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  _pep: {
    dataset: "_pep", domain: "servidores", entityType: "cpf",
    description: "Pessoas Expostas Politicamente",
    keyCol: "CPF",
    fields: {
      cpf:              { column: "CPF",                    type: "document" },
      nome_pep:         { column: "Nome_PEP",               type: "text" },
      funcao:           { column: "Descrição_Função",       type: "text" },
      nivel_funcao:     { column: "Nível_Função",           type: "text" },
      orgao:            { column: "Nome_Órgão",             type: "text" },
      data_inicio:      { column: "Data_Início_Exercício",  type: "date" },
      data_fim:         { column: "Data_Fim_Exercício",     type: "date" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  _imoveisfuncionais: {
    dataset: "_imoveisfuncionais", domain: "imoveis", entityType: "cpf",
    description: "Imóveis funcionais da União",
    keyCol: "CPF",
    fields: {
      cpf:                { column: "CPF",                           type: "document" },
      nome_permissionario:{ column: "NOME PERMISSIONÁRIO",           type: "text" },
      cargo:              { column: "CARGO OU FUNÇÃO DE CONFIANÇA",  type: "text" },
      orgao_exercicio:    { column: "ÓRGÃO EXERCÍCIO DO PERMISSIONÁRIO", type: "text" },
      data_inicio_ocupacao:{ column: "DATA INÍCIO OCUPAÇÃO",         type: "date" },
      endereco:           { column: "ENDEREÇO",                      type: "text" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  // ─── RFB ────────────────────────────────────────────────────────────────────

  _rfb_empresas: {
    dataset: "_rfb_empresas", domain: "rfb", entityType: "cnpj",
    description: "Empresas cadastradas na Receita Federal",
    keyCol: "cnpj_basico",
    fields: {
      cnpj_basico:      { column: "cnpj_basico",          type: "document" },
      razao_social:     { column: "razao_social",         type: "text" },
      natureza_juridica:{ column: "natureza_juridica",    type: "text" },
      capital_social:   { column: "capital_social",       type: "text" },
      porte:            { column: "porte",                type: "text" },
    },
    auditFields: [],
  },

  _rfb_estabelecimentos: {
    dataset: "_rfb_estabelecimentos", domain: "rfb", entityType: "cnpj",
    description: "Estabelecimentos cadastrados na Receita Federal",
    keyCol: "cnpj_completo",
    fields: {
      cnpj_completo:    { column: "cnpj_completo",        type: "document" },
      cnpj_basico:      { column: "cnpj_basico",          type: "text" },
      nome_fantasia:    { column: "nome_fantasia",         type: "text" },
      situacao:         { column: "situacao_cadastral",    type: "text" },
      uf:               { column: "uf",                   type: "text" },
      municipio:        { column: "municipio",            type: "text" },
      cnae_principal:   { column: "cnae_principal",       type: "text" },
      data_inicio:      { column: "data_inicio_atividade",type: "date" },
    },
    auditFields: [],
  },

  _rfb_socios: {
    dataset: "_rfb_socios", domain: "rfb", entityType: "cnpj",
    description: "Quadro societário das empresas",
    keyCol: "cnpj_basico",
    fields: {
      cnpj_basico:        { column: "cnpj_basico",           type: "document" },
      nome_socio:         { column: "nome_socio",            type: "text" },
      cpf_cnpj_socio:     { column: "cpf_cnpj_socio",       type: "text" },
      qualificacao_socio: { column: "qualificacao_socio",    type: "text" },
      data_entrada:       { column: "data_entrada_sociedade",type: "date" },
    },
    auditFields: [],
  },

  _rfb_simples: {
    dataset: "_rfb_simples", domain: "rfb", entityType: "cnpj",
    description: "Empresas optantes pelo Simples Nacional e MEI",
    keyCol: "cnpj_basico",
    fields: {
      cnpj_basico:     { column: "cnpj_basico",        type: "document" },
      opcao_simples:   { column: "opcao_simples",      type: "text" },
      data_opcao:      { column: "data_opcao_simples", type: "date" },
      opcao_mei:       { column: "opcao_mei",          type: "text" },
    },
    auditFields: [],
  },

  _renunciasfiscais: {
    dataset: "_renunciasfiscais", domain: "rfb", entityType: "cnpj",
    description: "Renúncias fiscais e benefícios tributários",
    keyCol: "CNPJ",
    fields: {
      cnpj:            { column: "CNPJ",               type: "document" },
      razao_social:    { column: "Razão Social",        type: "text" },
      uf:              { column: "UF",                  type: "text" },
      beneficio_fiscal:{ column: "Benefício Fiscal",    type: "text" },
      tributo:         { column: "Tributo",             type: "text" },
      valor_renuncia:  { column: "Valor Renúncia Fiscal (R$)", type: "money_string" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  // ─── VIAGENS ─────────────────────────────────────────────────────────────────

  _viagens: {
    dataset: "_viagens", domain: "viagens", entityType: "cpf",
    description: "Viagens a serviço — diárias e passagens",
    keyCol: "CPF viajante",
    fields: {
      cpf_viajante:    { column: "CPF viajante",           type: "document" },
      nome:            { column: "Nome",                   type: "text" },
      cargo:           { column: "Cargo",                  type: "text" },
      orgao_superior:  { column: "Nome do órgão superior", type: "text" },
      destinos:        { column: "Destinos",               type: "text" },
      valor_diarias:   { column: "Valor diárias",          type: "money_string" },
      valor_passagens: { column: "Valor passagens",        type: "money_string" },
      valor_total:     { column: "Valor",                  type: "money_string" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  // ─── EMENDAS ─────────────────────────────────────────────────────────────────

  _emendas: {
    dataset: "_emendas", domain: "emendas", entityType: "cnpj_ou_cpf",
    description: "Emendas parlamentares",
    keyCol: "Código do Favorecido",
    fields: {
      codigo_favorecido: { column: "Código do Favorecido",    type: "document" },
      nome_favorecido:   { column: "Favorecido",              type: "text" },
      autor_emenda:      { column: "Nome do Autor da Emenda", type: "text" },
      uf:                { column: "UF",                      type: "text" },
      valor_recebido:    { column: "Valor Recebido",          type: "money_string" },
      valor_pago:        { column: "Valor Pago",              type: "money_string" },
    },
    auditFields: ["_audit_url_download","_audit_data_disponibilizacao_gov","_audit_arquivo_csv_origem","_audit_linha_csv"],
  },

  // ─── FINDINGS PRÉ-COMPUTADOS ─────────────────────────────────────────────────

  _findings_ceis_x_despesas: {
    dataset: "_findings_ceis_x_despesas", domain: "findings", entityType: "cnpj_ou_cpf",
    description: "PRÉ-COMPUTADO: Sancionados CEIS × recursos públicos recebidos após sanção",
    keyCol: "documento",
    fields: {
      documento:           { column: "documento",           type: "document" },
      nome_sancionado:     { column: "nome_sancionado",     type: "text" },
      categoria_sancao:    { column: "categoria_sancao",    type: "text" },
      data_inicio_sancao:  { column: "data_inicio_sancao",  type: "date" },
      orgao_superior:      { column: "orgao_superior",      type: "text" },
      uf:                  { column: "uf",                  type: "text" },
      qtd_registros:       { column: "qtd_registros",       type: "integer" },
      valor_total:         { column: "valor_recebido_total",type: "decimal" },
      primeiro_pagamento:  { column: "primeiro_pagamento",  type: "date" },
      ultimo_pagamento:    { column: "ultimo_pagamento",    type: "date" },
    },
    auditFields: ["sancao_url","sancao_data_base","despesa_url","despesa_data_base"],
    isFinding: true,
  },

};

export function getDatasetMeta(dataset) {
  return SEMANTIC_CATALOG[dataset] || null;
}

export function listDatasets() {
  return Object.keys(SEMANTIC_CATALOG);
}

export function listFindings() {
  return Object.entries(SEMANTIC_CATALOG)
    .filter(([, m]) => m.isFinding)
    .map(([k]) => k);
}

export function getDatasetsByDomain(domain) {
  return Object.entries(SEMANTIC_CATALOG)
    .filter(([, m]) => m.domain === domain)
    .map(([k]) => k);
}