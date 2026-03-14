// datasetGraph.js
// Edges entre datasets — cada edge é um cruzamento possível
// Ordered by analytical priority

export const DATASET_GRAPH = [

  // ─── FINDINGS PRÉ-COMPUTADOS (prioridade máxima) ────────────────────────────
  // Consultados primeiro — sub-segundo

  {
    id: "findings_ceis_x_despesas",
    leftDataset: "_findings_ceis_x_despesas",
    rightDataset: null,   // single table, não precisa de join
    joinType: "findings",
    confidence: "high",
    cardinality: "1:1",
    leftKey:  { semanticField: "documento", column: "documento" },
    rightKey: null,
    allowedEntityTypes: ["cnpj", "cpf"],
    temporalRules: [],
    description: "PRÉ-COMPUTADO: CEIS × despesas (use este, não recalcule)",
  },

  // ─── SANÇÕES × DESPESAS PÚBLICAS ────────────────────────────────────────────

  {
    id: "ceis_x_despesas",
    leftDataset: "_ceis",
    rightDataset: "_despesas_favorecidos",
    joinType: "direct",
    confidence: "high",
    cardinality: "1:N",
    leftKey:  { semanticField: "cnpj_cpf_sancionado", column: "CPF OU CNPJ DO SANCIONADO" },
    rightKey: { semanticField: "codigo_favorecido",   column: "Código Favorecido" },
    allowedEntityTypes: ["cnpj", "cpf"],
    temporalRules: [
      { type: "after_start", baseField: "DATA INÍCIO SANÇÃO",
        relatedFieldExpression: "TRY_STRPTIME('01/' || \"Ano e mês do lançamento\", '%d/%m/%Y')" }
    ],
  },

  {
    id: "cnep_x_despesas",
    leftDataset: "_cnep",
    rightDataset: "_despesas_favorecidos",
    joinType: "direct",
    confidence: "high",
    cardinality: "1:N",
    leftKey:  { semanticField: "cnpj_cpf_sancionado", column: "CPF OU CNPJ DO SANCIONADO" },
    rightKey: { semanticField: "codigo_favorecido",   column: "Código Favorecido" },
    allowedEntityTypes: ["cnpj", "cpf"],
    temporalRules: [
      { type: "after_start", baseField: "DATA INÍCIO SANÇÃO",
        relatedFieldExpression: "TRY_STRPTIME('01/' || \"Ano e mês do lançamento\", '%d/%m/%Y')" }
    ],
  },

  {
    id: "ceaf_x_despesas",
    leftDataset: "_ceaf",
    rightDataset: "_despesas_favorecidos",
    joinType: "direct",
    confidence: "high",
    cardinality: "1:N",
    leftKey:  { semanticField: "cnpj_cpf_sancionado", column: "CPF OU CNPJ DO SANCIONADO" },
    rightKey: { semanticField: "codigo_favorecido",   column: "Código Favorecido" },
    allowedEntityTypes: ["cnpj", "cpf"],
    temporalRules: [
      { type: "after_start", baseField: "DATA INÍCIO SANÇÃO",
        relatedFieldExpression: "TRY_STRPTIME('01/' || \"Ano e mês do lançamento\", '%d/%m/%Y')" }
    ],
  },

  {
    id: "cepim_x_despesas",
    leftDataset: "_cepim",
    rightDataset: "_despesas_favorecidos",
    joinType: "direct",
    confidence: "high",
    cardinality: "1:N",
    leftKey:  { semanticField: "cnpj_entidade",     column: "CNPJ ENTIDADE" },
    rightKey: { semanticField: "codigo_favorecido", column: "Código Favorecido" },
    allowedEntityTypes: ["cnpj"],
    temporalRules: [],
  },

  // ─── SANÇÕES × BENEFÍCIOS SOCIAIS ───────────────────────────────────────────

  {
    id: "ceis_x_bolsafamilia",
    leftDataset: "_ceis",
    rightDataset: "_bolsafamilia_pagamentos",
    joinType: "direct",
    confidence: "high",
    cardinality: "1:N",
    leftKey:  { semanticField: "cnpj_cpf_sancionado", column: "CPF OU CNPJ DO SANCIONADO" },
    rightKey: { semanticField: "cpf_favorecido",      column: "CPF FAVORECIDO" },
    allowedEntityTypes: ["cpf"],
    temporalRules: [
      { type: "after_start", baseField: "DATA INÍCIO SANÇÃO",
        relatedFieldExpression: "TRY_STRPTIME('01/' || \"MÊS COMPETÊNCIA\", '%d/%m/%Y')" }
    ],
  },

  {
    id: "ceis_x_novobolsafamilia",
    leftDataset: "_ceis",
    rightDataset: "_novobolsafamilia",
    joinType: "direct",
    confidence: "high",
    cardinality: "1:N",
    leftKey:  { semanticField: "cnpj_cpf_sancionado", column: "CPF OU CNPJ DO SANCIONADO" },
    rightKey: { semanticField: "cpf_favorecido",      column: "CPF FAVORECIDO" },
    allowedEntityTypes: ["cpf"],
    temporalRules: [
      { type: "after_start", baseField: "DATA INÍCIO SANÇÃO",
        relatedFieldExpression: "TRY_STRPTIME('01/' || \"MÊS COMPETÊNCIA\", '%d/%m/%Y')" }
    ],
  },

  {
    id: "ceis_x_auxiliobrasil",
    leftDataset: "_ceis",
    rightDataset: "_auxiliobrasil",
    joinType: "direct",
    confidence: "high",
    cardinality: "1:N",
    leftKey:  { semanticField: "cnpj_cpf_sancionado", column: "CPF OU CNPJ DO SANCIONADO" },
    rightKey: { semanticField: "cpf_favorecido",      column: "CPF FAVORECIDO" },
    allowedEntityTypes: ["cpf"],
    temporalRules: [
      { type: "after_start", baseField: "DATA INÍCIO SANÇÃO",
        relatedFieldExpression: "TRY_STRPTIME('01/' || \"MÊS COMPETÊNCIA\", '%d/%m/%Y')" }
    ],
  },

  {
    id: "ceis_x_auxilioemergencial",
    leftDataset: "_ceis",
    rightDataset: "_auxilioemergencial",
    joinType: "direct",
    confidence: "high",
    cardinality: "1:N",
    leftKey:  { semanticField: "cnpj_cpf_sancionado", column: "CPF OU CNPJ DO SANCIONADO" },
    rightKey: { semanticField: "cpf_beneficiario",    column: "CPF BENEFICIÁRIO" },
    allowedEntityTypes: ["cpf"],
    temporalRules: [
      { type: "after_start", baseField: "DATA INÍCIO SANÇÃO",
        relatedFieldExpression: "TRY_STRPTIME('01/' || \"MÊS DISPONIBILIZAÇÃO\", '%d/%m/%Y')" }
    ],
  },

  {
    id: "ceis_x_bpc",
    leftDataset: "_ceis",
    rightDataset: "_bpc",
    joinType: "direct",
    confidence: "high",
    cardinality: "1:N",
    leftKey:  { semanticField: "cnpj_cpf_sancionado", column: "CPF OU CNPJ DO SANCIONADO" },
    rightKey: { semanticField: "cpf_beneficiario",    column: "CPF BENEFICIÁRIO" },
    allowedEntityTypes: ["cpf"],
    temporalRules: [
      { type: "after_start", baseField: "DATA INÍCIO SANÇÃO",
        relatedFieldExpression: "TRY_STRPTIME('01/' || \"MÊS COMPETÊNCIA\", '%d/%m/%Y')" }
    ],
  },

  {
    id: "ceis_x_segurodefeso",
    leftDataset: "_ceis",
    rightDataset: "_segurodefeso",
    joinType: "direct",
    confidence: "high",
    cardinality: "1:N",
    leftKey:  { semanticField: "cnpj_cpf_sancionado", column: "CPF OU CNPJ DO SANCIONADO" },
    rightKey: { semanticField: "cpf_favorecido",      column: "CPF FAVORECIDO" },
    allowedEntityTypes: ["cpf"],
    temporalRules: [],
  },

  // ─── SERVIDORES × IMÓVEIS ────────────────────────────────────────────────────

  {
    id: "servidores_x_imoveis",
    leftDataset: "_servidores",
    rightDataset: "_imoveisfuncionais",
    joinType: "direct",
    confidence: "high",
    cardinality: "1:N",
    leftKey:  { semanticField: "cpf", column: "CPF" },
    rightKey: { semanticField: "cpf", column: "CPF" },
    allowedEntityTypes: ["cpf"],
    temporalRules: [],
  },

  // ─── SERVIDORES × BENEFÍCIOS (acúmulo indevido) ──────────────────────────────

  {
    id: "servidores_x_bolsafamilia",
    leftDataset: "_servidores",
    rightDataset: "_bolsafamilia_pagamentos",
    joinType: "direct",
    confidence: "high",
    cardinality: "1:N",
    leftKey:  { semanticField: "cpf", column: "CPF" },
    rightKey: { semanticField: "cpf_favorecido", column: "CPF FAVORECIDO" },
    allowedEntityTypes: ["cpf"],
    temporalRules: [],
  },

  {
    id: "servidores_x_novobolsafamilia",
    leftDataset: "_servidores",
    rightDataset: "_novobolsafamilia",
    joinType: "direct",
    confidence: "high",
    cardinality: "1:N",
    leftKey:  { semanticField: "cpf", column: "CPF" },
    rightKey: { semanticField: "cpf_favorecido", column: "CPF FAVORECIDO" },
    allowedEntityTypes: ["cpf"],
    temporalRules: [],
  },

  {
    id: "servidores_x_auxilioemergencial",
    leftDataset: "_servidores",
    rightDataset: "_auxilioemergencial",
    joinType: "direct",
    confidence: "high",
    cardinality: "1:N",
    leftKey:  { semanticField: "cpf", column: "CPF" },
    rightKey: { semanticField: "cpf_beneficiario", column: "CPF BENEFICIÁRIO" },
    allowedEntityTypes: ["cpf"],
    temporalRules: [],
  },

  // ─── PEP × PAGAMENTOS ────────────────────────────────────────────────────────

  {
    id: "pep_x_despesas",
    leftDataset: "_pep",
    rightDataset: "_despesas_favorecidos",
    joinType: "direct",
    confidence: "medium",
    cardinality: "1:N",
    leftKey:  { semanticField: "cpf", column: "CPF" },
    rightKey: { semanticField: "codigo_favorecido", column: "Código Favorecido" },
    allowedEntityTypes: ["cpf"],
    temporalRules: [],
  },

  // ─── RFB × SANÇÕES ───────────────────────────────────────────────────────────

  {
    id: "rfb_x_ceis",
    leftDataset: "_rfb_estabelecimentos",
    rightDataset: "_ceis",
    joinType: "direct",
    confidence: "high",
    cardinality: "1:N",
    leftKey:  { semanticField: "cnpj_completo",       column: "cnpj_completo" },
    rightKey: { semanticField: "cnpj_cpf_sancionado", column: "CPF OU CNPJ DO SANCIONADO" },
    allowedEntityTypes: ["cnpj"],
    temporalRules: [],
  },

  {
    id: "rfb_x_despesas",
    leftDataset: "_rfb_estabelecimentos",
    rightDataset: "_despesas_favorecidos",
    joinType: "direct",
    confidence: "high",
    cardinality: "1:N",
    leftKey:  { semanticField: "cnpj_completo",     column: "cnpj_completo" },
    rightKey: { semanticField: "codigo_favorecido", column: "Código Favorecido" },
    allowedEntityTypes: ["cnpj"],
    temporalRules: [],
  },

  {
    id: "rfb_socios_x_ceis",
    leftDataset: "_rfb_socios",
    rightDataset: "_ceis",
    joinType: "direct",
    confidence: "medium",
    cardinality: "1:N",
    leftKey:  { semanticField: "cpf_cnpj_socio",      column: "cpf_cnpj_socio" },
    rightKey: { semanticField: "cnpj_cpf_sancionado", column: "CPF OU CNPJ DO SANCIONADO" },
    allowedEntityTypes: ["cnpj", "cpf"],
    temporalRules: [],
  },

  // ─── NOTAS FISCAIS × SANÇÕES ─────────────────────────────────────────────────

  {
    id: "notasfiscais_x_ceis",
    leftDataset: "_notasfiscais",
    rightDataset: "_ceis",
    joinType: "direct",
    confidence: "medium",
    cardinality: "1:N",
    leftKey:  { semanticField: "cnpj_emitente",       column: "CPF/CNPJ Emitente" },
    rightKey: { semanticField: "cnpj_cpf_sancionado", column: "CPF OU CNPJ DO SANCIONADO" },
    allowedEntityTypes: ["cnpj"],
    temporalRules: [],
  },

];

export function getEdgesForDataset(dataset) {
  return DATASET_GRAPH.filter(
    e => e.leftDataset === dataset || e.rightDataset === dataset
  );
}

export function findDirectEdge(leftDataset, rightDataset, entityType = null) {
  return DATASET_GRAPH.find(edge => {
    const samePair =
      (edge.leftDataset === leftDataset && edge.rightDataset === rightDataset) ||
      (edge.leftDataset === rightDataset && edge.rightDataset === leftDataset);
    if (!samePair) return false;
    if (!entityType) return true;
    return (edge.allowedEntityTypes || []).includes(entityType);
  }) || null;
}

export function findFindingsEdge(entityType) {
  return DATASET_GRAPH.filter(
    e => e.joinType === "findings" &&
    (e.allowedEntityTypes || []).includes(entityType)
  );
}