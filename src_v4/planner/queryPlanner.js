import { DATASET_GRAPH } from "../catalog/datasetGraph.js";
import { SEMANTIC_CATALOG } from "../catalog/semanticCatalog.js";

const DOMAIN_SIGNALS = {
  sancoes: ["sancao","sancoes","sancionado","ceis","cnep","cepim","inidone","impedimento","suspensao"],
  despesas: ["recebeu","receber","recebimento","recursos publicos","recursos federais","despesas","pagamentos","favorecido","repasse","transferencia"],
  beneficios: ["bolsa familia","auxilio","bpc","seguro defeso","beneficio","pede meia","peti"],
  imoveis: ["imovel funcional","imoveis funcionais","residencia funcional","permissionario"],
  servidores: ["servidor","servidores","cargo","lotacao","remuneracao"],
  rfb: ["empresa","estabelecimento","cnae","receita federal","razao social"],
  contratos: ["contrato","licitacao","compra","pregao","concorrencia"],
  viagens: ["viagem","viagens","diaria","passagem","deslocamento"],
  emendas: ["emenda","emendas","parlamentar","deputado","senador"]
};

const DOMAIN_PREFIX = {
  sancoes:"sancao", despesas:"despesa", beneficios:"beneficio",
  servidores:"servidor", imoveis:"imovel", rfb:"rfb",
  contratos:"contrato", viagens:"viagem", emendas:"emenda", findings:"finding"
};

function detectDomains(q) {
  const detected = new Set();
  for (const [domain, signals] of Object.entries(DOMAIN_SIGNALS)) {
    if (signals.some(s => q.includes(s))) detected.add(domain);
  }
  return detected;
}

function detectIntent(q) {
  const s = ["resuma","resumo","resumir","total","soma","valor total","quanto","montante","agregado"];
  return s.some(x => q.includes(x)) ? "summary" : "detail";
}

function detectEntityType(entities) {
  if (entities?.cnpj) return "cnpj";
  if (entities?.cpf)  return "cpf";
  return "unknown";
}

function scoreEdge(edge) {
  let score = 0;
  if (edge.confidence === "high")   score += 100;
  if (edge.confidence === "medium") score += 50;
  if (edge.joinType === "findings") score += 200;
  if (edge.joinType === "direct")   score += 40;
  if (edge.temporalRules?.length)   score += 20;
  return score;
}

export function buildQueryPlan({ normalizedQuery, entities }) {
  const q          = String(normalizedQuery || "");
  const intent     = detectIntent(q);
  const entityType = detectEntityType(entities);
  const domains    = detectDomains(q);

  const byEntity = DATASET_GRAPH.filter(edge =>
    entityType === "unknown" ||
    (edge.allowedEntityTypes || []).includes(entityType)
  );

  const byDomain = byEntity.filter(edge => {
    if (edge.joinType === "findings") {
      return SEMANTIC_CATALOG[edge.leftDataset]?.domain === "findings";
    }
    const leftDomain  = SEMANTIC_CATALOG[edge.leftDataset]?.domain;
    const rightDomain = SEMANTIC_CATALOG[edge.rightDataset]?.domain;
    return domains.has(leftDomain) && domains.has(rightDomain);
  });

  if (byDomain.length === 0) {
    return {
      mode:"unsupported", intent, entityType,
      baseDataset:null, relatedDataset:null, edge:null,
      filters:{}, projectionMode:null, auditPrefixes:{}
    };
  }

  byDomain.sort((a, b) => scoreEdge(b) - scoreEdge(a));
  const best = byDomain[0];

  const filters = {};
  if (entityType === "cnpj") filters.cnpj = entities.cnpj;
  if (entityType === "cpf")  filters.cpf  = entities.cpf;

  const leftDomain  = SEMANTIC_CATALOG[best.leftDataset]?.domain;
  const rightDomain = best.rightDataset ? SEMANTIC_CATALOG[best.rightDataset]?.domain : leftDomain;

  return {
    mode: best.joinType === "findings" ? "findings" : "cross_dataset",
    intent, entityType,
    baseDataset:    best.leftDataset,
    relatedDataset: best.rightDataset,
    edge: best, filters,
    projectionMode: intent === "summary" ? "summary" : "detail",
    auditPrefixes: {
      base:    DOMAIN_PREFIX[leftDomain]  || "base",
      related: DOMAIN_PREFIX[rightDomain] || "rel"
    }
  };
}
