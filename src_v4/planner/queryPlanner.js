import { DATASET_GRAPH } from "../catalog/datasetGraph.js";
import { SEMANTIC_CATALOG } from "../catalog/semanticCatalog.js";

const DOMAIN_SIGNALS = {
  sancoes:   ["sancao","sancoes","sancionado","ceis","cnep","ceaf","cepim","inidone","impedimento","suspensao","punicao","penalidade","leniencia","acordo"],
  despesas:  ["recebeu","receber","recebimento","recursos publicos","recursos federais","despesas","pagamentos","favorecido","repasse","transferencia"],
  beneficios:["bolsa familia","auxilio brasil","auxilio emergencial","bpc","seguro defeso","beneficio","pede meia","peti","novo bolsa","garantia safra"],
  imoveis:   ["imovel funcional","imoveis funcionais","residencia funcional","permissionario","ocupacao funcional"],
  servidores:["servidor","servidores","cargo","lotacao","remuneracao","funcional","militar","bacen","siape"],
  rfb:       ["empresa","estabelecimento","cnae","receita federal","razao social","simples nacional","mei","socio","quadro societario"],
  contratos: ["contrato","licitacao","compra","pregao","concorrencia","apostilamento"],
  viagens:   ["viagem","viagens","diaria","passagem","deslocamento","voo","trecho"],
  emendas:   ["emenda","emendas","parlamentar","deputado","senador"],
  cpgf:      ["cartao","cpgf","cpcc","cartao corporativo","cartao de pagamento"],
  convenios: ["convenio","convenios"],
  nf:        ["nota fiscal","notas fiscais","nfe"],
  renuncia:  ["renuncia fiscal","beneficio fiscal","isencao"],
  pep:       ["pessoa exposta politicamente","pep","cargo politico"],
};

const DOMAIN_PREFIX = {
  sancoes:"sancao",despesas:"despesa",beneficios:"beneficio",servidores:"servidor",
  imoveis:"imovel",rfb:"rfb",contratos:"contrato",viagens:"viagem",emendas:"emenda",
  cpgf:"cpgf",convenios:"convenio",nf:"nf",renuncia:"renuncia",pep:"pep",findings:"finding"
};

const DOMAIN_PRIMARY_TABLE = {
  sancoes:"_ceis",despesas:"_despesas_favorecidos",beneficios:"_bolsafamilia_pagamentos",
  imoveis:"_imoveisfuncionais",servidores:"_servidores",rfb:"_rfb_estabelecimentos",
  contratos:"_compras",viagens:"_viagens",emendas:"_emendas",cpgf:"_cpgf",
  convenios:"_convenios",nf:"_notasfiscais",renuncia:"_renunciasfiscais",pep:"_pep",
};

const TABLE_OVERRIDES = [
  {signals:["cnep"],table:"_cnep"},
  {signals:["ceaf","expulsao"],table:"_ceaf"},
  {signals:["cepim","impedida"],table:"_cepim"},
  {signals:["acordo","leniencia"],table:"_acordos"},
  {signals:["novo bolsa"],table:"_novobolsafamilia"},
  {signals:["auxilio emergencial"],table:"_auxilioemergencial"},
  {signals:["auxilio brasil"],table:"_auxiliobrasil"},
  {signals:["bpc","prestacao continuada"],table:"_bpc"},
  {signals:["seguro defeso"],table:"_segurodefeso"},
  {signals:["garantia safra"],table:"_garantiasafra"},
  {signals:["pede meia"],table:"_pedemeia"},
  {signals:["peti"],table:"_peti"},
  {signals:["socio","quadro societario"],table:"_rfb_socios"},
  {signals:["simples nacional","mei"],table:"_rfb_simples"},
  {signals:["renuncia fiscal"],table:"_renunciasfiscais"},
  {signals:["nota fiscal","nfe"],table:"_notasfiscais"},
  {signals:["cpgf","cartao corporativo"],table:"_cpgf"},
  {signals:["cpcc"],table:"_cpcc"},
  {signals:["licitacao","licitacoes"],table:"_licitacoes"},
  {signals:["convenio","convenios"],table:"_convenios"},
  {signals:["transferencia","transferencias"],table:"_transferencias"},
  {signals:["pep","exposta politicamente"],table:"_pep"},
  {signals:["imovel funcional","imoveis funcionais","permissionario"],table:"_imoveisfuncionais"},
  {signals:["viagem","viagens","diaria","passagem"],table:"_viagens"},
  {signals:["emenda","emendas parlamentares"],table:"_emendas"},
];

function detectDomains(q){const d=new Set();for(const[domain,signals]of Object.entries(DOMAIN_SIGNALS)){if(signals.some(s=>q.includes(s)))d.add(domain);}return d;}
function detectIntent(q){const s=["resuma","resumo","resumir","total","soma","valor total","quanto","montante","agregado"];return s.some(x=>q.includes(x))?"summary":"detail";}
function detectEntityType(e){if(e?.cnpj)return"cnpj";if(e?.cpf)return"cpf";return"unknown";}
function scoreEdge(e){let s=0;if(e.confidence==="high")s+=100;if(e.confidence==="medium")s+=50;if(e.joinType==="findings")s+=200;if(e.joinType==="direct")s+=40;if(e.temporalRules?.length)s+=20;return s;}

function resolveTable(q,domains){
  for(const ov of TABLE_OVERRIDES){if(ov.signals.some(s=>q.includes(s)))return ov.table;}
  for(const domain of domains){if(DOMAIN_PRIMARY_TABLE[domain])return DOMAIN_PRIMARY_TABLE[domain];}
  return null;
}

function resolveFilter(table,entities){
  const meta=SEMANTIC_CATALOG[table];if(!meta)return null;
  if(entities.cnpj)return{col:meta.keyCol,value:entities.cnpj,type:"exact"};
  if(entities.cpf)return{col:meta.keyCol,value:entities.cpf,type:"exact"};
  if(entities.companyName){
    const f=Object.values(meta.fields).find(f=>f.type==="text"&&["nome","razao","entidade","contratado","favorecido","fantasia"].some(k=>f.column.toLowerCase().includes(k)));
    if(f)return{col:f.column,value:entities.companyName,type:"like"};
  }
  if(entities.personName){
    const f=Object.values(meta.fields).find(f=>f.type==="text"&&["nome","name"].some(k=>f.column.toLowerCase().includes(k)));
    if(f)return{col:f.column,value:entities.personName,type:"like"};
  }
  if(entities.agency){
    const f=Object.values(meta.fields).find(f=>f.type==="text"&&["orgao","ministerio"].some(k=>f.column.toLowerCase().includes(k)));
    if(f)return{col:f.column,value:entities.agency,type:"like"};
  }
  return null;
}

export function buildQueryPlan({normalizedQuery,entities}){
  const q=String(normalizedQuery||"");
  const intent=detectIntent(q);
  const entityType=detectEntityType(entities);
  const domains=detectDomains(q);

  const wantsCross=domains.has("sancoes")&&(domains.has("despesas")||domains.has("beneficios"));
  if(wantsCross&&(entityType==="cnpj"||entityType==="cpf")){
    const fe=DATASET_GRAPH.filter(e=>e.joinType==="findings"&&(e.allowedEntityTypes||[]).includes(entityType));
    if(fe.length>0){
      return{mode:"findings",intent,entityType,baseDataset:fe[0].leftDataset,relatedDataset:null,edge:fe[0],filters:{cnpj:entities.cnpj,cpf:entities.cpf},projectionMode:intent==="summary"?"summary":"detail",auditPrefixes:{base:"finding",related:"finding"}};
    }
  }

  if(entityType!=="unknown"&&domains.size>=2){
    const candidates=DATASET_GRAPH.filter(e=>{
      if(e.joinType==="findings")return false;
      if(!(e.allowedEntityTypes||[]).includes(entityType))return false;
      const ld=SEMANTIC_CATALOG[e.leftDataset]?.domain;
      const rd=SEMANTIC_CATALOG[e.rightDataset]?.domain;
      return domains.has(ld)&&domains.has(rd);
    });
    if(candidates.length>0){
      candidates.sort((a,b)=>scoreEdge(b)-scoreEdge(a));
      const best=candidates[0];
      const ld=SEMANTIC_CATALOG[best.leftDataset]?.domain;
      const rd=SEMANTIC_CATALOG[best.rightDataset]?.domain;
      return{mode:"cross_dataset",intent,entityType,baseDataset:best.leftDataset,relatedDataset:best.rightDataset,edge:best,filters:{cnpj:entities.cnpj,cpf:entities.cpf},projectionMode:intent==="summary"?"summary":"detail",auditPrefixes:{base:DOMAIN_PREFIX[ld]||"base",related:DOMAIN_PREFIX[rd]||"rel"}};
    }
  }

  const table=resolveTable(q,domains);
  if(table&&SEMANTIC_CATALOG[table]){
    const filter=resolveFilter(table,entities);
    return{mode:"single_table",intent,entityType,baseDataset:table,relatedDataset:null,edge:null,filters:{cnpj:entities.cnpj,cpf:entities.cpf},tableFilter:filter,projectionMode:intent==="summary"?"summary":"detail",topN:entities.topN||20,auditPrefixes:{base:DOMAIN_PREFIX[SEMANTIC_CATALOG[table]?.domain]||"base"}};
  }

  return{mode:"unsupported",intent,entityType,baseDataset:null,relatedDataset:null,edge:null,filters:{},projectionMode:null,auditPrefixes:{}};
}
