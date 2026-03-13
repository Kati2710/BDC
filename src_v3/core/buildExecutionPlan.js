import { resolveJoinPath } from "./resolveJoinPath.js";
import { selectBestJoinPath } from "./selectBestJoinPath.js";

export function buildExecutionPlan({ normalizedQuery, intent, entities, domain }) {
  const q = String(normalizedQuery || "");

  const hasCnpj = !!entities?.cnpj;
  const hasCpf = !!entities?.cpf;
  const hasOrgao = !!entities?.orgao;

  const asksSummary =
    q.includes("resuma") ||
    q.includes("resumo") ||
    q.includes("resumir") ||
    q.includes("total") ||
    q.includes("somatorio") ||
    q.includes("somatório") ||
    q.includes("soma") ||
    q.includes("quanto recebeu") ||
    q.includes("quanto recebeu no total") ||
    q.includes("valor total") ||
    q.includes("recebeu recursos");

  const output = asksSummary ? "summary" : "detail";

  const mentionsSancao =
    q.includes("sancao") ||
    q.includes("sancoes") ||
    q.includes("ceis") ||
    q.includes("cnep");

  const mentionsRecebimentos =
    q.includes("recebeu") ||
    q.includes("recursos publicos") ||
    q.includes("recursos federais") ||
    q.includes("despesas") ||
    q.includes("favorecido") ||
    q.includes("pagamentos");

  const mentionsViagens =
    q.includes("viagem") ||
    q.includes("viagens") ||
    q.includes("passagem") ||
    q.includes("diaria") ||
    q.includes("diarias");

  const mentionsImoveis =
    q.includes("imovel funcional") ||
    q.includes("imoveis funcionais") ||
    q.includes("residencia funcional") ||
    q.includes("permissionario");

  const desiredTables = [];

  if (mentionsSancao) desiredTables.push("_ceis");
  if (mentionsRecebimentos) desiredTables.push("_despesas_favorecidos");
  if (mentionsViagens) desiredTables.push("_viagens");
  if (mentionsImoveis) desiredTables.push("_imoveisfuncionais");
  if (hasOrgao) desiredTables.push("_servidores");
  if (domain === "rfb") desiredTables.push("_rfb_estabelecimentos", "_rfb_empresas");

  const candidateJoins = resolveJoinPath({
    entities,
    desiredTables
  });

  if (hasCnpj && mentionsSancao && mentionsRecebimentos) {
    return {
      mode: "cross_dataset",
      pattern: "cnpj_sancoes_recebimentos",
      baseTable: "_ceis",
      relatedTables: ["_despesas_favorecidos"],
      joins: selectBestJoinPath({
        joins: candidateJoins,
        baseTable: "_ceis",
        relatedTables: ["_despesas_favorecidos"]
      }),
      filters: {
        cnpj: entities.cnpj,
        afterSanction: true
      },
      output
    };
  }

  if (hasCpf && mentionsViagens && mentionsImoveis) {
    return {
      mode: "cross_dataset",
      pattern: "cpf_viagens_imoveis",
      baseTable: "_viagens",
      relatedTables: ["_imoveisfuncionais"],
      joins: selectBestJoinPath({
        joins: candidateJoins,
        baseTable: "_viagens",
        relatedTables: ["_imoveisfuncionais"]
      }),
      filters: {
        cpf: entities.cpf
      },
      output
    };
  }

  if (hasOrgao && mentionsImoveis) {
    return {
      mode: "cross_dataset",
      pattern: "orgao_servidores_imoveis",
      baseTable: "_imoveisfuncionais",
      relatedTables: ["_servidores"],
      joins: selectBestJoinPath({
        joins: candidateJoins,
        baseTable: "_imoveisfuncionais",
        relatedTables: ["_servidores"]
      }),
      filters: {
        orgao: entities.orgao
      },
      output
    };
  }

  return {
    mode: "single_dataset",
    pattern: "default_single_dataset",
    baseTable: null,
    relatedTables: [],
    joins: [],
    filters: {},
    output
  };
}