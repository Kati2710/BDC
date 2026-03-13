export function buildPlan({ domain, qtype }) {

  if (domain === "viagens" && qtype === "listing") {
    return {
      strategy: "template",
      template: "viagens_by_name"
    };
  }

  if (domain === "acordos") {
    return {
      strategy: "template",
      template: "acordos_by_empresa"
    };
  }

  if (domain === "sancoes") {
    return {
      strategy: "template",
      template: "sancoes_by_cnpj"
    };
  }

  if (domain === "imoveis") {
    return {
      strategy: "template",
      template: "imoveis_por_orgao"
    };
  }

  if (domain === "rfb") {
    return {
      strategy: "template",
      template: "empresa_por_cnpj"
    };
  }

  return {
    strategy: "llm_fallback"
  };
}