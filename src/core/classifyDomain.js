export function classifyDomain(query) {

  const q = query.toLowerCase();

  if (q.includes("leniencia") || q.includes("acordo")) {
    return "acordos";
  }

  if (q.includes("ceis") || q.includes("sanção") || q.includes("sancao")) {
    return "sancoes";
  }

  if (q.includes("viagem") || q.includes("passagem") || q.includes("diaria")) {
    return "viagens";
  }

  if (q.includes("imovel") || q.includes("imóveis funcionais")) {
    return "imoveis";
  }

  if (q.includes("servidor")) {
    return "servidores";
  }

  if (q.includes("cnpj") || q.includes("empresa") || q.includes("razão social")) {
    return "rfb";
  }

  if (q.includes("despesa") || q.includes("pagamento")) {
    return "despesas";
  }

  return "unknown";
}