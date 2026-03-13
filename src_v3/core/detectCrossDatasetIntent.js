export function detectCrossDatasetIntent(normalizedQuery, entities = {}) {
  const q = String(normalizedQuery || "");

  const hasCnpj = !!entities.cnpj;

  const mentionsSancao =
    q.includes("sancao") ||
    q.includes("sancoes") ||
    q.includes("ceis") ||
    q.includes("cnep") ||
    q.includes("punida") ||
    q.includes("punido");

  const mentionsRecebimentos =
    q.includes("recebeu") ||
    q.includes("receber") ||
    q.includes("recursos publicos") ||
    q.includes("recursos federais") ||
    q.includes("pagamentos") ||
    q.includes("despesas") ||
    q.includes("favorecido");

  const mentionsAfter =
    q.includes("depois da sancao") ||
    q.includes("apos a sancao") ||
    q.includes("apos sancao") ||
    q.includes("após a sanção") ||
    q.includes("depois da sanção");

  if (hasCnpj && mentionsSancao && mentionsRecebimentos) {
    return {
      type: "cross_dataset",
      strategy: "empresa_sancionada_recebimentos"
    };
  }

  if (hasCnpj && mentionsSancao && mentionsAfter) {
    return {
      type: "cross_dataset",
      strategy: "empresa_sancionada_recebimentos"
    };
  }

  return {
    type: "single_dataset",
    strategy: null
  };
}