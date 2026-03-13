export const DATASET_GRAPH = {
  cnpj: [
    {
      leftTable: "_ceis",
      leftColumn: "CPF OU CNPJ DO SANCIONADO",
      rightTable: "_despesas_favorecidos",
      rightColumn: "Código Favorecido",
      relationType: "direct"
    },
    {
      leftTable: "_ceis",
      leftColumn: "CPF OU CNPJ DO SANCIONADO",
      rightTable: "_rfb_estabelecimentos",
      rightColumn: "cnpj_completo",
      relationType: "direct"
    },
    {
      leftTable: "_cnep",
      leftColumn: "CPF OU CNPJ DO SANCIONADO",
      rightTable: "_despesas_favorecidos",
      rightColumn: "Código Favorecido",
      relationType: "direct"
    },
    {
      leftTable: "_cnep",
      leftColumn: "CPF OU CNPJ DO SANCIONADO",
      rightTable: "_rfb_estabelecimentos",
      rightColumn: "cnpj_completo",
      relationType: "direct"
    },
    {
      leftTable: "_cepim",
      leftColumn: "CNPJ ENTIDADE",
      rightTable: "_despesas_favorecidos",
      rightColumn: "Código Favorecido",
      relationType: "direct"
    },
    {
      leftTable: "_rfb_estabelecimentos",
      leftColumn: "cnpj_completo",
      rightTable: "_despesas_favorecidos",
      rightColumn: "Código Favorecido",
      relationType: "direct"
    },
    {
      leftTable: "_rfb_estabelecimentos",
      leftColumn: "cnpj_completo",
      rightTable: "_convenios",
      rightColumn: "NOME CONVENENTE",
      relationType: "weak_name_match"
    }
  ],

  cnpj_basico: [
    {
      leftTable: "_rfb_empresas",
      leftColumn: "cnpj_basico",
      rightTable: "_rfb_estabelecimentos",
      rightColumn: "cnpj_basico",
      relationType: "direct"
    },
    {
      leftTable: "_rfb_empresas",
      leftColumn: "cnpj_basico",
      rightTable: "_rfb_socios",
      rightColumn: "cnpj_basico",
      relationType: "direct"
    },
    {
      leftTable: "_rfb_empresas",
      leftColumn: "cnpj_basico",
      rightTable: "_rfb_simples",
      rightColumn: "cnpj_basico",
      relationType: "direct"
    }
  ],

  cpf: [
    {
      leftTable: "_servidores",
      leftColumn: "CPF",
      rightTable: "_viagens",
      rightColumn: "CPF viajante",
      relationType: "direct"
    },
    {
      leftTable: "_servidores",
      leftColumn: "CPF",
      rightTable: "_imoveisfuncionais",
      rightColumn: "CPF",
      relationType: "direct"
    },
    {
      leftTable: "_servidores",
      leftColumn: "CPF",
      rightTable: "_pep",
      rightColumn: "CPF",
      relationType: "direct"
    },
    {
      leftTable: "_servidores",
      leftColumn: "CPF",
      rightTable: "_cpgf",
      rightColumn: "CPF PORTADOR",
      relationType: "direct"
    }
  ]
};

export function getGraphEdgesByKey(key) {
  return DATASET_GRAPH[key] || [];
}

export function findEdgesForTable(tableName) {
  const out = [];

  for (const [key, edges] of Object.entries(DATASET_GRAPH)) {
    for (const edge of edges) {
      if (edge.leftTable === tableName || edge.rightTable === tableName) {
        out.push({ key, ...edge });
      }
    }
  }

  return out;
}