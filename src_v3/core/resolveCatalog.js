import { TABLES_CATALOG } from "../catalog/tables.js";

const DOMAIN_TABLE_MAP = {
  viagens: "_viagens",
  sancoes: "_ceis",
  acordos: "_acordos",
  imoveis: "_imoveisfuncionais",
  servidores: "_servidores",
  rfb: "_rfb_empresas",
  despesas: "_despesas_favorecidos"
};

export function resolveCatalog(domain) {
  const table = DOMAIN_TABLE_MAP[domain] || null;

  if (!table) {
    return {
      domain,
      table: null,
      catalog: null
    };
  }

  return {
    domain,
    table,
    catalog: TABLES_CATALOG[table] || null
  };
}