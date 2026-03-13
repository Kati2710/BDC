import fs from "fs";
import path from "path";

const schemaPath = path.resolve("./schema_compact.json");
const outputPath = path.resolve("./src/catalog/tables.js");

const raw = fs.readFileSync(schemaPath, "utf-8");
const schema = JSON.parse(raw);

function inferSearchColumns(columns) {
  const preferredPatterns = [
    /nome/i,
    /razao social/i,
    /razão social/i,
    /fantasia/i,
    /\bcpf\b/i,
    /\bcnpj\b/i,
    /documento/i,
    /processo/i,
    /municipio/i,
    /município/i,
    /orgao/i,
    /órgão/i,
    /cargo/i,
    /funcao/i,
    /função/i,
    /favorecido/i,
    /permissionario/i,
    /permissionário/i,
  ];

  return columns.filter((col) =>
    preferredPatterns.some((pattern) => pattern.test(col))
  );
}

function inferDateColumns(columns) {
  return columns.filter((col) =>
    /data|mês|mes|ano|período|periodo/i.test(col)
  );
}

function inferValueColumns(columns) {
  return columns.filter((col) =>
    /valor|quantidade|total|remuneração|remuneracao|multa/i.test(col)
  );
}

const catalog = {};

for (const [tableName, columns] of Object.entries(schema)) {
  const auditColumns = columns.filter((c) => c.startsWith("_audit"));
  const normalColumns = columns.filter((c) => !c.startsWith("_audit"));

  catalog[tableName] = {
    table: tableName,
    columns: normalColumns,
    auditColumns,
    searchColumns: inferSearchColumns(normalColumns),
    dateColumns: inferDateColumns(normalColumns),
    valueColumns: inferValueColumns(normalColumns),
    hasAudit: auditColumns.length > 0,
  };
}

const fileContent = `export const TABLES_CATALOG = ${JSON.stringify(catalog, null, 2)};\n`;

fs.writeFileSync(outputPath, fileContent, "utf-8");

console.log(`✅ Catálogo gerado em: ${outputPath}`);
console.log(`📦 Total de tabelas: ${Object.keys(catalog).length}`);