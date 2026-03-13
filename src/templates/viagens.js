export function viagens_by_name(name, limit = 10) {

return `
SELECT
"Identificador do processo de viagem",
"Número da Proposta (PCDP)",
"Nome do órgão superior",
"Nome do órgão pagador",
"CPF viajante",
"Nome",
"Cargo",
"Função",
"Período - Data de início",
"Período - Data de fim",
"Destinos",
"Motivo",
"Valor diárias",
"Valor passagens",
"Valor devolução",
"Valor outros gastos",
"Situação",
"Viagem Urgente",
"País - Destino ida",
"UF - Destino ida",
"Cidade - Destino ida",
"Meio de transporte",
"Valor",
_audit_arquivo_csv_origem,
_audit_linha_csv,
_audit_url_download,
_audit_data_disponibilizacao_gov

FROM _viagens

WHERE "Nome" ILIKE '%${name}%'

ORDER BY "Período - Data de início" DESC

LIMIT ${limit}
`;
}