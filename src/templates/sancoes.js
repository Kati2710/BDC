export function sancoes_by_cnpj(cnpj) {

return `
SELECT
"CPF OU CNPJ DO SANCIONADO",
"NOME DO SANCIONADO",
"UF DO SANCIONADO",
"ÓRGÃO/ENTIDADE SANCIONADORA",
"DATA INÍCIO SANÇÃO",
"DATA FINAL SANÇÃO",
"TIPO SANÇÃO",
"FUNDAMENTAÇÃO LEGAL",
"DATA PUBLICAÇÃO",
"VALOR MULTA",

_audit_arquivo_csv_origem,
_audit_linha_csv,
_audit_url_download,
_audit_data_disponibilizacao_gov

FROM _ceis

WHERE "CPF OU CNPJ DO SANCIONADO" = '${cnpj}'

ORDER BY "DATA INÍCIO SANÇÃO" DESC

LIMIT 100
`;
}