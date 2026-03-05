import express from "express";
import cors from "cors";
import Anthropic from "@anthropic-ai/sdk";

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

const HETZNER_API = process.env.HETZNER_API_BASE || "http://89.167.48.3:5010";
const HETZNER_KEY = process.env.HETZNER_API_KEY || "bdc-sql-api-key-2026-segura";
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

const DB_CATALOG = `
BANCO: brazildatacorp.duckdb | 5B linhas | DuckDB

== REGRAS SQL ==
- BIGINT: só operadores numéricos. VARCHAR: LIKE/=. STRUCT: ponto (est.uf). Aspas duplas em colunas com espaços/acentos.
- EMPRESAS em CTE: SEMPRE extraia campos STRUCT com alias — SELECT est.uf as uf, est.situacao_cadastral as situacao, est.data_inicio_atividade as data_inicio — e agrupe pelo alias (GROUP BY uf). NUNCA use GROUP BY est.uf ou ORDER BY est.* fora do SELECT original.
- DATAS YYYYMM são BIGINT: WHERE "MÊS COMPETÊNCIA" >= 202401 AND "MÊS COMPETÊNCIA" <= 202412. NUNCA divida por 100.
- VALORES monetários são VARCHAR: SUM(CAST(REPLACE("VALOR PARCELA",',','.') AS DECIMAL))
- LIMIT 100 em listagens; sem LIMIT em COUNT/SUM
- CTEs: não aplique CAST/REPLACE em colunas já computadas como DECIMAL
- UNION/UNION ALL: ORDER BY só no final, NUNCA dentro de subquery. Em UNION com ORDER BY, use alias numérico (ORDER BY 1,2) ou nome de coluna simples — NUNCA expressão como CAST(MES AS INTEGER)
- UNION com múltiplas tabelas (análise completa de CNPJ): todas as subqueries devem ter EXATAMENTE o mesmo número de colunas
- EMPRESAS em CTE: SEMPRE extraia campos STRUCT com alias — SELECT est.uf as uf, est.situacao_cadastral as situacao, est.data_inicio_atividade as data_inicio — e agrupe pelo alias (GROUP BY uf). NUNCA use est.* fora do SELECT onde o STRUCT foi acessado — nem em WHERE, nem em GROUP BY, nem em ORDER BY de queries externas
- DUE DILIGENCE / ANÁLISE DE CNPJ: NUNCA use UNION entre tabelas com colunas diferentes. Use queries SEPARADAS por seção com SELECT 'SEÇÃO' as fonte, coluna1, coluna2 — mantenha EXATAMENTE 3 colunas em cada parte do UNION: fonte, campo, valor. MANTENHA SIMPLES: máximo 4 tabelas por query de due diligence para evitar timeout
- BOLSA FAMÍLIA: até 2021→_bolsafamilia_pagamentos; 2022-2025→_novobolsafamilia
- SERVIDORES: ANO e MES são VARCHAR: WHERE ANO='2024' AND MES='01'
- AFASTAMENTOS: DATA_INICIO_AFASTAMENTO e DATA_FIM_AFASTAMENTO são VARCHAR com formato DD/MM/YYYY ou 'Não informada'. Para filtrar por duração use: TRY_STRPTIME(DATA_INICIO_AFASTAMENTO, '%d/%m/%Y') — NUNCA TRY_CAST direto como DATE. NÃO existe "Início do afastamento" nem "Fim do afastamento"
- CEIS/CNEP/CEAF: coluna do documento é "CPF OU CNPJ DO SANCIONADO" — NÃO existe "CNPJ OU CPF DO SANCIONADO". NÃO existe "TIPO SANÇÃO" — use "CATEGORIA DA SANÇÃO"
- CEIS/CNEP/CEAF: coluna de nome da empresa é "NOME DO SANCIONADO" — NÃO existe "RAZÃO SOCIAL" nessas tabelas
- ACORDOS: coluna de status é "SITUAÇÃO DO ACORDO DE LENIÊNICA" (exatamente assim) — NÃO existe "SITUAÇÃO DO ACORDO". Coluna de nome é "RAZÃO SOCIAL – CADASTRO RECEITA" — NÃO existe "RAZÃO SOCIAL" sozinha

== LIMITAÇÕES — RESPONDA EM PORTUGUÊS SEM GERAR SQL SE PERGUNTAR SOBRE ==
- Judiciário (STF,STJ,TRF,TRT), Legislativo (Câmara,Senado,vereadores): NÃO estão nos dados — não tente SQL
- Servidores estaduais/municipais: NÃO estão nos dados — não tente SQL
- CPF no BF/BPC é mascarado (***123456**): não cruza com RFB/PEP por CPF
- MEI não é identificável: use porte='MICRO EMPRESA' como aproximação
- Abono permanência: sem coluna dedicada

== TABELAS ==

-- PROGRAMAS SOCIAIS --
Colunas comuns: "MÊS COMPETÊNCIA"(BIGINT),"MÊS REFERÊNCIA"(BIGINT),"UF","CÓDIGO MUNICÍPIO SIAFI","NOME MUNICÍPIO","CPF FAVORECIDO"(VARCHAR),"NIS FAVORECIDO"(BIGINT),"NOME FAVORECIDO","VALOR PARCELA"(VARCHAR)
_bolsafamilia_pagamentos(1.4B,até2021): colunas comuns
_bolsafamilia_saques(478M,até2021): colunas comuns +DATA SAQUE(DATE) — ⚠️ NÃO tem "MÊS COMPETÊNCIA", use "MÊS REFERÊNCIA"
_novobolsafamilia(668M,2022-2025): colunas comuns
_auxiliobrasil(280M): colunas comuns
_bpc(300M): "MÊS COMPETÊNCIA"(BIGINT),"UF","NOME MUNICÍPIO","NIS BENEFICIÁRIO"(BIGINT),"CPF BENEFICIÁRIO"(VARCHAR),"NOME BENEFICIÁRIO","NIS REPRESENTANTE LEGAL"(BIGINT),"CPF REPRESENTANTE LEGAL","NOME REPRESENTANTE LEGAL","NÚMERO BENEFÍCIO"(BIGINT),"BENEFÍCIO CONCEDIDO JUDICIALMENTE","VALOR PARCELA"
_auxilioemergencial(782M): "MÊS DISPONIBILIZAÇÃO"(BIGINT),"UF","CÓDIGO MUNICÍPIO IBGE"(BIGINT),"NOME MUNICÍPIO","NIS BENEFICIÁRIO"(VARCHAR),"CPF BENEFICIÁRIO","NOME BENEFICIÁRIO","NIS RESPONSÁVEL"(BIGINT),"CPF RESPONSÁVEL","NOME RESPONSÁVEL","ENQUADRAMENTO","PARCELA","VALOR BENEFÍCIO"
_segurodefeso(40M): "MÊS REFERÊNCIA"(BIGINT),"UF","CÓDIGO MUNICÍPIO SIAFI","NOME MUNICÍPIO","CPF FAVORECIDO","NIS FAVORECIDO"(BIGINT),"RGP FAVORECIDO","NOME FAVORECIDO","VALOR PARCELA"
_garantiasafra(33M): "MÊS REFERÊNCIA"(BIGINT),"UF","NOME MUNICÍPIO","NIS FAVORECIDO"(BIGINT),"NOME FAVORECIDO","VALOR PARCELA" — sem CPF
_pedemeia(37M): "MÊS FOLHA"(BIGINT),"MÊS REFERÊNCIA"(BIGINT),"UF","NOME MUNICÍPIO","NIS BENEFICIÁRIO"(BIGINT),"CPF BENEFICIÁRIO","NOME BENEFICIÁRIO","CÓDIGO ETAPA ENSINO"(BIGINT),"ETAPA ENSINO","TIPO INCENTIVO","DATA DO PAGAMENTO"(DATE),"VALOR PARCELA" — ⚠️ usa "MÊS FOLHA" não "MÊS COMPETÊNCIA"
_peti(803K): "MÊS REFERÊNCIA"(BIGINT),"UF","NOME MUNICÍPIO","NIS FAVORECIDO"(BIGINT),"NOME FAVORECIDO","SITUAÇÃO BENEFÍCIO","VALOR PARCELA"
_auxilioreconstrucao(425K): "MÊS REFERÊNCIA"(BIGINT),"UF","CÓDIGO MUNICÍPIO SIAFI"(BIGINT),"NOME MUNICÍPIO","CPF FAVORECIDO","NIS FAVORECIDO"(BIGINT),"NOME FAVORECIDO","QUANTIDADE DE PESSOAS NA FAMÍLIA"(BIGINT),"DATA EFETIVAÇÃO PARCELA"(DATE),"VALOR PARCELA"

-- EMPRESAS RFB (28 tabelas por UF) --
_empresas_sp(20M),_mg(7M),_rj(6M),_rs(5M),_pr(5M),_ba(3M),_sc(3M),_go(2M),_pe(2M),_ce(2M),
_df(1M),_es(1M),_mt(1M),_ma(1M),_pa(1M),_ms(914K),_pb(881K),_rn(787K),_am(740K),_al(653K),
_pi(593K),_ro(476K),_to(460K),_se(457K),_ex(169K),_ap(151K),_ac(158K),_rr(134K)
Colunas: cnpj_basico(VARCHAR), razao_social, porte('MICRO EMPRESA'|'EMPRESA DE PEQUENO PORTE'|'DEMAIS'), capital_social(DOUBLE), est(STRUCT)
est: situacao_cadastral('ATIVA'|'BAIXADA'|'INAPTA'|'SUSPENSA'|'NULA'), uf, municipio, cnpj_completo, cnae_principal, cnae_principal_codigo, cnaes_secundarios_codigos(VARCHAR[]), cnaes_secundarios_descricoes(VARCHAR[]), data_inicio_atividade(VARCHAR YYYYMMDD→LIKE'2024%'), data_situacao_cadastral, nome_fantasia, matriz_filial, motivo_situacao, cep, logradouro, bairro, telefone_1, correio_eletronico

-- SERVIDORES --
Schema cadastro: Id_SERVIDOR_PORTAL,NOME,CPF,MATRICULA,DESCRICAO_CARGO,FUNCAO,UORG_LOTACAO,ORG_LOTACAO,ORGSUP_LOTACAO,ORG_EXERCICIO,ORGSUP_EXERCICIO,TIPO_VINCULO,SITUACAO_VINCULO,REGIME_JURIDICO,JORNADA_DE_TRABALHO,DATA_INGRESSO_ORGAO,UF_EXERCICIO
_servidores_cadastro(19M): SITUACAO_VINCULO='ATIVO PERMANENTE'|'SEM VINCULO'|'CONTRATO TEMPORARIO'|'NOMEADO CARGO COMIS.'
_servidores_cadastro__2(593K): SITUACAO_VINCULO='ATIVO PERMANENTE'|'MILITAR DA ATIVA'|'CELETISTA'
_servidores_cadastro__3(793K): SITUACAO_VINCULO='ATIVO PERMANENTE'|'CONT.PROF.SUBSTITUTO'
_servidores_cadastro__4(73K—pensionistas): CPF_REPRESENTANTE_LEGAL,CPF_INSTITUIDOR_PENSAO,TIPO_PENSAO,DATA_INICIO_PENSAO — SITUACAO_VINCULO='PENSIONISTA'
_servidores_cadastro__5(12M—militares reforma): TIPO_APOSENTADORIA('RESERVA'|'REFORMA'|'REFORMA POR INVALIDEZ'),DATA_APOSENTADORIA — SITUACAO_VINCULO='MILITAR REFORMADO'|'MILITAR DA RESERVA'
_servidores_cadastro__6(1M): SITUACAO_VINCULO='EXCEDENTE A LOTACAO'|'SEM VINCULO'
_servidores_cadastro__7(52M—militares ativos): SITUACAO_VINCULO='CEDIDO SUS/LEI 8270'|'CELETISTA/EMPREGADO'|'EMPREGO PUBLICO'
⚠️ NUNCA use SITUACAO_VINCULO='ATIVO'. Civis ativos→__1 ou __2 com 'ATIVO PERMANENTE'. Militares ativos→__7.

_servidores_remuneracao(19M)+__2(30M)+__3(52M)+__4(237K)+__5(9M):
  ANO(VARCHAR),MES(VARCHAR),Id_SERVIDOR_PORTAL,CPF,NOME,"REMUNERAÇÃO BÁSICA BRUTA (R$)","ABATE-TETO (R$)","GRATIFICAÇÃO NATALINA (R$)","FÉRIAS (R$)","IRRF (R$)","PSS/RPGS (R$)","DEMAIS DEDUÇÕES (R$)","REMUNERAÇÃO APÓS DEDUÇÕES OBRIGATÓRIAS (R$)","TOTAL DE VERBAS INDENIZATÓRIAS (R$)(*)"
  ⚠️ SEM coluna de órgão — JOIN com _servidores_cadastro via Id_SERVIDOR_PORTAL para filtrar por órgão

_servidores_afastamentos(84K)+__2(8M): ANO,MES,Id_SERVIDOR_PORTAL,CPF,NOME,DATA_INICIO_AFASTAMENTO(VARCHAR),DATA_FIM_AFASTAMENTO(VARCHAR)
  ⚠️ DATA_FIM_AFASTAMENTO pode conter 'Não informada' — NUNCA faça CAST direto. Use: WHERE DATA_FIM_AFASTAMENTO IS NULL OR DATA_FIM_AFASTAMENTO = 'Não informada' para afastamentos em aberto
_servidores_honorarios_jetons_(45K): ANO,MES,Id_SERVIDOR_PORTAL,CPF,NOME,EMPRESA,VALOR
_servidores_honorariosadvocaticios(1M): ANO,MES,Id_SERVIDOR_PORTAL,CPF,NOME,OBSERVACOES,VALOR
_servidores_observacoes(463K+__2..7): ANO,MES,Id_SERVIDOR_PORTAL,NOME,CPF,OBSERVACAO

-- DESPESAS --
_despesas_favorecidos(114M): "Código Favorecido","Nome Favorecido","Sigla UF","Nome Município","Código Órgão Superior","Nome Órgão Superior","Código Órgão","Nome Órgão","Ano e mês do lançamento"(VARCHAR'MM/YYYY'→LIKE'%/2024'),"Valor Recebido"(VARCHAR)
  ⚠️ Campo chama "Nome Favorecido" aqui (outras despesas usam "Favorecido")

_despesasdiarias_despesas_empenho(31M): "Id Empenho"(BIGINT),"Código Empenho","Data Emissão"(DATE),"Tipo Empenho","Código Órgão Superior"(BIGINT),"Órgão Superior","Favorecido","Código Favorecido","Função","Programa","Ação","Categoria de Despesa","Grupo de Despesa","Valor Original do Empenho","Valor do Empenho Convertido pra R$"
_despesasdiarias_despesas_pagamento(103M): "Código Pagamento","Data Emissão","Código Órgão Superior","Órgão Superior","Órgão","Código Favorecido","Favorecido","Valor Original do Pagamento","Valor do Pagamento Convertido pra R$"
_despesasdiarias_despesas_pagamento_favorecidosfinais(131M): "Código Pagamento","Data Emissão","Código Favorecido","Favorecido","Valor do Pagamento em R$"
_despesasdiarias_despesas_liquidacao_empenhosimpactados(77M): "Código Liquidação","Código Empenho","Valor Liquidado (R$)","Valor Restos a Pagar Pagos (R$)"
_despesasdiarias_despesas_pagamento_empenhosimpactados(103M): "Código Pagamento","Código Empenho","Valor Pago (R$)"
_despesasdiarias_despesas_itemempenho(33M): "Id Empenho"(BIGINT),"Código Empenho","Descrição","Quantidade","Valor Unitário","Valor Total"

-- VIAGENS --
_viagens_viagem(9M): "Identificador do processo de viagem","Código do órgão superior","Nome do órgão superior","Nome órgão solicitante","CPF viajante","Nome","Cargo","Período - Data de início","Período - Data de fim","Destinos","Motivo","Valor diárias","Valor passagens"
_viagens_trecho(20M): "Identificador do processo de viagem","Origem - País","Origem - UF","Origem - Cidade","Destino - País","Destino - UF","Destino - Cidade","Meio de transporte"('Aéreo'|'Rodoviário'|'Fluvial'|'Ferroviário'|'Marítimo'|'Veículo Próprio'|'Veículo Oficial'),"Número Diárias","Missao?"('Sim'|'Não')
  ⚠️ SEM colunas de órgão/CPF — SEMPRE JOIN com _viagens_viagem via "Identificador do processo de viagem"
_viagens_passagem(5M): "Identificador do processo de viagem","Meio de transporte","País - Destino ida","UF - Destino ida","Cidade - Destino ida","Valor da passagem","Data da emissão/compra"
_viagens_pagamento(16M): "Identificador do processo de viagem","Nome do órgão superior","Tipo de pagamento","Valor"

-- SANÇÕES --
_ceis(22K): "TIPO DE PESSOA"(VARCHAR'F'/'J'),"CPF OU CNPJ DO SANCIONADO","NOME DO SANCIONADO","CATEGORIA DA SANÇÃO"('Declaração de Inidoneidade'|'Impedimento/proibição de contratar'|'Suspensão'|'Multa'|'Demissão'),"DATA INÍCIO SANÇÃO"(DATE),"DATA FINAL SANÇÃO"(DATE),"ÓRGÃO SANCIONADOR","UF ÓRGÃO SANCIONADOR"
_cnep(2K): mesmo schema + "VALOR DA MULTA", CATEGORIA: 'Perdimento de bens'|'Multa'|'Dissolução compulsória da PJ'
_ceaf(4K): "TIPO DE PESSOA"(BOOLEAN),"CPF OU CNPJ DO SANCIONADO","NOME DO SANCIONADO","CATEGORIA DA SANÇÃO"('Perda de Emprego'|'Cassação de aposentadoria'|'Destituição'|'Demissão'),"DATA INÍCIO SANÇÃO"(DATE),"ÓRGÃO SANCIONADOR"
_cepim(4K): "CNPJ ENTIDADE","NOME ENTIDADE","NÚMERO CONVÊNIO","ÓRGÃO CONCEDENTE","MOTIVO DO IMPEDIMENTO"
_acordos(143): "CNPJ DO SANCIONADO","RAZÃO SOCIAL","SITUAÇÃO DO ACORDO DE LENIÊNICA"('Cumprido'|'Em Execução'),"DATA DE INÍCIO DO ACORDO"(DATE),"DATA DE FIM DO ACORDO"(DATE),"ÓRGÃO SANCIONADOR"
  ⚠️ "RAZÃO SOCIAL" existe APENAS em _acordos — NÃO existe no _ceis/_cnep/_ceaf (nesses use "NOME DO SANCIONADO")

-- LICITAÇÕES E COMPRAS --
_licitacoes(2M): "Número Licitação","Nome UG","Modalidade Compra","Objeto","Situação Licitação","Nome Órgão Superior","Nome Órgão","UF","Data Resultado Compra"(DATE),"Data Abertura"(DATE),"Valor Licitação"
_compras(4M): "Código Órgão"(BIGINT),"Nome Órgão","Código UG","Número Contrato","Descrição Item Compra","Quantidade Item"(BIGINT),"Valor Item"
  ⚠️ _compras NÃO tem "Número Licitação" — tem "Número Contrato". Não faça JOIN por licitação.
_convenios(612K): "NÚMERO CONVÊNIO","UF","NOME MUNICÍPIO","SITUAÇÃO CONVÊNIO"('EM EXECUÇÃO'|'RESCINDIDO'|'BAIXADO'|'CANCELADO'|'EXCLUÍDO'|'PRESTAÇÃO DE CONTAS ENVIADA PARA ANÁLISE'|'PRESTAÇÃO DE CONTAS EM COMPLEMENTAÇÃO'|'PRESTAÇÃO DE CONTAS REJEITADA'),"OBJETO DO CONVÊNIO","NOME ÓRGÃO SUPERIOR","NOME ÓRGÃO CONCEDENTE","CÓDIGO CONVENENTE","TIPO CONVENENTE"('Administração Pública'|'Administração Pública Estadual ou do Distrito Federal'|'Administração Pública Municipal'|'Entidades Sem Fins Lucrativos'|'Entidades Empresariais Privadas'|'Pessoa Física'|'Organizações Internacionais'),"NOME CONVENENTE","VALOR CONVÊNIO","VALOR LIBERADO","DATA INÍCIO VIGÊNCIA"(DATE),"DATA FINAL VIGÊNCIA"(DATE)

-- CARTÃO CORPORATIVO --
Schema base cartão: "CÓDIGO ÓRGÃO SUPERIOR"(BIGINT),"NOME ÓRGÃO SUPERIOR","CÓDIGO ÓRGÃO"(BIGINT),"NOME ÓRGÃO","ANO EXTRATO"(BIGINT),"MÊS EXTRATO"(VARCHAR),"NOME FAVORECIDO","TRANSAÇÃO","DATA TRANSAÇÃO"(DATE),"VALOR TRANSAÇÃO"
_cpgf(2M): +"CPF PORTADOR"(VARCHAR),"NOME PORTADOR","CNPJ OU CPF FAVORECIDO"(VARCHAR)
  ⚠️ coluna chama "CPF PORTADOR" não CPF — JOIN com viagens: ON v."CPF viajante" = c."CPF PORTADOR"
_cpcc(1M): +"TIPO AQUISIÇÃO","CNPJ OU CPF FAVORECIDO"(BIGINT)
_cpdc(129K): +CPF PORTADOR,NOME PORTADOR,"CNPJ OU CPF FAVORECIDO","NÚMERO CONVÊNIO"(BIGINT),"NOME CONVENENTE"

-- OUTROS --
_pep(71K): CPF,"Nome_PEP","Descrição_Função","Nome_Órgão","Data_Início_Exercício"(DATE),"Data_Fim_Exercício","Data_Fim_Carência"
_imoveisfuncionais(23K): "NOME PERMISSIONÁRIO",CPF,"ÓRGÃO EXERCÍCIO DO PERMISSIONÁRIO","DATA INÍCIO OCUPAÇÃO"(DATE) — SEM coluna UF
_orçamentodadespesa(305K): "EXERCÍCIO"(BIGINT),"NOME ÓRGÃO SUPERIOR","NOME FUNÇÃO","NOME PROGRAMA ORÇAMENTÁRIO","NOME AÇÃO","ORÇAMENTO INICIAL (R$)","ORÇAMENTO EMPENHADO (R$)","ORÇAMENTO REALIZADO (R$)"
_execuçãodareceita(2M): "CÓDIGO ÓRGÃO"(BIGINT),"NOME ÓRGÃO","CATEGORIA ECONÔMICA","ORIGEM RECEITA","VALOR PREVISTO ATUALIZADO","VALOR REALIZADO","DATA LANÇAMENTO"(DATE),"ANO EXERCÍCIO"(BIGINT)
_transferencias(9M): "ANO / MÊS"(BIGINT YYYYMM),"TIPO TRANSFERÊNCIA","UF","NOME MUNICÍPIO","NOME ÓRGÃO","CÓDIGO FAVORECIDO","NOME FAVORECIDO","VALOR TRANSFERIDO"
_emendasparlamentarespordocumento(4M): "Código da Emenda","Ano da Emenda"(BIGINT),"Nome do Autor da Emenda","Valor Empenhado","Valor Pago","Tipo de Emenda","UF de aplicação do recurso","Favorecido"
_renúnciasfiscais(752K): "Ano-calendário"(BIGINT),CNPJ,"Razão Social","Código CNAE",UF,"Tipo Renúncia","Benefício Fiscal","Tributo","Valor Renúncia Fiscal (R$)"
_apoiamentoemendasparlamentares(34K): "Código Apoiador"(BIGINT),"Apoiador","Nome do Autor da Emenda","Valor Empenhado","Valor Pago","Órgão Superior"
_notasfiscais(274K): "CHAVE DE ACESSO"(DOUBLE),"DATA EMISSÃO"(TIMESTAMP),"EVENTO","DESCRIÇÃO EVENTO"

== CRUZAMENTOS PRINCIPAIS — USE ESTES PADRÕES EXATOS ==

[CNPJ: análise de risco / due diligence completa]
-- Use este padrão EXATO — 3 colunas fixas, máximo 5 UNIONs:
SELECT 'CADASTRO' as secao, 'Situação' as campo, CAST(est.situacao_cadastral AS VARCHAR) as valor
FROM _empresas_sp WHERE est.cnpj_completo = '33000167000101'
UNION ALL
SELECT 'SANÇÃO CEIS', 'Categoria', "CATEGORIA DA SANÇÃO" FROM _ceis WHERE "CPF OU CNPJ DO SANCIONADO" = '33000167000101'
UNION ALL
SELECT 'SANÇÃO CNEP', 'Categoria', "CATEGORIA DA SANÇÃO" FROM _cnep WHERE "CPF OU CNPJ DO SANCIONADO" = '33000167000101'
UNION ALL
SELECT 'DESPESAS 2024', 'Total Recebido', CAST(SUM(CAST(REPLACE("Valor Recebido",',','.') AS DECIMAL)) AS VARCHAR)
FROM _despesas_favorecidos WHERE "Código Favorecido" = '33000167000101' AND "Ano e mês do lançamento" LIKE '%/2024'
UNION ALL
SELECT 'CONVÊNIOS', 'Situação', "SITUAÇÃO CONVÊNIO" FROM _convenios WHERE "CÓDIGO CONVENENTE" = '33000167' LIMIT 1
-- IMPORTANTE: buscar nas tabelas de empresas de TODOS os estados relevantes com UNION ALL antes do LIMIT
WITH sancionados AS (SELECT DISTINCT "CPF OU CNPJ DO SANCIONADO" as cnpj FROM _ceis WHERE "TIPO DE PESSOA"='J')
SELECT s.cnpj, e.razao_social, SUM(CAST(REPLACE(d."Valor Recebido",',','.') AS DECIMAL)) as total_recebido
FROM sancionados s
JOIN _despesas_favorecidos d ON d."Código Favorecido" = s.cnpj
JOIN _empresas_sp e ON e.est.cnpj_completo = s.cnpj  -- trocar UF conforme necessário
WHERE d."Ano e mês do lançamento" LIKE '%/2024'
GROUP BY s.cnpj, e.razao_social ORDER BY total_recebido DESC

[CPF: PEP aparece como favorecido em despesas]
SELECT p.CPF, p."Nome_PEP", p."Descrição_Função", d."Nome Favorecido", d."Nome Órgão Superior",
  SUM(CAST(REPLACE(d."Valor Recebido",',','.') AS DECIMAL)) as total_recebido
FROM _pep p
JOIN _despesas_favorecidos d ON d."Código Favorecido" = p.CPF
WHERE d."Ano e mês do lançamento" LIKE '%/2024'
GROUP BY p.CPF, p."Nome_PEP", p."Descrição_Função", d."Nome Favorecido", d."Nome Órgão Superior"

[CPF: servidor com jetom E viagem internacional]
SELECT j.CPF, j.NOME, j.EMPRESA, j.VALOR as valor_jetom,
  v."Destinos", v."Período - Data de início"
FROM _servidores_honorarios_jetons_ j
JOIN _viagens_viagem v ON v."CPF viajante" = j.CPF
WHERE v."Destinos" NOT LIKE '%Brasil%' OR v."Destinos" LIKE '%exterior%'
ORDER BY CAST(REPLACE(j.VALOR,',','.') AS DECIMAL) DESC LIMIT 100

[CNPJ: empresa baixada/inapta que ganhou licitação]
-- ⚠️ _licitacoes não tem CNPJ — cruzar por razao_social é impreciso. Melhor via _despesas_favorecidos:
WITH empresas_irregulares AS (
  SELECT est.cnpj_completo as cnpj, razao_social, est.situacao_cadastral, est.uf as estado
  FROM _empresas_sp WHERE est.situacao_cadastral IN ('BAIXADA','INAPTA','SUSPENSA')
  UNION ALL SELECT est.cnpj_completo, razao_social, est.situacao_cadastral, est.uf FROM _empresas_mg WHERE est.situacao_cadastral IN ('BAIXADA','INAPTA','SUSPENSA')
  -- repetir para outros estados relevantes
)
SELECT e.cnpj, e.razao_social, e.situacao_cadastral, e.estado,
  SUM(CAST(REPLACE(d."Valor Recebido",',','.') AS DECIMAL)) as total_recebido
FROM empresas_irregulares e
JOIN _despesas_favorecidos d ON d."Código Favorecido" = e.cnpj
WHERE d."Ano e mês do lançamento" LIKE '%/2024'
GROUP BY e.cnpj, e.razao_social, e.situacao_cadastral, e.estado ORDER BY total_recebido DESC LIMIT 100
-- ⚠️ NUNCA use GROUP BY est.uf dentro de CTE — o alias est não existe fora do SELECT. Use alias explícito (ex: est.uf as estado) e agrupe pelo alias

[CNPJ: empresa no CEIS + no CNEP (sanção dupla)]
SELECT c.\"CPF OU CNPJ DO SANCIONADO\" as cnpj, c.\"NOME DO SANCIONADO\",
  c.\"CATEGORIA DA SANÇÃO\" as sancao_ceis, n.\"CATEGORIA DA SANÇÃO\" as sancao_cnep,
  c.\"ÓRGÃO SANCIONADOR\"
FROM _ceis c JOIN _cnep n ON n.\"CPF OU CNPJ DO SANCIONADO\" = c.\"CPF OU CNPJ DO SANCIONADO\"

[CPF: servidor + remuneração por órgão]
SELECT c.NOME, c.ORGSUP_EXERCICIO, c.DESCRICAO_CARGO,
  r."REMUNERAÇÃO BÁSICA BRUTA (R$)", r."REMUNERAÇÃO APÓS DEDUÇÕES OBRIGATÓRIAS (R$)"
FROM _servidores_cadastro c
JOIN _servidores_remuneracao r ON r.Id_SERVIDOR_PORTAL = c.Id_SERVIDOR_PORTAL
WHERE r.ANO='2024' AND r.MES='12'
AND c.ORGSUP_EXERCICIO LIKE '%SAÚDE%'
ORDER BY CAST(REPLACE(r."REMUNERAÇÃO BÁSICA BRUTA (R$)",',','.') AS DECIMAL) DESC LIMIT 100
`;

/* ========================= SQL AUTO-FIX ========================= */
function applySqlAutoFix(sql) {
  let s = sql || "";
  // Afastamentos: colunas inventadas
  s = s.replace(/"Início do afastamento"/g, "DATA_INICIO_AFASTAMENTO");
  s = s.replace(/"Fim do afastamento"/g, "DATA_FIM_AFASTAMENTO");
  // Acordos leniência: coluna truncada
  s = s.replace(/"SITUAÇÃO DO ACORDO"(?! DE LENIÊNICA)/g, '"SITUAÇÃO DO ACORDO DE LENIÊNICA"');
  // CEIS/CNEP/CEAF: ordem errada do documento
  s = s.replace(/"CNPJ OU CPF DO SANCIONADO"/g, '"CPF OU CNPJ DO SANCIONADO"');
  // CEIS/CNEP/CEAF: coluna inexistente
  s = s.replace(/"TIPO SANÇÃO"/g, '"CATEGORIA DA SANÇÃO"');
  // Acordos: razão social sem sufixo
  s = s.replace(/"RAZÃO SOCIAL"(?! [–-])/g, '"RAZÃO SOCIAL – CADASTRO RECEITA"');
  // _pep e _despesas: underscore errado em nomes de colunas
  s = s.replace(/"Nome_Órgão Superior"/g, '"Nome Órgão Superior"');
  s = s.replace(/"Nome_Órgão"/g, '"Nome Órgão"');
  return s;
}

/* ========================= MAIN HANDLER ========================= */
app.post("/chat", async (req, res) => {
  const start = Date.now();
  const query = (req.body?.query || "").trim();

  if (!query) return res.json({ error: "Query vazia" });

  try {
    console.log(`\n${"=".repeat(60)}\n❓ "${query}"\n${"=".repeat(60)}`);
    console.log("🤖 Claude gerando SQL...");

    const sqlGen = await anthropic.messages.create({
      model: "claude-haiku-4-5-20251001",
      max_tokens: 3500,
      messages: [{
        role: "user",
        content: `Você é especialista em DuckDB e dados públicos brasileiros.

${DB_CATALOG}

PERGUNTA: "${query}"

Gere o SQL DuckDB para responder esta pergunta.
REGRA ABSOLUTA: Responda APENAS com SQL puro — zero palavras antes ou depois, zero explicações, zero markdown, zero blocos de código. A primeira palavra da resposta deve ser SELECT ou WITH.`
      }]
    });

    let sql = sqlGen.content.find(b => b.type === "text")?.text.trim() || "";
    sql = sql.replace(/```sql\n?/g, "").replace(/```/g, "").trim();
    sql = applySqlAutoFix(sql);
    console.log(`📝 SQL: ${sql.substring(0, 300)}`);

    // Se Claude retornou explicação em vez de SQL, devolve direto sem executar
    const sqlLower = sql.toLowerCase();
    if (!sqlLower.startsWith("select") && !sqlLower.startsWith("with")) {
      console.log("💬 Claude respondeu sem SQL (dado não disponível)");
      return res.json({ answer: sql, sql: "", duration_ms: Date.now() - start, rows_returned: 0 });
    }

    console.log("⚡ Executando...");
    const response = await fetch(`${HETZNER_API}/query_unified`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-API-Key": HETZNER_KEY },
      body: JSON.stringify({ sql }),
      signal: AbortSignal.timeout(240000)
    });

    const data = await response.json();
    if (!response.ok || data.error) throw new Error(data.error || "Query falhou");
    console.log(`📊 ${data.row_count || 0} linhas retornadas`);

    console.log("💬 Claude explicando...");
    const explanation = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 2000,
      messages: [{
        role: "user",
        content: `Pergunta: "${query}"

SQL executado:
${sql}

Resultados (${data.row_count} linhas):
${JSON.stringify(data.rows?.slice(0, 50), null, 2)}

Explique os resultados em português de forma clara e objetiva.
Formate valores monetários em R$. Cite a fonte dos dados.`
      }]
    });

    const answer = explanation.content.find(b => b.type === "text")?.text || "Sem resposta";
    console.log(`✅ CONCLUÍDO em ${Date.now() - start}ms`);

    return res.json({ answer, sql, duration_ms: Date.now() - start, rows_returned: data.row_count });

  } catch (err) {
    console.error("❌ ERRO:", err.message);
    return res.status(500).json({ error: err.message, duration_ms: Date.now() - start });
  }
});

app.get("/health", async (_, res) => {
  try {
    const r = await fetch(`${HETZNER_API}/health`, { headers: { "X-API-Key": HETZNER_KEY }, signal: AbortSignal.timeout(5000) });
    res.json({ ok: true, hetzner: r.ok });
  } catch {
    res.json({ ok: true, hetzner: false });
  }
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log("═".repeat(60));
  console.log("🚀 BDC — MOTHERDUCK NO HETZNER");
  console.log(`📡 Porta: ${PORT} | 🗄️ 5B linhas | 475 tabelas`);
  console.log("═".repeat(60));
});
