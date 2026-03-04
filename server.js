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
BANCO: brazildatacorp.duckdb | 5 bilhões de linhas | 475 tabelas | Motor: DuckDB

== REGRAS CRÍTICAS ==
1. TIPOS reais — respeite ao filtrar:
   - BIGINT: operadores numéricos (=, >, <, /) — NUNCA use LIKE ou SUBSTRING em BIGINT
   - VARCHAR: pode usar LIKE, SUBSTRING, =
   - DATE: comparação direta com strings '2024-01-01' ou funções YEAR(), MONTH()
   - DOUBLE: aritmética normal
   - STRUCT: acesse campos com ponto — est.situacao_cadastral, est.municipio, est.uf
2. DATAS YYYYMM (MÊS COMPETÊNCIA, MÊS REFERÊNCIA, MÊS DISPONIBILIZAÇÃO, ANO / MÊS) são BIGINT:
   - Ano: WHERE "MÊS COMPETÊNCIA" / 100 = 2024
   - Mês: WHERE "MÊS COMPETÊNCIA" = 202401
3. VALORES MONETÁRIOS são VARCHAR com vírgula: SUM(CAST(REPLACE("VALOR PARCELA", ',', '.') AS DECIMAL))
4. EMPRESAS RFB — coluna est é STRUCT:
   - est.situacao_cadastral: 'ATIVA', 'BAIXADA', 'INAPTA', 'SUSPENSA', 'NULA'
   - NUNCA use código como '02' — use texto 'ATIVA'
5. BOLSA FAMÍLIA: até 2021 → _bolsafamilia_pagamentos; 2022-2025 → _novobolsafamilia
6. SERVIDORES — ANO e MES são VARCHAR: WHERE ANO = '2024' AND MES = '01'
7. LIMIT 100 em queries de listagem; sem LIMIT em COUNT/SUM/agregações
8. Aspas duplas obrigatórias em colunas com espaços/acentos/parênteses

== TABELAS ==

_bolsafamilia_pagamentos (1.4B — até 2021):
  "MÊS COMPETÊNCIA"(BIGINT), "MÊS REFERÊNCIA"(BIGINT), "UF"(VARCHAR), "CÓDIGO MUNICÍPIO SIAFI"(VARCHAR), "NOME MUNICÍPIO"(VARCHAR), "CPF FAVORECIDO"(VARCHAR), "NIS FAVORECIDO"(BIGINT), "NOME FAVORECIDO"(VARCHAR), "VALOR PARCELA"(VARCHAR)

_bolsafamilia_saques (478M — até 2021):
  "MÊS COMPETÊNCIA"(BIGINT), "MÊS REFERÊNCIA"(BIGINT), "UF"(VARCHAR), "CÓDIGO MUNICÍPIO SIAFI"(VARCHAR), "NOME MUNICÍPIO"(VARCHAR), "CPF FAVORECIDO"(VARCHAR), "NIS FAVORECIDO"(BIGINT), "NOME FAVORECIDO"(VARCHAR), "DATA SAQUE"(DATE), "VALOR PARCELA"(VARCHAR)

_novobolsafamilia (668M — 2022 a 2025):
  "MÊS COMPETÊNCIA"(BIGINT), "MÊS REFERÊNCIA"(BIGINT), "UF"(VARCHAR), "CÓDIGO MUNICÍPIO SIAFI"(VARCHAR), "NOME MUNICÍPIO"(VARCHAR), "CPF FAVORECIDO"(VARCHAR), "NIS FAVORECIDO"(BIGINT), "NOME FAVORECIDO"(VARCHAR), "VALOR PARCELA"(VARCHAR)

_auxilioemergencial (782M):
  "MÊS DISPONIBILIZAÇÃO"(BIGINT), "UF"(VARCHAR), "CÓDIGO MUNICÍPIO IBGE"(BIGINT), "NOME MUNICÍPIO"(VARCHAR), "NIS BENEFICIÁRIO"(VARCHAR), "CPF BENEFICIÁRIO"(VARCHAR), "NOME BENEFICIÁRIO"(VARCHAR), "NIS RESPONSÁVEL"(BIGINT), "CPF RESPONSÁVEL"(VARCHAR), "NOME RESPONSÁVEL"(VARCHAR), "ENQUADRAMENTO"(VARCHAR), "PARCELA"(VARCHAR), "OBSERVAÇÃO"(VARCHAR), "VALOR BENEFÍCIO"(VARCHAR)

_auxiliobrasil (280M):
  "MÊS COMPETÊNCIA"(BIGINT), "MÊS REFERÊNCIA"(BIGINT), "UF"(VARCHAR), "CÓDIGO MUNICÍPIO SIAFI"(VARCHAR), "NOME MUNICÍPIO"(VARCHAR), "CPF FAVORECIDO"(VARCHAR), "NIS FAVORECIDO"(BIGINT), "NOME FAVORECIDO"(VARCHAR), "VALOR PARCELA"(VARCHAR)

_bpc (300M):
  "MÊS COMPETÊNCIA"(BIGINT), "MÊS REFERÊNCIA"(BIGINT), "UF"(VARCHAR), "CÓDIGO MUNICÍPIO SIAFI"(VARCHAR), "NOME MUNICÍPIO"(VARCHAR), "NIS BENEFICIÁRIO"(BIGINT), "CPF BENEFICIÁRIO"(VARCHAR), "NOME BENEFICIÁRIO"(VARCHAR), "NIS REPRESENTANTE LEGAL"(BIGINT), "CPF REPRESENTANTE LEGAL"(VARCHAR), "NOME REPRESENTANTE LEGAL"(VARCHAR), "NÚMERO BENEFÍCIO"(BIGINT), "BENEFÍCIO CONCEDIDO JUDICIALMENTE"(VARCHAR), "VALOR PARCELA"(VARCHAR)

_segurodefeso (40M):
  "MÊS REFERÊNCIA"(BIGINT), "UF"(VARCHAR), "CÓDIGO MUNICÍPIO SIAFI"(VARCHAR), "NOME MUNICÍPIO"(VARCHAR), "CPF FAVORECIDO"(VARCHAR), "NIS FAVORECIDO"(BIGINT), "RGP FAVORECIDO"(VARCHAR), "NOME FAVORECIDO"(VARCHAR), "VALOR PARCELA"(VARCHAR)

_garantiasafra (33M):
  "MÊS REFERÊNCIA"(BIGINT), "UF"(VARCHAR), "CÓDIGO MUNICÍPIO SIAFI"(VARCHAR), "NOME MUNICÍPIO"(VARCHAR), "NIS FAVORECIDO"(BIGINT), "NOME FAVORECIDO"(VARCHAR), "VALOR PARCELA"(VARCHAR)

_pedemeia (37M):
  "MÊS FOLHA"(BIGINT), "MÊS REFERÊNCIA"(BIGINT), "UF"(VARCHAR), "CÓDIGO MUNICÍPIO SIAFI"(VARCHAR), "NOME MUNICÍPIO"(VARCHAR), "NIS BENEFICIÁRIO"(BIGINT), "CPF BENEFICIÁRIO"(VARCHAR), "NOME BENEFICIÁRIO"(VARCHAR), "CÓDIGO ETAPA ENSINO"(BIGINT), "ETAPA ENSINO"(VARCHAR), "TIPO INCENTIVO"(VARCHAR), "DATA DO PAGAMENTO"(DATE), "VALOR PARCELA"(VARCHAR)

_peti (803K):
  "MÊS REFERÊNCIA"(BIGINT), "UF"(VARCHAR), "CÓDIGO SIAFI MUNICÍPIO"(VARCHAR), "NOME MUNICÍPIO"(VARCHAR), "NIS FAVORECIDO"(BIGINT), "NOME FAVORECIDO"(VARCHAR), "SITUAÇÃO BENEFÍCIO"(VARCHAR), "VALOR PARCELA"(VARCHAR)

_auxilioreconstrucao (425K):
  "MÊS REFERÊNCIA"(BIGINT), "UF"(VARCHAR), "CÓDIGO MUNICÍPIO SIAFI"(BIGINT), "NOME MUNICÍPIO"(VARCHAR), "CPF FAVORECIDO"(VARCHAR), "NIS FAVORECIDO"(BIGINT), "NOME FAVORECIDO"(VARCHAR), "QUANTIDADE DE PESSOAS NA FAMÍLIA"(BIGINT), "DATA EFETIVAÇÃO PARCELA"(DATE), "VALOR PARCELA"(VARCHAR)

-- EMPRESAS RECEITA FEDERAL (28 tabelas) --
_empresas_sp(20M), _empresas_mg(7M), _empresas_rj(6M), _empresas_rs(5M), _empresas_pr(5M),
_empresas_ba(3M), _empresas_sc(3M), _empresas_go(2M), _empresas_pe(2M), _empresas_ce(2M),
_empresas_df(1M), _empresas_es(1M), _empresas_mt(1M), _empresas_ma(1M), _empresas_pa(1M),
_empresas_ms(914K), _empresas_pb(881K), _empresas_rn(787K), _empresas_am(740K), _empresas_al(653K),
_empresas_pi(593K), _empresas_ro(476K), _empresas_to(460K), _empresas_se(457K), _empresas_ex(169K),
_empresas_ap(151K), _empresas_ac(158K), _empresas_rr(134K)
Todas com: cnpj_basico(VARCHAR 8 dígitos), razao_social(VARCHAR), porte(VARCHAR), capital_social(DOUBLE), est(STRUCT)
est campos: est.situacao_cadastral ('ATIVA','BAIXADA','INAPTA','SUSPENSA','NULA'), est.municipio, est.uf,
            est.cnpj_completo, est.cnae_principal, est.cnae_principal_codigo,
            est.cnaes_secundarios_codigos(VARCHAR[]), est.cnaes_secundarios_descricoes(VARCHAR[]),
            est.data_inicio_atividade, est.data_situacao_cadastral, est.nome_fantasia,
            est.matriz_filial, est.motivo_situacao, est.cep, est.bairro, est.logradouro,
            est.numero, est.telefone_1, est.correio_eletronico

-- SERVIDORES --
_servidores_cadastro (19M — civis ativos):
  Id_SERVIDOR_PORTAL(VARCHAR), NOME(VARCHAR), CPF(VARCHAR), MATRICULA(VARCHAR), DESCRICAO_CARGO(VARCHAR), CLASSE_CARGO(VARCHAR), NIVEL_CARGO(VARCHAR), FUNCAO(VARCHAR), COD_UORG_LOTACAO(VARCHAR), UORG_LOTACAO(VARCHAR), COD_ORG_LOTACAO(VARCHAR), ORG_LOTACAO(VARCHAR), COD_ORGSUP_LOTACAO(VARCHAR), ORGSUP_LOTACAO(VARCHAR), COD_ORG_EXERCICIO(VARCHAR), ORG_EXERCICIO(VARCHAR), COD_ORGSUP_EXERCICIO(VARCHAR), ORGSUP_EXERCICIO(VARCHAR), TIPO_VINCULO(VARCHAR), SITUACAO_VINCULO(VARCHAR), REGIME_JURIDICO(VARCHAR), JORNADA_DE_TRABALHO(VARCHAR), DATA_INGRESSO_ORGAO(VARCHAR), UF_EXERCICIO(VARCHAR)
_servidores_cadastro__2(593K), __3(793K), __6(1M) — mesmo schema
_servidores_cadastro__4 (73K — pensionistas): Id_SERVIDOR_PORTAL, NOME, CPF, CPF_REPRESENTANTE_LEGAL, NOME_REPRESENTANTE_LEGAL, CPF_INSTITUIDOR_PENSAO, NOME_INSTITUIDOR_PENSAO, TIPO_PENSAO, DATA_INICIO_PENSAO, ORG_LOTACAO_INSTITUIDOR_PENSAO
_servidores_cadastro__5 (12M — aposentados): Id_SERVIDOR_PORTAL, NOME, CPF, MATRICULA, TIPO_APOSENTADORIA, DATA_APOSENTADORIA, DESCRICAO_CARGO, UORG_LOTACAO, ORG_LOTACAO, ORGSUP_LOTACAO, TIPO_VINCULO, SITUACAO_VINCULO
_servidores_cadastro__7 (52M — militares): mesmo schema que _servidores_cadastro

_servidores_remuneracao (19M):
  ANO(VARCHAR), MES(VARCHAR), Id_SERVIDOR_PORTAL(VARCHAR), CPF(VARCHAR), NOME(VARCHAR), "REMUNERAÇÃO BÁSICA BRUTA (R$)"(VARCHAR), "REMUNERAÇÃO BÁSICA BRUTA (U$)"(VARCHAR), "ABATE-TETO (R$)"(VARCHAR), "GRATIFICAÇÃO NATALINA (R$)"(VARCHAR), "FÉRIAS (R$)"(VARCHAR), "OUTRAS REMUNERAÇÕES EVENTUAIS (R$)"(VARCHAR), "IRRF (R$)"(VARCHAR), "PSS/RPGS (R$)"(VARCHAR), "DEMAIS DEDUÇÕES (R$)"(VARCHAR), "PENSÃO MILITAR (R$)"(VARCHAR), "FUNDO DE SAÚDE (R$)"(VARCHAR), "REMUNERAÇÃO APÓS DEDUÇÕES OBRIGATÓRIAS (R$)"(VARCHAR), "TOTAL DE VERBAS INDENIZATÓRIAS (R$)(*)"(VARCHAR)
  ANO e MES são VARCHAR: WHERE ANO = '2024' AND MES = '01'
_servidores_remuneracao__2(30M), __3(52M), __4(237K), __5(9M) — mesmo schema

_servidores_afastamentos (84K): ANO(VARCHAR), MES(VARCHAR), Id_SERVIDOR_PORTAL(VARCHAR), CPF(VARCHAR), NOME(VARCHAR), DATA_INICIO_AFASTAMENTO(VARCHAR), DATA_FIM_AFASTAMENTO(VARCHAR)
_servidores_afastamentos__2 (8M): mesmo schema

_servidores_honorarios_jetons_ (45K): ANO(VARCHAR), MES(VARCHAR), Id_SERVIDOR_PORTAL(VARCHAR), CPF(VARCHAR), NOME(VARCHAR), EMPRESA(VARCHAR), VALOR(VARCHAR)
_servidores_honorariosadvocaticios (1M): ANO(VARCHAR), MES(VARCHAR), Id_SERVIDOR_PORTAL(VARCHAR), CPF(VARCHAR), NOME(VARCHAR), OBSERVACOES(VARCHAR), VALOR(VARCHAR)
_servidores_observacoes(463K), __2(40K), __3(8M), __4(3K), __5(1M), __6(17K), __7(918K):
  ANO(VARCHAR), MES(VARCHAR), Id_SERVIDOR_PORTAL(VARCHAR), NOME(VARCHAR), CPF(VARCHAR), OBSERVACAO(VARCHAR)

-- DESPESAS --
_despesasdiarias_despesas_empenho (31M):
  "Id Empenho"(BIGINT), "Código Empenho"(VARCHAR), "Data Emissão"(DATE), "Tipo Empenho"(VARCHAR), "Código Órgão Superior"(BIGINT), "Órgão Superior"(VARCHAR), "Código Órgão"(BIGINT), "Órgão"(VARCHAR), "Código Unidade Gestora"(BIGINT), "Unidade Gestora"(VARCHAR), "Código Função"(VARCHAR), "Função"(VARCHAR), "Código Favorecido"(VARCHAR), "Favorecido"(VARCHAR), "Código Programa"(VARCHAR), "Programa"(VARCHAR), "Código Ação"(VARCHAR), "Ação"(VARCHAR), "Código Categoria de Despesa"(BIGINT), "Categoria de Despesa"(VARCHAR), "Código Grupo de Despesa"(BIGINT), "Grupo de Despesa"(VARCHAR), "Valor Original do Empenho"(VARCHAR), "Valor do Empenho Convertido pra R$"(VARCHAR)

_despesasdiarias_despesas_pagamento (103M):
  "Código Pagamento"(VARCHAR), "Data Emissão"(VARCHAR), "Tipo OB"(VARCHAR), "Código Órgão Superior"(VARCHAR), "Órgão Superior"(VARCHAR), "Código Órgão"(VARCHAR), "Órgão"(VARCHAR), "Código Unidade Gestora"(VARCHAR), "Código Favorecido"(VARCHAR), "Favorecido"(VARCHAR), "Valor Original do Pagamento"(VARCHAR), "Valor do Pagamento Convertido pra R$"(VARCHAR)

_despesasdiarias_despesas_pagamento_favorecidosfinais (131M):
  "Código Pagamento"(VARCHAR), "Código Lista"(VARCHAR), "Data Emissão"(VARCHAR), "Código Favorecido"(VARCHAR), "Favorecido"(VARCHAR), "Valor do Pagamento em R$"(VARCHAR)

_despesasdiarias_despesas_liquidacao_empenhosimpactados (77M):
  "Código Liquidação"(VARCHAR), "Código Empenho"(VARCHAR), "Código Natureza Despesa Completa"(VARCHAR), "Valor Liquidado (R$)"(VARCHAR), "Valor Restos a Pagar Pagos (R$)"(VARCHAR)

_despesasdiarias_despesas_pagamento_empenhosimpactados (103M):
  "Código Pagamento"(VARCHAR), "Código Empenho"(VARCHAR), "Valor Pago (R$)"(VARCHAR)

_despesasdiarias_despesas_itemempenho (33M):
  "Id Empenho"(BIGINT), "Código Empenho"(VARCHAR), "Descrição"(VARCHAR), "Quantidade"(VARCHAR), "Valor Unitário"(VARCHAR), "Valor Total"(VARCHAR)

_despesas_favorecidos (114M):
  "Código Favorecido"(VARCHAR), "Nome Favorecido"(VARCHAR), "Sigla UF"(VARCHAR), "Nome Município"(VARCHAR), "Código Órgão Superior"(BIGINT), "Nome Órgão Superior"(VARCHAR), "Código Órgão"(BIGINT), "Nome Órgão"(VARCHAR), "Código Unidade Gestora"(BIGINT), "Nome Unidade Gestora"(VARCHAR), "Ano e mês do lançamento"(VARCHAR), "Valor Recebido"(VARCHAR)

-- VIAGENS --
_viagens_viagem (9M):
  "Identificador do processo de viagem"(VARCHAR), "Número da Proposta (PCDP)"(VARCHAR), "Situação"(VARCHAR), "Viagem Urgente"(VARCHAR), "Código do órgão superior"(VARCHAR), "Nome do órgão superior"(VARCHAR), "Código órgão solicitante"(VARCHAR), "Nome órgão solicitante"(VARCHAR), "CPF viajante"(VARCHAR), "Nome"(VARCHAR), "Cargo"(VARCHAR), "Função"(VARCHAR), "Período - Data de início"(VARCHAR), "Período - Data de fim"(VARCHAR), "Destinos"(VARCHAR), "Motivo"(VARCHAR), "Valor diárias"(VARCHAR), "Valor passagens"(VARCHAR), "Valor devolução"(VARCHAR), "Valor outros gastos"(VARCHAR)

_viagens_pagamento (16M):
  "Identificador do processo de viagem"(VARCHAR), "Código do órgão superior"(VARCHAR), "Nome do órgão superior"(VARCHAR), "Codigo do órgão pagador"(VARCHAR), "Nome do órgao pagador"(VARCHAR), "Tipo de pagamento"(VARCHAR), "Valor"(VARCHAR)

_viagens_passagem (5M):
  "Identificador do processo de viagem"(VARCHAR), "Número da Proposta (PCDP)"(VARCHAR), "Meio de transporte"(VARCHAR), "País - Origem ida"(VARCHAR), "UF - Origem ida"(VARCHAR), "Cidade - Origem ida"(VARCHAR), "País - Destino ida"(VARCHAR), "UF - Destino ida"(VARCHAR), "Cidade - Destino ida"(VARCHAR), "País - Origem volta"(VARCHAR), "UF - Origem volta"(VARCHAR), "Cidade - Origem volta"(VARCHAR), "Pais - Destino volta"(VARCHAR), "UF - Destino volta"(VARCHAR), "Cidade - Destino volta"(VARCHAR), "Valor da passagem"(VARCHAR), "Taxa de serviço"(VARCHAR), "Data da emissão/compra"(VARCHAR), "Hora da emissão/compra"(VARCHAR)

_viagens_trecho (20M):
  "Identificador do processo de viagem"(VARCHAR), "Número da Proposta (PCDP)"(VARCHAR), "Sequência Trecho"(VARCHAR), "Origem - Data"(VARCHAR), "Origem - País"(VARCHAR), "Origem - UF"(VARCHAR), "Origem - Cidade"(VARCHAR), "Destino - Data"(VARCHAR), "Destino - País"(VARCHAR), "Destino - UF"(VARCHAR), "Destino - Cidade"(VARCHAR), "Meio de transporte"(VARCHAR), "Número Diárias"(VARCHAR), "Missao?"(VARCHAR)

-- SANÇÕES --
_ceis (22K): "CPF OU CNPJ DO SANCIONADO"(VARCHAR), "NOME DO SANCIONADO"(VARCHAR), "RAZÃO SOCIAL - CADASTRO RECEITA"(VARCHAR), "CATEGORIA DA SANÇÃO"(VARCHAR), "DATA INÍCIO SANÇÃO"(DATE), "DATA FINAL SANÇÃO"(DATE), "ÓRGÃO SANCIONADOR"(VARCHAR), "UF ÓRGÃO SANCIONADOR"(VARCHAR)
_cnep (2K): mesmo schema + "VALOR DA MULTA"(VARCHAR)
_cepim (4K): "CNPJ ENTIDADE"(VARCHAR), "NOME ENTIDADE"(VARCHAR), "NÚMERO CONVÊNIO"(VARCHAR), "ÓRGÃO CONCEDENTE"(VARCHAR), "MOTIVO DO IMPEDIMENTO"(VARCHAR)
_ceaf (4K): "CADASTRO"(VARCHAR), "CÓDIGO DA SANÇÃO"(BIGINT), "TIPO DE PESSOA"(BOOLEAN), "CPF OU CNPJ DO SANCIONADO"(VARCHAR), "NOME DO SANCIONADO"(VARCHAR), "CATEGORIA DA SANÇÃO"(VARCHAR), "NÚMERO DO DOCUMENTO"(VARCHAR), "NÚMERO DO PROCESSO"(VARCHAR), "DATA INÍCIO SANÇÃO"(DATE), "DATA FINAL SANÇÃO"(DATE), "DATA PUBLICAÇÃO"(DATE), "DATA DO TRÂNSITO EM JULGADO"(DATE), "ABRAGÊNCIA DA SANÇÃO"(VARCHAR), "CARGO EFETIVO"(VARCHAR), "FUNÇÃO OU CARGO DE CONFIANÇA"(VARCHAR), "ÓRGÃO DE LOTAÇÃO"(VARCHAR), "ÓRGÃO SANCIONADOR"(VARCHAR), "UF ÓRGÃO SANCIONADOR"(VARCHAR), "FUNDAMENTAÇÃO LEGAL"(VARCHAR)
_acordos (143): "ID DO ACORDO"(BIGINT), "CNPJ DO SANCIONADO"(VARCHAR), "RAZÃO SOCIAL – CADASTRO RECEITA"(VARCHAR), "SITUAÇÃO DO ACORDO DE LENIÊNICA"(VARCHAR), "DATA DE INÍCIO DO ACORDO"(DATE), "DATA DE FIM DO ACORDO"(DATE), "ÓRGÃO SANCIONADOR"(VARCHAR)

-- LICITAÇÕES E COMPRAS --
_licitacoes (2M): "Número Licitação"(VARCHAR), "Código UG"(VARCHAR), "Nome UG"(VARCHAR), "Código Modalidade Compra"(BIGINT), "Modalidade Compra"(VARCHAR), "Número Processo"(VARCHAR), "Objeto"(VARCHAR), "Situação Licitação"(VARCHAR), "Nome Órgão Superior"(VARCHAR), "Nome Órgão"(VARCHAR), "UF"(VARCHAR), "Município"(VARCHAR), "Data Resultado Compra"(DATE), "Data Abertura"(DATE), "Valor Licitação"(VARCHAR)
_compras (4M): "Código Órgão"(BIGINT), "Nome Órgão"(VARCHAR), "Código UG"(VARCHAR), "Nome UG"(VARCHAR), "Número Contrato"(VARCHAR), "Descrição Item Compra"(VARCHAR), "Quantidade Item"(BIGINT), "Valor Item"(VARCHAR)
_convenios (612K): "NÚMERO CONVÊNIO"(VARCHAR), "UF"(VARCHAR), "CÓDIGO SIAFI MUNICÍPIO"(VARCHAR), "NOME MUNICÍPIO"(VARCHAR), "SITUAÇÃO CONVÊNIO"(VARCHAR), "NÚMERO ORIGINAL"(VARCHAR), "NÚMERO PROCESSO DO CONVÊNIO"(VARCHAR), "OBJETO DO CONVÊNIO"(VARCHAR), "CÓDIGO ÓRGÃO SUPERIOR"(BIGINT), "NOME ÓRGÃO SUPERIOR"(VARCHAR), "CÓDIGO ÓRGÃO CONCEDENTE"(BIGINT), "NOME ÓRGÃO CONCEDENTE"(VARCHAR), "CÓDIGO UG CONCEDENTE"(BIGINT), "NOME UG CONCEDENTE"(VARCHAR), "CÓDIGO CONVENENTE"(VARCHAR), "TIPO CONVENENTE"(VARCHAR), "NOME CONVENENTE"(VARCHAR), "TIPO ENTE CONVENENTE"(VARCHAR), "TIPO INSTRUMENTO"(VARCHAR), "VALOR CONVÊNIO"(VARCHAR), "VALOR LIBERADO"(VARCHAR), "DATA PUBLICAÇÃO"(DATE), "DATA INÍCIO VIGÊNCIA"(DATE), "DATA FINAL VIGÊNCIA"(DATE), "VALOR CONTRAPARTIDA"(VARCHAR), "DATA ÚLTIMA LIBERAÇÃO"(DATE), "VALOR ÚLTIMA LIBERAÇÃO"(VARCHAR)

_notasfiscais (274K): "CHAVE DE ACESSO"(DOUBLE), "MODELO"(VARCHAR), "SÉRIE"(BIGINT), "NÚMERO"(BIGINT), "NATUREZA DA OPERAÇÃO"(VARCHAR), "DATA EMISSÃO"(TIMESTAMP), "EVENTO"(VARCHAR), "DATA/HORA EVENTO"(TIMESTAMP), "DESCRIÇÃO EVENTO"(VARCHAR), "MOTIVO EVENTO"(VARCHAR)
_favorecidospj (81): "COD_NATJURIDICA"(BIGINT), "DESC_NATJURIDICA"(VARCHAR), "COD_TIPO_NATJURIDICA"(BIGINT), "DESC_TIPO_NATJURIDICA"(VARCHAR)

-- CARTÃO CORPORATIVO --
_cpgf (2M): "CÓDIGO ÓRGÃO SUPERIOR"(BIGINT), "NOME ÓRGÃO SUPERIOR"(VARCHAR), "CÓDIGO ÓRGÃO"(BIGINT), "NOME ÓRGÃO"(VARCHAR), "ANO EXTRATO"(BIGINT), "MÊS EXTRATO"(VARCHAR), "CPF PORTADOR"(VARCHAR), "NOME PORTADOR"(VARCHAR), "CNPJ OU CPF FAVORECIDO"(VARCHAR), "NOME FAVORECIDO"(VARCHAR), "TRANSAÇÃO"(VARCHAR), "DATA TRANSAÇÃO"(DATE), "VALOR TRANSAÇÃO"(VARCHAR)
_cpcc (1M): "CÓDIGO ÓRGÃO SUPERIOR"(BIGINT), "NOME ÓRGÃO SUPERIOR"(VARCHAR), "CÓDIGO ÓRGÃO"(BIGINT), "NOME ÓRGÃO"(VARCHAR), "CÓDIGO UNIDADE GESTORA"(BIGINT), "ANO EXTRATO"(BIGINT), "MÊS EXTRATO"(VARCHAR), "TIPO AQUISIÇÃO"(VARCHAR), "CNPJ OU CPF FAVORECIDO"(BIGINT), "NOME FAVORECIDO"(VARCHAR), "TRANSAÇÃO"(VARCHAR), "DATA TRANSAÇÃO"(DATE), "VALOR TRANSAÇÃO"(VARCHAR)
_cpdc (129K): "CÓDIGO ÓRGÃO SUPERIOR"(BIGINT), "NOME ÓRGÃO SUPERIOR"(VARCHAR), "CÓDIGO ÓRGÃO"(BIGINT), "NOME ÓRGÃO"(VARCHAR), "CÓDIGO UNIDADE GESTORA"(BIGINT), "ANO EXTRATO"(BIGINT), "MÊS EXTRATO"(VARCHAR), "CPF PORTADOR"(VARCHAR), "NOME PORTADOR"(VARCHAR), "CNPJ OU CPF FAVORECIDO"(VARCHAR), "NOME FAVORECIDO"(VARCHAR), "EXECUTOR DESPESA"(VARCHAR), "NÚMERO CONVÊNIO"(BIGINT), "CÓDIGO CONVENENTE"(VARCHAR), "NOME CONVENENTE"(VARCHAR), "REPASSE"(VARCHAR), "TRANSAÇÃO"(VARCHAR), "DATA TRANSAÇÃO"(DATE), "VALOR TRANSAÇÃO"(VARCHAR)

-- OUTROS --
_pep (71K): "CPF"(VARCHAR), "Nome_PEP"(VARCHAR), "Sigla_Função"(VARCHAR), "Descrição_Função"(VARCHAR), "Nível_Função"(VARCHAR), "Nome_Órgão"(VARCHAR), "Data_Início_Exercício"(DATE), "Data_Fim_Exercício"(VARCHAR), "Data_Fim_Carência"(VARCHAR)
_renúnciasfiscais (752K): "Ano-calendário"(BIGINT), "CNPJ"(VARCHAR), "Razão Social"(VARCHAR), "Nome Fantasia"(VARCHAR), "Código CNAE"(VARCHAR), "CNAE"(VARCHAR), "Município"(VARCHAR), "UF"(VARCHAR), "Tipo Renúncia"(VARCHAR), "Benefício Fiscal"(VARCHAR), "Fundamento Legal"(VARCHAR), "Descrição"(VARCHAR), "Tributo"(VARCHAR), "Forma Tributação"(VARCHAR), "Valor Renúncia Fiscal (R$)"(VARCHAR)
_emendas (70K): "Código da Emenda"(BIGINT), "Nome Função"(VARCHAR), "Localidade do gasto"(VARCHAR), "Tipo de Emenda"(VARCHAR), "Convenente"(VARCHAR), "Valor Convênio"(VARCHAR)
_emendasparlamentarespordocumento (4M): "Código da Emenda"(VARCHAR), "Ano da Emenda"(BIGINT), "Nome do Autor da Emenda"(VARCHAR), "Número da emenda"(VARCHAR), "Valor Empenhado"(VARCHAR), "Valor Pago"(VARCHAR), "Tipo de Emenda"(VARCHAR), "UF de aplicação do recurso"(VARCHAR), "Favorecido"(VARCHAR), "Fase da despesa"(VARCHAR)
_transferencias (9M): "ANO / MÊS"(BIGINT YYYYMM), "TIPO TRANSFERÊNCIA"(VARCHAR), "TIPO FAVORECIDO"(VARCHAR), "UF"(VARCHAR), "CÓDIGO MUNICÍPIO SIAFI"(VARCHAR), "NOME MUNICÍPIO"(VARCHAR), "NOME ÓRGÃO"(VARCHAR), "CÓDIGO FAVORECIDO"(VARCHAR), "NOME FAVORECIDO"(VARCHAR), "VALOR TRANSFERIDO"(VARCHAR)
_imoveisfuncionais (23K): "NOME PERMISSIONÁRIO"(VARCHAR), "CPF"(VARCHAR), "CARGO OU FUNÇÃO DE CONFIANÇA"(VARCHAR), "ÓRGÃO EXERCÍCIO DO PERMISSIONÁRIO"(VARCHAR), "DATA INÍCIO OCUPAÇÃO"(DATE)
_execuçãodareceita (2M): "CÓDIGO ÓRGÃO SUPERIOR"(BIGINT), "NOME ÓRGÃO SUPERIOR"(VARCHAR), "CÓDIGO ÓRGÃO"(BIGINT), "NOME ÓRGÃO"(VARCHAR), "CÓDIGO UNIDADE GESTORA"(BIGINT), "NOME UNIDADE GESTORA"(VARCHAR), "CATEGORIA ECONÔMICA"(VARCHAR), "ORIGEM RECEITA"(VARCHAR), "ESPÉCIE RECEITA"(VARCHAR), "DETALHAMENTO"(VARCHAR), "VALOR PREVISTO ATUALIZADO"(VARCHAR), "VALOR LANÇADO"(VARCHAR), "VALOR REALIZADO"(VARCHAR), "PERCENTUAL REALIZADO"(VARCHAR), "DATA LANÇAMENTO"(DATE), "ANO EXERCÍCIO"(BIGINT)
_orçamentodadespesa (305K): "EXERCÍCIO"(BIGINT), "NOME ÓRGÃO SUPERIOR"(VARCHAR), "NOME ÓRGÃO SUBORDINADO"(VARCHAR), "NOME FUNÇÃO"(VARCHAR), "NOME PROGRAMA ORÇAMENTÁRIO"(VARCHAR), "NOME AÇÃO"(VARCHAR), "ORÇAMENTO INICIAL (R$)"(VARCHAR), "ORÇAMENTO ATUALIZADO (R$)"(VARCHAR), "ORÇAMENTO EMPENHADO (R$)"(VARCHAR), "ORÇAMENTO REALIZADO (R$)"(VARCHAR)
_apoiamentoemendasparlamentares (34K): "Código Apoiador"(BIGINT), "Apoiador"(VARCHAR), "Data do Apoio"(TIMESTAMP), "Empenho"(VARCHAR), "Código da Emenda"(BIGINT), "Nome do Autor da Emenda"(VARCHAR), "Valor Empenhado"(VARCHAR), "Valor Pago"(VARCHAR), "Órgão Superior"(VARCHAR)
`;

/* ========================= MAIN HANDLER ========================= */
app.post("/chat", async (req, res) => {
  const start = Date.now();
  const query = (req.body?.query || "").trim();

  if (!query) return res.json({ error: "Query vazia" });

  try {
    console.log(`\n${"=".repeat(60)}\n❓ "${query}"\n${"=".repeat(60)}`);
    console.log("🤖 Claude gerando SQL...");

    const sqlGen = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 1500,
      messages: [{
        role: "user",
        content: `Você é especialista em DuckDB e dados públicos brasileiros.

${DB_CATALOG}

PERGUNTA: "${query}"

Gere o SQL DuckDB para responder esta pergunta.
Responda APENAS com SQL puro — sem explicações, sem markdown, sem blocos de código.`
      }]
    });

    let sql = sqlGen.content.find(b => b.type === "text")?.text.trim() || "";
    sql = sql.replace(/```sql\n?/g, "").replace(/```/g, "").trim();
    console.log(`📝 SQL: ${sql.substring(0, 300)}`);

    console.log("⚡ Executando...");
    const response = await fetch(`${HETZNER_API}/query_unified`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-API-Key": HETZNER_KEY },
      body: JSON.stringify({ sql }),
      signal: AbortSignal.timeout(120000)
    });

    const data = await response.json();
    if (!response.ok || data.error) throw new Error(data.error || "Query falhou");
    console.log(`📊 ${data.row_count || 0} linhas retornadas`);

    console.log("💬 Claude explicando...");
    const explanation = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 1500,
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
