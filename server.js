import express from "express";
import cors from "cors";
import Anthropic from "@anthropic-ai/sdk";

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

const HETZNER_API = process.env.HETZNER_API_BASE || "http://89.167.48.3:5010";
const HETZNER_KEY = process.env.HETZNER_API_KEY || "bdc-sql-api-key-2026-segura";
const anthropic   = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
const TAVILY_KEY  = process.env.TAVILY_API_KEY   || "";
const S2_KEY      = process.env.S2_API_KEY        || "luDwHjoEjo9o0YcfcNi4J6f88oXQ9Um7VQkWCncj";

/* ─── TAVILY WEB SEARCH ─── */
async function tavilySearch(query, maxResults = 5) {
  if (!TAVILY_KEY) return null;
  try {
    const res = await fetch("https://api.tavily.com/search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        api_key: TAVILY_KEY,
        query,
        max_results: maxResults,
        search_depth: "advanced",
        include_answer: true,
        include_domains: [
          "portaldatransparencia.gov.br", "cgu.gov.br", "rfb.gov.br",
          "gov.br", "ibge.gov.br", "bcb.gov.br", "tcu.gov.br",
          "g1.globo.com", "uol.com.br", "valor.com.br", "agenciabrasil.ebc.com.br"
        ]
      }),
      signal: AbortSignal.timeout(10000)
    });
    if (!res.ok) return null;
    const d = await res.json();
    return {
      answer: d.answer || null,
      results: (d.results || []).map(r => ({
        title: r.title, url: r.url,
        content: r.content?.slice(0, 500),
        published_date: r.published_date || null
      }))
    };
  } catch (e) {
    console.warn("⚠️ Tavily erro:", e.message);
    return null;
  }
}

/* ─── SEMANTIC SCHOLAR ─── */
async function s2Search(query, limit = 5) {
  try {
    const params = new URLSearchParams({
      query,
      limit,
      fields: "title,authors,year,abstract,externalIds,openAccessPdf,citationCount"
    });
    const res = await fetch(`https://api.semanticscholar.org/graph/v1/paper/search?${params}`, {
      headers: { "x-api-key": S2_KEY },
      signal: AbortSignal.timeout(8000)
    });
    if (!res.ok) return null;
    const d = await res.json();
    return (d.data || []).slice(0, limit).map(p => ({
      title: p.title,
      authors: (p.authors || []).slice(0, 3).map(a => a.name).join(", "),
      year: p.year,
      abstract: p.abstract?.slice(0, 400),
      url: p.openAccessPdf?.url || `https://www.semanticscholar.org/paper/${p.paperId}`,
      citations: p.citationCount || 0
    }));
  } catch (e) {
    console.warn("⚠️ S2 erro:", e.message);
    return null;
  }
}

/* ─── DETECTA SE QUERY PRECISA DE CONTEXTO EXTERNO ─── */
function needsExternalContext(query, rowCount, sql) {
  const q = query.toLowerCase();
  // Precisa de web search se: sem resultados OU pergunta sobre contexto/notícia/análise
  const noData = rowCount === 0;
  const contextKeywords = ["escândalo", "investigação", "cpi", "operação", "notícia", "recente",
    "contexto", "histórico", "por que", "análise", "impacto", "consequência",
    "processo", "denúncia", "acusação", "preso", "condenado"];
  const hasContextKw = contextKeywords.some(k => q.includes(k));
  // Precisa de S2 se: pergunta acadêmica ou de setor
  const s2Keywords = ["estudo", "pesquisa", "artigo", "literatura", "acadêmico",
    "setor", "indústria", "correlação", "evidência", "análise setorial"];
  const hasS2Kw = s2Keywords.some(k => q.includes(k));
  return { needsWeb: noData || hasContextKw, needsS2: hasS2Kw };
}

const DB_CATALOG = `
BANCO: brazildatacorp.duckdb | 5B linhas | DuckDB

== REGRAS SQL ==
- BIGINT: só operadores numéricos. VARCHAR: LIKE/=. STRUCT: ponto (est.uf). Aspas duplas em colunas com espaços/acentos.
- EMPRESAS em CTE: SEMPRE extraia campos STRUCT com alias — SELECT est.uf as uf, est.situacao_cadastral as situacao, est.data_inicio_atividade as data_inicio, est.cnae_principal as cnae, est.cnae_principal_codigo as cnae_cod, est.cnaes_secundarios_codigos as cnaes_sec — e agrupe pelo alias. NUNCA use est.* fora do SELECT onde o STRUCT foi acessado.
- DATAS YYYYMM são BIGINT: WHERE "MÊS COMPETÊNCIA" >= 202401 AND "MÊS COMPETÊNCIA" <= 202412. NUNCA divida por 100.
- VALORES monetários são VARCHAR: SUM(CAST(REPLACE(REPLACE(coluna,'.',''),',','.') AS DECIMAL)) — isso vale para "Valor Licitação", "VALOR TRANSFERIDO", "VALOR LIBERADO", "VALOR CONVÊNIO" e qualquer coluna monetária — NUNCA use DECIMAL(18,3) direto
- LIMIT 100 em listagens; sem LIMIT em COUNT/SUM
- CTEs: não aplique CAST/REPLACE em colunas já computadas como DECIMAL
- UNION/UNION ALL: ORDER BY só no final, NUNCA dentro de subquery. Use alias numérico (ORDER BY 1,2) — NUNCA expressão como CAST(MES AS INTEGER)
- UNION com múltiplas tabelas: todas as subqueries devem ter EXATAMENTE o mesmo número de colunas
- DUE DILIGENCE / ANÁLISE DE CNPJ: máximo 4 tabelas por UNION, 3 colunas fixas: fonte, campo, valor
- BOLSA FAMÍLIA: até 2021→_bolsafamilia_pagamentos; 2022-2025→_novobolsafamilia
- SERVIDORES: ANO e MES são VARCHAR: WHERE ANO='2024' AND MES='01'
- AFASTAMENTOS: DATA_INICIO_AFASTAMENTO e DATA_FIM_AFASTAMENTO são VARCHAR DD/MM/YYYY ou 'Não informada'. Use TRY_STRPTIME(col, '%d/%m/%Y') — NUNCA CAST direto. NÃO existe "Início do afastamento" nem "Fim do afastamento"
- CEIS/CNEP/CEAF: coluna do documento é "CPF OU CNPJ DO SANCIONADO". NÃO existe "CNPJ OU CPF DO SANCIONADO". NÃO existe "TIPO SANÇÃO" — use "CATEGORIA DA SANÇÃO"
- CEIS/CNEP/CEAF: coluna de nome é "NOME DO SANCIONADO" — NÃO existe "RAZÃO SOCIAL" nessas tabelas
- ACORDOS: status é "SITUAÇÃO DO ACORDO DE LENIÊNICA" — NÃO existe "SITUAÇÃO DO ACORDO". Nome é "RAZÃO SOCIAL – CADASTRO RECEITA"
- VALORES monetários com ponto de milhar + vírgula decimal (ex: "10.313.296,80"): use CAST(REPLACE(REPLACE(col, '.', ''), ',', '.') AS DECIMAL) — padrão EXATO com DOIS REPLACE aninhados. Isso vale para "Valor diárias","Valor passagens","Valor Licitação","VALOR TRANSFERIDO","VALOR LIBERADO","VALOR CONVÊNIO","Valor Renúncia Fiscal (R$)","ORÇAMENTO REALIZADO (R$)","ORÇAMENTO ATUALIZADO (R$)" e qualquer coluna monetária fora da folha de servidores
- DATAS VARCHAR em viagens: "Período - Data de início" e "Período - Data de fim" são VARCHAR 'YYYY-MM-DD'. Para ano: SUBSTRING("Período - Data de início",1,4). NUNCA use EXTRACT/date_part em VARCHAR
- DATAS DATE (não VARCHAR): para extrair ano de coluna DATE use CAST(EXTRACT(YEAR FROM col) AS VARCHAR) ou SUBSTRING(CAST(col AS VARCHAR),1,4) — NUNCA SUBSTRING direto em DATE
- WINDOW FUNCTIONS: NUNCA use funções de janela (OVER()) em WHERE. Use QUALIFY ou subconsulta
- SERVIDORES pensionistas (_cadastro__4): coluna de órgão chama ORGSUP_LOTACAO_INSTITUIDOR_PENSAO — NÃO existe ORGSUP_LOTACAO nessa tabela
- DESPESAS empenho (_despesasdiarias_despesas_empenho): coluna órgão é "Órgão Superior" e "NOME ÓRGÃO SUPERIOR" — NÃO é "Nome Órgão Superior"
- CEPIM: JOIN com outras tabelas via CNPJ é impreciso pois "CNPJ ENTIDADE" no CEPIM é apenas o CNPJ base (8 dígitos) sem filial. Para cruzar com _convenios use "CÓDIGO CONVENENTE" LIKE c."CNPJ ENTIDADE" || '%'
- CEPIM: coluna é "CNPJ ENTIDADE" (VARCHAR) — JOIN com convenios via "CÓDIGO CONVENENTE" ou razao_social aproximado, NÃO por CNPJ direto pois formatos diferem
- CNAES em array: cnaes_secundarios_codigos é VARCHAR[] — para filtrar use array_contains(est.cnaes_secundarios_codigos, '6201') NUNCA use LIKE em array
- NÃO existem tabelas empresas_baixadas, empresas_inaptas, empresas_ativas — use _empresas_UF com filtro em est.situacao_cadastral
- SERVIDORES pensionistas (_cadastro__4): colunas específicas CPF_REPRESENTANTE_LEGAL, CPF_INSTITUIDOR_PENSAO, TIPO_PENSAO, DATA_INICIO_PENSAO — NÃO tem ORGSUP_LOTACAO nem ORGSUP_EXERCICIO
- DESPESAS: _despesas_favorecidos → "Nome Órgão Superior". _despesasdiarias_despesas_empenho → "Órgão Superior" (sem "Nome"). _licitacoes → "Nome Órgão Superior" (com "Nome", case misto). NÃO misture os nomes entre tabelas
- WINDOW FUNCTIONS em CTE: alias computado (ex: total_gasto) NÃO pode ser usado em GROUP BY externo — use subconsulta ou repita a expressão

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

  // Corrige REPLACE(col,'.',) malformado (sem o '' final) → REPLACE duplo correto
  s = s.replace(/REPLACE\(("(?:[^"]+)"),\s*'\.',\s*\)/g,
    `REPLACE(REPLACE($1, '.', ''), ',', '.')`);
  // Corrige REPLACE(col,',',) malformado
  s = s.replace(/REPLACE\(("(?:[^"]+)"),\s*',',\s*\)/g,
    `REPLACE(REPLACE($1, '.', ''), ',', '.')`);

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
  s = s.replace(/"RAZÃO SOCIAL"(?! [–\-])/g, '"RAZÃO SOCIAL – CADASTRO RECEITA"');
  // _pep: underscore errado
  s = s.replace(/"Nome_Órgão Superior"/g, '"Nome Órgão Superior"');
  s = s.replace(/"Nome_Órgão"/g, '"Nome Órgão"');
  // Pensionistas: coluna errada
  s = s.replace(/([^_])ORGSUP_LOTACAO(?!_)/g, '$1ORGSUP_LOTACAO_INSTITUIDOR_PENSAO');
  // Despesas empenho: "Órgão Superior" (sem "Nome") — NÃO converter licitacoes que tem "Nome Órgão Superior"
  // REMOVIDO: não converter "Nome Órgão Superior" pois _licitacoes usa exatamente esse nome
  // DATE com SUBSTRING: precisa cast para VARCHAR
  s = s.replace(/SUBSTRING\("DATA LANÇAMENTO",\s*1,\s*7\)/g, 'SUBSTRING(CAST("DATA LANÇAMENTO" AS VARCHAR),1,7)');
  s = s.replace(/SUBSTRING\(("Data Emissão"),\s*1,\s*(\d+)\)/g, 'SUBSTRING(CAST($1 AS VARCHAR),1,$2)');
  // REPLACE simples em colunas monetárias → REPLACE duplo
  const monetaryCols = [
    '"Valor diárias"', '"Valor passagens"', '"Valor Licitação"',
    '"VALOR TRANSFERIDO"', '"VALOR LIBERADO"', '"VALOR CONVÊNIO"',
    '"Valor Renúncia Fiscal (R$)"', '"ORÇAMENTO REALIZADO (R$)"',
    '"ORÇAMENTO ATUALIZADO (R$)"'
  ];
  for (const col of monetaryCols) {
    const escaped = col.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    // só aplica se ainda não tem REPLACE duplo
    const alreadyDouble = new RegExp(`REPLACE\\(REPLACE\\(${escaped}`);
    if (!alreadyDouble.test(s)) {
      const single = new RegExp(`REPLACE\\(${escaped},\\s*'\\.',\\s*''\\)`, 'g');
      s = s.replace(single, `REPLACE(REPLACE(${col}, '.', ''), ',', '.')`);
      const comma = new RegExp(`REPLACE\\(${escaped},\\s*',',\\s*'\\.'\\)`, 'g');
      s = s.replace(comma, `REPLACE(REPLACE(${col}, '.', ''), ',', '.')`);
    }
  }
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
REGRA ABSOLUTA: Responda APENAS com SQL puro — zero palavras antes ou depois, zero explicações, zero markdown, zero blocos de código. A primeira palavra da resposta deve ser SELECT ou WITH.
AUDITORIA: Quando possível, inclua no SELECT as colunas _audit_url_origem, _audit_data_publicacao, _audit_arquivo_origem de pelo menos uma das tabelas principais consultadas (apenas 1 linha de auditoria por fonte é suficiente — use MIN() ou LIMIT 1 em subquery). Isso permite rastreabilidade da fonte.`
      }]
    });

    let sql = sqlGen.content.find(b => b.type === "text")?.text.trim() || "";
    sql = sql.replace(/```sql\n?/g, "").replace(/```/g, "").trim();
    sql = applySqlAutoFix(sql);
    console.log(`📝 SQL: ${sql.substring(0, 300)}`);

    // Se Claude retornou explicação em vez de SQL, tenta web search como fallback
    const sqlLower = sql.toLowerCase();
    if (!sqlLower.startsWith("select") && !sqlLower.startsWith("with")) {
      console.log("💬 Claude respondeu sem SQL — tentando contexto web...");
      let fallbackAnswer = sql;
      if (TAVILY_KEY) {
        const web = await tavilySearch(query, 4);
        if (web?.results?.length) {
          const webCtx = web.results.map((r,i) => `[${i+1}] ${r.title}\nURL: ${r.url}\n${r.content||""}`).join("\n\n");
          const fallback = await anthropic.messages.create({
            model: "claude-sonnet-4-5-20250929",
            max_tokens: 1500,
            messages: [{ role: "user", content: `Pergunta: "${query}"\n\nOs dados não estão na base BDC. Use o contexto web abaixo para responder. Cite as fontes [1],[2] etc e inclua seção ## Fontes.\n\n${webCtx}` }]
          });
          fallbackAnswer = fallback.content.find(b => b.type==="text")?.text || sql;
        }
      }
      return res.json({ answer: fallbackAnswer, sql: "", duration_ms: Date.now() - start, rows_returned: 0 });
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

    // ── CONTEXTO EXTERNO: web search + Semantic Scholar quando necessário ──
    const { needsWeb, needsS2 } = needsExternalContext(query, data.row_count || 0, sql);
    let webContext = null, s2Context = null;

    if (needsWeb && TAVILY_KEY) {
      console.log("🌐 Tavily buscando contexto web...");
      webContext = await tavilySearch(query);
      if (webContext) console.log(`🌐 Tavily: ${webContext.results.length} resultados`);
    }
    if (needsS2) {
      console.log("📚 Semantic Scholar buscando literatura...");
      s2Context = await s2Search(query);
      if (s2Context) console.log(`📚 S2: ${s2Context.length} artigos`);
    }

    const webSection = webContext ? `

CONTEXTO WEB (Tavily — use para enriquecer a resposta e adicionar às Fontes):
${webContext.answer ? `Resumo: ${webContext.answer}\n` : ""}${webContext.results.map((r,i) => `[W${i+1}] ${r.title}\n     URL: ${r.url}\n     ${r.content || ""}`).join("\n")}` : "";

    const s2Section = s2Context?.length ? `

LITERATURA ACADÊMICA (Semantic Scholar — cite quando relevante):
${s2Context.map((p,i) => `[A${i+1}] ${p.title} (${p.year}) — ${p.authors}\n     ${p.abstract || ""}\n     URL: ${p.url} | Citações: ${p.citations}`).join("\n\n")}` : "";

    console.log("💬 Claude explicando...");
    const explanation = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 2500,
      messages: [{
        role: "user",
        content: `Você é um analista de dados públicos brasileiros. Responda à pergunta abaixo integrando TODAS as fontes disponíveis: dados do banco BDC, contexto web e literatura acadêmica quando presentes.

PERGUNTA: "${query}"

SQL EXECUTADO:
${sql}

RESULTADOS DO BANCO BDC (${data.row_count} linhas):
${JSON.stringify(data.rows?.slice(0, 50), null, 2)}${webSection}${s2Section}

REGRAS OBRIGATÓRIAS:

1. Cite cada fonte UMA VEZ com [N] na primeira vez que a usa — não repita a mesma citação em cada frase.
2. Ao final, seção "## Fontes" com cada citação numerada e detalhada.
3. Colunas de auditoria nos resultados (_audit_url_origem, _audit_data_publicacao, _audit_arquivo_origem): USE-AS para construir citações exatas.
4. Resultados web [W1],[W2]...: cite como fonte complementar quando usados.
5. Artigos acadêmicos [A1],[A2]...: cite quando enriquecerem a análise.
6. Se o banco retornou 0 linhas mas há contexto web, responda com base no contexto web e explique que o dado específico não está na base BDC.

MAPEAMENTO TABELAS → INSTITUIÇÕES:
- _ceis, _cnep, _ceaf, _cepim, _acordos → CGU – Portal da Transparência
- _pep → CGU – Pessoas Politicamente Expostas
- _bolsafamilia*, _novobolsafamilia, _bpc, _auxilioemergencial → MDS
- _servidores_* → SEGES/MGI
- _viagens_*, _cpgf* → CGU – Portal da Transparência
- _despesas_*, _despesasdiarias_* → SOF/STN – SIAFI
- _convenios* → CGU – SICONV/Transferegov
- _licitacoes*, _compras* → SEGES – Portal de Compras
- _empresas_* → RFB – Receita Federal (CNPJ)
- _renuncias* → SOF – Secretaria de Orçamento Federal

Formate valores em R$. Seja preciso e objetivo.`
      }]
    });

    const answer = explanation.content.find(b => b.type === "text")?.text || "Sem resposta";
    console.log(`✅ CONCLUÍDO em ${Date.now() - start}ms`);

    // Auto-save conversa (não bloqueia resposta)
    const convId   = req.body?.conv_id || null;
    const userEmail= req.body?.user    || "anonymous";
    let   savedConvId = convId;
    (async () => {
      try {
        // Salva pergunta do usuário
        const r1 = await fetch(`${HETZNER_API}/conversations/message`, {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-API-Key": HETZNER_KEY },
          body: JSON.stringify({ user: userEmail, conv_id: convId, role: "user", content: query })
        });
        const d1 = await r1.json();
        savedConvId = d1.conv_id;
        // Salva resposta do assistente
        await fetch(`${HETZNER_API}/conversations/message`, {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-API-Key": HETZNER_KEY },
          body: JSON.stringify({
            user: userEmail, conv_id: savedConvId, role: "assistant",
            content: answer, sql_used: sql,
            row_count: data.row_count || 0, duration_ms: Date.now() - start
          })
        });
        console.log(`💾 Salvo conv_id: ${savedConvId}`);
      } catch(e) { console.warn("⚠️ Erro ao salvar conversa:", e.message); }
    })();

    return res.json({ answer, sql, duration_ms: Date.now() - start, rows_returned: data.row_count, conv_id: savedConvId });

  } catch (err) {
    console.error("❌ ERRO:", err.message);
    return res.status(500).json({ error: err.message, duration_ms: Date.now() - start });
  }
});

/* ========================= CONVERSATIONS PROXY ========================= */

// Lista conversas do usuário
app.get("/conversations", async (req, res) => {
  try {
    const user = req.query.user || "";
    const r = await fetch(`${HETZNER_API}/conversations?user=${encodeURIComponent(user)}`, {
      headers: { "X-API-Key": HETZNER_KEY }
    });
    const d = await r.json();
    res.json(d);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Busca mensagens de uma conversa
app.get("/conversations/:id", async (req, res) => {
  try {
    const r = await fetch(`${HETZNER_API}/conversations/${req.params.id}`, {
      headers: { "X-API-Key": HETZNER_KEY }
    });
    const d = await r.json();
    res.json(d);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Salva mensagem
app.post("/conversations/message", async (req, res) => {
  try {
    const r = await fetch(`${HETZNER_API}/conversations/message`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-API-Key": HETZNER_KEY },
      body: JSON.stringify(req.body)
    });
    const d = await r.json();
    res.json(d);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Deleta conversa
app.delete("/conversations/:id", async (req, res) => {
  try {
    const r = await fetch(`${HETZNER_API}/conversations/${req.params.id}`, {
      method: "DELETE",
      headers: { "X-API-Key": HETZNER_KEY }
    });
    const d = await r.json();
    res.json(d);
  } catch(e) { res.status(500).json({ error: e.message }); }
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
