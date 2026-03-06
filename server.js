import express from "express";
import cors from "cors";
import Anthropic from "@anthropic-ai/sdk";
import { createRequire } from "module";
import { existsSync } from "fs";

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

const HETZNER_API = process.env.HETZNER_API_BASE || "http://89.167.48.3:5010";
const HETZNER_KEY = process.env.HETZNER_API_KEY || "bdc-sql-api-key-2026-segura";
const anthropic   = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
const TAVILY_KEY  = process.env.TAVILY_API_KEY   || "";
const S2_KEY      = process.env.S2_API_KEY        || "luDwHjoEjo9o0YcfcNi4J6f88oXQ9Um7VQkWCncj";

/* ─── SCHEMA DINÂMICO ─── */
let SCHEMA = {};
try {
  const require = createRequire(import.meta.url);
  SCHEMA = require("./schema_compact.json");
  console.log(`📋 Schema carregado: ${Object.keys(SCHEMA).length} tabelas`);
} catch (e) {
  console.warn("⚠️ schema_compact.json não encontrado — schema injection desativado");
}

// Mapa keyword → tabelas relevantes
const TABLE_KEYWORDS = {
  "_bolsafamilia_pagamentos":           ["bolsa família","bolsa familia","bolsafamilia"],
  "_bolsafamilia_saques":               ["saque bolsa","bolsa família saque"],
  "_novobolsafamilia":                  ["novo bolsa","bolsa família 202","bolsa familia 202"],
  "_bpc":                               ["bpc","prestação continuada","benefício assistencial"],
  "_auxilioemergencial":                ["auxílio emergencial","auxilio emergencial","covid"],
  "_auxiliobrasil":                     ["auxílio brasil","auxilio brasil"],
  "_segurodefeso":                      ["seguro defeso","pescador"],
  "_garantiasafra":                     ["garantia safra","safra"],
  "_pedemeia":                          ["pé de meia","pe de meia","poupança escolar"],
  "_peti":                              ["peti","trabalho infantil"],
  "_auxilioreconstrucao":               ["auxílio reconstrução","auxilio reconstrucao","enchente","calamidade"],
  "_ceis":                              ["ceis","sancionad","impedid","inidone","lista negra"],
  "_cnep":                              ["cnep","multa empresa","dissolução compulsória"],
  "_ceaf":                              ["ceaf","demitid","cassação aposentadoria","perda emprego"],
  "_cepim":                             ["cepim","entidade impedida","impedimento convênio"],
  "_acordos":                           ["acordo leniência","acordo leniencia","leniência","leniencia"],
  "_pep":                               ["pep","politicamente exposto","pessoa política"],
  "_despesas_favorecidos":              ["despesa","favorecido","recebeu recurso","valor recebido","recursos federais","pagamento federal"],
  "_despesasdiarias_despesas_empenho":  ["empenho","nota empenho"],
  "_despesasdiarias_despesas_pagamento":["pagamento siafi","pagamento empenho"],
  "_convenios":                         ["convênio","convenio","siconv","transferegov"],
  "_licitacoes":                        ["licitação","licitacao","pregão","pregao","dispensa","concorrência"],
  "_compras":                           ["compra","contrato federal","item compra"],
  "_transferencias":                    ["transferência","transferencia","repasse federal","fundo a fundo"],
  "_viagens_viagem":                    ["viagem","diária","diaria","passagem","deslocamento","missão"],
  "_viagens_trecho":                    ["trecho","origem destino","meio transporte","voo"],
  "_viagens_passagem":                  ["passagem aérea","bilhete"],
  "_cpgf":                              ["cartão corporativo","cartao corporativo","cpgf","cartão governo"],
  "_cpcc":                              ["cpcc","cartão combustível"],
  "_cpdc":                              ["cpdc","cartão convenio"],
  "_servidores_cadastro":               ["servidor","servidora","funcionário federal","funcionario federal","cargo federal","lotação","lotacao","vínculo","vinculo","efetivo","comissionado"],
  "_servidores_remuneracao":            ["remuneração","remuneracao","salário federal","salario federal","contracheque","folha pagamento"],
  "_servidores_afastamentos":           ["afastamento","licença","licenca","ausência"],
  "_servidores_honorarios_jetons_":     ["jetom","jeton","honorário","honorario","conselho"],
  "_servidores_honorariosadvocaticios": ["honorário advocatício","honorario advocaticio"],
  "_imoveisfuncionais":                 ["imóvel funcional","imovel funcional","residência funcional"],
  "_renunciasfiscais":                  ["renúncia fiscal","renuncia fiscal","benefício fiscal","isenção fiscal"],
  "_orçamentodadespesa":                ["orçamento","orcamento","dotação","loa","ploa"],
  "_execuçãodareceita":                 ["receita federal","arrecadação","arrecadacao","execução receita"],
  "_emendasparlamentarespordocumento":  ["emenda parlamentar","emenda","parlamentar"],
  "_apoiamentoemendasparlamentares":    ["apoiamento emenda","apoiador emenda"],
  "_notasfiscais":                      ["nota fiscal","nfe","chave acesso"],
};

function getSchemaBlock(query) {
  const q = query.toLowerCase();
  const matched = new Set();

  // Keyword matching
  for (const [table, keywords] of Object.entries(TABLE_KEYWORDS)) {
    if (keywords.some(k => q.includes(k))) {
      matched.add(table);
    }
  }

  // Empresas RFB: detecta por keywords
  const empresaKw = ["empresa","cnpj","razão social","razao social","inapt","baix","ativ","estabelecimento","sócio","socio","capital social","cnae","porte","matriz","filial","mei","microempresa"];
  if (empresaKw.some(k => q.includes(k))) {
    matched.add("_empresas_sp"); // representativa (todas têm mesmo schema)
  }

  // Servidores: inclui todas as variantes se qualquer match
  if (matched.has("_servidores_cadastro")) {
    ["_servidores_cadastro__2","_servidores_cadastro__3","_servidores_cadastro__4",
     "_servidores_cadastro__5","_servidores_cadastro__6","_servidores_cadastro__7"].forEach(t => matched.add(t));
  }
  if (matched.has("_servidores_remuneracao")) {
    ["_servidores_remuneracao__2","_servidores_remuneracao__3"].forEach(t => matched.add(t));
  }
  if (matched.has("_servidores_afastamentos")) {
    matched.add("_servidores_afastamentos__2");
  }

  if (matched.size === 0 || Object.keys(SCHEMA).length === 0) return "";

  const lines = [];
  for (const table of matched) {
    if (SCHEMA[table]) {
      const cols = SCHEMA[table].filter(c => !c.startsWith("_audit"));
      lines.push(`${table}: ${cols.join(", ")}`);
    }
  }

  if (lines.length === 0) return "";
  return `\n== SCHEMA EXATO (use SOMENTE estas colunas — não invente outras) ==\n${lines.join("\n")}\n`;
}

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
      query, limit,
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
  const noData = rowCount === 0;
  const contextKeywords = ["escândalo","investigação","cpi","operação","notícia","recente",
    "contexto","histórico","por que","análise","impacto","consequência",
    "processo","denúncia","acusação","preso","condenado"];
  const hasContextKw = contextKeywords.some(k => q.includes(k));
  const s2Keywords = ["estudo","pesquisa","artigo","literatura","acadêmico",
    "setor","indústria","correlação","evidência","análise setorial"];
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
- AFASTAMENTOS: DATA_INICIO_AFASTAMENTO e DATA_FIM_AFASTAMENTO são VARCHAR com formatos MISTOS ('DD/MM/YYYY' ou 'YYYY-MM-DD'). Use SEMPRE COALESCE dos dois formatos: COALESCE(TRY_STRPTIME(col,'%d/%m/%Y'), TRY_STRPTIME(col,'%Y-%m-%d')) — NUNCA use um único formato pois causará parse error. Para afastamentos em aberto: DATA_FIM_AFASTAMENTO IS NULL OR DATA_FIM_AFASTAMENTO = 'Não informada'. NÃO existe "Início do afastamento" nem "Fim do afastamento"
- CEIS/CNEP/CEAF: coluna do documento é "CPF OU CNPJ DO SANCIONADO". NÃO existe "CNPJ OU CPF DO SANCIONADO". NÃO existe "TIPO SANÇÃO" — use "CATEGORIA DA SANÇÃO"
- CEIS/CNEP/CEAF: coluna de nome é "NOME DO SANCIONADO" — NÃO existe "RAZÃO SOCIAL" nessas tabelas
- ACORDOS: status é "SITUAÇÃO DO ACORDO DE LENIÊNICA" — NÃO existe "SITUAÇÃO DO ACORDO". Nome é "RAZÃO SOCIAL – CADASTRO RECEITA"
- VALORES monetários com ponto de milhar + vírgula decimal (ex: "10.313.296,80"): use CAST(REPLACE(REPLACE(col, '.', ''), ',', '.') AS DECIMAL) — padrão EXATO com DOIS REPLACE aninhados
- DATAS VARCHAR em viagens: "Período - Data de início" e "Período - Data de fim" são VARCHAR 'YYYY-MM-DD'. Para ano: SUBSTRING("Período - Data de início",1,4). NUNCA use EXTRACT/date_part em VARCHAR
- DATAS DATE (não VARCHAR): para extrair ano de coluna DATE use CAST(EXTRACT(YEAR FROM col) AS VARCHAR) ou SUBSTRING(CAST(col AS VARCHAR),1,4) — NUNCA SUBSTRING direto em DATE
- WINDOW FUNCTIONS: NUNCA use funções de janela (OVER()) em WHERE. Use QUALIFY ou subconsulta
- SERVIDORES pensionistas (_cadastro__4): coluna de órgão chama ORGSUP_LOTACAO_INSTITUIDOR_PENSAO — NÃO existe ORGSUP_LOTACAO nessa tabela
- SERVIDORES cadastro (_servidores_cadastro): coluna de órgão chama ORGSUP_LOTACAO e ORGSUP_EXERCICIO
- CEPIM: JOIN com outras tabelas via CNPJ é impreciso pois "CNPJ ENTIDADE" no CEPIM é apenas o CNPJ base (8 dígitos) sem filial
- CNAES em array: cnaes_secundarios_codigos é VARCHAR[] — para filtrar use array_contains(est.cnaes_secundarios_codigos, '6201') NUNCA use LIKE em array
- NÃO existem tabelas empresas_baixadas, empresas_inaptas, empresas_ativas — use _empresas_UF com filtro em est.situacao_cadastral
- WINDOW FUNCTIONS em CTE: alias computado NÃO pode ser usado em GROUP BY externo — use subconsulta ou repita a expressão

== LIMITAÇÕES — RESPONDA EM PORTUGUÊS SEM GERAR SQL SE PERGUNTAR SOBRE ==
- Judiciário (STF,STJ,TRF,TRT), Legislativo (Câmara,Senado,vereadores): NÃO estão nos dados
- Servidores estaduais/municipais: NÃO estão nos dados
- CPF no BF/BPC é mascarado (***123456**): não cruza com RFB/PEP por CPF
- MEI não é identificável: use porte='MICRO EMPRESA' como aproximação

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
_servidores_cadastro__4(73K—pensionistas): CPF_REPRESENTANTE_LEGAL,CPF_INSTITUIDOR_PENSAO,TIPO_PENSAO,DATA_INICIO_PENSAO — ORGSUP_LOTACAO_INSTITUIDOR_PENSAO
_servidores_cadastro__5(12M—militares reforma): TIPO_APOSENTADORIA,DATA_APOSENTADORIA
_servidores_cadastro__6(1M): SITUACAO_VINCULO='EXCEDENTE A LOTACAO'|'SEM VINCULO'
_servidores_cadastro__7(52M—militares ativos): SITUACAO_VINCULO='CEDIDO SUS/LEI 8270'|'CELETISTA/EMPREGADO'|'EMPREGO PUBLICO'
⚠️ NUNCA use SITUACAO_VINCULO='ATIVO'. Civis ativos→__1 ou __2 com 'ATIVO PERMANENTE'. Militares ativos→__7.

_servidores_remuneracao(19M)+__2(30M)+__3(52M)+__4(237K)+__5(9M):
  ANO(VARCHAR),MES(VARCHAR),Id_SERVIDOR_PORTAL,CPF,NOME,"REMUNERAÇÃO BÁSICA BRUTA (R$)","ABATE-TETO (R$)","GRATIFICAÇÃO NATALINA (R$)","FÉRIAS (R$)","IRRF (R$)","PSS/RPGS (R$)","DEMAIS DEDUÇÕES (R$)","REMUNERAÇÃO APÓS DEDUÇÕES OBRIGATÓRIAS (R$)","TOTAL DE VERBAS INDENIZATÓRIAS (R$)(*)"
  ⚠️ SEM coluna de órgão — JOIN com _servidores_cadastro via Id_SERVIDOR_PORTAL

_servidores_afastamentos(84K)+__2(8M): ANO,MES,Id_SERVIDOR_PORTAL,CPF,NOME,DATA_INICIO_AFASTAMENTO(VARCHAR),DATA_FIM_AFASTAMENTO(VARCHAR)
_servidores_honorarios_jetons_(45K): ANO,MES,Id_SERVIDOR_PORTAL,CPF,NOME,EMPRESA,VALOR
_servidores_honorariosadvocaticios(1M): ANO,MES,Id_SERVIDOR_PORTAL,CPF,NOME,OBSERVACOES,VALOR
_servidores_observacoes(463K+__2..7): ANO,MES,Id_SERVIDOR_PORTAL,NOME,CPF,OBSERVACAO

-- DESPESAS --
_despesas_favorecidos(114M): "Código Favorecido","Nome Favorecido","Sigla UF","Nome Município","Código Órgão Superior","Nome Órgão Superior","Código Órgão","Nome Órgão","Ano e mês do lançamento"(VARCHAR'MM/YYYY'→LIKE'%/2024'),"Valor Recebido"(VARCHAR)
_despesasdiarias_despesas_empenho(31M): "Id Empenho"(BIGINT),"Código Empenho","Data Emissão"(DATE),"Tipo Empenho","Código Órgão Superior"(BIGINT),"Órgão Superior","Favorecido","Código Favorecido","Função","Programa","Ação","Categoria de Despesa","Grupo de Despesa","Valor Original do Empenho","Valor do Empenho Convertido pra R$"
_despesasdiarias_despesas_pagamento(103M): "Código Pagamento","Data Emissão","Código Órgão Superior","Órgão Superior","Órgão","Código Favorecido","Favorecido","Valor Original do Pagamento","Valor do Pagamento Convertido pra R$"
_despesasdiarias_despesas_pagamento_favorecidosfinais(131M): "Código Pagamento","Data Emissão","Código Favorecido","Favorecido","Valor do Pagamento em R$"
_despesasdiarias_despesas_liquidacao_empenhosimpactados(77M): "Código Liquidação","Código Empenho","Valor Liquidado (R$)","Valor Restos a Pagar Pagos (R$)"
_despesasdiarias_despesas_pagamento_empenhosimpactados(103M): "Código Pagamento","Código Empenho","Valor Pago (R$)"
_despesasdiarias_despesas_itemempenho(33M): "Id Empenho"(BIGINT),"Código Empenho","Descrição","Quantidade","Valor Unitário","Valor Total"

-- VIAGENS --
_viagens_viagem(9M): "Identificador do processo de viagem","Código do órgão superior","Nome do órgão superior","Nome órgão solicitante","CPF viajante","Nome","Cargo","Período - Data de início","Período - Data de fim","Destinos","Motivo","Valor diárias","Valor passagens"
_viagens_trecho(20M): "Identificador do processo de viagem","Origem - País","Origem - UF","Origem - Cidade","Destino - País","Destino - UF","Destino - Cidade","Meio de transporte","Número Diárias","Missao?"
_viagens_passagem(5M): "Identificador do processo de viagem","Meio de transporte","País - Destino ida","UF - Destino ida","Cidade - Destino ida","Valor da passagem","Data da emissão/compra"
_viagens_pagamento(16M): "Identificador do processo de viagem","Nome do órgão superior","Tipo de pagamento","Valor"

-- SANÇÕES --
_ceis(22K): "TIPO DE PESSOA"(VARCHAR'F'/'J'),"CPF OU CNPJ DO SANCIONADO","NOME DO SANCIONADO","CATEGORIA DA SANÇÃO","DATA INÍCIO SANÇÃO"(DATE),"DATA FINAL SANÇÃO"(DATE),"ÓRGÃO SANCIONADOR","UF ÓRGÃO SANCIONADOR"
_cnep(2K): mesmo schema + "VALOR DA MULTA"
_ceaf(4K): "TIPO DE PESSOA"(BOOLEAN),"CPF OU CNPJ DO SANCIONADO","NOME DO SANCIONADO","CATEGORIA DA SANÇÃO","DATA INÍCIO SANÇÃO"(DATE),"ÓRGÃO SANCIONADOR"
_cepim(4K): "CNPJ ENTIDADE","NOME ENTIDADE","NÚMERO CONVÊNIO","ÓRGÃO CONCEDENTE","MOTIVO DO IMPEDIMENTO"
_acordos(143): "CNPJ DO SANCIONADO","RAZÃO SOCIAL – CADASTRO RECEITA","SITUAÇÃO DO ACORDO DE LENIÊNICA","DATA DE INÍCIO DO ACORDO"(DATE),"DATA DE FIM DO ACORDO"(DATE),"ÓRGÃO SANCIONADOR"

-- LICITAÇÕES E COMPRAS --
_licitacoes(2M): "Número Licitação","Nome UG","Modalidade Compra","Objeto","Situação Licitação","Nome Órgão Superior","Nome Órgão","UF","Data Resultado Compra"(DATE),"Data Abertura"(DATE),"Valor Licitação"
  ⚠️ NÃO tem coluna de CNPJ/CPF do vencedor
_compras(4M): "Código Órgão"(BIGINT),"Nome Órgão","Código UG","Número Contrato","Descrição Item Compra","Quantidade Item"(BIGINT),"Valor Item"
_convenios(612K): "NÚMERO CONVÊNIO","UF","NOME MUNICÍPIO","SITUAÇÃO CONVÊNIO","OBJETO DO CONVÊNIO","NOME ÓRGÃO SUPERIOR","NOME ÓRGÃO CONCEDENTE","CÓDIGO CONVENENTE","TIPO CONVENENTE","NOME CONVENENTE","VALOR CONVÊNIO","VALOR LIBERADO","DATA INÍCIO VIGÊNCIA"(DATE),"DATA FINAL VIGÊNCIA"(DATE)

-- CARTÃO CORPORATIVO --
_cpgf(2M): "CÓDIGO ÓRGÃO SUPERIOR"(BIGINT),"NOME ÓRGÃO SUPERIOR","CÓDIGO ÓRGÃO"(BIGINT),"NOME ÓRGÃO","ANO EXTRATO"(BIGINT),"MÊS EXTRATO"(VARCHAR),"CPF PORTADOR"(VARCHAR),"NOME PORTADOR","NOME FAVORECIDO","TRANSAÇÃO","DATA TRANSAÇÃO"(DATE),"VALOR TRANSAÇÃO","CNPJ OU CPF FAVORECIDO"(VARCHAR)
_cpcc(1M): +"TIPO AQUISIÇÃO","CNPJ OU CPF FAVORECIDO"(BIGINT)
_cpdc(129K): +"CPF PORTADOR","NOME PORTADOR","CNPJ OU CPF FAVORECIDO","NÚMERO CONVÊNIO"(BIGINT),"NOME CONVENENTE"

-- OUTROS --
_pep(71K): CPF,"Nome_PEP","Descrição_Função","Nome_Órgão","Data_Início_Exercício"(DATE),"Data_Fim_Exercício","Data_Fim_Carência"
_imoveisfuncionais(23K): "NOME PERMISSIONÁRIO",CPF,"ÓRGÃO EXERCÍCIO DO PERMISSIONÁRIO","DATA INÍCIO OCUPAÇÃO"(DATE)
_orçamentodadespesa(305K): "EXERCÍCIO"(BIGINT),"NOME ÓRGÃO SUPERIOR","NOME FUNÇÃO","NOME PROGRAMA ORÇAMENTÁRIO","NOME AÇÃO","ORÇAMENTO INICIAL (R$)","ORÇAMENTO EMPENHADO (R$)","ORÇAMENTO REALIZADO (R$)"
_execuçãodareceita(2M): "CÓDIGO ÓRGÃO"(BIGINT),"NOME ÓRGÃO","CATEGORIA ECONÔMICA","ORIGEM RECEITA","VALOR PREVISTO ATUALIZADO","VALOR REALIZADO","DATA LANÇAMENTO"(DATE),"ANO EXERCÍCIO"(BIGINT)
_transferencias(9M): "ANO / MÊS"(BIGINT YYYYMM),"TIPO TRANSFERÊNCIA","UF","NOME MUNICÍPIO","NOME ÓRGÃO","CÓDIGO FAVORECIDO","NOME FAVORECIDO","VALOR TRANSFERIDO"
_emendasparlamentarespordocumento(4M): "Código da Emenda","Ano da Emenda"(BIGINT),"Nome do Autor da Emenda","Valor Empenhado","Valor Pago","Tipo de Emenda","UF de aplicação do recurso","Favorecido"
_renúnciasfiscais(752K): "Ano-calendário"(BIGINT),CNPJ,"Razão Social","Código CNAE",UF,"Tipo Renúncia","Benefício Fiscal","Tributo","Valor Renúncia Fiscal (R$)"
_apoiamentoemendasparlamentares(34K): "Código Apoiador"(BIGINT),"Apoiador","Nome do Autor da Emenda","Valor Empenhado","Valor Pago","Órgão Superior"
_notasfiscais(274K): "CHAVE DE ACESSO"(DOUBLE),"DATA EMISSÃO"(TIMESTAMP),"EVENTO","DESCRIÇÃO EVENTO"

== CRUZAMENTOS PRINCIPAIS ==

[CNPJ: due diligence]
SELECT 'CADASTRO' as secao, 'Situação' as campo, CAST(est.situacao_cadastral AS VARCHAR) as valor
FROM _empresas_sp WHERE est.cnpj_completo = '33000167000101'
UNION ALL SELECT 'SANÇÃO CEIS', 'Categoria', "CATEGORIA DA SANÇÃO" FROM _ceis WHERE "CPF OU CNPJ DO SANCIONADO" = '33000167000101'
UNION ALL SELECT 'SANÇÃO CNEP', 'Categoria', "CATEGORIA DA SANÇÃO" FROM _cnep WHERE "CPF OU CNPJ DO SANCIONADO" = '33000167000101'
UNION ALL SELECT 'DESPESAS 2024', 'Total Recebido', CAST(SUM(CAST(REPLACE("Valor Recebido",',','.') AS DECIMAL)) AS VARCHAR)
FROM _despesas_favorecidos WHERE "Código Favorecido" = '33000167000101' AND "Ano e mês do lançamento" LIKE '%/2024'
UNION ALL SELECT 'CONVÊNIOS', 'Situação', "SITUAÇÃO CONVÊNIO" FROM _convenios WHERE "CÓDIGO CONVENENTE" = '33000167' LIMIT 1

[CEIS × transferências]
WITH sancionados AS (SELECT DISTINCT "CPF OU CNPJ DO SANCIONADO" as cnpj FROM _ceis WHERE "TIPO DE PESSOA"='J')
SELECT s.cnpj, SUM(CAST(REPLACE(d."Valor Recebido",',','.') AS DECIMAL)) as total_recebido
FROM sancionados s JOIN _despesas_favorecidos d ON d."Código Favorecido" = s.cnpj
WHERE d."Ano e mês do lançamento" LIKE '%/2024'
GROUP BY s.cnpj ORDER BY total_recebido DESC

[Servidor + remuneração]
SELECT c.NOME, c.ORGSUP_EXERCICIO, r."REMUNERAÇÃO BÁSICA BRUTA (R$)"
FROM _servidores_cadastro c JOIN _servidores_remuneracao r ON r.Id_SERVIDOR_PORTAL = c.Id_SERVIDOR_PORTAL
WHERE r.ANO='2024' AND r.MES='12' ORDER BY CAST(REPLACE(r."REMUNERAÇÃO BÁSICA BRUTA (R$)",',','.') AS DECIMAL) DESC LIMIT 100
`;

/* ========================= SQL AUTO-FIX ========================= */
function applySqlAutoFix(sql) {
  let s = sql || "";

  s = s.replace(/TRY_STRPTIME\(([^,]+),\s*'%d\/%m\/%Y'\)/g,
    `COALESCE(TRY_STRPTIME($1, '%d/%m/%Y'), TRY_STRPTIME($1, '%Y-%m-%d'))`);
  s = s.replace(/TRY_STRPTIME\(([^,]+),\s*'%Y-%m-%d'\)/g,
    `COALESCE(TRY_STRPTIME($1, '%d/%m/%Y'), TRY_STRPTIME($1, '%Y-%m-%d'))`);

  s = s.replace(/"Início do afastamento"/g, "DATA_INICIO_AFASTAMENTO");
  s = s.replace(/"Fim do afastamento"/g, "DATA_FIM_AFASTAMENTO");
  s = s.replace(/"SITUAÇÃO DO ACORDO"(?! DE LENIÊNICA)/g, '"SITUAÇÃO DO ACORDO DE LENIÊNICA"');
  s = s.replace(/"CNPJ OU CPF DO SANCIONADO"/g, '"CPF OU CNPJ DO SANCIONADO"');
  s = s.replace(/"TIPO SANÇÃO"/g, '"CATEGORIA DA SANÇÃO"');
  s = s.replace(/"RAZÃO SOCIAL"(?! [–\-])/g, '"RAZÃO SOCIAL – CADASTRO RECEITA"');
  s = s.replace(/"Nome_Órgão Superior"/g, '"Nome Órgão Superior"');
  s = s.replace(/"Nome_Órgão"/g, '"Nome Órgão"');
  s = s.replace(/(_cadastro__4\b.*?)ORGSUP_LOTACAO(?!_INSTITUIDOR)/gs,
    '$1ORGSUP_LOTACAO_INSTITUIDOR_PENSAO');
  s = s.replace(/SUBSTRING\("DATA LANÇAMENTO",\s*1,\s*7\)/g, 'SUBSTRING(CAST("DATA LANÇAMENTO" AS VARCHAR),1,7)');
  s = s.replace(/SUBSTRING\(("Data Emissão"),\s*1,\s*(\d+)\)/g, 'SUBSTRING(CAST($1 AS VARCHAR),1,$2)');

  const monetaryCols = [
    '"Valor diárias"', '"Valor passagens"', '"Valor Licitação"',
    '"VALOR TRANSFERIDO"', '"VALOR LIBERADO"', '"VALOR CONVÊNIO"',
    '"Valor Renúncia Fiscal (R$)"', '"ORÇAMENTO REALIZADO (R$)"',
    '"ORÇAMENTO ATUALIZADO (R$)"'
  ];
  for (const col of monetaryCols) {
    const escaped = col.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
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

    // Schema dinâmico: injeta apenas colunas das tabelas relevantes
    const schemaBlock = getSchemaBlock(query);
    if (schemaBlock) {
      const tables = (schemaBlock.match(/^_\w+:/gm) || []).map(t => t.replace(':',''));
      console.log(`📋 Schema injetado para: ${tables.join(', ')}`);
    }

    console.log("🤖 Claude gerando SQL...");

    const sqlGen = await anthropic.messages.create({
      model: "claude-haiku-4-5-20251001",
      max_tokens: 3500,
      messages: [{
        role: "user",
        content: `Você é especialista em DuckDB e dados públicos brasileiros.

${DB_CATALOG}${schemaBlock}

PERGUNTA: "${query}"

Gere o SQL DuckDB para responder esta pergunta.
REGRA ABSOLUTA: Responda APENAS com SQL puro — zero palavras antes ou depois, zero explicações, zero markdown, zero blocos de código. A primeira palavra da resposta deve ser SELECT ou WITH.
AUDITORIA: Quando possível, inclua no SELECT as colunas _audit_url_origem, _audit_data_publicacao, _audit_arquivo_origem de pelo menos uma das tabelas principais consultadas.`
      }]
    });

    let sql = sqlGen.content.find(b => b.type === "text")?.text?.trim() || "";
    sql = sql.replace(/```sql\n?/g, "").replace(/```/g, "").trim();
    sql = applySqlAutoFix(sql);
    console.log(`📝 SQL: ${sql.substring(0, 300)}`);

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

    // ── RETRY: até 2 tentativas com autocorreção ──
    let data, sql_final = sql;
    for (let attempt = 1; attempt <= 2; attempt++) {
      const response = await fetch(`${HETZNER_API}/query_unified`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-API-Key": HETZNER_KEY },
        body: JSON.stringify({ sql: sql_final }),
        signal: AbortSignal.timeout(240000)
      });
      data = await response.json();

      if (!response.ok || data.error) {
        if (attempt === 2) throw new Error(data.error || "Query falhou após 2 tentativas");
        console.log(`⚠️ Tentativa ${attempt} falhou: ${data.error}`);
        console.log("🔄 Haiku corrigindo SQL...");
        const fix = await anthropic.messages.create({
          model: "claude-haiku-4-5-20251001",
          max_tokens: 2000,
          messages: [{ role: "user", content:
`Você gerou este SQL para DuckDB que retornou um erro. Corrija APENAS o problema indicado.

PERGUNTA ORIGINAL: "${query}"
${schemaBlock}
SQL COM ERRO:
${sql_final}

ERRO RETORNADO:
${data.error}

REGRAS:
- Responda APENAS com o SQL corrigido, sem explicações
- Se a coluna não existe, use somente colunas listadas no SCHEMA EXATO acima
- Não invente colunas — use apenas o que foi mencionado no SQL original
- Mantenha a lógica original da query` }]
        });
        sql_final = fix.content.find(b => b.type==="text")?.text?.trim() || sql_final;
        sql_final = sql_final.replace(/```sql\n?/g,"").replace(/```/g,"").trim();
        sql_final = applySqlAutoFix(sql_final);
        console.log(`🔄 SQL corrigido: ${sql_final.substring(0,200)}`);
      } else {
        break;
      }
    }

    sql = sql_final;
    console.log(`📊 ${data.row_count || 0} linhas retornadas`);

    // ── CONTEXTO EXTERNO ──
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

    const webSection = webContext ? `\nCONTEXTO WEB:\n${webContext.answer ? `Resumo: ${webContext.answer}\n` : ""}${webContext.results.map((r,i) => `[W${i+1}] ${r.title}\n     URL: ${r.url}\n     ${r.content || ""}`).join("\n")}` : "";
    const s2Section = s2Context?.length ? `\nLITERATURA ACADÊMICA:\n${s2Context.map((p,i) => `[A${i+1}] ${p.title} (${p.year}) — ${p.authors}\n     ${p.abstract || ""}\n     URL: ${p.url}`).join("\n\n")}` : "";

    console.log("💬 Claude explicando...");
    const explanation = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 2500,
      messages: [{
        role: "user",
        content: `Você é um analista de dados públicos brasileiros. Responda integrando TODAS as fontes disponíveis.

PERGUNTA: "${query}"

SQL EXECUTADO:
${sql}

RESULTADOS DO BANCO BDC (${data.row_count} linhas):
${JSON.stringify(data.rows?.slice(0, 50), null, 2)}${webSection}${s2Section}

REGRAS:
1. Cite cada fonte UMA VEZ com [N] na primeira vez que a usa.
2. Ao final, seção "## Fontes" com cada citação numerada.
3. Colunas _audit_* nos resultados: USE-AS para construir citações exatas.
4. Se 0 linhas mas há contexto web, responda com base no contexto web.

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

    const convId   = req.body?.conv_id || null;
    const userEmail= req.body?.user    || "anonymous";
    let   savedConvId = convId;
    (async () => {
      try {
        const r1 = await fetch(`${HETZNER_API}/conversations/message`, {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-API-Key": HETZNER_KEY },
          body: JSON.stringify({ user: userEmail, conv_id: convId, role: "user", content: query })
        });
        const d1 = await r1.json();
        savedConvId = d1.conv_id;
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
app.get("/conversations", async (req, res) => {
  try {
    const user = req.query.user || "";
    const r = await fetch(`${HETZNER_API}/conversations?user=${encodeURIComponent(user)}`, { headers: { "X-API-Key": HETZNER_KEY } });
    res.json(await r.json());
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get("/conversations/:id", async (req, res) => {
  try {
    const r = await fetch(`${HETZNER_API}/conversations/${req.params.id}`, { headers: { "X-API-Key": HETZNER_KEY } });
    res.json(await r.json());
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post("/conversations/message", async (req, res) => {
  try {
    const r = await fetch(`${HETZNER_API}/conversations/message`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-API-Key": HETZNER_KEY },
      body: JSON.stringify(req.body)
    });
    res.json(await r.json());
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete("/conversations/:id", async (req, res) => {
  try {
    const r = await fetch(`${HETZNER_API}/conversations/${req.params.id}`, { method: "DELETE", headers: { "X-API-Key": HETZNER_KEY } });
    res.json(await r.json());
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
