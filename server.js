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
  "_convenios":                         ["convênio","convenio","siconv","transferegov"],
  "_licitacoes":                        ["licitação","licitacao","pregão","pregao","dispensa","concorrência"],
  "_compras":                           ["compra","contrato federal","item compra"],
  "_transferencias":                    ["transferência","transferencia","repasse federal","fundo a fundo"],
  "_viagens":                           ["viagem","diária","diaria","passagem","deslocamento","missão"],
  "_cpgf":                              ["cartão corporativo","cartao corporativo","cpgf","cartão governo"],
  "_cpcc":                              ["cpcc","cartão combustível"],
  "_cpdc":                              ["cpdc","cartão convenio"],
  "_servidores":                        ["servidor","servidora","funcionário federal","funcionario federal","cargo federal","lotação","remuneração federal","salário federal"],
  "_imoveisfuncionais":                 ["imóvel funcional","imovel funcional","residência funcional"],
  "_renunciasfiscais":                  ["renúncia fiscal","renuncia fiscal","benefício fiscal","isenção fiscal"],
  "_orcamentodadespesa":                ["orçamento","orcamento","dotação","loa","ploa"],
  "_execucaodareceita":                 ["receita federal","arrecadação","arrecadacao","execução receita"],
  "_emendasparlamentarespordocumento":  ["emenda parlamentar","emenda","parlamentar"],
  "_notasfiscais":                      ["nota fiscal","nfe","chave acesso"],
  "_rfb_empresas":                      ["empresa","cnpj","razão social","razao social","capital social","porte","mei","microempresa","natureza juridica"],
  "_rfb_estabelecimentos":              ["estabelecimento","cnae","situacao cadastral","ativa","baixada","inapta","matriz","filial","municipio empresa","uf empresa"],
  "_rfb_socios":                        ["sócio","socio","quadro societario","representante legal","participação societária"],
  "_rfb_simples":                       ["simples nacional","simples","optante simples","mei optante"],
};

function getSchemaBlock(query) {
  const q = query.toLowerCase();
  const matched = new Set();

  for (const [table, keywords] of Object.entries(TABLE_KEYWORDS)) {
    if (keywords.some(k => q.includes(k))) {
      matched.add(table);
    }
  }

  // RFB: se detectar empresa/cnpj, inclui todas as 4 tabelas RFB
  const empresaKw = ["empresa","cnpj","razão social","razao social","inapt","baix","ativ","estabelecimento","sócio","socio","capital social","cnae","porte","matriz","filial","mei","microempresa"];
  if (empresaKw.some(k => q.includes(k))) {
    matched.add("_rfb_empresas");
    matched.add("_rfb_estabelecimentos");
    matched.add("_rfb_socios");
    matched.add("_rfb_simples");
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
BANCO: brazildatacorp.duckdb | 7B linhas | 41 tabelas | DuckDB

== REGRAS SQL ==
- BIGINT: só operadores numéricos. VARCHAR: LIKE/=. Aspas duplas em colunas com espaços/acentos.
- DATAS YYYYMM são BIGINT: WHERE "MÊS COMPETÊNCIA" >= 202401 AND "MÊS COMPETÊNCIA" <= 202412.
- VALORES monetários são VARCHAR: SUM(CAST(REPLACE(REPLACE(coluna,'.',''),',','.') AS DECIMAL))
- LIMIT 100 em listagens; sem LIMIT em COUNT/SUM
- UNION/UNION ALL: ORDER BY só no final. Use alias numérico (ORDER BY 1,2)
- WINDOW FUNCTIONS: NUNCA use OVER() em WHERE. Use QUALIFY ou subconsulta
- BOLSA FAMÍLIA: até 2021→_bolsafamilia_pagamentos; 2022-2025→_novobolsafamilia
- SERVIDORES: ANO e MES são VARCHAR: WHERE ANO='2024' AND MES='01'
- AFASTAMENTOS: use COALESCE(TRY_STRPTIME(col,'%d/%m/%Y'), TRY_STRPTIME(col,'%Y-%m-%d'))
- CEIS/CNEP/CEAF: coluna do documento é "CPF OU CNPJ DO SANCIONADO". Coluna de sanção é "CATEGORIA DA SANÇÃO"
- ACORDOS: status é "SITUAÇÃO DO ACORDO DE LENIÊNICA". Nome é "RAZÃO SOCIAL – CADASTRO RECEITA"
- DATAS VARCHAR em viagens: SUBSTRING("Período - Data de início",1,4) para ano
- CNAES em array: use array_contains(cnaes_secundarios_codigos, '6201') — NUNCA LIKE em array
- BUSCA POR NOME DE PESSOA: SEMPRE use ILIKE '%nome%' — NUNCA use = 'nome exato' pois nomes têm variações de grafia

== EMPRESAS RFB — ARQUITETURA (4 tabelas unificadas, todas as UFs) ==
_rfb_empresas(66M): cnpj_basico(VARCHAR), razao_social, natureza_juridica_codigo, natureza_juridica, qualificacao_responsavel_codigo, qualificacao_responsavel, capital_social(DOUBLE), porte_codigo, porte('MICRO EMPRESA'|'EMPRESA DE PEQUENO PORTE'|'DEMAIS'), ente_federativo
_rfb_estabelecimentos(69M): cnpj_basico, cnpj_completo, cnpj_ordem, cnpj_dv, situacao_cadastral('ATIVA'|'BAIXADA'|'INAPTA'|'SUSPENSA'|'NULA'), data_situacao_cadastral, motivo_situacao, motivo_situacao_codigo, nome_fantasia, matriz_filial('MATRIZ'|'FILIAL'), matriz_filial_codigo, cnae_principal_codigo, cnae_principal, cnaes_secundarios_codigos(VARCHAR[]), cnaes_secundarios_descricoes(VARCHAR[]), uf, municipio, municipio_codigo, logradouro, tipo_logradouro, numero, complemento, bairro, cep, ddd_1, telefone_1, ddd_2, telefone_2, correio_eletronico, data_inicio_atividade, situacao_especial, data_situacao_especial, pais, pais_codigo, cidade_exterior
_rfb_socios(27M): cnpj_basico, nome_socio, cpf_cnpj_socio, identificador_socio, identificador_socio_codigo, qualificacao_socio, qualificacao_socio_codigo, data_entrada_sociedade, pais, pais_codigo, representante_legal, nome_representante, qualificacao_representante_legal, qualificacao_representante_legal_codigo, faixa_etaria, faixa_etaria_codigo
_rfb_simples(66M): cnpj_basico, opcao_simples, opcao_simples_codigo, data_opcao_simples, data_exclusao_simples, opcao_mei, opcao_mei_codigo, data_opcao_mei, data_exclusao_mei
⚠️ JOIN empresas × estabelecimentos: ON e.cnpj_basico = est.cnpj_basico
⚠️ NÃO existem tabelas _empresas_UF — use as 4 tabelas acima

== LIMITAÇÕES ==
- Judiciário, Legislativo, servidores estaduais/municipais: NÃO estão nos dados
- MEI: use opcao_mei='SIM' em _rfb_simples

== CPF — REGRAS CRÍTICAS ==
CPF MASCARADO (NÃO usar em JOIN):
  _bolsafamilia_pagamentos, _bolsafamilia_saques, _novobolsafamilia, _auxiliobrasil,
  _bpc, _auxilioemergencial, _segurodefeso, _garantiasafra, _pedemeia, _peti, _auxilioreconstrucao

CPF COMPLETO (pode usar em JOIN):
  _pep, _servidores, _cpgf, _cpcc, _cpdc, _viagens, _ceis, _cnep, _ceaf, _imoveisfuncionais

== TABELAS ==

-- PROGRAMAS SOCIAIS --
_bolsafamilia_pagamentos(1.48B,até2021): "MÊS COMPETÊNCIA"(BIGINT),"UF","CPF FAVORECIDO","NIS FAVORECIDO"(BIGINT),"NOME FAVORECIDO","VALOR PARCELA"
_bolsafamilia_saques(1.28B,até2021): mesmas colunas +"DATA SAQUE"(DATE) — usa "MÊS REFERÊNCIA" não "MÊS COMPETÊNCIA"
_novobolsafamilia(668M,2022-2025): "MÊS COMPETÊNCIA"(BIGINT),"UF","CPF FAVORECIDO","NIS FAVORECIDO"(BIGINT),"NOME FAVORECIDO","VALOR PARCELA"
_auxiliobrasil(294M): mesmas colunas comuns
_bpc(440M): "MÊS COMPETÊNCIA"(BIGINT),"UF","NOME MUNICÍPIO","NIS BENEFICIÁRIO"(BIGINT),"CPF BENEFICIÁRIO","NOME BENEFICIÁRIO","VALOR PARCELA"
_auxilioemergencial(782M): "MÊS DISPONIBILIZAÇÃO"(BIGINT),"UF","CPF BENEFICIÁRIO","NOME BENEFICIÁRIO","VALOR BENEFÍCIO"
_segurodefeso(40M): "MÊS REFERÊNCIA"(BIGINT),"UF","CPF FAVORECIDO","NOME FAVORECIDO","VALOR PARCELA"
_garantiasafra(33M): "MÊS REFERÊNCIA"(BIGINT),"UF","NOME MUNICÍPIO","NIS FAVORECIDO"(BIGINT),"NOME FAVORECIDO","VALOR PARCELA"
_pedemeia(37M): "MÊS FOLHA"(BIGINT),"UF","CPF BENEFICIÁRIO","NOME BENEFICIÁRIO","ETAPA ENSINO","VALOR PARCELA"
_peti(803K): "MÊS REFERÊNCIA"(BIGINT),"UF","NOME MUNICÍPIO","NIS FAVORECIDO"(BIGINT),"NOME FAVORECIDO","VALOR PARCELA"
_auxilioreconstrucao(425K): "MÊS REFERÊNCIA"(BIGINT),"UF","CPF FAVORECIDO","NOME FAVORECIDO","VALOR PARCELA"

-- SERVIDORES --
_servidores(424M total — cadastro+remuneração+afastamentos+jetons):
  cadastro: Id_SERVIDOR_PORTAL,NOME,CPF,MATRICULA,DESCRICAO_CARGO,ORGSUP_LOTACAO,ORG_LOTACAO,TIPO_VINCULO,SITUACAO_VINCULO,UF_EXERCICIO
  remuneração: ANO(VARCHAR),MES(VARCHAR),Id_SERVIDOR_PORTAL,CPF,NOME,"REMUNERAÇÃO BÁSICA BRUTA (R$)","REMUNERAÇÃO APÓS DEDUÇÕES OBRIGATÓRIAS (R$)"
  afastamentos: ANO,MES,Id_SERVIDOR_PORTAL,CPF,NOME,DATA_INICIO_AFASTAMENTO(VARCHAR),DATA_FIM_AFASTAMENTO(VARCHAR)
  ⚠️ SITUACAO_VINCULO: civis='ATIVO PERMANENTE', militares='MILITAR DA ATIVA'
  ⚠️ Para remuneração completa use tabelas: _servidores + _servidores__2 + _servidores__3 + _servidores__4 + _servidores__5

-- DESPESAS --
_despesas_favorecidos(114M): "Código Favorecido","Nome Favorecido","Sigla UF","Nome Órgão Superior","Ano e mês do lançamento"(VARCHAR'MM/YYYY'),"Valor Recebido"(VARCHAR)
_despesasdiarias(594M): colunas variam por ano (107-112) — principais: "Código Empenho","Data Emissão","Órgão Superior","Favorecido","Código Favorecido","Valor do Pagamento Convertido pra R$"

-- VIAGENS --
_viagens(50M): "Identificador do processo de viagem","CPF viajante","Nome","Cargo","Período - Data de início","Período - Data de fim","Destinos","Valor diárias","Valor passagens"

-- SANÇÕES --
_ceis(22K): "TIPO DE PESSOA","CPF OU CNPJ DO SANCIONADO","NOME DO SANCIONADO","CATEGORIA DA SANÇÃO","DATA INÍCIO SANÇÃO"(DATE),"DATA FINAL SANÇÃO"(DATE),"ÓRGÃO SANCIONADOR"
_cnep(2K): mesmo schema +"VALOR DA MULTA"
_ceaf(4K): "CPF OU CNPJ DO SANCIONADO","NOME DO SANCIONADO","CATEGORIA DA SANÇÃO","DATA INÍCIO SANÇÃO"(DATE)
_cepim(4K): "CNPJ ENTIDADE","NOME ENTIDADE","MOTIVO DO IMPEDIMENTO"
_acordos(298): "CNPJ DO SANCIONADO","RAZÃO SOCIAL – CADASTRO RECEITA","SITUAÇÃO DO ACORDO DE LENIÊNICA","DATA DE INÍCIO DO ACORDO"(DATE)

-- LICITAÇÕES E COMPRAS --
_licitacoes(99M): "Número Licitação","Modalidade Compra","Objeto","Nome Órgão Superior","UF","Data Resultado Compra"(DATE),"Valor Licitação"
_compras(6M): "Código Órgão"(BIGINT),"Nome Órgão","Descrição Item Compra","Quantidade Item"(BIGINT),"Valor Item"
_convenios(1.8M): "NÚMERO CONVÊNIO","UF","SITUAÇÃO CONVÊNIO","OBJETO DO CONVÊNIO","NOME ÓRGÃO CONCEDENTE","NOME CONVENENTE","VALOR CONVÊNIO","VALOR LIBERADO","DATA INÍCIO VIGÊNCIA"(DATE)

-- CARTÃO CORPORATIVO --
_cpgf(1.8M): "NOME ÓRGÃO SUPERIOR","ANO EXTRATO"(BIGINT),"MÊS EXTRATO","CPF PORTADOR","NOME PORTADOR","NOME FAVORECIDO","VALOR TRANSAÇÃO","DATA TRANSAÇÃO"(DATE)
_cpcc(1.3M): similar + "TIPO AQUISIÇÃO"
_cpdc(129K): similar + "NÚMERO CONVÊNIO"(BIGINT)

-- OUTROS --
_pep(71K): CPF,"Nome_PEP","Descrição_Função","Nome_Órgão","Data_Início_Exercício"(DATE),"Data_Fim_Exercício"(VARCHAR)
_imoveisfuncionais(51K): "NOME PERMISSIONÁRIO",CPF,"ÓRGÃO EXERCÍCIO DO PERMISSIONÁRIO","DATA INÍCIO OCUPAÇÃO"(DATE)
_transferencias(9.5M): "ANO / MÊS"(BIGINT YYYYMM),"TIPO TRANSFERÊNCIA","UF","NOME FAVORECIDO","VALOR TRANSFERIDO"
_emendasparlamentarespordocumento(4.4M): "Ano da Emenda"(BIGINT),"Nome do Autor da Emenda","Valor Empenhado","Valor Pago","UF de aplicação do recurso","Favorecido"
_renunciasfiscais(3.3M): "Ano-calendário"(BIGINT),CNPJ,"Razão Social","Código CNAE",UF,"Tipo Renúncia","Valor Renúncia Fiscal (R$)"
_notasfiscais(33M): "CHAVE DE ACESSO"(DOUBLE),"DATA EMISSÃO"(TIMESTAMP),"EVENTO","DESCRIÇÃO EVENTO"
_orcamentodadespesa(332K): "EXERCÍCIO"(BIGINT),"NOME ÓRGÃO SUPERIOR","NOME AÇÃO","ORÇAMENTO INICIAL (R$)","ORÇAMENTO REALIZADO (R$)"
_execucaodareceita(1.7M): "CÓDIGO ÓRGÃO"(BIGINT),"NOME ÓRGÃO","VALOR PREVISTO ATUALIZADO","VALOR REALIZADO","DATA LANÇAMENTO"(DATE)

== CRUZAMENTOS PRINCIPAIS ==

[CNPJ: due diligence]
WITH emp AS (
  SELECT e.cnpj_basico, e.razao_social, e.porte, e.capital_social,
         est.situacao_cadastral, est.uf, est.municipio, est.cnae_principal,
         est.data_inicio_atividade
  FROM _rfb_empresas e
  JOIN _rfb_estabelecimentos est ON est.cnpj_basico = e.cnpj_basico
  WHERE est.cnpj_completo = '33000167000101'
  LIMIT 1
)
SELECT 'CADASTRO RFB' as secao, 'Razão Social / Situação / UF' as campo,
  razao_social || ' | ' || situacao_cadastral || ' | ' || uf as valor FROM emp
UNION ALL
SELECT 'SANÇÃO CEIS', 'Categoria', "CATEGORIA DA SANÇÃO"
FROM _ceis WHERE "CPF OU CNPJ DO SANCIONADO" = '33000167000101'
UNION ALL
SELECT 'DESPESAS 2024', 'Total Recebido',
  CAST(SUM(CAST(REPLACE(REPLACE("Valor Recebido",'.',''),',','.') AS DECIMAL)) AS VARCHAR)
FROM _despesas_favorecidos
WHERE "Código Favorecido" = '33000167000101' AND "Ano e mês do lançamento" LIKE '%/2024'

[Servidor + remuneração]
SELECT c.NOME, c.ORGSUP_EXERCICIO, r."REMUNERAÇÃO BÁSICA BRUTA (R$)"
FROM _servidores c JOIN _servidores__2 r ON r.Id_SERVIDOR_PORTAL = c.Id_SERVIDOR_PORTAL
WHERE r.ANO='2024' AND r.MES='12'
ORDER BY CAST(REPLACE(r."REMUNERAÇÃO BÁSICA BRUTA (R$)",',','.') AS DECIMAL) DESC LIMIT 100

[CEIS × despesas]
WITH sancionados AS (SELECT DISTINCT "CPF OU CNPJ DO SANCIONADO" as cnpj FROM _ceis WHERE "TIPO DE PESSOA"='J')
SELECT s.cnpj, SUM(CAST(REPLACE(REPLACE(d."Valor Recebido",'.',''),',','.') AS DECIMAL)) as total
FROM sancionados s JOIN _despesas_favorecidos d ON d."Código Favorecido" = s.cnpj
WHERE d."Ano e mês do lançamento" LIKE '%/2024'
GROUP BY s.cnpj ORDER BY total DESC LIMIT 20
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
  // Corrige referências às antigas tabelas _empresas_UF
  s = s.replace(/_empresas_[a-z]{2}\b/g, (match) => {
    const uf = match.replace('_empresas_', '').toUpperCase();
    return `_rfb_estabelecimentos WHERE uf='${uf}'`;
  });
  s = s.replace(/SUBSTRING\("DATA LANÇAMENTO",\s*1,\s*7\)/g, 'SUBSTRING(CAST("DATA LANÇAMENTO" AS VARCHAR),1,7)');
  s = s.replace(/SUBSTRING\(("Data Emissão"),\s*1,\s*(\d+)\)/g, 'SUBSTRING(CAST($1 AS VARCHAR),1,$2)');
  s = s.replace(/SUBSTRING\(("Período - Data de início"),\s*1,\s*(\d+)\)/g, 'SUBSTRING(CAST($1 AS VARCHAR),1,$2)');
  s = s.replace(/SUBSTRING\(("Período - Data de fim"),\s*1,\s*(\d+)\)/g, 'SUBSTRING(CAST($1 AS VARCHAR),1,$2)');
  s = s.replace(/SUBSTRING\(("DATA SAQUE"),\s*1,\s*(\d+)\)/g, 'SUBSTRING(CAST($1 AS VARCHAR),1,$2)');
  s = s.replace(/SUBSTRING\(("Data_Início_Exercício"),\s*1,\s*(\d+)\)/g, 'SUBSTRING(CAST($1 AS VARCHAR),1,$2)');

  const monetaryCols = [
    '"Valor diárias"', '"Valor passagens"', '"Valor Licitação"',
    '"VALOR TRANSFERIDO"', '"VALOR LIBERADO"', '"VALOR CONVÊNIO"',
    '"Valor Renúncia Fiscal (R$)"', '"ORÇAMENTO REALIZADO (R$)"',
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

REGRA DE AUDITORIA — CRÍTICA E OBRIGATÓRIA — NÃO IGNORE:
As tabelas PT (não RFB) têm colunas _audit_* que DEVEM aparecer no SELECT final.
Tabelas com _audit_*: _ceis, _cnep, _ceaf, _cepim, _acordos, _despesas_favorecidos, _servidores, _viagens, _renunciasfiscais, _transferencias, _convenios, _licitacoes, _cpgf, _bolsafamilia_pagamentos, _novobolsafamilia, e todas as demais PT.

CASO 1 — SELECT simples (sem GROUP BY): inclua diretamente no SELECT final:
  _audit_arquivo_csv_origem, _audit_linha_csv, _audit_url_download, _audit_data_disponibilizacao_gov

CASO 2 — Agregação (GROUP BY / COUNT / SUM): inclua na CTE ou subquery que lê a tabela original:
  MAX(_audit_url_download) as fonte_url,
  MAX(_audit_data_disponibilizacao_gov) as fonte_data,
  MAX(_audit_arquivo_csv_origem) as fonte_arquivo
  E propague essas colunas até o SELECT final.

REGRA ABSOLUTA: Responda APENAS com SQL puro — zero palavras antes ou depois, zero explicações, zero markdown, zero blocos de código. A primeira palavra da resposta deve ser SELECT ou WITH.`
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
            model: "claude-sonnet-4-20250514",
            max_tokens: 1500,
            messages: [{ role: "user", content: `Pergunta: "${query}"\n\nOs dados não estão na base BDC. Use o contexto web abaixo para responder. Cite as fontes [1],[2] etc e inclua seção ## Fontes.\n\n${webCtx}` }]
          });
          fallbackAnswer = fallback.content.find(b => b.type==="text")?.text || sql;
        }
      }
      return res.json({ answer: fallbackAnswer, sql: "", duration_ms: Date.now() - start, rows_returned: 0 });
    }

    console.log("⚡ Executando...");

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
`Corrija este SQL DuckDB.

PERGUNTA: "${query}"
${schemaBlock}
SQL COM ERRO:
${sql_final}

ERRO:
${data.error}

Responda APENAS com SQL corrigido, sem explicações.` }]
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

    const { needsWeb, needsS2 } = needsExternalContext(query, data.row_count || 0, sql);
    let webContext = null, s2Context = null;

    if (needsWeb && TAVILY_KEY) {
      webContext = await tavilySearch(query);
    }
    if (needsS2) {
      s2Context = await s2Search(query);
    }

    const webSection = webContext ? `\nCONTEXTO WEB:\n${webContext.answer ? `Resumo: ${webContext.answer}\n` : ""}${webContext.results.map((r,i) => `[W${i+1}] ${r.title}\n     URL: ${r.url}\n     ${r.content || ""}`).join("\n")}` : "";
    const s2Section = s2Context?.length ? `\nLITERATURA ACADÊMICA:\n${s2Context.map((p,i) => `[A${i+1}] ${p.title} (${p.year})\n     ${p.abstract || ""}`).join("\n\n")}` : "";

    console.log("💬 Claude explicando...");
    const explanation = await anthropic.messages.create({
      model: "claude-sonnet-4-20250514",
      max_tokens: 2500,
      messages: [{
        role: "user",
        content: `Você é um analista de dados públicos brasileiros com acesso a fontes rastreáveis até o arquivo original.

PERGUNTA: "${query}"

SQL EXECUTADO:
${sql}

RESULTADOS (${data.row_count} linhas):
${JSON.stringify(data.rows?.slice(0, 50), null, 2)}${webSection}${s2Section}

MAPEAMENTO TABELAS → INSTITUIÇÕES:
- _ceis, _cnep, _ceaf, _cepim, _acordos → CGU – Portal da Transparência
- _rfb_* → RFB – Receita Federal (CNPJ)
- _bolsafamilia*, _novobolsafamilia, _bpc, _auxilioemergencial → MDS
- _servidores_* → SEGES/MGI
- _viagens_*, _cpgf* → CGU – Portal da Transparência
- _despesas_*, _despesasdiarias* → SOF/STN – SIAFI
- _convenios* → CGU – SICONV/Transferegov
- _licitacoes*, _compras* → SEGES – Portal de Compras

== RASTREABILIDADE — DIFERENCIAL BDC ==
Os resultados contêm colunas _audit_* com a origem exata de cada dado.
OBRIGATÓRIO: Use essas colunas para construir citações precisas.

Para cada dado relevante na resposta, cite no formato:
> 📄 Fonte: [nome_arquivo_csv] • Publicado em [data_disponibilizacao] • Linha [linha_csv]
> 🔗 Download original: [url_download]

Se os resultados tiverem fonte_url / fonte_arquivo / fonte_data (agregações), use-os da mesma forma.

Ao final, seção **## Fontes** listando todos os arquivos CSV originais citados com suas URLs.

Isso prova que cada número vem de um arquivo oficial do governo — rastreável, verificável, auditável.

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
  console.log("🚀 BDC — BRAZILDATACORP");
  console.log(`📡 Porta: ${PORT} | 🗄️ 7B linhas | 41 tabelas`);
  console.log("═".repeat(60));
});
