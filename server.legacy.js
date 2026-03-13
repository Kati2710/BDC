import dotenv from "dotenv";
dotenv.config();

import express from "express";
import cors from "cors";
import Anthropic from "@anthropic-ai/sdk";
import { createRequire } from "module";
import crypto from "crypto";

import { classifyDomain } from "./src/core/classifyDomain.js";
import { classifyQuestionType } from "./src/core/classifyQuestionType.js";
import { buildPlan } from "./src/core/planner.js";
import { TABLES_CATALOG } from "./src/catalog/tables.js";
import { viagens_by_name } from "./src/templates/viagens.js";
import { sancoes_by_cnpj } from "./src/templates/sancoes.js";

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

/* ========================= ENV / CONFIG ========================= */
const HETZNER_API = process.env.HETZNER_API_BASE || "http://89.167.48.3:5010";
const HETZNER_KEY = process.env.HETZNER_API_KEY;
const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
const TAVILY_KEY = process.env.TAVILY_API_KEY || "";
const S2_KEY = process.env.S2_API_KEY || "";
const PORT = Number(process.env.PORT || 10000);

if (!HETZNER_KEY) throw new Error("HETZNER_API_KEY ausente");
if (!ANTHROPIC_KEY) throw new Error("ANTHROPIC_API_KEY ausente");

const anthropic = new Anthropic({ apiKey: ANTHROPIC_KEY });

/* ========================= SCHEMA DINÂMICO ========================= */
let SCHEMA = {};
try {
  const require = createRequire(import.meta.url);
  SCHEMA = require("./schema_compact.json");
  console.log(`📋 Schema carregado: ${Object.keys(SCHEMA).length} tabelas`);
} catch {
  console.warn("⚠️ schema_compact.json não encontrado — schema injection desativado");
}

const TABLE_KEYWORDS = {
  "_bolsafamilia_pagamentos": ["bolsa família", "bolsa familia", "bolsafamilia"],
  "_bolsafamilia_saques": ["saque bolsa", "bolsa família saque"],
  "_novobolsafamilia": ["novo bolsa", "bolsa família 202", "bolsa familia 202"],
  "_bpc": ["bpc", "prestação continuada", "benefício assistencial"],
  "_auxilioemergencial": ["auxílio emergencial", "auxilio emergencial", "covid"],
  "_auxiliobrasil": ["auxílio brasil", "auxilio brasil"],
  "_segurodefeso": ["seguro defeso", "pescador"],
  "_garantiasafra": ["garantia safra", "safra"],
  "_pedemeia": ["pé de meia", "pe de meia", "poupança escolar"],
  "_peti": ["peti", "trabalho infantil"],
  "_auxilioreconstrucao": ["auxílio reconstrução", "auxilio reconstrucao", "enchente", "calamidade"],
  "_ceis": ["ceis", "sancionad", "impedid", "inidone", "lista negra"],
  "_cnep": ["cnep", "multa empresa", "dissolução compulsória"],
  "_ceaf": ["ceaf", "demitid", "cassação aposentadoria", "perda emprego"],
  "_cepim": ["cepim", "entidade impedida", "impedimento convênio"],
  "_acordos": ["acordo leniência", "acordo leniencia", "leniência", "leniencia"],
  "_pep": ["pep", "politicamente exposto", "pessoa política"],
  "_despesas_favorecidos": ["despesa", "favorecido", "recebeu recurso", "valor recebido", "recursos federais", "pagamento federal"],
  "_convenios": ["convênio", "convenio", "siconv", "transferegov"],
  "_licitacoes": ["licitação", "licitacao", "pregão", "pregao", "dispensa", "concorrência"],
  "_compras": ["compra", "contrato federal", "item compra"],
  "_transferencias": ["transferência", "transferencia", "repasse federal", "fundo a fundo"],
  "_viagens": ["viagem", "viagens", "diária", "diarias", "diaria", "passagem", "passagens", "deslocamento", "missão", "missao"],
  "_cpgf": ["cartão corporativo", "cartao corporativo", "cpgf", "cartão governo"],
  "_cpcc": ["cpcc", "cartão combustível"],
  "_cpdc": ["cpdc", "cartão convenio"],
  "_servidores": ["servidor", "servidora", "funcionário federal", "funcionario federal", "cargo federal", "lotação", "lotacao", "remuneração federal", "remuneracao federal", "salário federal", "salario federal"],
  "_imoveisfuncionais": ["imóvel funcional", "imovel funcional", "imóveis funcionais", "imoveis funcionais", "residência funcional", "residencia funcional", "permissionário", "permissionario"],
  "_renunciasfiscais": ["renúncia fiscal", "renuncia fiscal", "benefício fiscal", "isenção fiscal"],
  "_orcamentodadespesa": ["orçamento", "orcamento", "dotação", "loa", "ploa"],
  "_execucaodareceita": ["receita federal", "arrecadação", "arrecadacao", "execução receita"],
  "_emendasparlamentarespordocumento": ["emenda parlamentar", "emenda", "parlamentar"],
  "_notasfiscais": ["nota fiscal", "nfe", "chave acesso"],
  "_rfb_empresas": ["empresa", "cnpj", "razão social", "razao social", "capital social", "porte", "mei", "microempresa", "natureza juridica"],
  "_rfb_estabelecimentos": ["estabelecimento", "cnae", "situacao cadastral", "ativa", "baixada", "inapta", "matriz", "filial", "municipio empresa", "uf empresa"],
  "_rfb_socios": ["sócio", "socio", "quadro societario", "representante legal", "participação societária"],
  "_rfb_simples": ["simples nacional", "simples", "optante simples", "mei optante"],
};

function getSchemaBlock(query) {
  const q = query.toLowerCase();
  const matched = new Set();

  for (const [table, keywords] of Object.entries(TABLE_KEYWORDS)) {
    if (keywords.some((k) => q.includes(k))) matched.add(table);
  }

  if (q.includes("viagem") || q.includes("viagens") || q.includes("passagem") || q.includes("diária") || q.includes("diaria")) {
    matched.add("_viagens");
  }
  if (q.includes("imóvel funcional") || q.includes("imovel funcional") || q.includes("imóveis funcionais") || q.includes("imoveis funcionais")) {
    matched.add("_imoveisfuncionais");
  }
  if (q.includes("servidor") || q.includes("ministério da defesa") || q.includes("ministerio da defesa")) {
    matched.add("_servidores");
  }

  const empresaKw = [
    "empresa", "cnpj", "razão social", "razao social", "inapt", "baix", "ativ",
    "estabelecimento", "sócio", "socio", "capital social", "cnae", "porte",
    "matriz", "filial", "mei", "microempresa"
  ];
  if (empresaKw.some((k) => q.includes(k))) {
    matched.add("_rfb_empresas");
    matched.add("_rfb_estabelecimentos");
    matched.add("_rfb_socios");
    matched.add("_rfb_simples");
  }

  if (matched.size === 0 || Object.keys(SCHEMA).length === 0) return "";

  const lines = [];
  for (const table of matched) {
    if (SCHEMA[table]) {
      const cols = SCHEMA[table].filter((c) => !c.startsWith("_audit"));
      lines.push(`${table}: ${cols.join(", ")}`);
    }
  }

  if (lines.length === 0) return "";
  return `\n== SCHEMA EXATO (use SOMENTE estas colunas — não invente outras) ==\n${lines.join("\n")}\n`;
}

/* ========================= EXTERNAL SEARCH ========================= */
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
          "portaldatransparencia.gov.br",
          "cgu.gov.br",
          "rfb.gov.br",
          "gov.br",
          "ibge.gov.br",
          "bcb.gov.br",
          "tcu.gov.br",
          "g1.globo.com",
          "uol.com.br",
          "valor.com.br",
          "agenciabrasil.ebc.com.br"
        ]
      }),
      signal: AbortSignal.timeout(10000)
    });

    if (!res.ok) return null;
    const d = await res.json();

    return {
      answer: d.answer || null,
      results: (d.results || []).map((r) => ({
        title: r.title,
        url: r.url,
        content: r.content?.slice(0, 500) || "",
        published_date: r.published_date || null
      }))
    };
  } catch (e) {
    console.warn("⚠️ Tavily erro:", e.message);
    return null;
  }
}

async function s2Search(query, limit = 5) {
  if (!S2_KEY) return null;

  try {
    const params = new URLSearchParams({
      query,
      limit: String(limit),
      fields: "title,authors,year,abstract,externalIds,openAccessPdf,citationCount,paperId"
    });

    const res = await fetch(`https://api.semanticscholar.org/graph/v1/paper/search?${params.toString()}`, {
      headers: { "x-api-key": S2_KEY },
      signal: AbortSignal.timeout(8000)
    });

    if (!res.ok) return null;
    const d = await res.json();

    return (d.data || []).slice(0, limit).map((p) => ({
      title: p.title,
      authors: (p.authors || []).slice(0, 3).map((a) => a.name).join(", "),
      year: p.year,
      abstract: p.abstract?.slice(0, 400) || "",
      url: p.openAccessPdf?.url || (p.paperId ? `https://www.semanticscholar.org/paper/${p.paperId}` : null),
      citations: p.citationCount || 0
    }));
  } catch (e) {
    console.warn("⚠️ S2 erro:", e.message);
    return null;
  }
}

function needsExternalContext(query, rowCount) {
  const q = query.toLowerCase();

  const noData = rowCount === 0;
  const explicitWebIntent = [
    "notícia", "noticias", "recente", "recentes", "contexto", "histórico", "historico",
    "por que", "análise", "analise", "impacto", "consequência", "consequencia",
    "escândalo", "escandalo", "investigação", "investigacao", "cpi", "operação",
    "operacao", "denúncia", "denuncia", "acusação", "acusacao", "preso", "condenado"
  ].some((k) => q.includes(k));

  const explicitAcademicIntent = [
    "estudo", "pesquisa", "artigo", "literatura", "acadêmico", "academico",
    "correlação", "correlacao", "evidência", "evidencia", "análise setorial", "analise setorial"
  ].some((k) => q.includes(k));

  return {
    needsWeb: noData || explicitWebIntent,
    needsS2: explicitAcademicIntent
  };
}

/* ========================= CATALOG ========================= */
const DB_CATALOG = `
BANCO: brazildatacorp.duckdb | 7B linhas | 41 tabelas | DuckDB

== REGRAS SQL ==
- BIGINT: só operadores numéricos. VARCHAR: LIKE/=. Aspas duplas em colunas com espaços/acentos.
- DATAS YYYYMM são BIGINT: WHERE "MÊS COMPETÊNCIA" >= 202401 AND "MÊS COMPETÊNCIA" <= 202412.
- VALORES monetários são VARCHAR: SUM(CAST(REPLACE(REPLACE(coluna,'.',''),',','.') AS DECIMAL))
- LIMIT 100 em listagens por padrão; use mais apenas se o usuário pedir explicitamente
- sem LIMIT em COUNT/SUM/AVG/MIN/MAX
- UNION/UNION ALL: ORDER BY só no final. Use alias numérico (ORDER BY 1,2)
- WINDOW FUNCTIONS: NUNCA use OVER() em WHERE. Use QUALIFY ou subconsulta
- BOLSA FAMÍLIA: até 2021→_bolsafamilia_pagamentos; 2022-2025→_novobolsafamilia
- SERVIDORES: ANO e MES são VARCHAR: WHERE ANO='2024' AND MES='01'
- AFASTAMENTOS: use COALESCE(TRY_STRPTIME(col,'%d/%m/%Y'), TRY_STRPTIME(col,'%Y-%m-%d'))
- CEIS/CNEP/CEAF: coluna do documento é "CPF OU CNPJ DO SANCIONADO". Coluna de sanção é "CATEGORIA DA SANÇÃO"
- ACORDOS: status é "SITUAÇÃO DO ACORDO DE LENIÊNICA". Nome é "RAZÃO SOCIAL – CADASTRO RECEITA"
- DATAS VARCHAR em viagens: SUBSTRING("Período - Data de início",1,4) para ano
- CNAES em array: use array_contains(cnaes_secundarios_codigos, '6201') — NUNCA LIKE em array
- BUSCA POR NOME DE PESSOA: SEMPRE use ILIKE '%nome%' — NUNCA use = 'nome exato'
- NUNCA use tabelas antigas _empresas_UF. Use somente _rfb_empresas, _rfb_estabelecimentos, _rfb_socios, _rfb_simples
- Quando a pergunta mencionar servidores de um órgão e a tabela principal não identificar isso sozinha, prefira JOIN com _servidores
- Em tabelas PT com _audit_*, sempre inclua _audit_* no SELECT final, ou propague fonte_* em agregações

== EMPRESAS RFB — ARQUITETURA ==
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
  ⚠️ Para remuneração completa use tabelas: _servidores + _servidores__2 + _servidores__3 + _servidores__4 + _servidores__5

-- DESPESAS --
_despesas_favorecidos(114M): "Código Favorecido","Nome Favorecido","Sigla UF","Nome Órgão Superior","Ano e mês do lançamento"(VARCHAR'MM/YYYY'),"Valor Recebido"(VARCHAR)
_despesasdiarias(594M): colunas variam por ano — principais: "Código Empenho","Data Emissão","Órgão Superior","Favorecido","Código Favorecido","Valor do Pagamento Convertido pra R$"

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
`;

/* ========================= HELPERS ========================= */
function cleanGeneratedSql(sql) {
  return (sql || "")
    .replace(/```sql\n?/gi, "")
    .replace(/```/g, "")
    .trim()
    .replace(/;+\s*$/, "")
    .trim();
}

function applySqlAutoFix(sql) {
  let s = sql || "";

  s = s.replace(/TRY_STRPTIME\(([^,]+),\s*'%d\/%m\/%Y'\)/g,
    "COALESCE(TRY_STRPTIME($1, '%d/%m/%Y'), TRY_STRPTIME($1, '%Y-%m-%d'))");
  s = s.replace(/TRY_STRPTIME\(([^,]+),\s*'%Y-%m-%d'\)/g,
    "COALESCE(TRY_STRPTIME($1, '%d/%m/%Y'), TRY_STRPTIME($1, '%Y-%m-%d'))");

  s = s.replace(/"Início do afastamento"/g, "DATA_INICIO_AFASTAMENTO");
  s = s.replace(/"Fim do afastamento"/g, "DATA_FIM_AFASTAMENTO");
  s = s.replace(/"SITUAÇÃO DO ACORDO"(?! DE LENIÊNICA)/g, '"SITUAÇÃO DO ACORDO DE LENIÊNICA"');
  s = s.replace(/"CNPJ OU CPF DO SANCIONADO"/g, '"CPF OU CNPJ DO SANCIONADO"');
  s = s.replace(/\bILIKE\(([^,]+),\s*('[^']*')\)/g, "$1 ILIKE $2");
  s = s.replace(/"TIPO SANÇÃO"/g, '"CATEGORIA DA SANÇÃO"');

  s = s.replace(/SUBSTRING\("DATA LANÇAMENTO",\s*1,\s*7\)/g, 'SUBSTRING(CAST("DATA LANÇAMENTO" AS VARCHAR),1,7)');
  s = s.replace(/SUBSTRING\(("Data Emissão"),\s*1,\s*(\d+)\)/g, "SUBSTRING(CAST($1 AS VARCHAR),1,$2)");
  s = s.replace(/SUBSTRING\(("Período - Data de início"),\s*1,\s*(\d+)\)/g, "SUBSTRING(CAST($1 AS VARCHAR),1,$2)");
  s = s.replace(/SUBSTRING\(("Período - Data de fim"),\s*1,\s*(\d+)\)/g, "SUBSTRING(CAST($1 AS VARCHAR),1,$2)");
  s = s.replace(/SUBSTRING\(("DATA SAQUE"),\s*1,\s*(\d+)\)/g, "SUBSTRING(CAST($1 AS VARCHAR),1,$2)");
  s = s.replace(/SUBSTRING\(("Data_Início_Exercício"),\s*1,\s*(\d+)\)/g, "SUBSTRING(CAST($1 AS VARCHAR),1,$2)");

  const monetaryCols = [
    '"Valor diárias"',
    '"Valor passagens"',
    '"Valor Licitação"',
    '"VALOR TRANSFERIDO"',
    '"VALOR LIBERADO"',
    '"VALOR CONVÊNIO"',
    '"Valor Renúncia Fiscal (R$)"',
    '"ORÇAMENTO REALIZADO (R$)"',
  ];

  for (const col of monetaryCols) {
    const escaped = col.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const alreadyDouble = new RegExp(`REPLACE\\(REPLACE\\(${escaped}`);
    if (!alreadyDouble.test(s)) {
      const single = new RegExp(`REPLACE\\(${escaped},\\s*'\\.',\\s*''\\)`, "g");
      s = s.replace(single, `REPLACE(REPLACE(${col}, '.', ''), ',', '.')`);
      const comma = new RegExp(`REPLACE\\(${escaped},\\s*',',\\s*'\\.'\\)`, "g");
      s = s.replace(comma, `REPLACE(REPLACE(${col}, '.', ''), ',', '.')`);
    }
  }

  return s;
}

function isListLikeQuery(query) {
  const q = (query || "").toLowerCase();
  const listWords = [
    "mostre", "mostrar", "liste", "listar", "quais são", "quais sao",
    "últimas", "ultimas", "ultimos", "últimos", "detalhes", "dados completos"
  ];
  const aggWords = [
    "quantos", "qtd", "total", "soma", "somar", "média", "media",
    "count", "avg", "sum", "mínimo", "minimo", "máximo", "maximo"
  ];
  return listWords.some((w) => q.includes(w)) && !aggWords.some((w) => q.includes(w));
}

function hasExplicitLimit(sql) {
  return /\blimit\s+\d+\b/i.test(sql || "");
}

function isAggregateSql(sql) {
  return /\b(count|sum|avg|min|max)\s*\(/i.test(sql || "");
}

function addDefaultLimitIfNeeded(sql, query) {
  const s = (sql || "").trim();
  if (!s) return s;
  if (!isListLikeQuery(query)) return s;
  if (hasExplicitLimit(s)) return s;
  if (isAggregateSql(s)) return s;
  return `${s}\nLIMIT 100`;
}

function validateReadOnlySql(sql) {
  const cleaned = cleanGeneratedSql(sql);
  const normalized = cleaned.replace(/\s+/g, " ").toLowerCase();

  if (!(normalized.startsWith("select") || normalized.startsWith("with"))) {
    throw new Error("Apenas consultas SELECT/WITH são permitidas");
  }

  if (/_empresas_[a-z]{2}\b/i.test(normalized)) {
    throw new Error("SQL referenciou tabela antiga _empresas_UF; use _rfb_estabelecimentos com filtro por uf");
  }

  const forbidden = [
    /\bdelete\b/,
    /\binsert\b/,
    /\bupdate\b/,
    /\bdrop\b/,
    /\balter\b/,
    /\btruncate\b/,
    /\bcreate\b/,
    /\breplace\s+into\b/,
    /\battach\b/,
    /\bdetach\b/,
    /\bcopy\b/,
    /\bexport\b/,
    /\bimport\b/,
    /\bcall\b/,
    /\bpragma\b/,
    /\binstall\b/,
    /\bload\b/
  ];

  for (const pattern of forbidden) {
    if (pattern.test(normalized)) {
      throw new Error("SQL contém operação não permitida");
    }
  }

  if (cleaned.includes(";")) {
    throw new Error("SQL contém múltiplas instruções, o que não é permitido");
  }

  return true;
}

async function safeJson(response) {
  const text = await response.text();
  try {
    return JSON.parse(text);
  } catch {
    return { error: text || `HTTP ${response.status}` };
  }
}

function maskDocument(value) {
  if (value == null) return value;
  const str = String(value).trim();

  if (/^\d{11}$/.test(str)) {
    return `${str.slice(0, 3)}.***.***-${str.slice(-2)}`;
  }
  if (/^\d{14}$/.test(str)) {
    return `${str.slice(0, 2)}.***.***/****-${str.slice(-2)}`;
  }

  return value;
}

function maskSensitiveRows(rows = []) {
  return rows.map((row) => {
    const masked = { ...row };
    for (const key of Object.keys(masked)) {
      if (
        /(cpf|cnpj)/i.test(key) &&
        (typeof masked[key] === "string" || typeof masked[key] === "number")
      ) {
        masked[key] = maskDocument(masked[key]);
      }
    }
    return masked;
  });
}

function extractSources(rows = []) {
  const seen = new Set();
  const out = [];

  for (const row of rows) {
    const arquivo = row?._audit_arquivo_csv_origem || row?.fonte_arquivo || null;
    const linha = row?._audit_linha_csv ?? row?.fonte_linha ?? null;
    const url = row?._audit_url_download || row?.fonte_url || null;
    const data = row?._audit_data_disponibilizacao_gov || row?.fonte_data || null;

    const key = JSON.stringify([arquivo, linha, url, data]);
    if (!seen.has(key) && (arquivo || url || data)) {
      seen.add(key);
      out.push({
        arquivo,
        linha,
        url,
        data_disponibilizacao: data
      });
    }
  }

  return out;
}

function computeConfidence({ rowCount, usedWeb, sql }) {
  if (!sql) return "low";
  if (usedWeb && rowCount === 0) return "medium";
  if (rowCount > 0) return "high";
  return "low";
}

function truncateRowsForExplanation(rows = []) {
  return rows.slice(0, 15);
}

function isAnthropicLowCreditError(message = "") {
  return message.includes("credit balance is too low");
}

function isAnthropicAuthError(message = "") {
  return message.includes("Invalid authentication credentials");
}

/* ========================= BDC V2 HELPERS ========================= */
function extractCnpjFromQuery(query) {
  const digits = (query || "").replace(/\D/g, "");
  if (digits.length >= 14) return digits.slice(0, 14);
  return null;
}

function extractViagensNameFromQuery(query) {
  const q = (query || "").trim();

  let m = q.match(/viagens?\s+de\s+(.+?)(?:\s+com|\s+dos|\s+das|\s*$)/i);
  if (m?.[1]) return m[1].trim();

  m = q.match(/últimas?\s+\d+\s+viagens?\s+de\s+(.+?)(?:\s+com|\s+dos|\s+das|\s*$)/i);
  if (m?.[1]) return m[1].trim();

  return null;
}

function extractRequestedLimit(query, fallback = 10) {
  const m = (query || "").match(/\b(\d{1,3})\b/);
  if (!m) return fallback;
  const n = Number(m[1]);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.min(n, 100);
}

function buildSqlFromPlan(plan, query) {
  if (!plan || plan.strategy !== "template") return null;

  switch (plan.template) {
    case "viagens_by_name": {
      const name = extractViagensNameFromQuery(query);
      const limit = extractRequestedLimit(query, 10);
      if (!name) return null;
      return viagens_by_name(name, limit);
    }

    case "sancoes_by_cnpj": {
      const cnpj = extractCnpjFromQuery(query);
      if (!cnpj) return null;
      return sancoes_by_cnpj(cnpj);
    }

    default:
      return null;
  }
}

/* ========================= MAIN HANDLER ========================= */
app.post("/chat", async (req, res) => {
  const start = Date.now();
  const requestId = crypto.randomUUID();
  const query = (req.body?.query || "").trim();

  if (!query) {
    return res.status(400).json({
      ok: false,
      error: "Query vazia",
      request_id: requestId,
      duration_ms: Date.now() - start
    });
  }

  try {
    console.log(`\n${"=".repeat(60)}\n[${requestId}] ❓ "${query}"\n${"=".repeat(60)}`);

    const domain = classifyDomain(query);
    const qtype = classifyQuestionType(query);
    const plan = buildPlan({ domain, qtype, query });

    console.log(`[${requestId}] 🧭 domain: ${domain}`);
    console.log(`[${requestId}] 🧠 qtype: ${qtype}`);
    console.log(`[${requestId}] 🗺️ planner strategy: ${plan?.strategy || "unknown"}${plan?.template ? ` | template: ${plan.template}` : ""}`);

    const schemaBlock = getSchemaBlock(query);
    if (schemaBlock) {
      const tables = (schemaBlock.match(/^_\w+:/gm) || []).map((t) => t.replace(":", ""));
      console.log(`[${requestId}] 📋 Schema injetado para: ${tables.join(", ")}`);
    }

    let sql = null;

    if (plan?.strategy === "template") {
      sql = buildSqlFromPlan(plan, query);

      if (sql) {
        sql = cleanGeneratedSql(sql);
        sql = applySqlAutoFix(sql);
        sql = addDefaultLimitIfNeeded(sql, query);
        validateReadOnlySql(sql);

        const templateTableGuess =
          domain === "viagens" ? "_viagens" :
          domain === "sancoes" ? "_ceis" :
          null;

        if (templateTableGuess && TABLES_CATALOG[templateTableGuess]) {
          console.log(`[${requestId}] 🧩 Template aplicado com catálogo: ${templateTableGuess}`);
        } else {
          console.log(`[${requestId}] 🧩 Template aplicado`);
        }
      } else {
        console.log(`[${requestId}] ⚠️ Planner escolheu template, mas parâmetros não puderam ser extraídos. Usando fallback Claude.`);
      }
    }

    if (!sql) {
      console.log(`[${requestId}] 🤖 Claude gerando SQL...`);

      const sqlGen = await anthropic.messages.create({
        model: "claude-haiku-4-5-20251001",
        max_tokens: 1200,
        messages: [{
          role: "user",
          content: `Você é especialista em DuckDB e dados públicos brasileiros.

${DB_CATALOG}${schemaBlock}

PERGUNTA: "${query}"

Gere o SQL DuckDB para responder esta pergunta.

REGRA DE AUDITORIA — CRÍTICA E OBRIGATÓRIA:
As tabelas PT (não RFB) têm colunas _audit_* que DEVEM aparecer no SELECT final.
Tabelas com _audit_* incluem _ceis, _cnep, _ceaf, _cepim, _acordos, _despesas_favorecidos, _servidores, _viagens, _renunciasfiscais, _transferencias, _convenios, _licitacoes, _cpgf, _bolsafamilia_pagamentos, _novobolsafamilia e outras bases PT.

CASO 1 — SELECT simples (sem GROUP BY):
inclua diretamente no SELECT final:
_audit_arquivo_csv_origem, _audit_linha_csv, _audit_url_download, _audit_data_disponibilizacao_gov

CASO 2 — Agregação:
inclua na CTE ou subquery:
MAX(_audit_url_download) as fonte_url,
MAX(_audit_data_disponibilizacao_gov) as fonte_data,
MAX(_audit_arquivo_csv_origem) as fonte_arquivo
e propague até o SELECT final.

REGRAS ABSOLUTAS:
- Responda APENAS com SQL puro
- Zero explicações
- Zero markdown
- A primeira palavra da resposta deve ser SELECT ou WITH
- Somente leitura
- Nunca use tabelas _empresas_UF
- Em listagens, use LIMIT 100 por padrão, salvo se o usuário pedir explicitamente mais
- Quando a pergunta envolver servidores de um órgão, prefira JOIN com _servidores se isso melhorar a precisão`
        }]
      });

      sql = sqlGen.content.find((b) => b.type === "text")?.text?.trim() || "";
      console.log(`[${requestId}] SQL bruto gerado: ${sql}`);

      sql = cleanGeneratedSql(sql);
      sql = applySqlAutoFix(sql);
      sql = addDefaultLimitIfNeeded(sql, query);

      try {
        validateReadOnlySql(sql);
      } catch (validationErr) {
        console.log(`[${requestId}] ⚠️ SQL inicial falhou na validação: ${validationErr.message}`);
        const fixForValidation = await anthropic.messages.create({
          model: "claude-haiku-4-5-20251001",
          max_tokens: 1200,
          messages: [{
            role: "user",
            content: `Corrija este SQL DuckDB para ficar estritamente somente leitura e compatível com as regras.

PERGUNTA: "${query}"
${schemaBlock}

REGRAS:
- Responda APENAS com SQL corrigido
- Sem explicações
- Sem markdown
- Permita apenas SELECT ou WITH
- Nunca use _empresas_UF
- Em listagens, use LIMIT 100 por padrão
- Use _audit_* quando a tabela PT tiver essas colunas

SQL:
${sql}

ERRO DE VALIDAÇÃO:
${validationErr.message}`
          }]
        });

        sql = fixForValidation.content.find((b) => b.type === "text")?.text?.trim() || sql;
        console.log(`[${requestId}] SQL bruto corrigido pós-validação: ${sql}`);
        sql = cleanGeneratedSql(sql);
        sql = applySqlAutoFix(sql);
        sql = addDefaultLimitIfNeeded(sql, query);
        validateReadOnlySql(sql);
      }
    }

    console.log(`[${requestId}] 📝 SQL: ${sql.substring(0, 300)}`);

    const sqlLower = sql.toLowerCase();
    if (!sqlLower.startsWith("select") && !sqlLower.startsWith("with")) {
      console.log(`[${requestId}] 💬 Claude respondeu sem SQL — tentando contexto web...`);

      let fallbackAnswer = sql;
      let usedWebFallback = false;

      if (TAVILY_KEY) {
        const web = await tavilySearch(query, 4);
        if (web?.results?.length) {
          usedWebFallback = true;

          const webCtx = web.results
            .map((r, i) => `[${i + 1}] ${r.title}\nURL: ${r.url}\n${r.content || ""}`)
            .join("\n\n");

          const fallback = await anthropic.messages.create({
            model: "claude-haiku-4-5-20251001",
            max_tokens: 1000,
            messages: [{
              role: "user",
              content: `Pergunta: "${query}"

Os dados não estão na base BDC. Use somente o contexto web abaixo para responder.
Não invente fatos.
Cite as fontes [1], [2] etc.
Inclua seção ## Fontes.

${webCtx}`
            }]
          });

          fallbackAnswer = fallback.content.find((b) => b.type === "text")?.text || sql;
        }
      }

      return res.json({
        ok: true,
        answer: fallbackAnswer,
        sql: "",
        duration_ms: Date.now() - start,
        rows_returned: 0,
        conv_id: req.body?.conv_id || null,
        request_id: requestId,
        confidence: "low",
        sources: [],
        meta: {
          used_web: usedWebFallback,
          used_s2: false,
          model_sql: sql ? "template_or_unknown" : "claude-haiku-4-5-20251001",
          model_answer: "claude-haiku-4-5-20251001",
          domain,
          qtype,
          planner_strategy: plan?.strategy || null,
          planner_template: plan?.template || null
        }
      });
    }

    console.log(`[${requestId}] ⚡ Executando...`);

    let data;
    let sqlFinal = sql;

    for (let attempt = 1; attempt <= 2; attempt++) {
      const response = await fetch(`${HETZNER_API}/query_unified`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-API-Key": HETZNER_KEY
        },
        body: JSON.stringify({ sql: sqlFinal }),
        signal: AbortSignal.timeout(240000)
      });

      data = await safeJson(response);

      if (!response.ok || data.error) {
        if (attempt === 2) {
          throw new Error(data.error || "Query falhou após 2 tentativas");
        }

        console.log(`[${requestId}] ⚠️ Tentativa ${attempt} falhou: ${data.error}`);

        if (plan?.strategy === "template") {
          console.log(`[${requestId}] ⚠️ Template falhou; entrando em fallback Claude para correção...`);
        } else {
          console.log(`[${requestId}] 🔄 Haiku corrigindo SQL...`);
        }

        const fix = await anthropic.messages.create({
          model: "claude-haiku-4-5-20251001",
          max_tokens: 1200,
          messages: [{
            role: "user",
            content: `Corrija este SQL DuckDB.

PERGUNTA: "${query}"
${schemaBlock}

REGRAS:
- Responda APENAS com SQL corrigido
- Sem explicações
- Sem markdown
- Somente leitura
- Nunca use _empresas_UF
- Em listagens, use LIMIT 100 por padrão
- Preserve _audit_* quando existir na tabela PT

SQL COM ERRO:
${sqlFinal}

ERRO:
${data.error}`
          }]
        });

        sqlFinal = fix.content.find((b) => b.type === "text")?.text?.trim() || sqlFinal;
        console.log(`[${requestId}] SQL bruto corrigido na tentativa ${attempt}: ${sqlFinal}`);

        sqlFinal = cleanGeneratedSql(sqlFinal);
        sqlFinal = applySqlAutoFix(sqlFinal);
        sqlFinal = addDefaultLimitIfNeeded(sqlFinal, query);
        validateReadOnlySql(sqlFinal);

        console.log(`[${requestId}] 🔄 SQL corrigido: ${sqlFinal.substring(0, 200)}`);
      } else {
        break;
      }
    }

    sql = sqlFinal;
    const rowCount = Number(data.row_count || 0);
    const rawRows = Array.isArray(data.rows) ? data.rows : [];
    const safeRows = maskSensitiveRows(truncateRowsForExplanation(rawRows));
    const sources = extractSources(rawRows);

    console.log(`[${requestId}] 📊 ${rowCount} linhas retornadas`);

    const { needsWeb, needsS2 } = needsExternalContext(query, rowCount);
    let webContext = null;
    let s2Context = null;

    if (needsWeb && TAVILY_KEY) {
      webContext = await tavilySearch(query);
    }
    if (needsS2 && S2_KEY) {
      s2Context = await s2Search(query);
    }

    const webSection = webContext
      ? `\nCONTEXTO WEB:\n${webContext.answer ? `Resumo: ${webContext.answer}\n` : ""}${webContext.results.map((r, i) =>
          `[W${i + 1}] ${r.title}\nURL: ${r.url}\n${r.content || ""}`
        ).join("\n\n")}`
      : "";

    const s2Section = s2Context?.length
      ? `\nLITERATURA ACADÊMICA:\n${s2Context.map((p, i) =>
          `[A${i + 1}] ${p.title} (${p.year})\n${p.abstract || ""}`
        ).join("\n\n")}`
      : "";

    console.log(`[${requestId}] 💬 Claude explicando...`);

    const answerModel =
      rowCount <= 10 && !webContext && !s2Context
        ? "claude-haiku-4-5-20251001"
        : "claude-sonnet-4-20250514";

    const explanation = await anthropic.messages.create({
      model: answerModel,
      max_tokens: 1500,
      messages: [{
        role: "user",
        content: `Você é um analista de dados públicos brasileiros com acesso a fontes rastreáveis até o arquivo original.

PERGUNTA: "${query}"

SQL EXECUTADO:
${sql}

RESULTADOS (${rowCount} linhas):
${JSON.stringify(safeRows, null, 2)}${webSection}${s2Section}

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
OBRIGATÓRIO: use essas colunas para construir citações precisas.

Formato de citação:
> 📄 Fonte: [nome_arquivo_csv] • Publicado em [data_disponibilizacao] • Linha [linha_csv]
> 🔗 Download original: [url_download]

Se os resultados tiverem fonte_url / fonte_arquivo / fonte_data, use-os da mesma forma.

Regras:
- Use apenas os dados presentes em RESULTADOS e nos blocos CONTEXTO WEB / LITERATURA
- Não invente colunas, datas, totais, nomes ou interpretações não suportadas
- Se um dado não estiver explícito, diga que não foi encontrado
- Se houver ambiguidade de pessoa/nome, destaque isso claramente
- Mantenha CPFs e CNPJs mascarados na resposta textual
- Formate valores em R$
- Seja preciso e objetivo
- Ao final, inclua seção **## Fontes** listando os arquivos CSV originais citados com suas URLs`
      }]
    });

    const answer = explanation.content.find((b) => b.type === "text")?.text || "Sem resposta";
    const duration = Date.now() - start;

    console.log(`[${requestId}] 💾 Salvando conversa...`);

    const convId = req.body?.conv_id || null;
    const userEmail = req.body?.user || "anonymous";
    let savedConvId = convId;

    try {
      const r1 = await fetch(`${HETZNER_API}/conversations/message`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-API-Key": HETZNER_KEY
        },
        body: JSON.stringify({
          user: userEmail,
          conv_id: convId,
          role: "user",
          content: query
        })
      });

      const d1 = await safeJson(r1);
      savedConvId = d1.conv_id || convId;

      await fetch(`${HETZNER_API}/conversations/message`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-API-Key": HETZNER_KEY
        },
        body: JSON.stringify({
          user: userEmail,
          conv_id: savedConvId,
          role: "assistant",
          content: answer,
          sql_used: sql,
          row_count: rowCount,
          duration_ms: duration
        })
      });

      console.log(`[${requestId}] ✅ Conversa salva: ${savedConvId}`);
    } catch (e) {
      console.warn(`[${requestId}] ⚠️ Erro ao salvar conversa:`, e.message);
    }

    const usedWeb = !!webContext;
    const usedS2 = !!(s2Context?.length);
    const confidence = computeConfidence({ rowCount, usedWeb, sql });

    console.log(`[${requestId}] ✅ CONCLUÍDO em ${duration}ms`);

    return res.json({
      ok: true,
      answer,
      sql,
      duration_ms: duration,
      rows_returned: rowCount,
      conv_id: savedConvId,
      request_id: requestId,
      confidence,
      sources,
      meta: {
        used_web: usedWeb,
        used_s2: usedS2,
        model_sql: plan?.strategy === "template" && buildSqlFromPlan(plan, query)
          ? "template"
          : "claude-haiku-4-5-20251001",
        model_answer: answerModel,
        domain,
        qtype,
        planner_strategy: plan?.strategy || null,
        planner_template: plan?.template || null
      }
    });
  } catch (err) {
    const msg = String(err.message || "");

    if (isAnthropicLowCreditError(msg)) {
      return res.status(402).json({
        ok: false,
        error: "Créditos da Anthropic insuficientes. Verifique Plans & Billing da API.",
        request_id: requestId,
        duration_ms: Date.now() - start
      });
    }

    if (isAnthropicAuthError(msg)) {
      return res.status(401).json({
        ok: false,
        error: "ANTHROPIC_API_KEY inválida ou não reconhecida.",
        request_id: requestId,
        duration_ms: Date.now() - start
      });
    }

    console.error(`[${requestId}] ❌ ERRO:`, err.message);
    return res.status(500).json({
      ok: false,
      error: err.message,
      request_id: requestId,
      duration_ms: Date.now() - start
    });
  }
});

/* ========================= CONVERSATIONS PROXY ========================= */
app.get("/conversations", async (req, res) => {
  try {
    const user = req.query.user || "";
    const r = await fetch(`${HETZNER_API}/conversations?user=${encodeURIComponent(user)}`, {
      headers: { "X-API-Key": HETZNER_KEY }
    });
    res.status(r.status).json(await safeJson(r));
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/conversations/:id", async (req, res) => {
  try {
    const r = await fetch(`${HETZNER_API}/conversations/${req.params.id}`, {
      headers: { "X-API-Key": HETZNER_KEY }
    });
    res.status(r.status).json(await safeJson(r));
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/conversations/message", async (req, res) => {
  try {
    const r = await fetch(`${HETZNER_API}/conversations/message`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-API-Key": HETZNER_KEY
      },
      body: JSON.stringify(req.body)
    });
    res.status(r.status).json(await safeJson(r));
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.delete("/conversations/:id", async (req, res) => {
  try {
    const r = await fetch(`${HETZNER_API}/conversations/${req.params.id}`, {
      method: "DELETE",
      headers: { "X-API-Key": HETZNER_KEY }
    });
    res.status(r.status).json(await safeJson(r));
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/health", async (_, res) => {
  try {
    const r = await fetch(`${HETZNER_API}/health`, {
      headers: { "X-API-Key": HETZNER_KEY },
      signal: AbortSignal.timeout(5000)
    });

    res.json({
      ok: true,
      hetzner: r.ok,
      catalog_tables: Object.keys(TABLES_CATALOG || {}).length
    });
  } catch {
    res.json({
      ok: true,
      hetzner: false,
      catalog_tables: Object.keys(TABLES_CATALOG || {}).length
    });
  }
});

/* ========================= START ========================= */
app.listen(PORT, () => {
  console.log("═".repeat(60));
  console.log("🚀 BDC — BRAZILDATACORP");
  console.log(`📡 Porta: ${PORT} | 🗄️ 7B linhas | 41 tabelas`);
  console.log(`🧩 Catálogo carregado: ${Object.keys(TABLES_CATALOG || {}).length} tabelas`);
  console.log("═".repeat(60));
});