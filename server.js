// server.js — BDC V6 (corrigido p/ regressão) — DuckDB SQL + AutoFix robusto
import express from "express";
import cors from "cors";
import Anthropic from "@anthropic-ai/sdk";

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

const HETZNER_API = process.env.HETZNER_API_BASE || "http://89.167.48.3:5010";
const HETZNER_KEY = process.env.HETZNER_API_KEY || "bdc-sql-api-key-2026-segura";
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

/* ========================= CATALOGO / REGRAS ========================= */
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
- strftime: NÃO use strftime() — use SUBSTRING(col,1,4) para ano, SUBSTRING(col,6,2) para mês em colunas DATE/VARCHAR. Para DATE→string use CAST(EXTRACT(YEAR FROM col) AS VARCHAR)
- CEPIM: coluna é "CNPJ ENTIDADE" (VARCHAR) — JOIN com convenios via "NÚMERO CONVÊNIO" (preferido) ou "CÓDIGO CONVENENTE", NÃO por CNPJ direto pois formatos diferem
- CNAES em array: cnaes_secundarios_codigos é VARCHAR[] — para filtrar use array_contains(est.cnaes_secundarios_codigos, '6201') NUNCA use LIKE em array
- NÃO existem tabelas empresas_baixadas, empresas_inaptas, empresas_ativas — use _empresas_UF com filtro em est.situacao_cadastral
- SERVIDORES pensionistas (_cadastro__4): colunas específicas CPF_REPRESENTANTE_LEGAL,CPF_INSTITUIDOR_PENSAO,TIPO_PENSAO,DATA_INICIO_PENSAO — NÃO tem ORGSUP_LOTACAO nem ORGSUP_EXERCICIO
- DESPESAS: coluna de órgão em _despesas_favorecidos é "Nome Órgão Superior" — NÃO existe "NOME ÓRGÃO" nem "Órgão Superior" nessa tabela
- WINDOW FUNCTIONS em CTE: alias computado (ex: total_gasto) NÃO pode ser usado em GROUP BY externo — use subconsulta ou repita a expressão

== LIMITAÇÕES — RESPONDA EM PORTUGUÊS SEM GERAR SQL SE PERGUNTAR SOBRE ==
- Judiciário (STF,STJ,TRF,TRT), Legislativo (Câmara,Senado,vereadores): NÃO estão nos dados — não tente SQL
- Servidores estaduais/municipais: NÃO estão nos dados — não tente SQL
- CPF no BF/BPC é mascarado (***123456**): não cruza com RFB/PEP por CPF
- MEI não é identificável: use porte='MICRO EMPRESA' como aproximação
- Abono permanência: sem coluna dedicada

== TABELAS ==
(… seu catálogo completo continua igual ao que você já colou …)
`;

/* ========================= SQL AUTO-FIX (V6 HARDENED) ========================= */
function applySqlAutoFix(sql) {
  let s = sql || "";

  // Normaliza lixo de markdown
  s = s.replace(/```sql\s*/gi, "").replace(/```/g, "").trim();

  /* 1) COLUNAS INVENTADAS / NOMES ERRADOS */
  s = s.replace(/"Início do afastamento"/g, "DATA_INICIO_AFASTAMENTO");
  s = s.replace(/"Fim do afastamento"/g, "DATA_FIM_AFASTAMENTO");

  s = s.replace(/"SITUAÇÃO DO ACORDO"(?! DE LENIÊNICA)/g, '"SITUAÇÃO DO ACORDO DE LENIÊNICA"');

  s = s.replace(/"CNPJ OU CPF DO SANCIONADO"/g, '"CPF OU CNPJ DO SANCIONADO"');
  s = s.replace(/"TIPO SANÇÃO"/g, '"CATEGORIA DA SANÇÃO"');

  s = s.replace(/"RAZÃO SOCIAL"(?! [–-])/g, '"RAZÃO SOCIAL – CADASTRO RECEITA"');

  s = s.replace(/"Nome_Órgão Superior"/g, '"Nome Órgão Superior"');
  s = s.replace(/"Nome_Órgão"/g, '"Nome Órgão"');

  /* 2) VIAGENS: ANO EM VARCHAR (evita SUBSTRING errado + EXTRACT/DATE_PART em VARCHAR) */
  // SUBSTRING(col,7,4) -> SUBSTRING(col,1,4) para colunas de período
  s = s.replace(
    /SUBSTRING\(\s*("[^"]+"\."Período - Data de início"|"Período - Data de início")\s*,\s*7\s*,\s*4\s*\)/g,
    "SUBSTRING($1, 1, 4)"
  );
  s = s.replace(
    /SUBSTRING\(\s*("[^"]+"\."Período - Data de fim"|"Período - Data de fim")\s*,\s*7\s*,\s*4\s*\)/g,
    "SUBSTRING($1, 1, 4)"
  );

  // EXTRACT(YEAR FROM "Período - Data de início") -> CAST(SUBSTRING(...,1,4) AS BIGINT)
  s = s.replace(
    /EXTRACT\s*\(\s*YEAR\s+FROM\s+("Período - Data de início")\s*\)/gi,
    "CAST(SUBSTRING($1, 1, 4) AS BIGINT)"
  );
  s = s.replace(
    /EXTRACT\s*\(\s*YEAR\s+FROM\s+("Período - Data de fim")\s*\)/gi,
    "CAST(SUBSTRING($1, 1, 4) AS BIGINT)"
  );

  // date_part('year', "Período - Data de início") -> substring
  s = s.replace(
    /date_part\s*\(\s*'year'\s*,\s*("Período - Data de início")\s*\)/gi,
    "CAST(SUBSTRING($1, 1, 4) AS BIGINT)"
  );

  /* 3) MONETÁRIO: REPLACE quebrado / 4 argumentos / aspas faltando */
  // REPLACE(x,'.',) -> REPLACE(x,'.','')
  s = s.replace(/REPLACE\(\s*([^,]+)\s*,\s*'\.'\s*,\s*\)/g, "REPLACE($1,'.','')");

  // REPLACE(REPLACE(x,'.',),',','.') -> REPLACE(REPLACE(x,'.',''),',','.')
  s = s.replace(
    /REPLACE\(\s*REPLACE\(\s*([^,]+)\s*,\s*'\.'\s*,\s*\)\s*,\s*','\s*,\s*'\.'\s*\)/g,
    "REPLACE(REPLACE($1,'.',''),',','.')"
  );

  // Padrão (quase sempre) correto para valores comuns (sem forçar quando já está ok)
  // Corrige CAST(REPLACE(col,'.',''),',','.') AS DECIMAL  -> CAST(REPLACE(REPLACE(col,'.',''),',','.') AS DECIMAL)
  s = s.replace(
    /CAST\(\s*REPLACE\(\s*([^,]+)\s*,\s*'\.'\s*,\s*''\s*\)\s*,\s*','\s*,\s*'\.'\s*\)\s*AS\s*DECIMAL\s*\)/gi,
    "CAST(REPLACE(REPLACE($1,'.',''),',','.') AS DECIMAL)"
  );

  /* 4) SANÇÕES: alias errado (c -> ci) quando Claude cria ci/cn */
  s = s.replace(/\bc\."CPF OU CNPJ DO SANCIONADO"\b/g, 'ci."CPF OU CNPJ DO SANCIONADO"');
  s = s.replace(/\bc\."NOME DO SANCIONADO"\b/g, 'ci."NOME DO SANCIONADO"');
  s = s.replace(/\bc\."CATEGORIA DA SANÇÃO"\b/g, 'ci."CATEGORIA DA SANÇÃO"');
  s = s.replace(/\bCASE\s+WHEN\s+c\./gi, "CASE WHEN ci.");

  /* 5) PENSIONISTAS: coluna inexistente */
  s = s.replace(/\bORGSUP_LOTACAO\b/g, "ORGSUP_LOTACAO_INSTITUIDOR_PENSAO");

  /* 6) UNION: remove ORDER BY antes de UNION (DuckDB não deixa em subselect/CTE de union do jeito que Claude faz) */
  s = s.replace(/ORDER BY[\s\S]*?(?=\s+UNION\s+ALL|\s+UNION\s+)/gi, "");

  /* 7) CEPIM x CONVÊNIOS: evita coluna inventada no _convenios */
  s = s.replace(/c\."CNPJ ENTIDADE"/g, 'c."CÓDIGO CONVENENTE"');
  s = s.replace(/"CNPJ ENTIDADE"\s+AS\s+cnpj_entidade/gi, '"CÓDIGO CONVENENTE" AS codigo_convenente');

  return s;
}

/* ========================= HELPERS ========================= */
function isSqlLike(text) {
  const t = (text || "").trim().toLowerCase();
  return t.startsWith("select") || t.startsWith("with");
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
      messages: [
        {
          role: "user",
          content: `Você é especialista em DuckDB e dados públicos brasileiros.

${DB_CATALOG}

PERGUNTA: "${query}"

Gere o SQL DuckDB para responder esta pergunta.
REGRA ABSOLUTA: Responda APENAS com SQL puro — zero palavras antes ou depois, zero explicações, zero markdown, zero blocos de código. A primeira palavra da resposta deve ser SELECT ou WITH.`,
        },
      ],
    });

    let sql = sqlGen.content.find((b) => b.type === "text")?.text?.trim() || "";
    sql = applySqlAutoFix(sql);

    console.log(`📝 SQL (primeiros 300): ${sql.substring(0, 300)}`);

    // Se Claude não entregou SQL (ex.: "não há dados"), devolve sem executar
    if (!isSqlLike(sql)) {
      console.log("💬 Claude respondeu sem SQL (dado não disponível / limitação)");
      return res.json({
        answer: sql,
        sql: "",
        duration_ms: Date.now() - start,
        rows_returned: 0,
      });
    }

    console.log("⚡ Executando no Hetzner...");
    const response = await fetch(`${HETZNER_API}/query_unified`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-API-Key": HETZNER_KEY },
      body: JSON.stringify({ sql }),
      signal: AbortSignal.timeout(240000),
    });

    // Se a API falhar sem JSON, captura texto
    let data;
    try {
      data = await response.json();
    } catch (e) {
      const txt = await response.text().catch(() => "");
      throw new Error(txt || `Query falhou (HTTP ${response.status})`);
    }

    if (!response.ok || data?.error) throw new Error(data?.error || "Query falhou");
    console.log(`📊 ${data.row_count || 0} linhas retornadas`);

    console.log("💬 Claude explicando...");
    const explanation = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 2000,
      messages: [
        {
          role: "user",
          content: `Pergunta: "${query}"

SQL executado:
${sql}

Resultados (${data.row_count} linhas):
${JSON.stringify(data.rows?.slice(0, 50), null, 2)}

Explique os resultados em português de forma clara e objetiva.
Formate valores monetários em R$. Cite a fonte dos dados.`,
        },
      ],
    });

    const answer = explanation.content.find((b) => b.type === "text")?.text || "Sem resposta";
    console.log(`✅ CONCLUÍDO em ${Date.now() - start}ms`);

    return res.json({
      answer,
      sql,
      duration_ms: Date.now() - start,
      rows_returned: data.row_count,
    });
  } catch (err) {
    console.error("❌ ERRO:", err?.message || err);
    return res.status(500).json({ error: err?.message || String(err), duration_ms: Date.now() - start });
  }
});

app.get("/health", async (_, res) => {
  try {
    const r = await fetch(`${HETZNER_API}/health`, {
      headers: { "X-API-Key": HETZNER_KEY },
      signal: AbortSignal.timeout(5000),
    });
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
