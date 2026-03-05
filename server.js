// server.js — BDC Chat (corrigido p/ falhas #64/#84/#96/#98 + anti-timeout + segurança)
// Node 18+ (fetch global). Se Node < 18, instale node-fetch e faça import.

import express from "express";
import cors from "cors";
import Anthropic from "@anthropic-ai/sdk";

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

/* ========================= CONFIG ========================= */
const HETZNER_API = process.env.HETZNER_API_BASE || "http://89.167.48.3:5010";
const HETZNER_KEY = process.env.HETZNER_API_KEY || "bdc-sql-api-key-2026-segura";

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

const PORT = process.env.PORT || 10000;

// Timeouts
const HETZNER_TIMEOUT_MS = Number(process.env.HETZNER_TIMEOUT_MS || 120000);
const CLAUDE_SQL_TIMEOUT_MS = Number(process.env.CLAUDE_SQL_TIMEOUT_MS || 45000);
const CLAUDE_EXPLAIN_TIMEOUT_MS = Number(process.env.CLAUDE_EXPLAIN_TIMEOUT_MS || 45000);

// Proteção de execução SQL
const MAX_SQL_CHARS = Number(process.env.MAX_SQL_CHARS || 20000);
const MAX_RETURN_ROWS_FOR_PROMPT = Number(process.env.MAX_RETURN_ROWS_FOR_PROMPT || 50);

// Bloqueios (evita LLM gerar comandos perigosos)
const FORBIDDEN_SQL = [
  /\battach\b/i,
  /\bcopy\b/i,
  /\binstall\b/i,
  /\bload\b/i,
  /\bpragma\b/i,
  /\bexport\b/i,
  /\bset\b/i,
  /\bcreate\b/i, // mantenha se você não quer DDL vindo do chat
  /\bdrop\b/i,
  /\balter\b/i,
  /\bdelete\b/i,
  /\binsert\b/i,
  /\bupdate\b/i,
  /\bvacuum\b/i,
  /\bcall\b/i,
];

/* ========================= HELPERS ========================= */
function abortableTimeout(ms) {
  return AbortSignal.timeout(ms);
}

function stripCodeFences(s) {
  return (s || "").replace(/```sql\s*/gi, "").replace(/```/g, "").trim();
}

function isSqlStartOk(sql) {
  const s = (sql || "").trim().toLowerCase();
  return s.startsWith("select") || s.startsWith("with");
}

function hasForbiddenSql(sql) {
  return FORBIDDEN_SQL.some((rx) => rx.test(sql));
}

function looksLikeNoSqlAnswer(sqlOrText) {
  // Se Claude insistir em texto ou começar com algo fora SELECT/WITH, tratamos como "resposta sem SQL"
  return !isSqlStartOk(sqlOrText);
}

function safePreview(sql, n = 350) {
  const s = (sql || "").replace(/\s+/g, " ").trim();
  if (s.length <= n) return s;
  return s.slice(0, n) + "…";
}

function normalizeQuery(q) {
  return (q || "").trim();
}

// Regras que forçam "não gerar SQL" para perguntas impossíveis com os dados
function shouldForceNoSql(userQuery) {
  const q = (userQuery || "").toLowerCase();

  // #84 — PEP (CPF) + "tem empresa ativa na Receita" requer CPF->CNPJ (sócios/vínculo), que NÃO está no catálogo
  const pep = /\bpep\b|pessoa(s)? politicamente exposta(s)?/i.test(userQuery);
  const empresaAtiva = /\bempresa\b.*\bativa\b|\btem\b.*\bempresa\b/i.test(userQuery);
  const receita = /\breceita\b|\brfb\b/i.test(userQuery);
  if (pep && empresaAtiva && receita) return true;

  // Cruzamentos por CPF->CNPJ em geral
  if (/\bcpf\b/i.test(userQuery) && /\bcnpj\b/i.test(userQuery) && /\bcruz/i.test(q)) return true;

  return false;
}

// Um mini “catálogo” de dicas anti-timeout (não muda seu banco, mas guia o Claude)
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
- DUE DILIGENCE / ANÁLISE DE CNPJ:
  - NÃO varrer 28 tabelas sem reduzir cedo: sempre comece com WITH alvo AS (SELECT 'CNPJ' AS cnpj)
  - Preferir 3 colunas no UNION: fonte, campo, valor
- AFASTAMENTOS: datas DD/MM/YYYY ou 'Não informada' — filtrar com TRY_STRPTIME(...,'%d/%m/%Y'), nunca TRY_CAST direto.

== LIMITAÇÕES — RESPONDA EM PORTUGUÊS SEM GERAR SQL SE PERGUNTAR SOBRE ==
- Judiciário/Legislativo e servidores estaduais/municipais: NÃO estão nos dados.
- CPF no BF/BPC é mascarado: não cruza com RFB/PEP por CPF.
- PEP (CPF) + "tem empresa ativa na Receita" requer CPF→CNPJ (tabela de sócios/vínculos) — NÃO está disponível: responda SEM SQL.

== TABELAS IMPORTANTES ==
- _acordos: coluna de nome é "RAZÃO SOCIAL – CADASTRO RECEITA" (não existe "RAZÃO SOCIAL" simples)
- _ceis/_cnep/_ceaf: nome é "NOME DO SANCIONADO"
- _despesas_favorecidos: "Ano e mês do lançamento" (VARCHAR 'MM/YYYY'), "Valor Recebido"(VARCHAR)
- EMPRESAS RFB: _empresas_sp ... (28 UFs), est.cnpj_completo, est.situacao_cadastral, est.uf, est.data_inicio_atividade
`;

/* ========================= PROMPTS ========================= */
function sqlPrompt(userQuery) {
  return `Você é especialista em DuckDB e dados públicos brasileiros.

${DB_CATALOG}

PERGUNTA: "${userQuery}"

Gere o SQL DuckDB para responder esta pergunta.
REGRAS ABSOLUTAS:
1) Responda APENAS com SQL puro — zero palavras antes/depois, zero explicações, zero markdown.
2) A primeira palavra deve ser SELECT ou WITH.
3) Não use comandos perigosos (ATTACH/COPY/PRAGMA/INSTALL/LOAD/EXPORT/SET/CREATE/DROP/ALTER/INSERT/UPDATE/DELETE).
4) Para perguntas por CNPJ específico, reduza cedo: comece com WITH alvo AS (SELECT 'CNPJ' AS cnpj).
5) Em due diligence, use UNION ALL com 3 colunas: fonte, campo, valor.`;
}

function explainPrompt(userQuery, sql, rows, rowCount) {
  return `Pergunta: "${userQuery}"

SQL executado:
${sql}

Resultados (${rowCount} linhas):
${JSON.stringify(rows, null, 2)}

Explique os resultados em português, claro e objetivo.
- Formate valores monetários em R$ (ex: R$ 1.234.567,89).
- Cite a fonte (nome da(s) tabela(s)) usada(s) na resposta.
- Se o resultado estiver vazio, diga exatamente que não houve registros encontrados.`;
}

/* ========================= MAIN HANDLER ========================= */
app.post("/chat", async (req, res) => {
  const start = Date.now();
  const query = normalizeQuery(req.body?.query);

  if (!query) return res.json({ error: "Query vazia" });

  try {
    console.log(`\n${"=".repeat(72)}\n❓ "${query}"\n${"=".repeat(72)}`);

    // 1) LIMITAÇÕES: força resposta sem SQL
    if (shouldForceNoSql(query)) {
      const msg =
        "Não consigo gerar SQL para isso com segurança, porque este banco não tem o relacionamento necessário (ex.: CPF→CNPJ para ligar PEP a empresas da Receita). " +
        "Posso responder com o que existe nos dados (por exemplo, PEP como favorecido em despesas), mas sem afirmar vínculo com empresas RFB.";
      return res.json({
        answer: msg,
        sql: "",
        duration_ms: Date.now() - start,
        rows_returned: 0,
      });
    }

    // 2) Geração de SQL
    console.log("🤖 Claude gerando SQL...");
    const sqlGen = await anthropic.messages.create(
      {
        model: process.env.CLAUDE_SQL_MODEL || "claude-sonnet-4-5-20250929",
        max_tokens: 3500,
        messages: [{ role: "user", content: sqlPrompt(query) }],
      },
      { signal: abortableTimeout(CLAUDE_SQL_TIMEOUT_MS) }
    );

    let sql =
      stripCodeFences(sqlGen.content.find((b) => b.type === "text")?.text) || "";

    // 2.1) Sanitiza tamanho
    if (sql.length > MAX_SQL_CHARS) {
      throw new Error(
        `SQL muito grande (${sql.length} chars). Ajuste o prompt ou aumente MAX_SQL_CHARS.`
      );
    }

    console.log(`📝 SQL (preview): ${safePreview(sql, 500)}`);

    // 2.2) Se Claude devolveu texto (não SQL), retorna sem executar
    if (looksLikeNoSqlAnswer(sql)) {
      console.log("💬 Claude respondeu sem SQL (provável limitação dos dados)");
      return res.json({
        answer: sql || "Sem resposta",
        sql: "",
        duration_ms: Date.now() - start,
        rows_returned: 0,
      });
    }

    // 2.3) Bloqueia comandos perigosos
    if (hasForbiddenSql(sql)) {
      throw new Error("SQL bloqueado: contém comando perigoso (DDL/DML/PRAGMA/etc).");
    }

    // 3) Executa no Hetzner
    console.log("⚡ Executando no Hetzner...");
    const response = await fetch(`${HETZNER_API}/query_unified`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-API-Key": HETZNER_KEY },
      body: JSON.stringify({ sql }),
      signal: abortableTimeout(HETZNER_TIMEOUT_MS),
    });

    const data = await response.json().catch(() => ({}));
    if (!response.ok || data?.error) {
      throw new Error(data?.error || `Query falhou (HTTP ${response.status})`);
    }

    const rowCount = Number(data?.row_count || 0);
    const rows = Array.isArray(data?.rows) ? data.rows : [];

    console.log(`📊 ${rowCount} linhas retornadas`);

    // 4) Explica com Claude (somente um pedaço das linhas para não estourar tokens)
    console.log("💬 Claude explicando...");
    const rowsForPrompt = rows.slice(0, MAX_RETURN_ROWS_FOR_PROMPT);

    const explanation = await anthropic.messages.create(
      {
        model: process.env.CLAUDE_EXPLAIN_MODEL || "claude-sonnet-4-5-20250929",
        max_tokens: 2000,
        messages: [
          {
            role: "user",
            content: explainPrompt(query, sql, rowsForPrompt, rowCount),
          },
        ],
      },
      { signal: abortableTimeout(CLAUDE_EXPLAIN_TIMEOUT_MS) }
    );

    const answer =
      explanation.content.find((b) => b.type === "text")?.text?.trim() ||
      "Sem resposta";

    console.log(`✅ CONCLUÍDO em ${Date.now() - start}ms`);

    return res.json({
      answer,
      sql,
      duration_ms: Date.now() - start,
      rows_returned: rowCount,
    });
  } catch (err) {
    const msg = err?.name === "AbortError" ? "Timeout" : err?.message || "Erro";
    console.error("❌ ERRO:", msg);
    return res.status(500).json({ error: msg, duration_ms: Date.now() - start });
  }
});

/* ========================= HEALTH ========================= */
app.get("/health", async (_, res) => {
  try {
    const r = await fetch(`${HETZNER_API}/health`, {
      headers: { "X-API-Key": HETZNER_KEY },
      signal: abortableTimeout(5000),
    });
    res.json({ ok: true, hetzner: r.ok });
  } catch {
    res.json({ ok: true, hetzner: false });
  }
});

/* ========================= START ========================= */
app.listen(PORT, () => {
  console.log("═".repeat(60));
  console.log("🚀 BDC — MOTHERDUCK NO HETZNER");
  console.log(`📡 Porta: ${PORT}`);
  console.log(`🔗 Hetzner API: ${HETZNER_API}`);
  console.log("═".repeat(60));
});
