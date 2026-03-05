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
- VALORES monetários são VARCHAR: SUM(CAST(REPLACE(REPLACE(coluna,'.',''),',','.') AS DECIMAL))
- strftime: NÃO use strftime() — use SUBSTRING(col,1,4) para ano
- CEIS/CNEP/CEAF: tabela é _ceis/_cnep/_ceaf (sempre com underscore)
- CEPIM: tabela é _cepim
- SERVIDORES pensionistas (_servidores_cadastro__4): NÃO tem ORGSUP_LOTACAO nem ORGSUP_EXERCICIO
(… mantenha seu catálogo completo aqui …)
`;

/* ========================= SQL AUTO-FIX (BLINDADO) ========================= */
function applySqlAutoFix(sql) {
  let s = (sql || "").replace(/```sql\s*/gi, "").replace(/```/g, "").trim();

  // --- 0) tabelas sem underscore (59 explodiu por isso) ---
  s = s.replace(/\bFROM\s+ceis\b/gi, "FROM _ceis");
  s = s.replace(/\bJOIN\s+ceis\b/gi, "JOIN _ceis");
  s = s.replace(/\bFROM\s+cnep\b/gi, "FROM _cnep");
  s = s.replace(/\bJOIN\s+cnep\b/gi, "JOIN _cnep");
  s = s.replace(/\bFROM\s+ceaf\b/gi, "FROM _ceaf");
  s = s.replace(/\bJOIN\s+ceaf\b/gi, "JOIN _ceaf");
  s = s.replace(/\bFROM\s+cepim\b/gi, "FROM _cepim");
  s = s.replace(/\bJOIN\s+cepim\b/gi, "JOIN _cepim");

  // --- 1) pensionistas: coluna inexistente ---
  s = s.replace(/\bORGSUP_LOTACAO\b/g, "ORGSUP_LOTACAO_INSTITUIDOR_PENSAO");

  // --- 2) viagen: ano em VARCHAR (evita EXTRACT/date_part em VARCHAR) ---
  s = s.replace(
    /EXTRACT\s*\(\s*YEAR\s+FROM\s+("Período - Data de início")\s*\)/gi,
    "CAST(SUBSTRING($1,1,4) AS BIGINT)"
  );
  s = s.replace(
    /date_part\s*\(\s*'year'\s*,\s*("Período - Data de início")\s*\)/gi,
    "CAST(SUBSTRING($1,1,4) AS BIGINT)"
  );

  // --- 3) FIX BLINDADO DO ERRO ATUAL (15/15): "AS DECIMAL" dentro do REPLACE ---
  // Converte QUALQUER:
  //   REPLACE(REPLACE(REPLACE(X,'.',''),',','.') AS DECIMAL)
  // em:
  //   CAST(REPLACE(REPLACE(X,'.',''),',','.') AS DECIMAL)

  // 3.1) caso “limpo” com os 3 replaces padrão (pegando X)
  s = s.replace(
    /REPLACE\s*\(\s*REPLACE\s*\(\s*REPLACE\s*\(\s*([\s\S]*?)\s*,\s*'\.'\s*,\s*''\s*\)\s*,\s*','\s*,\s*'\.'\s*\)\s*AS\s*DECIMAL\s*\)/gi,
    "CAST(REPLACE(REPLACE($1,'.',''),',','.') AS DECIMAL)"
  );

  // 3.2) caso ainda embrulhado em CAST(...) (vai virar CAST(CAST(... AS DECIMAL)) e isso é OK)
  s = s.replace(
    /CAST\s*\(\s*REPLACE\s*\(\s*REPLACE\s*\(\s*REPLACE\s*\(\s*([\s\S]*?)\s*,\s*'\.'\s*,\s*''\s*\)\s*,\s*','\s*,\s*'\.'\s*\)\s*AS\s*DECIMAL\s*\)\s*\)/gi,
    "CAST(REPLACE(REPLACE($1,'.',''),',','.') AS DECIMAL)"
  );

  // 3.3) fallback ultra-agressivo: se apareceu "... ) AS DECIMAL" logo após "...',','.')"
  // (pega variações onde o Claude bagunça 1-2 parênteses)
  s = s.replace(
    /REPLACE\s*\(\s*REPLACE\s*\(\s*REPLACE\s*\(\s*([\s\S]*?)\)\s*AS\s*DECIMAL\s*\)/gi,
    "CAST($1 AS DECIMAL)"
  );

  // --- 4) REPLACE quebrado: REPLACE(x,'.',) -> REPLACE(x,'.','') ---
  s = s.replace(/REPLACE\(\s*([^,]+)\s*,\s*'\.'\s*,\s*\)/g, "REPLACE($1,'.','')");

  // --- 5) UNION: remove ORDER BY antes de UNION ---
  s = s.replace(/ORDER BY[\s\S]*?(?=\s+UNION\s+ALL|\s+UNION\s+)/gi, "");

  return s;
}

function isSqlLike(text) {
  const t = (text || "").trim().toLowerCase();
  return t.startsWith("select") || t.startsWith("with");
}

app.post("/chat", async (req, res) => {
  const start = Date.now();
  const query = (req.body?.query || "").trim();
  if (!query) return res.json({ error: "Query vazia" });

  try {
    console.log(`\n${"=".repeat(60)}\n❓ "${query}"\n${"=".repeat(60)}`);

    const sqlGen = await anthropic.messages.create({
      model: "claude-haiku-4-5-20251001",
      max_tokens: 3500,
      messages: [{
        role: "user",
        content: `Você é especialista em DuckDB e dados públicos brasileiros.

${DB_CATALOG}

PERGUNTA: "${query}"

Gere o SQL DuckDB para responder esta pergunta.
REGRA ABSOLUTA: Responda APENAS com SQL puro — zero palavras antes ou depois, zero explicações, zero markdown, zero blocos de código. A primeira palavra deve ser SELECT ou WITH.`
      }]
    });

    let sql = sqlGen.content.find(b => b.type === "text")?.text?.trim() || "";
    sql = applySqlAutoFix(sql);

    console.log(`📝 SQL (primeiros 450): ${sql.substring(0, 450)}`);

    if (!isSqlLike(sql)) {
      return res.json({ answer: sql, sql: "", duration_ms: Date.now() - start, rows_returned: 0 });
    }

    const response = await fetch(`${HETZNER_API}/query_unified`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-API-Key": HETZNER_KEY },
      body: JSON.stringify({ sql }),
      signal: AbortSignal.timeout(240000),
    });

    let data;
    try {
      data = await response.json();
    } catch {
      const txt = await response.text().catch(() => "");
      throw new Error(txt || `Query falhou (HTTP ${response.status})`);
    }

    if (!response.ok || data?.error) throw new Error(data?.error || "Query falhou");

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

Explique em português. Formate moeda em R$. Cite a fonte dos dados.`
      }]
    });

    const answer = explanation.content.find(b => b.type === "text")?.text || "Sem resposta";
    return res.json({ answer, sql, duration_ms: Date.now() - start, rows_returned: data.row_count });
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
