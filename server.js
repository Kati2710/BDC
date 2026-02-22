// server.js (COMPLETO — corrigido e “blindado” contra o erro do JOIN/UNNEST)
// Node 20 OK no Render

import express from "express";
import cors from "cors";
import Anthropic from "@anthropic-ai/sdk";
import path from "path";
import { fileURLToPath } from "url";

// ✅ Garante fetch/AbortController (Node 18+ já tem, mas assim fica explícito)
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();

/* ========================= CORS + JSON + STATIC ========================= */
app.use(
  cors({
    origin: "*",
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "X-API-Key", "x-api-key"],
  })
);
app.options("*", cors());
app.use(express.json({ limit: "1mb" }));
app.use(express.static(__dirname));

/* ========================= CONFIGURAÇÃO ========================= */
const HETZNER_SQL_URL = process.env.HETZNER_SQL_URL || "http://89.167.48.3:5002/sql";
const HETZNER_SQL_KEY = process.env.HETZNER_SQL_KEY || "";

const PT_SQL_URL = process.env.PT_SQL_URL || "http://89.167.48.3:5001";
const PT_SQL_KEY = process.env.PT_SQL_KEY || "bdc-pt-api-key-2026";

const QDRANT_URL = process.env.QDRANT_URL || "http://89.167.48.3:6333";
const QDRANT_COLLECTION = process.env.QDRANT_COLLECTION || "rfb_catalog";
const PT_COLLECTION = process.env.PT_COLLECTION || "pt_catalog";

if (!HETZNER_SQL_URL) console.warn("❌ Faltando HETZNER_SQL_URL");

/* ========================= HELPERS ========================= */
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchJSON(url, opts = {}, timeoutMs = 30000) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const r = await fetch(url, { ...opts, signal: controller.signal });
    const data = await r.json().catch(() => ({}));
    return { ok: r.ok, status: r.status, data };
  } finally {
    clearTimeout(t);
  }
}

function toSafeNumberOrString(v) {
  // ✅ evita perder precisão (cnpj_basico etc.)
  if (typeof v === "bigint") return v.toString();
  return v;
}

/* ========================= RFB - HETZNER SQL ========================= */
async function queryHetzner(sql) {
  const { ok, status, data } = await fetchJSON(
    HETZNER_SQL_URL,
    {
      method: "POST",
      headers: {
        "content-type": "application/json",
        // mantém x-api-key (se seu backend exigir X-API-Key, troque aqui)
        "x-api-key": HETZNER_SQL_KEY,
      },
      body: JSON.stringify({ sql }),
    },
    60000
  );

  if (!ok) throw new Error(data?.error || `Hetzner SQL erro HTTP ${status}`);
  return data.rows || [];
}

/* ========================= PT - SQL ========================= */
async function queryPT(sql, duckdb) {
  const { ok, status, data } = await fetchJSON(
    `${PT_SQL_URL}/sql`,
    {
      method: "POST",
      headers: { "content-type": "application/json", "X-API-Key": PT_SQL_KEY },
      body: JSON.stringify({ sql, duckdb }),
    },
    60000
  );
  if (!ok) throw new Error(data?.error || `PT SQL erro HTTP ${status}`);
  return data.rows || [];
}

async function queryPTAuto(sql, dataset, limit = 100) {
  const { ok, status, data } = await fetchJSON(
    `${PT_SQL_URL}/sql/auto`,
    {
      method: "POST",
      headers: { "content-type": "application/json", "X-API-Key": PT_SQL_KEY },
      body: JSON.stringify({ sql, dataset, limit }),
    },
    120000
  );
  if (!ok) throw new Error(data?.error || `PT SQL Auto erro HTTP ${status}`);
  return data;
}

async function getPTSchema(dataset) {
  const { data } = await fetchJSON(`${PT_SQL_URL}/schema?dataset=${encodeURIComponent(dataset)}`, {
    headers: { "X-API-Key": PT_SQL_KEY },
  });
  return data.schemas || [];
}

async function getPTDatasets() {
  const { data } = await fetchJSON(`${PT_SQL_URL}/datasets`, {
    headers: { "X-API-Key": PT_SQL_KEY },
  });
  return data.datasets || [];
}

/* ========================= RAG - QDRANT (baseline simples) ========================= */
async function searchRAG(query, collection, top_k = 3) {
  try {
    const { ok, data } = await fetchJSON(
      `${QDRANT_URL}/collections/${collection}/points/scroll`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ limit: 150, with_payload: true, with_vector: false }),
      },
      20000
    );
    if (!ok) return [];

    const points = data.result?.points || [];
    const keywords = query
      .toLowerCase()
      .split(/\s+/)
      .filter((w) => w.length > 2)
      .slice(0, 10);

    return points
      .map((p) => {
        const text = String(p.payload?.text || "").toLowerCase();
        const score = keywords.reduce((s, kw) => s + (text.includes(kw) ? 1 : 0), 0);
        return { score, point: p };
      })
      .filter((x) => x.score > 0)
      .sort((a, b) => b.score - a.score)
      .slice(0, top_k)
      .map((x) => ({
        text: x.point.payload?.text || "",
        metadata: x.point.payload?.metadata || {},
        score: x.score,
      }));
  } catch {
    return [];
  }
}

/* ========================= CLAUDE ========================= */
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

function cleanSQL(sql) {
  let s = String(sql || "")
    .replace(/```sql|```/gi, "")
    .trim()
    .replace(/;+$/, "");

  // ✅ precisa ter SELECT
  if (!/\bselect\b/i.test(s)) throw new Error("SQL inválida (precisa conter SELECT).");

  // ✅ bloqueia operações perigosas
  if (/\b(insert|update|delete|drop|alter|create|truncate)\b/i.test(s)) {
    throw new Error("Operação bloqueada (apenas SELECT).");
  }

  // ✅ bloqueia múltiplos statements (evita "SELECT ...; PRAGMA ...")
  if (s.includes(";")) throw new Error("SQL inválida (múltiplos comandos).");

  // ✅ bloqueia coisas perigosas do DuckDB (ajuste conforme seu endpoint)
  if (/\b(attach|detach|pragma|copy|read_csv|read_parquet|httpfs|install|load)\b/i.test(s)) {
    throw new Error("SQL bloqueada (comando não permitido).");
  }

  return s;
}

/**
 * ✅ Blindagem contra o erro que você viu:
 * "CROSS JOIN UNNEST(x) AS e AND e.campo=..."
 * -> troca o primeiro AND por WHERE
 *
 * E também tenta normalizar UNNEST para "AS t(item)" quando vier "AS e" sem coluna.
 *
 * Obs: não é perfeito, mas resolve o seu caso real e evita 500.
 */
function fixCommonLLMSQLBugs(sql) {
  let s = sql;

  // 1) Erro clássico: "... AS e AND ..."
  s = s.replace(
    /(CROSS\s+JOIN\s+UNNEST\s*\([^)]*\)\s+AS\s+\w+)\s+AND\b/gi,
    "$1 WHERE"
  );

  // 2) Se tem "CROSS JOIN UNNEST(... ) AS e" e depois "e." (alias usado),
  // tenta converter para "AS t(item)" e trocar "e." -> "item."
  // (faz só se não tiver "AS t(" já)
  if (/CROSS\s+JOIN\s+UNNEST\s*\([^)]*\)\s+AS\s+\w+/i.test(s) && !/AS\s+\w+\s*\(\s*\w+\s*\)/i.test(s)) {
    const m = s.match(/CROSS\s+JOIN\s+UNNEST\s*\([^)]*\)\s+AS\s+(\w+)/i);
    if (m && m[1]) {
      const alias = m[1];
      s = s.replace(/CROSS\s+JOIN\s+UNNEST\s*\(([^)]*)\)\s+AS\s+\w+/i, "CROSS JOIN UNNEST($1) AS t(item)");
      const re = new RegExp(`\\b${alias}\\.`, "g");
      s = s.replace(re, "item.");
    }
  }

  return s;
}

/* ========================= RFB SCHEMA CACHE ========================= */
let cachedRFBSchema = null;
let rfbCacheExpiry = null;

// ✅ Cache de schema “pesado” independente da pergunta
let cachedRFBSchemaBase = null;
let rfbBaseExpiry = null;

async function buildRFBSchemaBase() {
  // pega tabelas e colunas (pode demorar — por isso cache)
  const allTables = await queryHetzner(
    `SELECT table_schema, table_name
     FROM information_schema.tables
     WHERE table_schema='main'
     ORDER BY table_name`
  );

  let schema = "TABELAS RFB DISPONÍVEIS:\n\n";
  for (const t of allTables) {
    const columns = await queryHetzner(
      `SELECT column_name, data_type
       FROM information_schema.columns
       WHERE table_schema='${t.table_schema}'
         AND table_name='${t.table_name}'
       ORDER BY ordinal_position`
    );
    schema += `TABELA: ${t.table_schema}.${t.table_name}\n`;
    for (const col of columns) schema += `  • ${col.column_name} (${col.data_type})\n`;
    schema += "\n";
  }

  schema += "\nREGRAS: COUNT(DISTINCT cnpj_basico) para empresas únicas. LIMIT 50 padrão.\n";
  return schema;
}

async function getRFBSchema(userQuery = "") {
  const now = Date.now();

  // base cache
  if (!cachedRFBSchemaBase || now > rfbBaseExpiry) {
    cachedRFBSchemaBase = await buildRFBSchemaBase();
    rfbBaseExpiry = now + 3600_000; // 1h
  }

  // schema final (com RAG opcional)
  if (cachedRFBSchema && now < rfbCacheExpiry && !userQuery) return cachedRFBSchema;

  let schema = cachedRFBSchemaBase;

  if (userQuery) {
    const rag = await searchRAG(userQuery, QDRANT_COLLECTION, 3);
    if (rag.length > 0) {
      schema += "\n📚 CONTEXTO RAG:\n";
      rag.forEach((r, i) => (schema += `[${i + 1}] ${r.text}\n`));
    }
  }

  if (!userQuery) {
    cachedRFBSchema = schema;
    rfbCacheExpiry = now + 3600_000;
  }

  return schema;
}

/* ========================= SYSTEM PROMPTS (MUITO IMPORTANTE) ========================= */
const SYS_SQL_RFB = `
Você é especialista em SQL DuckDB.

REGRAS OBRIGATÓRIAS:
- Gere APENAS a query SQL, sem explicações, sem markdown.
- Apenas SELECT (nunca INSERT/UPDATE/DELETE/DDL).
- Nunca gere múltiplos comandos (sem ponto-e-vírgula no meio).
- Se usar CROSS JOIN UNNEST(), use SEMPRE:
  CROSS JOIN UNNEST(campo_array) AS t(item)
- Coloque TODOS os filtros no WHERE (nunca escreva AND logo após JOIN).
- Para empresas únicas, use COUNT(DISTINCT cnpj_basico).
- Use LIMIT 50 como padrão quando listar linhas.
`;

const SYS_SQL_PT = `
Você é especialista em SQL DuckDB para dados do Portal da Transparência brasileiro.

REGRAS OBRIGATÓRIAS:
- Gere APENAS a query SQL, sem explicações, sem markdown.
- A tabela principal se chama 'data'.
- Apenas SELECT.
- Nunca gere múltiplos comandos (sem ponto-e-vírgula no meio).
- Use LIMIT 50 como padrão quando listar linhas.
`;

/* ========================= ROTAS RFB ========================= */
app.post("/chat", async (req, res) => {
  const startTime = Date.now();
  try {
    const query = req.body?.query?.trim();
    if (!query) return res.json({ error: "Query vazia" });

    const schema = await getRFBSchema(query);

    const llmSQL = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929", // se der erro, troque pro model real da sua conta
      max_tokens: 500,
      temperature: 0,
      system: SYS_SQL_RFB.trim(),
      messages: [{ role: "user", content: `${schema}\n\nPERGUNTA: "${query}"\n\nSQL:` }],
    });

    // pega texto do Claude
    const raw = llmSQL?.content?.[0]?.text ?? "";
    let sql = cleanSQL(raw);
    sql = fixCommonLLMSQLBugs(sql); // ✅ AQUI corrige o bug do AND depois do JOIN

    const rows = await queryHetzner(sql);

    const data = rows.map((row) => {
      const clean = {};
      for (const [k, v] of Object.entries(row)) clean[k] = toSafeNumberOrString(v);
      return clean;
    });

    const llmExplain = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 400,
      temperature: 0.7,
      system: "Você é assistente brasileiro. Seja claro e objetivo.",
      messages: [
        {
          role: "user",
          content:
            `Pergunta: "${query}"\n` +
            `SQL: ${sql}\n` +
            `Resultado (amostra 5): ${JSON.stringify(data.slice(0, 5), null, 2)}\n\n` +
            `Explique em português:`,
        },
      ],
    });

    return res.json({
      answer: llmExplain?.content?.[0]?.text || "Sem resposta.",
      sql,
      rows: data,
      row_count: data.length,
      duration_ms: Date.now() - startTime,
    });
  } catch (err) {
    return res.status(500).json({ error: err.message, duration_ms: Date.now() - startTime });
  }
});

/* ========================= ROTAS PT ========================= */
app.post("/chat/pt", async (req, res) => {
  const startTime = Date.now();
  try {
    const query = req.body?.query?.trim();
    const dataset = req.body?.dataset?.trim();
    if (!query) return res.json({ error: "Query vazia" });

    console.log(`\n💬 PT Query: "${query}" | dataset: ${dataset || "auto"}`);

    const rag = await searchRAG(query, PT_COLLECTION, 5);

    let schemaContext = "PORTAL DA TRANSPARÊNCIA — DATASETS DISPONÍVEIS:\n\n";

    if (dataset) {
      const schemas = await getPTSchema(dataset);
      schemaContext += `DATASET: ${dataset}\n`;
      for (const s of schemas) {
        schemaContext += `  Tabela: ${s.table || "data"}\n`;
        const cols = s.columns_duckdb_table_info || s.columns || [];
        for (const c of cols) schemaContext += `    • ${c.name || c.column_name} (${c.type || c.data_type})\n`;
        schemaContext += "\n";
      }
    } else {
      const datasets = await getPTDatasets();
      schemaContext += "Datasets: " + datasets.join(", ") + "\n\n";
      schemaContext += "Especifique o dataset na sua pergunta para queries mais precisas.\n";
    }

    if (rag.length > 0) {
      schemaContext += "\n📚 CONTEXTO RAG:\n";
      rag.forEach((r, i) => (schemaContext += `[${i + 1}] ${r.text}\n`));
    }

    schemaContext += `
REGRAS:
- A tabela principal em cada duckdb se chama 'data'
- Use LIMIT 50 como padrão
- Para agregar múltiplos períodos, o sistema consultará vários duckdbs automaticamente
`;

    const llmSQL = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 500,
      temperature: 0,
      system: SYS_SQL_PT.trim(),
      messages: [{ role: "user", content: `${schemaContext}\n\nPERGUNTA: "${query}"\n\nSQL:` }],
    });

    const raw = llmSQL?.content?.[0]?.text ?? "";
    let sql = cleanSQL(raw);
    sql = fixCommonLLMSQLBugs(sql); // ✅ também vale pra PT se um dia usar UNNEST

    console.log(`📝 SQL PT: ${sql}`);

    let rows = [],
      duckdbs_queried = 0;

    if (!dataset) {
      throw new Error("Especifique o dataset. Ex: BolsaFamilia_Pagamentos, Servidores, DespesasDiarias...");
    }

    const result = await queryPTAuto(sql, dataset, 200);
    rows = result.rows || [];
    duckdbs_queried = result.duckdbs_queried || 0;

    const data = rows.map((row) => {
      const clean = {};
      for (const [k, v] of Object.entries(row)) clean[k] = toSafeNumberOrString(v);
      return clean;
    });

    const llmExplain = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 500,
      temperature: 0.7,
      system: "Você é assistente especialista em dados públicos brasileiros. Seja claro, objetivo e use separadores de milhar.",
      messages: [
        {
          role: "user",
          content:
            `Pergunta: "${query}"\n` +
            `Dataset: ${dataset}\n` +
            `SQL: ${sql}\n` +
            `Resultado (${data.length} linhas, ${duckdbs_queried} arquivos consultados):\n` +
            `${JSON.stringify(data.slice(0, 5), null, 2)}\n\n` +
            `Explique em português:`,
        },
      ],
    });

    return res.json({
      answer: llmExplain?.content?.[0]?.text || "Sem resposta.",
      sql,
      rows: data,
      row_count: data.length,
      duckdbs_queried,
      dataset,
      duration_ms: Date.now() - startTime,
      rag_hits: rag.length,
    });
  } catch (err) {
    console.error(`❌ PT Erro: ${err.message}`);
    return res.status(500).json({ error: err.message, duration_ms: Date.now() - startTime });
  }
});

app.get("/pt/datasets", async (_, res) => {
  try {
    const datasets = await getPTDatasets();
    res.json({ datasets, count: datasets.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/pt/schema", async (req, res) => {
  try {
    const dataset = req.query.dataset;
    if (!dataset) return res.status(400).json({ error: "Parâmetro 'dataset' obrigatório" });
    const schemas = await getPTSchema(dataset);
    res.json({ dataset, schemas, count: schemas.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ========================= HEALTH ========================= */
app.get("/health", async (_, res) => {
  let qdrantOk = false,
    ptOk = false;

  try {
    const { ok } = await fetchJSON(`${QDRANT_URL}/collections/${QDRANT_COLLECTION}`, {}, 8000);
    qdrantOk = ok;
  } catch {}

  try {
    const { ok } = await fetchJSON(`${PT_SQL_URL}/health`, { headers: { "X-API-Key": PT_SQL_KEY } }, 8000);
    ptOk = ok;
  } catch {}

  res.json({
    ok: true,
    timestamp: new Date().toISOString(),
    rfb: { hetzner_sql: !!HETZNER_SQL_URL, qdrant: qdrantOk },
    pt: { api: ptOk, url: PT_SQL_URL },
  });
});

/* ========================= START ========================= */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log("═".repeat(60));
  console.log("🚀 BrazilDataCorp API — RFB + Portal da Transparência");
  console.log("═".repeat(60));
  console.log(`📡 Porta: ${PORT}`);
  console.log(`🏢 RFB SQL: ${HETZNER_SQL_URL ? "✅" : "❌"}`);
  console.log(`📋 PT SQL: ${PT_SQL_URL}`);
  console.log(`📚 Qdrant: ${QDRANT_URL}`);
  console.log("═".repeat(60));
});
