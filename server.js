import express from "express";
import cors from "cors";
import Anthropic from "@anthropic-ai/sdk";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));

/* ========================= CONFIGURAÇÃO ========================= */
const HETZNER_SQL_URL  = process.env.HETZNER_SQL_URL || "http://89.167.48.3:5002/sql";
const HETZNER_SQL_KEY  = process.env.HETZNER_SQL_KEY || "";
const PT_SQL_URL       = process.env.PT_SQL_URL || "http://89.167.48.3:5001";
const PT_SQL_KEY       = process.env.PT_SQL_KEY || "bdc-pt-api-key-2026";
const QDRANT_URL       = process.env.QDRANT_URL || "http://89.167.48.3:6333";
const QDRANT_COLLECTION = process.env.QDRANT_COLLECTION || "rfb_catalog";
const PT_COLLECTION    = process.env.PT_COLLECTION || "pt_catalog";

if (!HETZNER_SQL_URL) console.warn("❌ Faltando HETZNER_SQL_URL");

/* ========================= RFB - HETZNER SQL ========================= */
async function queryHetzner(sql) {
  const r = await fetch(HETZNER_SQL_URL, {
    method: "POST",
    headers: { "content-type": "application/json", "x-api-key": HETZNER_SQL_KEY },
    body: JSON.stringify({ sql }),
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(data?.error || `Hetzner SQL erro HTTP ${r.status}`);
  return data.rows || [];
}

/* ========================= PT - SQL ========================= */
async function queryPT(sql, duckdb) {
  const r = await fetch(`${PT_SQL_URL}/sql`, {
    method: "POST",
    headers: { "content-type": "application/json", "X-API-Key": PT_SQL_KEY },
    body: JSON.stringify({ sql, duckdb }),
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(data?.error || `PT SQL erro HTTP ${r.status}`);
  return data.rows || [];
}

async function queryPTAuto(sql, dataset, limit = 100) {
  const r = await fetch(`${PT_SQL_URL}/sql/auto`, {
    method: "POST",
    headers: { "content-type": "application/json", "X-API-Key": PT_SQL_KEY },
    body: JSON.stringify({ sql, dataset, limit }),
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(data?.error || `PT SQL Auto erro HTTP ${r.status}`);
  return data;
}

async function getPTSchema(dataset) {
  const r = await fetch(`${PT_SQL_URL}/schema?dataset=${dataset}`, {
    headers: { "X-API-Key": PT_SQL_KEY },
  });
  const data = await r.json().catch(() => ({}));
  return data.schemas || [];
}

async function getPTDatasets() {
  const r = await fetch(`${PT_SQL_URL}/datasets`, {
    headers: { "X-API-Key": PT_SQL_KEY },
  });
  const data = await r.json().catch(() => ({}));
  return data.datasets || [];
}

/* ========================= RAG - QDRANT ========================= */
async function searchRAG(query, collection, top_k = 3) {
  try {
    const r = await fetch(`${QDRANT_URL}/collections/${collection}/points/scroll`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ limit: 100, with_payload: true, with_vector: false })
    });
    if (!r.ok) return [];
    const data = await r.json();
    const points = data.result?.points || [];
    const keywords = query.toLowerCase().split(/\s+/).filter(w => w.length > 2);
    return points
      .map(p => ({
        score: keywords.reduce((s, kw) => s + (p.payload?.text?.toLowerCase().match(new RegExp(kw, "g")) || []).length, 0),
        point: p
      }))
      .filter(x => x.score > 0)
      .sort((a, b) => b.score - a.score)
      .slice(0, top_k)
      .map(x => ({ text: x.point.payload.text, metadata: x.point.payload.metadata || {}, score: x.score }));
  } catch { return []; }
}

/* ========================= CLAUDE ========================= */
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

function cleanSQL(sql) {
  let s = sql.replace(/```sql|```/gi, "").trim().replace(/;+$/, "");
  if (!/select/i.test(s)) throw new Error("SQL inválida");
  if (/insert|update|delete|drop|alter|create/i.test(s)) throw new Error("Operação bloqueada");
  return s;
}

/* ========================= RFB SCHEMA CACHE ========================= */
let cachedRFBSchema = null;
let rfbCacheExpiry = null;

async function getRFBSchema(userQuery = "") {
  if (cachedRFBSchema && Date.now() < rfbCacheExpiry && !userQuery) return cachedRFBSchema;

  const allTables = await queryHetzner(`SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name`);
  let schema = "TABELAS RFB DISPONÍVEIS:\n\n";
  for (const t of allTables) {
    const columns = await queryHetzner(`SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='${t.table_schema}' AND table_name='${t.table_name}' ORDER BY ordinal_position`);
    schema += `TABELA: ${t.table_schema}.${t.table_name}\n`;
    for (const col of columns) schema += `  • ${col.column_name} (${col.data_type})\n`;
    schema += "\n";
  }

  if (userQuery) {
    const rag = await searchRAG(userQuery, QDRANT_COLLECTION, 3);
    if (rag.length > 0) {
      schema += "\n📚 CONTEXTO RAG:\n";
      rag.forEach((r, i) => schema += `[${i+1}] ${r.text}\n`);
    }
  }

  schema += "\nREGRAS: COUNT(DISTINCT cnpj_basico) para empresas únicas. LIMIT 50 padrão.\n";

  if (!userQuery) { cachedRFBSchema = schema; rfbCacheExpiry = Date.now() + 3600000; }
  return schema;
}

/* ========================= ROTAS RFB ========================= */
app.post("/chat", async (req, res) => {
  const startTime = Date.now();
  try {
    const query = req.body?.query?.trim();
    if (!query) return res.json({ error: "Query vazia" });

    const schema = await getRFBSchema(query);
    const llmSQL = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 500, temperature: 0,
      system: "Você é especialista SQL DuckDB. Gere APENAS a query SQL, sem explicações.",
      messages: [{ role: "user", content: `${schema}\n\nPERGUNTA: "${query}"\n\nSQL:` }]
    });

    const sql = cleanSQL(llmSQL.content[0].text);
    const rows = await queryHetzner(sql);
    const data = rows.map(row => {
      const clean = {};
      for (const [k, v] of Object.entries(row)) clean[k] = typeof v === "bigint" ? Number(v) : v;
      return clean;
    });

    const llmExplain = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 400, temperature: 0.7,
      system: "Você é assistente brasileiro. Seja claro e objetivo.",
      messages: [{ role: "user", content: `Pergunta: "${query}"\nSQL: ${sql}\nResultado: ${JSON.stringify(data.slice(0, 5), null, 2)}\n\nExplique em português:` }]
    });

    return res.json({ answer: llmExplain.content[0].text, sql, rows: data, row_count: data.length, duration_ms: Date.now() - startTime });
  } catch (err) {
    return res.status(500).json({ error: err.message, duration_ms: Date.now() - startTime });
  }
});

/* ========================= ROTAS PT ========================= */
app.post("/chat/pt", async (req, res) => {
  const startTime = Date.now();
  try {
    const query = req.body?.query?.trim();
    const dataset = req.body?.dataset?.trim(); // dataset específico (opcional)
    if (!query) return res.json({ error: "Query vazia" });

    console.log(`\n💬 PT Query: "${query}" | dataset: ${dataset || "auto"}`);

    // Busca RAG do PT
    const rag = await searchRAG(query, PT_COLLECTION, 5);

    // Monta schema context
    let schemaContext = "PORTAL DA TRANSPARÊNCIA — DATASETS DISPONÍVEIS:\n\n";

    if (dataset) {
      const schemas = await getPTSchema(dataset);
      schemaContext += `DATASET: ${dataset}\n`;
      for (const s of schemas) {
        schemaContext += `  Tabela: ${s.table || 'data'}\n`;
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
      rag.forEach((r, i) => schemaContext += `[${i+1}] ${r.text}\n`);
    }

    schemaContext += `
REGRAS:
- A tabela principal em cada duckdb se chama 'data'
- Use LIMIT 50 como padrão
- Para agregar múltiplos períodos, o sistema consultará vários duckdbs automaticamente
`;

    // Gera SQL
    const llmSQL = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 500, temperature: 0,
      system: "Você é especialista SQL DuckDB para dados do Portal da Transparência brasileiro. Gere APENAS a query SQL. A tabela se chama 'data'.",
      messages: [{ role: "user", content: `${schemaContext}\n\nPERGUNTA: "${query}"\n\nSQL:` }]
    });

    const sql = cleanSQL(llmSQL.content[0].text);
    console.log(`📝 SQL PT: ${sql}`);

    // Executa
    let rows = [], duckdbs_queried = 0;
    if (dataset) {
      const result = await queryPTAuto(sql, dataset, 200);
      rows = result.rows || [];
      duckdbs_queried = result.duckdbs_queried || 0;
    } else {
      throw new Error("Especifique o dataset. Ex: BolsaFamilia_Pagamentos, Servidores, DespesasDiarias...");
    }

    const data = rows.map(row => {
      const clean = {};
      for (const [k, v] of Object.entries(row)) clean[k] = typeof v === "bigint" ? Number(v) : v;
      return clean;
    });

    // Explica
    const llmExplain = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 500, temperature: 0.7,
      system: "Você é assistente especialista em dados públicos brasileiros. Seja claro, objetivo e use separadores de milhar.",
      messages: [{ role: "user", content: `Pergunta: "${query}"\nDataset: ${dataset}\nSQL: ${sql}\nResultado (${data.length} linhas, ${duckdbs_queried} arquivos consultados):\n${JSON.stringify(data.slice(0, 5), null, 2)}\n\nExplique em português:` }]
    });

    return res.json({
      answer: llmExplain.content[0].text,
      sql,
      rows: data,
      row_count: data.length,
      duckdbs_queried,
      dataset,
      duration_ms: Date.now() - startTime,
      rag_hits: rag.length
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
  let qdrantOk = false, ptOk = false;
  try { const r = await fetch(`${QDRANT_URL}/collections/${QDRANT_COLLECTION}`); qdrantOk = r.ok; } catch {}
  try { const r = await fetch(`${PT_SQL_URL}/health`); ptOk = r.ok; } catch {}

  res.json({
    ok: true,
    timestamp: new Date().toISOString(),
    rfb: { hetzner_sql: !!HETZNER_SQL_URL, qdrant: qdrantOk },
    pt: { api: ptOk, url: PT_SQL_URL }
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
