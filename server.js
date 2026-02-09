import express from "express";
import cors from "cors";
import Anthropic from "@anthropic-ai/sdk";

const app = express();
app.use(cors());
app.use(express.json());

/* ========================= HETZNER SQL API ========================= */
const HETZNER_SQL_URL = process.env.HETZNER_SQL_URL || "";
const HETZNER_SQL_KEY = process.env.HETZNER_SQL_KEY || "";

if (!HETZNER_SQL_URL) console.warn("❌ Faltando HETZNER_SQL_URL");
if (!HETZNER_SQL_KEY) console.warn("❌ Faltando HETZNER_SQL_KEY");

async function queryHetzner(sql) {
  const r = await fetch(HETZNER_SQL_URL, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-api-key": HETZNER_SQL_KEY,
    },
    body: JSON.stringify({ sql }),
  });

  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(data?.error || `Hetzner SQL API erro HTTP ${r.status}`);
  return data.rows || [];
}

/* ========================= SCHEMA COM CACHE ========================= */
let cachedSchema = null;
let cacheExpiry = null;
const CACHE_DURATION = 3600000; // 1 hora

async function getSchema() {
  if (cachedSchema && Date.now() < cacheExpiry) {
    console.log("📦 Schema em CACHE");
    return cachedSchema;
  }

  console.log("🔄 Buscando schema do Hetzner...");

  // Aqui você tem 2 opções:
  // (A) Se você mantiver INFORMATION_SCHEMA disponível no lado Hetzner (duckdb), use isso:
  const allTables = await queryHetzner(`
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema='main'
    ORDER BY table_name
  `);

  let schema = "TABELAS E COLUNAS DISPONÍVEIS:\n\n";

  for (const t of allTables) {
    const fullName = `${t.table_schema}.${t.table_name}`;
    const columns = await queryHetzner(`
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_schema='${t.table_schema}'
        AND table_name='${t.table_name}'
      ORDER BY ordinal_position
    `);

    schema += `TABELA: ${fullName}\n`;
    schema += `Colunas (${columns.length}):\n`;
    for (const col of columns) schema += `  • ${col.column_name} (${col.data_type})\n`;
    schema += "\n";
  }

  // suas regras do Claude continuam iguais:
  schema += `
═══════════════════════════════════════════════════════════════
REGRAS CRÍTICAS PARA GERAR SQL:
═══════════════════════════════════════════════════════════════
1. CONTAGEM:
   - Empresas ÚNICAS: COUNT(DISTINCT cnpj_basico)
   - Estabelecimentos: COUNT(*)
2. PERFORMANCE:
   - SEMPRE use LIMIT se não for agregação
   - Limite padrão: 50 linhas
`;

  cachedSchema = schema;
  cacheExpiry = Date.now() + CACHE_DURATION;
  console.log("✅ Schema em cache por 1 hora\n");
  return schema;
}

/* ========================= CLAUDE ========================= */
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

function cleanSQL(sql) {
  let s = sql.replace(/```sql|```/gi, "").trim().replace(/;+$/, "");
  if (!/select/i.test(s)) throw new Error("SQL inválida");
  if (/insert|update|delete|drop|alter|create/i.test(s)) throw new Error("Operação bloqueada");
  return s;
}

/* ========================= ROTA ========================= */
app.post("/chat", async (req, res) => {
  const startTime = Date.now();
  try {
    const query = req.body?.query?.trim();
    if (!query) return res.json({ error: "Query vazia" });

    const schema = await getSchema();

    const llmSQL = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 500,
      temperature: 0,
      system: "Você é especialista SQL DuckDB. Gere APENAS a query SQL, sem explicações. Use nomes completos de tabelas.",
      messages: [{
        role: "user",
        content: `${schema}\n\nPERGUNTA DO USUÁRIO: "${query}"\n\nGere a SQL:`
      }]
    });

    const sql = cleanSQL(llmSQL.content[0].text);

    // Executa no Hetzner (no lugar do MotherDuck)
    const rows = await queryHetzner(sql);

    const data = rows.map(row => {
      const clean = {};
      for (const [k, v] of Object.entries(row)) {
        clean[k] = typeof v === "bigint" ? Number(v) : v;
      }
      return clean;
    });

    const llmExplain = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 400,
      temperature: 0.7,
      system: "Você é assistente brasileiro. Seja claro, objetivo e use separadores de milhar (ex: 1.234.567).",
      messages: [{
        role: "user",
        content: `Pergunta: "${query}"\n\nSQL executada:\n${sql}\n\nResultado (primeiras 5 linhas):\n${JSON.stringify(data.slice(0, 5), null, 2)}\n\nExplique o resultado em português:`
      }]
    });

    const answer = llmExplain.content[0].text;
    return res.json({ answer, sql, rows: data, row_count: data.length, duration_ms: Date.now() - startTime });

  } catch (err) {
    return res.status(500).json({ error: err.message, duration_ms: Date.now() - startTime });
  }
});

app.get("/health", (_, res) => {
  res.json({
    ok: true,
    timestamp: new Date().toISOString(),
    cache: cachedSchema ? "active" : "empty",
    hetzner_url: !!HETZNER_SQL_URL
  });
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log("🚀 CHAT-RFB API RODANDO");
  console.log(`📡 Porta: ${PORT}`);
  console.log(`🔐 Hetzner SQL API: ${HETZNER_SQL_URL ? "✅ Configurado" : "❌ Faltando"}`);
  console.log(`🤖 Claude: ${process.env.ANTHROPIC_API_KEY ? "✅ Configurado" : "❌ Faltando"}`);
});
