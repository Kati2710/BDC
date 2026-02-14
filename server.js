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

// Serve arquivos estáticos (HTML)
app.use(express.static(__dirname));

/* ========================= CONFIGURAÇÃO ========================= */
const HETZNER_SQL_URL = process.env.HETZNER_SQL_URL || "";
const HETZNER_SQL_KEY = process.env.HETZNER_SQL_KEY || "";
const QDRANT_URL = process.env.QDRANT_URL || "http://89.167.48.3:6333";
const QDRANT_COLLECTION = process.env.QDRANT_COLLECTION || "rfb_catalog";

if (!HETZNER_SQL_URL) console.warn("❌ Faltando HETZNER_SQL_URL");
if (!HETZNER_SQL_KEY) console.warn("❌ Faltando HETZNER_SQL_KEY");

/* ========================= HETZNER SQL ========================= */
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

/* ========================= RAG - QDRANT ========================= */
async function searchRAG(query, top_k = 3) {
  try {
    const scrollUrl = `${QDRANT_URL}/collections/${QDRANT_COLLECTION}/points/scroll`;
    
    const r = await fetch(scrollUrl, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        limit: 100,
        with_payload: true,
        with_vector: false
      })
    });

    if (!r.ok) {
      console.warn(`⚠️  Qdrant erro: ${r.status}`);
      return [];
    }

    const data = await r.json();
    const points = data.result?.points || [];

    // Filtra por keywords
    const queryLower = query.toLowerCase();
    const keywords = queryLower.split(/\s+/).filter(w => w.length > 2);
    
    const scored = points.map(point => {
      const text = (point.payload?.text || "").toLowerCase();
      const score = keywords.reduce((sum, kw) => {
        const count = (text.match(new RegExp(kw, "g")) || []).length;
        return sum + count;
      }, 0);
      
      return { score, point };
    });

    return scored
      .filter(x => x.score > 0)
      .sort((a, b) => b.score - a.score)
      .slice(0, top_k)
      .map(x => ({
        text: x.point.payload.text,
        metadata: x.point.payload.metadata || {},
        score: x.score
      }));

  } catch (err) {
    console.warn(`⚠️  RAG offline: ${err.message}`);
    return [];
  }
}

/* ========================= SCHEMA COM CACHE + RAG ========================= */
let cachedSchema = null;
let cacheExpiry = null;
const CACHE_DURATION = 3600000; // 1 hora

async function getSchema(userQuery = "") {
  if (cachedSchema && Date.now() < cacheExpiry && !userQuery) {
    console.log("📦 Schema em CACHE");
    return cachedSchema;
  }

  console.log("🔄 Buscando schema do Hetzner...");

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

  // ADICIONA CONTEXTO DO RAG
  if (userQuery) {
    console.log(`🔍 Buscando contexto RAG para: "${userQuery}"`);
    const ragResults = await searchRAG(userQuery, 3);
    
    if (ragResults.length > 0) {
      schema += "\n" + "═".repeat(70) + "\n";
      schema += "📚 CONTEXTO RELEVANTE (RAG):\n";
      schema += "═".repeat(70) + "\n";
      
      ragResults.forEach((result, i) => {
        schema += `\n[${i+1}] ${result.text}\n`;
      });
      
      console.log(`✅ Adicionados ${ragResults.length} contextos do RAG`);
    }
  }

  schema += `
${"═".repeat(70)}
REGRAS CRÍTICAS PARA GERAR SQL:
${"═".repeat(70)}
1. CONTAGEM:
   - Empresas ÚNICAS: COUNT(DISTINCT cnpj_basico)
   - Estabelecimentos: COUNT(*)
2. PERFORMANCE:
   - SEMPRE use LIMIT se não for agregação
   - Limite padrão: 50 linhas
3. CONTEXTO RAG:
   - USE o contexto acima para entender melhor os dados
`;

  if (!userQuery) {
    cachedSchema = schema;
    cacheExpiry = Date.now() + CACHE_DURATION;
    console.log("✅ Schema em cache por 1 hora\n");
  }
  
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

/* ========================= ROTAS ========================= */

// Chat principal
app.post("/chat", async (req, res) => {
  const startTime = Date.now();
  try {
    const query = req.body?.query?.trim();
    if (!query) return res.json({ error: "Query vazia" });

    console.log(`\n💬 Query: "${query}"`);

    const schema = await getSchema(query);

    const llmSQL = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 500,
      temperature: 0,
      system: "Você é especialista SQL DuckDB. Gere APENAS a query SQL, sem explicações. Use nomes completos de tabelas. USE O CONTEXTO RAG.",
      messages: [{
        role: "user",
        content: `${schema}\n\nPERGUNTA DO USUÁRIO: "${query}"\n\nGere a SQL:`
      }]
    });

    const sql = cleanSQL(llmSQL.content[0].text);
    console.log(`📝 SQL: ${sql}`);

    const rows = await queryHetzner(sql);

    const data = rows.map(row => {
      const clean = {};
      for (const [k, v] of Object.entries(row)) {
        clean[k] = typeof v === "bigint" ? Number(v) : v;
      }
      return clean;
    });

    console.log(`📊 Resultado: ${data.length} linhas`);

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
    const duration = Date.now() - startTime;
    
    console.log(`✅ Resposta em ${duration}ms\n`);

    return res.json({ 
      answer, 
      sql, 
      rows: data, 
      row_count: data.length, 
      duration_ms: duration,
      rag_enabled: true
    });

  } catch (err) {
    console.error(`❌ Erro: ${err.message}`);
    return res.status(500).json({ 
      error: err.message, 
      duration_ms: Date.now() - startTime 
    });
  }
});

// Health check
app.get("/health", async (_, res) => {
  let qdrantStatus = "offline";
  let qdrantCount = 0;
  try {
    const r = await fetch(`${QDRANT_URL}/collections/${QDRANT_COLLECTION}`);
    if (r.ok) {
      const data = await r.json();
      qdrantStatus = "online";
      qdrantCount = data.result?.vectors_count || 0;
    }
  } catch {}

  res.json({
    ok: true,
    timestamp: new Date().toISOString(),
    cache: cachedSchema ? "active" : "empty",
    hetzner_sql: !!HETZNER_SQL_URL,
    claude: !!process.env.ANTHROPIC_API_KEY,
    rag: {
      status: qdrantStatus,
      url: QDRANT_URL,
      collection: QDRANT_COLLECTION,
      documents: qdrantCount
    }
  });
});

/* ========================= START ========================= */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log("\n" + "═".repeat(60));
  console.log("🚀 CHAT-RFB API COM RAG!");
  console.log("═".repeat(60));
  console.log(`📡 Porta: ${PORT}`);
  console.log(`🔐 Hetzner SQL: ${HETZNER_SQL_URL ? "✅" : "❌"}`);
  console.log(`🤖 Claude: ${process.env.ANTHROPIC_API_KEY ? "✅" : "❌"}`);
  console.log(`📚 Qdrant: ${QDRANT_URL}`);
  console.log(`📦 Collection: ${QDRANT_COLLECTION}`);
  console.log("═".repeat(60));
  console.log(`\n🌐 Acesse: http://localhost:${PORT}\n`);
});
