import express from "express";
import cors from "cors";
import Anthropic from "@anthropic-ai/sdk";

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

/* ========================= CONFIG ========================= */
const HETZNER_API_BASE = process.env.HETZNER_API_BASE || "http://89.167.48.3:5010";
const HETZNER_API_KEY = process.env.HETZNER_API_KEY || "bdc-sql-api-key-2026-segura";

const QDRANT_URL = process.env.QDRANT_URL || "http://89.167.48.3:6333";
const QDRANT_COLLECTION = process.env.QDRANT_COLLECTION || "rfb_catalog";
const PT_COLLECTION = process.env.PT_COLLECTION || "pt_catalog";

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

/* ========================= UTILS ========================= */
function cleanSQL(sql) {
  let s = String(sql || "").replace(/```sql|```/gi, "").trim().replace(/;+$/, "");
  if (!/\bselect\b/i.test(s)) throw new Error("SQL inválida (precisa SELECT)");
  if (/\b(insert|update|delete|drop|alter|create|truncate|attach|detach|pragma|copy|install|load|read_csv|read_parquet)\b/i.test(s)) {
    throw new Error("Operação bloqueada (apenas SELECT).");
  }
  if (s.includes(";")) throw new Error("SQL inválida (múltiplos comandos).");
  return s;
}

function fixUnnestAndBug(sql) {
  return sql.replace(
    /(CROSS\s+JOIN\s+UNNEST\s*\([^)]*\)\s+AS\s+\w+)\s+AND\b/gi,
    "$1 WHERE"
  );
}

async function fetchJSON(url, opts = {}, timeoutMs = 30000) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const r = await fetch(url, { ...opts, signal: controller.signal });
    const data = await r.json().catch(() => ({}));
    return { ok: r.ok, status: r.status, data };
  } catch (err) {
    const cause = err?.cause ? ` | cause: ${err.cause.code || ""} ${err.cause.message || err.cause}` : "";
    throw new Error(`fetch failed for ${url}${cause}`);
  } finally {
    clearTimeout(t);
  }
}

/* ========================= HETZNER API ========================= */
async function hetznerCatalog() {
  const { ok, status, data } = await fetchJSON(`${HETZNER_API_BASE}/catalog`, {
    headers: { "X-API-Key": HETZNER_API_KEY },
  }, 20000);
  if (!ok) throw new Error(data?.error || `Catalog erro HTTP ${status}`);
  return data;
}

async function hetznerSchema({ kind, dataset, uf }) {
  const qs = new URLSearchParams();
  qs.set("kind", kind);
  if (dataset) qs.set("dataset", dataset);
  if (uf) qs.set("uf", uf);

  const { ok, status, data } = await fetchJSON(`${HETZNER_API_BASE}/schema?${qs.toString()}`, {
    headers: { "X-API-Key": HETZNER_API_KEY },
  }, 20000);

  if (!ok) throw new Error(data?.error || `Schema erro HTTP ${status}`);
  return data.schema;
}

async function hetznerSQL({ kind, sql, dataset, uf, limit = 200 }) {
  const { ok, status, data } = await fetchJSON(`${HETZNER_API_BASE}/sql`, {
    method: "POST",
    headers: { "content-type": "application/json", "X-API-Key": HETZNER_API_KEY },
    body: JSON.stringify({ kind, sql, dataset, uf, limit }),
  }, 90000);

  if (!ok) throw new Error(data?.error || `SQL erro HTTP ${status}`);
  return data;
}

async function hetznerSQLAutoPT({ sql, dataset, limit = 200 }) {
  const { ok, status, data } = await fetchJSON(`${HETZNER_API_BASE}/sql/auto`, {
    method: "POST",
    headers: { "content-type": "application/json", "X-API-Key": HETZNER_API_KEY },
    body: JSON.stringify({ sql, dataset, limit }),
  }, 120000);

  if (!ok) throw new Error(data?.error || `SQL auto erro HTTP ${status}`);
  return data;
}

/* ========================= RAG ========================= */
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
    const keywords = query.toLowerCase().split(/\s+/).filter(w => w.length > 2).slice(0, 10);

    return points
      .map(p => {
        const text = String(p.payload?.text || "").toLowerCase();
        const score = keywords.reduce((s, kw) => s + (text.includes(kw) ? 1 : 0), 0);
        return { score, text: p.payload?.text || "" };
      })
      .filter(x => x.score > 0)
      .sort((a,b) => b.score - a.score)
      .slice(0, top_k)
      .map(x => x.text);
  } catch {
    return [];
  }
}

function schemaToText(schemaObj) {
  if (!schemaObj) return "SCHEMA indisponível.";
  if (schemaObj.columns && Array.isArray(schemaObj.columns)) {
    let s = "COLUNAS:\n";
    for (const c of schemaObj.columns) s += `- ${c.name} (${c.type})\n`;
    return s;
  }
  return JSON.stringify(schemaObj, null, 2);
}

/* ========================= CLAUDE DECISION ========================= */
async function claudeDecideSource({ question, catalog, ragText = "" }) {
  const system = `
Você é assistente inteligente de consulta de dados.

DATASETS DISPONÍVEIS:
${JSON.stringify(catalog, null, 2)}

REGRAS:
1. Analise a pergunta do usuário
2. Decida qual fonte usar:
   - "rfb" para perguntas sobre empresas, CNPJ, estabelecimentos
   - "pt" para Portal da Transparência (Bolsa Família, BPC, servidores, etc)
3. Se RFB, escolha a UF (estados brasileiros)
4. Se PT, escolha o dataset específico
5. Responda APENAS com JSON:
   {
     "kind": "rfb" ou "pt",
     "uf": "SP" (se kind=rfb),
     "dataset": "BolsaFamilia_Pagamentos" (se kind=pt)
   }
`.trim();

  const user = `
${ragText ? `CONTEXTO RAG:\n${ragText}\n\n` : ""}
PERGUNTA: "${question}"

Qual fonte usar? Responda apenas JSON:
`.trim();

  const llm = await anthropic.messages.create({
    model: "claude-sonnet-4-5-20250929",
    max_tokens: 200,
    temperature: 0,
    system,
    messages: [{ role: "user", content: user }],
  });

  const text = llm.content?.[0]?.text || "{}";
  const clean = text.replace(/```json|```/g, "").trim();
  return JSON.parse(clean);
}

async function claudeGenSQL({ question, schemaText, ragText = "", rulesExtra = "", lastError = "" }) {
  const system = `
Você é especialista em SQL DuckDB.

REGRAS:
- Responda APENAS com SQL puro, sem markdown.
- Apenas SELECT.
- Nunca gere múltiplos comandos.
- Use SOMENTE o schema fornecido.
- Se usar UNNEST:
  CROSS JOIN UNNEST(x) AS t(item)
  WHERE item.campo = '...'
- NUNCA escreva AND logo após JOIN.

${rulesExtra}
`.trim();

  const user = `
SCHEMA:
${schemaText}

${ragText ? `CONTEXTO RAG:\n${ragText}\n` : ""}

PERGUNTA: "${question}"
${lastError ? `\nERRO NA ÚLTIMA TENTATIVA:\n${lastError}\nCorrija a SQL.` : ""}

SQL:
`.trim();

  const llm = await anthropic.messages.create({
    model: "claude-sonnet-4-5-20250929",
    max_tokens: 650,
    temperature: 0,
    system,
    messages: [{ role: "user", content: user }],
  });

  console.log("🔍 SQL do Claude:", llm.content?.[0]?.text);
  let sql = cleanSQL(llm.content?.[0]?.text || "");
  sql = fixUnnestAndBug(sql);
  return sql;
}

async function runWithRetry(executorFn, ctx) {
  let sql = await claudeGenSQL(ctx);
  try {
    const out = await executorFn(sql);
    return { sql, out };
  } catch (e1) {
    const errMsg = String(e1?.message || e1);
    sql = await claudeGenSQL({ ...ctx, lastError: errMsg });
    const out = await executorFn(sql);
    return { sql, out };
  }
}

/* ========================= ROUTES ========================= */
app.post("/chat", async (req, res) => {
  const start = Date.now();
  try {
    const query = (req.body?.query || "").trim();
    if (!query) return res.json({ error: "Query vazia" });

    // 1. Busca catálogo
    const catalog = await hetznerCatalog();

    // 2. Claude decide qual fonte usar
    const rag = await searchRAG(query, QDRANT_COLLECTION, 3);
    const ragText = rag.length ? rag.map((t, i) => `[${i+1}] ${t}`).join("\n") : "";

    const decision = await claudeDecideSource({ question: query, catalog, ragText });

    // 3. Busca schema da fonte escolhida
    const schemaObj = await hetznerSchema(decision);
    const schemaText = schemaToText(schemaObj);

    // 4. Claude gera SQL
    let rulesExtra = "";
    if (decision.kind === "rfb") {
      rulesExtra = `
IMPORTANTE:
- O DuckDB já corresponde à UF ${decision.uf}. NÃO use filtro uf='${decision.uf}'.
- Para empresas únicas use COUNT(DISTINCT cnpj_basico).
`;
    } else {
      rulesExtra = `IMPORTANTE: a tabela principal se chama "data".`;
    }

    const executorFn = decision.kind === "rfb"
      ? (sql) => hetznerSQL({ kind: "rfb", sql, uf: decision.uf, limit: 200 })
      : (sql) => hetznerSQLAutoPT({ sql, dataset: decision.dataset, limit: 200 });

    const { sql, out } = await runWithRetry(executorFn, { question: query, schemaText, ragText, rulesExtra });

    return res.json({
      answer: `✅ Consulta executada (${decision.kind === "rfb" ? `UF ${decision.uf}` : decision.dataset}).`,
      sql,
      rows: out.rows || [],
      row_count: out.row_count ?? (out.rows?.length || 0),
      source: decision,
      duration_ms: Date.now() - start,
    });

  } catch (err) {
    return res.status(500).json({ error: err.message, duration_ms: Date.now() - start });
  }
});

app.get("/health", async (_, res) => {
  let hetznerOk = false, qdrantOk = false;

  try {
    const r = await fetch(`${HETZNER_API_BASE}/health`, { headers: { "X-API-Key": HETZNER_API_KEY } });
    hetznerOk = r.ok;
  } catch {}

  try {
    const r = await fetch(`${QDRANT_URL}/collections/${QDRANT_COLLECTION}`);
    qdrantOk = r.ok;
  } catch {}

  res.json({
    ok: true,
    timestamp: new Date().toISOString(),
    hetzner_api: { ok: hetznerOk, base: HETZNER_API_BASE },
    qdrant: { ok: qdrantOk, url: QDRANT_URL, collection: QDRANT_COLLECTION }
  });
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log("═".repeat(60));
  console.log("🚀 BrazilDataCorp API — RFB + PT (Decisão Inteligente)");
  console.log("═".repeat(60));
  console.log(`📡 Porta: ${PORT}`);
  console.log(`🧱 Hetzner API: ${HETZNER_API_BASE}`);
  console.log(`📚 Qdrant: ${QDRANT_URL}`);
  console.log("═".repeat(60));
});
