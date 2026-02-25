import express from "express";
import cors from "cors";
import Anthropic from "@anthropic-ai/sdk";

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

/* ========================= CONFIG ========================= */
const HETZNER_API_BASE = process.env.HETZNER_API_BASE || "http://89.167.48.3:5010";
const HETZNER_API_KEY = process.env.HETZNER_API_KEY || "bdc-sql-api-key-2026-segura";

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

/* ========================= UTILS ========================= */
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

/* ========================= TOOLS ========================= */
const tools = [
  {
    name: "find_datasets_semantic",
    description: "Busca os datasets mais relevantes usando IA semântica (embeddings). USE SEMPRE PRIMEIRO! Exemplo: 'acordo leniência' → encontra 'Acordos'",
    input_schema: {
      type: "object",
      properties: {
        query: { type: "string", description: "Descrição do que procura (ex: 'acordo leniência', 'bolsa família', 'servidores')" },
        top_k: { type: "number", default: 3, description: "Quantos datasets retornar (1-5)" }
      },
      required: ["query"]
    }
  },
  {
    name: "get_schema",
    description: "Obtém schema + exemplo. RFB: use 'uf' (ex: SP, MG). PT: use 'dataset' (ex: Acordos, BolsaFamilia_Pagamentos).",
    input_schema: {
      type: "object",
      properties: {
        kind: { type: "string", enum: ["rfb", "pt"] },
        uf: { type: "string", description: "Para RFB apenas (AC, AL, ..., TO)" },
        dataset: { type: "string", description: "Para PT apenas (Acordos, BolsaFamilia_Pagamentos, etc)" }
      },
      required: ["kind"]
    }
  },
  {
    name: "query_simple",
    description: "Executa SQL. SEMPRE use 'FROM data' para PT datasets. Exemplo: SELECT * FROM data WHERE coluna='valor'",
    input_schema: {
      type: "object",
      properties: {
        kind: { type: "string", enum: ["rfb", "pt"] },
        uf: { type: "string" },
        dataset: { type: "string" },
        sql: { type: "string", description: "SQL query. PT: FROM data | RFB: FROM rfb" }
      },
      required: ["kind", "sql"]
    }
  },
  {
    name: "query_multiple",
    description: "Executa múltiplas queries em PARALELO. PT: FROM data | RFB: FROM rfb",
    input_schema: {
      type: "object",
      properties: {
        queries: {
          type: "array",
          items: {
            type: "object",
            properties: {
              kind: { type: "string" },
              uf: { type: "string" },
              dataset: { type: "string" },
              sql: { type: "string" },
              label: { type: "string" }
            }
          }
        }
      },
      required: ["queries"]
    }
  },
  {
    name: "cross_results",
    description: "JOIN em memória de 2 resultados.",
    input_schema: {
      type: "object",
      properties: {
        left_results: { type: "array" },
        right_results: { type: "array" },
        left_key: { type: "string" },
        right_key: { type: "string" },
        join_type: { type: "string", enum: ["inner", "left"] }
      },
      required: ["left_results", "right_results", "left_key", "right_key"]
    }
  }
];

async function executeTool(toolName, toolInput) {
  console.log(`🔧 ${toolName}:`, JSON.stringify(toolInput, null, 2));
  
  if (toolName === "find_datasets_semantic") {
    const { ok, data } = await fetchJSON(`${HETZNER_API_BASE}/search_semantic`, {
      method: "POST",
      headers: { "content-type": "application/json", "X-API-Key": HETZNER_API_KEY },
      body: JSON.stringify({
        query: toolInput.query,
        top_k: toolInput.top_k || 3
      })
    });
    
    return ok ? data : { error: "Busca semântica falhou" };
  }
  
  if (toolName === "get_schema") {
    const qs = new URLSearchParams();
    qs.set("kind", toolInput.kind);
    
    // PT usa "dataset", RFB usa "uf"
    if (toolInput.kind === "rfb" && toolInput.uf) {
      qs.set("uf", toolInput.uf);
    } else if (toolInput.kind === "pt" && toolInput.dataset) {
      qs.set("dataset", toolInput.dataset);
    }
    
    const { ok, data } = await fetchJSON(`${HETZNER_API_BASE}/schema?${qs}`, {
      headers: { "X-API-Key": HETZNER_API_KEY }
    });
    
    if (!ok) return { error: "Schema não encontrado" };
    
    return {
      dataset: toolInput.dataset || toolInput.uf,
      table_name: "data",
      note: "IMPORTANTE: A tabela SEMPRE se chama 'data' (não use o nome do dataset!)",
      columns: (data.schema?.columns || []).slice(0, 15).map(c => `${c.name} (${c.type})`).join(", "),
      sample_row: data.sample?.[0] || null
    };
  }
  
  if (toolName === "query_simple") {
    const body = { kind: toolInput.kind, sql: toolInput.sql, limit: 200 };
    if (toolInput.uf) body.uf = toolInput.uf;
    if (toolInput.dataset) body.dataset = toolInput.dataset;
    
    const { ok, data } = await fetchJSON(`${HETZNER_API_BASE}/sql`, {
      method: "POST",
      headers: { "content-type": "application/json", "X-API-Key": HETZNER_API_KEY },
      body: JSON.stringify(body)
    }, 120000);
    
    return ok ? { rows: data.rows || [], count: data.row_count || 0 } : { error: data.error || "Query falhou" };
  }
  
  if (toolName === "query_multiple") {
    const promises = toolInput.queries.map(async (q) => {
      const body = { kind: q.kind, sql: q.sql, limit: 200 };
      if (q.uf) body.uf = q.uf;
      if (q.dataset) body.dataset = q.dataset;
      
      const { ok, data } = await fetchJSON(`${HETZNER_API_BASE}/sql`, {
        method: "POST",
        headers: { "content-type": "application/json", "X-API-Key": HETZNER_API_KEY },
        body: JSON.stringify(body)
      }, 120000);
      
      return {
        label: q.label || "unlabeled",
        success: ok,
        rows: ok ? (data.rows || []) : [],
        count: ok ? (data.row_count || 0) : 0,
        error: ok ? null : (data.error || "Query falhou")
      };
    });
    
    return { results: await Promise.all(promises) };
  }
  
  if (toolName === "cross_results") {
    const { left_results, right_results, left_key, right_key, join_type } = toolInput;
    
    const rightMap = new Map();
    for (const row of right_results) {
      const key = row[right_key];
      if (key) {
        if (!rightMap.has(key)) rightMap.set(key, []);
        rightMap.get(key).push(row);
      }
    }
    
    const joined = [];
    for (const leftRow of left_results) {
      const key = leftRow[left_key];
      const rightRows = rightMap.get(key) || [];
      
      if (rightRows.length > 0) {
        for (const rightRow of rightRows) {
          joined.push({ ...leftRow, ...rightRow });
        }
      } else if (join_type === "left") {
        joined.push(leftRow);
      }
    }
    
    return { joined, count: joined.length };
  }
  
  return { error: "Tool desconhecida" };
}

/* ========================= AGENTE ========================= */
async function runAgent(userQuestion) {
  const messages = [{
    role: "user",
    content: `PERGUNTA: "${userQuestion}"`
  }];
  
  const system = `Especialista em dados públicos. SEJA EFICIENTE:

REGRAS CRÍTICAS:
1. find_datasets_semantic UMA VEZ APENAS
2. TABELA se chama "data" (SEMPRE!)
3. PT: kind="pt", dataset="NomeDataset"
4. RFB: kind="rfb", uf="SP"

EXEMPLO CORRETO:
{
  kind: "pt",
  dataset: "Acordos",
  sql: "SELECT * FROM data WHERE coluna LIKE '%valor%' LIMIT 100"
}

CNPJ:
- PT: 14 dígitos (coluna varia: "CNPJ DO SANCIONADO", "CPF OU CNPJ")
- RFB: 8 dígitos (empresa.cnpj_basico)
- Cruzar: SUBSTRING(cnpj_completo, 1, 8)

WORKFLOW:
1. find_datasets_semantic → UMA VEZ
2. get_schema
3. query_multiple (tabela = "data")
4. RESPONDA com fontes (_audit_*)

Máximo 8 iterações.`;

  let iterations = 0;
  const maxIterations = 8;
  
  while (iterations < maxIterations) {
    iterations++;
    
    const response = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 1500,
      system,
      messages,
      tools
    });
    
    console.log(`🤖 Iter ${iterations}: ${response.stop_reason}`);
    
    messages.push({ role: "assistant", content: response.content });
    
    if (response.stop_reason === "end_turn") {
      const textBlocks = response.content.filter(b => b.type === "text");
      return textBlocks.map(b => b.text).join("\n\n");
    }
    
    if (response.stop_reason === "tool_use") {
      const toolResults = [];
      
      for (const block of response.content) {
        if (block.type === "tool_use") {
          const result = await executeTool(block.name, block.input);
          toolResults.push({
            type: "tool_result",
            tool_use_id: block.id,
            content: JSON.stringify(result)
          });
        }
      }
      
      messages.push({ role: "user", content: toolResults });
    } else {
      break;
    }
  }
  
  return "Não consegui completar. Reformule a pergunta.";
}

/* ========================= ROUTES ========================= */
app.post("/chat", async (req, res) => {
  const start = Date.now();
  try {
    const query = (req.body?.query || "").trim();
    if (!query) return res.json({ error: "Query vazia" });
    
    const answer = await runAgent(query);
    
    return res.json({ answer, duration_ms: Date.now() - start });
    
  } catch (err) {
    console.error("Erro:", err);
    return res.status(500).json({ error: err.message, duration_ms: Date.now() - start });
  }
});

app.get("/health", async (_, res) => {
  let hetznerOk = false;
  try {
    const r = await fetch(`${HETZNER_API_BASE}/health`, { 
      headers: { "X-API-Key": HETZNER_API_KEY } 
    });
    hetznerOk = r.ok;
  } catch {}
  
  res.json({
    ok: true,
    timestamp: new Date().toISOString(),
    hetzner_api: { ok: hetznerOk, base: HETZNER_API_BASE }
  });
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log("═".repeat(60));
  console.log("🚀 BDC — BUSCA SEMÂNTICA ATIVA");
  console.log("═".repeat(60));
  console.log(`📡 Porta: ${PORT}`);
  console.log(`🧱 API: ${HETZNER_API_BASE}`);
  console.log(`🎯 Embeddings: intfloat/multilingual-e5-large`);
  console.log("═".repeat(60));
});
