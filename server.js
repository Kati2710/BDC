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
    name: "search_catalog",
    description: "Busca arquivos disponíveis. Use para descobrir quais datasets existem.",
    input_schema: {
      type: "object",
      properties: {
        kind: { type: "string", enum: ["rfb", "pt"] },
        uf: { type: "string" },
        dataset: { type: "string" }
      },
      required: ["kind"]
    }
  },
  {
    name: "get_schema",
    description: "Obtém schema + exemplos. SEMPRE use antes de fazer queries para entender as colunas disponíveis.",
    input_schema: {
      type: "object",
      properties: {
        kind: { type: "string", enum: ["rfb", "pt"] },
        uf: { type: "string" },
        dataset: { type: "string" }
      },
      required: ["kind"]
    }
  },
  {
    name: "query_simple",
    description: "Executa 1 query SQL. Limite 200 linhas.",
    input_schema: {
      type: "object",
      properties: {
        kind: { type: "string", enum: ["rfb", "pt"] },
        uf: { type: "string" },
        dataset: { type: "string" },
        sql: { type: "string" }
      },
      required: ["kind", "sql"]
    }
  },
  {
    name: "query_multiple",
    description: "Executa MÚLTIPLAS queries em PARALELO. Use para buscar em vários datasets ao mesmo tempo. Retorna array de resultados.",
    input_schema: {
      type: "object",
      properties: {
        queries: {
          type: "array",
          items: {
            type: "object",
            properties: {
              kind: { type: "string", enum: ["rfb", "pt"] },
              uf: { type: "string" },
              dataset: { type: "string" },
              sql: { type: "string" },
              label: { type: "string", description: "Label para identificar resultado" }
            }
          }
        }
      },
      required: ["queries"]
    }
  },
  {
    name: "cross_results",
    description: "Cruza/junta resultados de 2 queries anteriores. Use após query_multiple para fazer JOIN de dados.",
    input_schema: {
      type: "object",
      properties: {
        left_results: { type: "array" },
        right_results: { type: "array" },
        left_key: { type: "string", description: "Coluna para join no left" },
        right_key: { type: "string", description: "Coluna para join no right" },
        join_type: { type: "string", enum: ["inner", "left"], default: "inner" }
      },
      required: ["left_results", "right_results", "left_key", "right_key"]
    }
  }
];

async function executeTool(toolName, toolInput) {
  console.log(`🔧 Tool: ${toolName}`, JSON.stringify(toolInput, null, 2));
  
  if (toolName === "search_catalog") {
    const qs = new URLSearchParams();
    qs.set("kind", toolInput.kind);
    if (toolInput.uf) qs.set("uf", toolInput.uf);
    if (toolInput.dataset) qs.set("dataset", toolInput.dataset);
    
    const { ok, data } = await fetchJSON(`${HETZNER_API_BASE}/catalog/search?${qs}`, {
      headers: { "X-API-Key": HETZNER_API_KEY }
    });
    
    return ok ? data : { error: "Falha ao buscar catálogo" };
  }
  
  if (toolName === "get_schema") {
    const qs = new URLSearchParams();
    qs.set("kind", toolInput.kind);
    if (toolInput.uf) qs.set("uf", toolInput.uf);
    if (toolInput.dataset) qs.set("dataset", toolInput.dataset);
    
    const { ok, data } = await fetchJSON(`${HETZNER_API_BASE}/schema?${qs}`, {
      headers: { "X-API-Key": HETZNER_API_KEY }
    });
    
    if (!ok) return { error: "Schema não encontrado" };
    
    return {
      table_name: data.table_name,
      columns: data.schema?.columns || [],
      sample: (data.sample || []).slice(0, 2)
    };
  }
  
  if (toolName === "query_simple") {
    const body = {
      kind: toolInput.kind,
      sql: toolInput.sql,
      limit: 200
    };
    
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
    // Executa todas em paralelo
    const promises = toolInput.queries.map(async (q) => {
      const body = {
        kind: q.kind,
        sql: q.sql,
        limit: 200
      };
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
    
    const results = await Promise.all(promises);
    return { results };
  }
  
  if (toolName === "cross_results") {
    // Join simples em memória
    const { left_results, right_results, left_key, right_key, join_type } = toolInput;
    
    // Cria map do right por chave
    const rightMap = new Map();
    for (const row of right_results) {
      const key = row[right_key];
      if (key) {
        if (!rightMap.has(key)) rightMap.set(key, []);
        rightMap.get(key).push(row);
      }
    }
    
    // Join
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

/* ========================= CLAUDE AGENTE ========================= */
async function runAgent(userQuestion) {
  const messages = [
    {
      role: "user",
      content: `Você é especialista em dados públicos brasileiros (RFB + Portal da Transparência).

ESTRATÉGIA:
1. Use get_schema para entender as colunas disponíveis
2. Para queries complexas:
   - Use query_multiple para buscar em paralelo
   - Use cross_results para cruzar dados
3. SEMPRE inclua colunas _audit_* nas queries para citar fontes
4. Responda em português com:
   - Resposta clara
   - Dados encontrados
   - Fontes (URLs, arquivos, datas) baseado nas colunas _audit_*

PERGUNTA: "${userQuestion}"`
    }
  ];
  
  const system = `Você é agente inteligente de análise de dados públicos.

CAPACIDADES:
- Consultar schemas dinâmicos (get_schema)
- Queries simples (query_simple)
- Queries paralelas (query_multiple)
- Cruzamentos (cross_results)

IMPORTANTE:
- Schemas mostram TODAS as colunas disponíveis
- Use os exemplos (sample) para entender a estrutura
- SEMPRE inclua _audit_* nas SELECT para rastreabilidade
- Para cruzamentos: faça queries separadas + cross_results

Responda em português natural com evidências.`;

  let iterations = 0;
  const maxIterations = 15;
  
  while (iterations < maxIterations) {
    iterations++;
    
    const response = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 4000,
      system,
      messages,
      tools
    });
    
    console.log(`\n🤖 Iteração ${iterations}:`, response.stop_reason);
    
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
  
  return "Desculpe, não consegui processar completamente. Tente reformular.";
}

/* ========================= ROUTES ========================= */
app.post("/chat", async (req, res) => {
  const start = Date.now();
  try {
    const query = (req.body?.query || "").trim();
    if (!query) return res.json({ error: "Query vazia" });
    
    const answer = await runAgent(query);
    
    return res.json({
      answer,
      duration_ms: Date.now() - start
    });
    
  } catch (err) {
    console.error("Erro:", err);
    return res.status(500).json({ 
      error: err.message, 
      duration_ms: Date.now() - start 
    });
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
  console.log("🚀 BrazilDataCorp — AGENTE UNIVERSAL");
  console.log("═".repeat(60));
  console.log(`📡 Porta: ${PORT}`);
  console.log(`🧱 Hetzner API: ${HETZNER_API_BASE}`);
  console.log(`🤖 Tools: schemas + multiple queries + cross join`);
  console.log("═".repeat(60));
});
