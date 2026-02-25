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

/* ========================= HELPERS ========================= */
function normalize(text) {
  return text
    .toLowerCase()
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "") // Remove acentos
    .replace(/[_\s-]/g, ""); // Remove espaços, underscores, hífens
}

function fuzzyMatchDataset(query, datasets) {
  const queryNorm = normalize(query);
  const matches = [];
  
  for (const ds of datasets) {
    const dsNorm = normalize(ds);
    if (dsNorm.includes(queryNorm) || queryNorm.includes(dsNorm)) {
      matches.push(ds);
    }
  }
  
  return matches;
}

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
    description: "Busca datasets disponíveis. DICA: Use busca fuzzy - 'acordo' encontra 'Acordos', 'bolsa familia' encontra 'BolsaFamilia_Pagamentos'.",
    input_schema: {
      type: "object",
      properties: {
        kind: { type: "string", enum: ["rfb", "pt"] },
        uf: { type: "string" },
        dataset: { type: "string" },
        fuzzy_query: { type: "string", description: "Busca fuzzy por nome de dataset (ex: 'acordo', 'leniencia')" }
      },
      required: ["kind"]
    }
  },
  {
    name: "get_schema",
    description: "Obtém schema + exemplos. Use para ver colunas disponíveis.",
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
    description: "Executa queries em PARALELO. Economiza tempo!",
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
  
  if (toolName === "search_catalog") {
    const qs = new URLSearchParams();
    qs.set("kind", toolInput.kind);
    if (toolInput.uf) qs.set("uf", toolInput.uf);
    if (toolInput.dataset) qs.set("dataset", toolInput.dataset);
    
    const { ok, data } = await fetchJSON(`${HETZNER_API_BASE}/catalog/search?${qs}`, {
      headers: { "X-API-Key": HETZNER_API_KEY }
    });
    
    if (!ok) return { error: "Falha ao buscar catálogo" };
    
    // Aplica busca fuzzy se fornecida
    if (toolInput.fuzzy_query && data.results) {
      const datasets = data.results.map(r => r.dataset).filter(Boolean);
      const matches = fuzzyMatchDataset(toolInput.fuzzy_query, datasets);
      return {
        ...data,
        fuzzy_matches: matches,
        hint: matches.length > 0 ? `Datasets que combinam com '${toolInput.fuzzy_query}': ${matches.join(', ')}` : null
      };
    }
    
    return data;
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
    
    // Retorna schema compacto
    return {
      dataset: toolInput.dataset || toolInput.uf,
      table_name: data.table_name,
      columns: (data.schema?.columns || []).map(c => `${c.name} (${c.type})`).join(", "),
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
    content: `PERGUNTA: "${userQuestion}"

Responda em português com dados + fontes (use colunas _audit_*).`
  }];
  
  const system = `Especialista em dados públicos brasileiros. SEJA DIRETO E RÁPIDO.

DATASETS PT (Portal da Transparência):
- Acordos = Acordos de Leniência
- BolsaFamilia_Pagamentos = Pagamentos Bolsa Família
- CPGF = Cartão Corporativo
- Convenios = Convênios
- Servidores = Servidores públicos
- (e outros 35+)

ESTRATÉGIA INTELIGENTE:
1. Identifique dataset pelo NOME (use fuzzy: "acordo" → "Acordos", "bolsa" → "BolsaFamilia_Pagamentos")
2. get_schema UMA VEZ do dataset certo
3. query_multiple para paralelizar (ex: Acordos + RFB ao mesmo tempo)
4. cross_results se precisar cruzar
5. RESPONDA (máx 5 iterações!)

CNPJ:
- PT datasets têm CNPJ completo (14 dígitos)
- RFB tem cnpj_basico (8 primeiros dígitos)
- Para cruzar: extraia primeiros 8 dígitos do CNPJ PT

NÃO faça:
- Buscar texto em datasets errados
- Múltiplas buscas no catálogo
- Queries de exploração

FAÇA:
- Ir DIRETO no dataset certo
- Incluir _audit_* sempre
- Ser conciso`;

  let iterations = 0;
  const maxIterations = 5;
  
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
  console.log("🚀 BDC — AGENTE INTELIGENTE + FUZZY SEARCH");
  console.log("═".repeat(60));
  console.log(`📡 Porta: ${PORT}`);
  console.log(`🧱 API: ${HETZNER_API_BASE}`);
  console.log(`🎯 Máx iterações: 5 | Tokens: 1500`);
  console.log("═".repeat(60));
});
