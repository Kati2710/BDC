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

/* ========================= TOOLS PARA CLAUDE ========================= */
const tools = [
  {
    name: "search_catalog",
    description: "Busca arquivos disponíveis no catálogo. Use ANTES de consultar dados para saber quais arquivos existem.",
    input_schema: {
      type: "object",
      properties: {
        kind: { type: "string", enum: ["rfb", "pt"], description: "Tipo de dados: rfb (empresas) ou pt (Portal Transparência)" },
        uf: { type: "string", description: "UF para RFB (ex: SP, MG, RJ)" },
        dataset: { type: "string", description: "Nome do dataset PT (ex: BolsaFamilia_Pagamentos)" },
        ref: { type: "string", description: "Período YYYYMM (ex: 202312) para PT" }
      },
      required: ["kind"]
    }
  },
  {
    name: "get_schema",
    description: "Obtém schema + exemplos de um dataset específico. Use para entender a estrutura dos dados.",
    input_schema: {
      type: "object",
      properties: {
        kind: { type: "string", enum: ["rfb", "pt"] },
        uf: { type: "string", description: "UF para RFB" },
        dataset: { type: "string", description: "Dataset PT" }
      },
      required: ["kind"]
    }
  },
  {
    name: "query_simple",
    description: "Executa query SQL SIMPLES em 1 arquivo. Use queries LEVES sem UNNEST complexo. Limite 200 linhas.",
    input_schema: {
      type: "object",
      properties: {
        kind: { type: "string", enum: ["rfb", "pt"] },
        uf: { type: "string" },
        dataset: { type: "string" },
        ref: { type: "string", description: "Período específico PT" },
        sql: { type: "string", description: "SQL SELECT simples" }
      },
      required: ["kind", "sql"]
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
    if (toolInput.ref) qs.set("ref", toolInput.ref);
    
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
    
    // Retorna apenas schema + 2 samples (não todos)
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
  
  return { error: "Tool desconhecida" };
}

/* ========================= CLAUDE AGENTE ========================= */
async function runAgent(userQuestion) {
  const messages = [
    {
      role: "user",
      content: `Você é especialista em dados públicos brasileiros (RFB e Portal da Transparência).

IMPORTANTE:
- Use search_catalog ANTES de fazer queries para saber quais arquivos existem
- Use get_schema para entender a estrutura dos dados
- Faça queries SIMPLES (evite UNNEST em milhões de linhas)
- Para contagens, use COUNT simples
- Se precisar filtrar estabelecimentos, explique que os dados estão nested e é lento
- Responda em português claro

PERGUNTA DO USUÁRIO: "${userQuestion}"`
    }
  ];
  
  const system = `Você é um agente de dados especializado em:
- Receita Federal (RFB): 27 UFs, dados de empresas CNPJ
- Portal da Transparência (PT): 40+ datasets, pagamentos, servidores, etc

ESTRATÉGIA:
1. Use search_catalog para ver arquivos disponíveis
2. Use get_schema para entender estrutura
3. Execute queries SIMPLES (sem UNNEST complexo)
4. Responda em português natural com os resultados

LIMITAÇÕES:
- Queries com UNNEST em milhões de linhas são LENTAS
- Prefira agregações simples
- Explique limitações quando necessário`;

  let iterations = 0;
  const maxIterations = 10;
  
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
      // Claude terminou - extrai resposta final
      const textBlocks = response.content.filter(b => b.type === "text");
      return textBlocks.map(b => b.text).join("\n\n");
    }
    
    if (response.stop_reason === "tool_use") {
      // Claude quer usar tools
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
  
  return "Desculpe, não consegui processar sua pergunta. Tente reformular.";
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
  console.log("🚀 BrazilDataCorp — AGENTE INTELIGENTE");
  console.log("═".repeat(60));
  console.log(`📡 Porta: ${PORT}`);
  console.log(`🧱 Hetzner API: ${HETZNER_API_BASE}`);
  console.log(`🤖 Claude Agent: Catálogo + Tools`);
  console.log("═".repeat(60));
});
