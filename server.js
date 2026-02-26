import express from "express";
import cors from "cors";
import Anthropic from "@anthropic-ai/sdk";

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

const HETZNER_API = process.env.HETZNER_API_BASE || "http://89.167.48.3:5010";
const HETZNER_KEY = process.env.HETZNER_API_KEY || "bdc-sql-api-key-2026-segura";
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

/* ========================= HELPERS ========================= */
async function fetchAPI(endpoint, opts = {}) {
  const url = `${HETZNER_API}${endpoint}`;
  const res = await fetch(url, {
    ...opts,
    headers: { ...opts.headers, "X-API-Key": HETZNER_KEY },
    signal: AbortSignal.timeout(120000)
  });
  return await res.json();
}

/* ========================= MAIN HANDLER ========================= */
app.post("/chat", async (req, res) => {
  const start = Date.now();
  const query = (req.body?.query || "").trim();
  
  if (!query) return res.json({ error: "Query vazia" });
  
  try {
    console.log(`\n${"=".repeat(60)}\n❓ PERGUNTA: "${query}"\n${"=".repeat(60)}`);
    
    // PASSO 1: Busca dataset semanticamente
    console.log("🔍 Buscando datasets relevantes...");
    const semantic = await fetchAPI("/search_semantic", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ query, top_k: 3 })
    });
    
    console.log("🔍 DEBUG semantic:", JSON.stringify(semantic, null, 2));
    
    const datasets = semantic.results || [];
    console.log(`📋 Encontrados: ${datasets.map(d => d.dataset).join(", ")}`);
    
    // PASSO 2: Claude decide e executa (1 CHAMADA SÓ!)
    console.log("🤖 Claude processando...");
    
    const response = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 2000,
      messages: [{
        role: "user",
        content: `Você é analista de dados públicos brasileiros.

DATASETS DISPONÍVEIS:
${datasets.map(d => `- ${d.dataset} (relevância: ${d.score.toFixed(2)})`).join("\n")}

DADOS:
- RFB: empresas por UF (tabela "rfb")
  - empresa.cnpj_basico (8 dígitos), empresa.razao_social, empresa.porte
  - estabelecimentos[1].uf, estabelecimentos[1].situacao_cadastral, estabelecimentos[1].municipio
  - socios[1].nome_socio
  
- PT: cada dataset tem tabela "data"
  - Acordos: "CNPJ DO SANCIONADO" (14 dígitos), "RAZÃO SOCIAL", "SITUAÇÃO DO ACORDO"
  - Outros datasets similares

FERRAMENTAS:
Para consultar dados, retorne JSON:
{
  "plan": "Explicação do que vai fazer",
  "queries": [
    {"kind": "rfb|pt", "uf": "SP", "dataset": "Acordos", "sql": "SELECT ..."}
  ]
}

PERGUNTA: "${query}"

Responda APENAS com o JSON das queries necessárias. Sem explicação extra.`
      }]
    });
    
    const text = response.content.find(b => b.type === "text")?.text || "{}";
    const plan = JSON.parse(text.replace(/```json\n?/g, "").replace(/```/g, ""));
    
    console.log(`📝 Plano: ${plan.plan}`);
    console.log(`📊 Queries: ${plan.queries?.length || 0}`);
    
    // PASSO 3: Executa queries
    const results = {};
    for (const q of (plan.queries || [])) {
      console.log(`⚡ Executando: ${q.kind} ${q.dataset || q.uf}`);
      
      const body = { kind: q.kind, sql: q.sql, limit: 200 };
      if (q.uf) body.uf = q.uf;
      if (q.dataset) body.dataset = q.dataset;
      
      const data = await fetchAPI("/sql", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body)
      });
      
      results[q.label || "result"] = data.rows || [];
      console.log(`  ✅ ${data.row_count || 0} linhas`);
    }
    
    // PASSO 4: Claude explica resultado
    console.log("💬 Claude explicando...");
    
    const explanation = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 1500,
      messages: [
        {
          role: "user",
          content: `Pergunta: "${query}"

Resultados:
${JSON.stringify(results, null, 2)}

Explique os resultados em português de forma clara e objetiva. SEMPRE cite as fontes dos dados (use colunas _audit_* se disponíveis).`
        }
      ]
    });
    
    const answer = explanation.content.find(b => b.type === "text")?.text || "Sem resposta";
    
    console.log(`✅ CONCLUÍDO em ${Date.now() - start}ms`);
    
    return res.json({
      answer,
      duration_ms: Date.now() - start,
      queries_executed: plan.queries?.length || 0
    });
    
  } catch (err) {
    console.error("❌ ERRO:", err.message);
    return res.status(500).json({ 
      error: err.message, 
      duration_ms: Date.now() - start 
    });
  }
});

app.get("/health", async (_, res) => {
  try {
    const r = await fetch(`${HETZNER_API}/health`, { 
      headers: { "X-API-Key": HETZNER_KEY },
      signal: AbortSignal.timeout(5000)
    });
    const ok = r.ok;
    res.json({ ok: true, hetzner: ok });
  } catch {
    res.json({ ok: true, hetzner: false });
  }
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log("═".repeat(60));
  console.log("🚀 BDC — ARQUITETURA SIMPLIFICADA");
  console.log("═".repeat(60));
  console.log(`📡 Porta: ${PORT}`);
  console.log(`🧱 API: ${HETZNER_API}`);
  console.log(`⚡ 2 chamadas Claude por pergunta`);
  console.log("═".repeat(60));
});
