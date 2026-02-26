import express from "express";
import cors from "cors";
import Anthropic from "@anthropic-ai/sdk";

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

const HETZNER_API = process.env.HETZNER_API_BASE || "http://89.167.48.3:5010";
const HETZNER_KEY = process.env.HETZNER_API_KEY || "bdc-sql-api-key-2026-segura";
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

/* ========================= MAIN HANDLER ========================= */
app.post("/chat", async (req, res) => {
  const start = Date.now();
  const query = (req.body?.query || "").trim();
  
  if (!query) return res.json({ error: "Query vazia" });
  
  try {
    console.log(`\n${"=".repeat(60)}\n❓ "${query}"\n${"=".repeat(60)}`);
    
    // PASSO 1: Claude gera SQL
    console.log("🤖 Claude gerando SQL...");
    
    const sqlGen = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 1000,
      messages: [{
        role: "user",
        content: `Você é analista de dados públicos brasileiros.

BANCO DE DADOS UNIFICADO:

Tabela: _acordos_auditado (143 linhas)
Colunas:
  - "ID DO ACORDO" (INT)
  - "CNPJ DO SANCIONADO" (VARCHAR 14 dígitos)
  - "RAZÃO SOCIAL – CADASTRO RECEITA" (VARCHAR)
  - "NOME FANTASIA – CADASTRO RECEITA" (VARCHAR)
  - "SITUAÇÃO DO ACORDO DE LENIÊNICA" (VARCHAR)
  - "DATA DE INÍCIO DO ACORDO" (DATE)
  - "DATA DE FIM DO ACORDO" (DATE)
  - "ÓRGÃO SANCIONADOR" (VARCHAR)
  - _audit_url_download, _audit_data_disponibilizacao_gov, _audit_periodicidade

Tabela: _empresas_sp (20 milhões de linhas - RFB SP flatten)
Colunas:
  - cnpj_basico (VARCHAR 8 dígitos)
  - razao_social (VARCHAR)
  - porte (VARCHAR)
  - capital_social (DECIMAL)
  - natureza_juridica (VARCHAR)
  - est (STRUCT com: uf, municipio, situacao_cadastral, bairro, cep, etc)

CRUZAMENTO RFB + PT:
Para ligar as duas tabelas use:
  SUBSTRING(a."CNPJ DO SANCIONADO", 1, 8) = e.cnpj_basico

EXEMPLO SQL:
SELECT 
  a."CNPJ DO SANCIONADO",
  a."RAZÃO SOCIAL – CADASTRO RECEITA",
  e.razao_social as razao_rfb,
  e.est.municipio,
  e.est.situacao_cadastral
FROM _acordos_auditado a
LEFT JOIN _empresas_sp e 
  ON SUBSTRING(a."CNPJ DO SANCIONADO", 1, 8) = e.cnpj_basico
WHERE e.est.uf = 'SP'
LIMIT 10

PERGUNTA DO USUÁRIO:
"${query}"

Responda APENAS com o SQL necessário. Sem explicações, sem markdown, apenas SQL puro.`
      }]
    });
    
    let sql = sqlGen.content.find(b => b.type === "text")?.text.trim() || "";
    sql = sql.replace(/```sql\n?/g, "").replace(/```/g, "").trim();
    
    console.log(`📝 SQL: ${sql.substring(0, 200)}...`);
    
    // PASSO 2: Executa SQL
    console.log("⚡ Executando...");
    
    const response = await fetch(`${HETZNER_API}/query_unified`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-API-Key": HETZNER_KEY
      },
      body: JSON.stringify({ sql }),
      signal: AbortSignal.timeout(120000)
    });
    
    const data = await response.json();
    
    if (!response.ok) {
      throw new Error(data.error || "Query falhou");
    }
    
    console.log(`📊 ${data.row_count || 0} linhas retornadas`);
    
    // PASSO 3: Claude explica
    console.log("💬 Claude explicando...");
    
    const explanation = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 1500,
      messages: [{
        role: "user",
        content: `Pergunta: "${query}"

Resultados (${data.row_count} linhas):
${JSON.stringify(data.rows, null, 2)}

Explique os resultados em português de forma clara. SEMPRE cite fontes usando as colunas _audit_* quando disponíveis.`
      }]
    });
    
    const answer = explanation.content.find(b => b.type === "text")?.text || "Sem resposta";
    
    console.log(`✅ CONCLUÍDO em ${Date.now() - start}ms`);
    
    return res.json({
      answer,
      duration_ms: Date.now() - start,
      rows_returned: data.row_count
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
    res.json({ ok: true, hetzner: r.ok });
  } catch {
    res.json({ ok: true, hetzner: false });
  }
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log("═".repeat(60));
  console.log("🚀 BDC — ARQUITETURA MOTHERDUCK NO HETZNER");
  console.log("═".repeat(60));
  console.log(`📡 Porta: ${PORT}`);
  console.log(`🧱 API: ${HETZNER_API}`);
  console.log(`🗄️ Banco: brazildatacorp.duckdb (unificado)`);
  console.log(`⚡ 2 chamadas Claude por pergunta`);
  console.log("═".repeat(60));
});
