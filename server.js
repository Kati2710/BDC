import express from "express";
import cors from "cors";
import duckdb from "duckdb";
import Anthropic from "@anthropic-ai/sdk";

const app = express();
app.use(cors());
app.use(express.json());

/* ========================= MOTHERDUCK ========================= */
const MD_DB = "md:chat_rfb";
const MD_TOKEN = process.env.MOTHERDUCK_TOKEN || "";
const db = new duckdb.Database(MD_DB, { motherduck_token: MD_TOKEN });

function queryMD(sql) {
  return new Promise((resolve, reject) => {
    const conn = db.connect();
    conn.all(sql, (err, rows) => {
      conn.close();
      if (err) return reject(err);
      resolve(rows);
    });
  });
}

/* ========================= SCHEMA COM CACHE ========================= */
let cachedSchema = null;
let cacheExpiry = null;
const CACHE_DURATION = 3600000; // 1 hora

async function getSchema() {
  // Retorna cache se válido
  if (cachedSchema && Date.now() < cacheExpiry) {
    console.log("📦 Schema em CACHE");
    return cachedSchema;
  }

  console.log("🔄 Buscando schema do MotherDuck...");
  
  // FILTRA só tabelas relevantes
  const allTables = await queryMD(`
    SELECT table_catalog, table_schema, table_name
    FROM information_schema.tables
    WHERE table_catalog IN ('chat_rfb', 'PortaldaTransparencia')
      AND table_schema = 'main'
    ORDER BY table_catalog, table_name
  `);
  
  console.log(`📋 Encontradas ${allTables.length} tabelas relevantes`);
  
  let schema = "TABELAS E COLUNAS DISPONÍVEIS:\n\n";
  
  for (const table of allTables) {
    const fullName = `${table.table_catalog}.${table.table_schema}.${table.table_name}`;
    
    console.log(`  ├─ ${fullName}`);
    
    const columns = await queryMD(`
      SELECT column_name, data_type 
      FROM information_schema.columns
      WHERE table_catalog = '${table.table_catalog}'
        AND table_schema = '${table.table_schema}'
        AND table_name = '${table.table_name}'
      ORDER BY ordinal_position
    `);
    
    schema += `TABELA: ${fullName}\n`;
    schema += `Colunas (${columns.length}):\n`;
    
    for (const col of columns) {
      schema += `  • ${col.column_name} (${col.data_type})\n`;
    }
    schema += "\n";
  }
  
  // ADICIONA REGRAS PRO CLAUDE
  schema += `
═══════════════════════════════════════════════════════════════
REGRAS CRÍTICAS PARA GERAR SQL:
═══════════════════════════════════════════════════════════════

1. CONTAGEM:
   - Empresas ÚNICAS: COUNT(DISTINCT cnpj_basico)
   - Estabelecimentos: COUNT(*)

2. FILTROS COMUNS RFB:
   - Ativas: WHERE situacao_cadastral = 'ATIVA'
   - Por estado: WHERE uf = 'SP'
   - MEI: WHERE opcao_mei = 'S'
   - Simples: WHERE opcao_simples = 'S'

3. JOIN COMPLIANCE (CEIS/CNEP + RFB):
   - CNPJ como string: CAST("CPF OU CNPJ DO SANCIONADO" AS VARCHAR)
   - Comparar com: CAST(e.cnpj AS VARCHAR)
   - Exemplo: CAST(c."CPF OU CNPJ DO SANCIONADO" AS VARCHAR) = CAST(e.cnpj AS VARCHAR)

4. COLUNAS COM ESPAÇOS:
   - SEMPRE use aspas duplas: "NOME DO SANCIONADO"
   - Colunas de auditoria: "_audit_url_download", "_audit_linha_csv", etc

5. AUDITORIA:
   - URL de origem: _audit_url_download
   - Data disponibilização gov: _audit_data_disponibilizacao_gov
   - Periodicidade: _audit_periodicidade_atualizacao_gov
   - Linha no CSV: _audit_linha_csv
   - Hash da linha: _audit_row_hash

6. PERFORMANCE:
   - SEMPRE use LIMIT se não for agregação (COUNT, SUM, etc)
   - Limite padrão: 50 linhas

7. TECNOLOGIA (CNAEs sem hífen):
   - 6201501, 6201502, 6202300, 6203100, 6204000, 6209100

EXEMPLOS DE QUERIES:

-- Empresas ativas de SP com sanções (COM AUDITORIA):
SELECT 
  e.razao_social, 
  e.uf, 
  c."CATEGORIA DA SANÇÃO",
  c._audit_url_download,
  c._audit_data_disponibilizacao_gov,
  c._audit_linha_csv
FROM chat_rfb.main.empresas e
INNER JOIN PortaldaTransparencia.main._ceis_auditado_limpo c
  ON CAST(c."CPF OU CNPJ DO SANCIONADO" AS VARCHAR) = CAST(e.cnpj AS VARCHAR)
WHERE e.situacao_cadastral = 'ATIVA' AND e.uf = 'SP'
LIMIT 50;

-- Empresas com multas CNEP (COM RASTREABILIDADE):
SELECT 
  e.razao_social,
  cn."VALOR DA MULTA",
  cn._audit_url_download,
  cn._audit_linha_csv
FROM chat_rfb.main.empresas e
INNER JOIN PortaldaTransparencia.main._cnep_auditado_limpo cn
  ON CAST(cn."CPF OU CNPJ DO SANCIONADO" AS VARCHAR) = CAST(e.cnpj AS VARCHAR)
WHERE e.situacao_cadastral = 'ATIVA'
LIMIT 50;

-- Quantas empresas únicas de tecnologia:
SELECT COUNT(DISTINCT cnpj_basico) as total
FROM chat_rfb.main.empresas
WHERE cnae_fiscal IN ('6201501','6201502','6202300');
`;

  // Salva no cache
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

    console.log("\n" + "=".repeat(60));
    console.log("❓ PERGUNTA:", query);
    console.log("=".repeat(60));

    // 1. Pega schema (cache se disponível)
    const schema = await getSchema();

    // 2. Claude gera SQL
    console.log("🤖 Claude gerando SQL...");
    const llmSQL = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 500,
      temperature: 0,
      system: "Você é especialista SQL DuckDB. Gere APENAS a query SQL, sem explicações. Use nomes completos de tabelas (database.schema.table). Use CAST para conversão de tipos.",
      messages: [{ 
        role: "user", 
        content: `${schema}\n\nPERGUNTA DO USUÁRIO: "${query}"\n\nGere a SQL:` 
      }]
    });

    const sql = cleanSQL(llmSQL.content[0].text);
    console.log("📝 SQL gerada:", sql.slice(0, 150) + (sql.length > 150 ? "..." : ""));

    // 3. Executa no MotherDuck
    console.log("⚡ Executando no MotherDuck...");
    const rows = await queryMD(sql);
    console.log(`📊 Retornou: ${rows.length} linha(s)`);

    // Converte BigInt para JSON
    const data = rows.map(row => {
      const clean = {};
      for (const [k, v] of Object.entries(row)) {
        clean[k] = typeof v === 'bigint' ? Number(v) : v;
      }
      return clean;
    });

    // 4. Claude explica
    console.log("💬 Claude explicando resultado...");
    const llmExplain = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 400,
      temperature: 0.7,
      system: "Você é assistente brasileiro de inteligência empresarial. Seja claro, objetivo e use separadores de milhar (ex: 1.234.567). Se houver colunas de auditoria (_audit_*), mencione a rastreabilidade quando relevante.",
      messages: [{ 
        role: "user", 
        content: `Pergunta: "${query}"\n\nSQL executada:\n${sql}\n\nResultado (primeiras 5 linhas):\n${JSON.stringify(data.slice(0, 5), null, 2)}\n\nExplique o resultado em português de forma clara e objetiva:` 
      }]
    });

    const answer = llmExplain.content[0].text;
    const duration = Date.now() - startTime;
    
    console.log("✅ CONCLUÍDO em", duration, "ms");
    console.log("📤 Resposta:", answer.slice(0, 100) + "...\n");

    return res.json({ 
      answer, 
      sql, 
      rows: data,
      row_count: data.length,
      duration_ms: duration
    });

  } catch (err) {
    const duration = Date.now() - startTime;
    console.error("❌ ERRO:", err.message);
    return res.status(500).json({ 
      error: err.message,
      duration_ms: duration
    });
  }
});

app.get("/health", (_, res) => {
  res.json({ 
    ok: true, 
    timestamp: new Date().toISOString(),
    cache: cachedSchema ? "active" : "empty"
  });
});

app.post("/clear-cache", (_, res) => {
  cachedSchema = null;
  cacheExpiry = null;
  console.log("🗑️  Cache limpo!");
  res.json({ ok: true, message: "Cache limpo com sucesso" });
});

/* ========================= START ========================= */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log("╔════════════════════════════════════════╗");
  console.log("║   🚀 CHAT-RFB API RODANDO             ║");
  console.log("╚════════════════════════════════════════╝");
  console.log(`📡 Porta: ${PORT}`);
  console.log(`🔐 MotherDuck: ${MD_TOKEN ? "✅ Configurado" : "❌ Faltando"}`);
  console.log(`🤖 Claude: ${process.env.ANTHROPIC_API_KEY ? "✅ Configurado" : "❌ Faltando"}`);
  console.log("");
});
