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

/* ========================= PEGA SCHEMA REAL ========================= */
async function getSchema() {
  try {
    // Lista TODOS os databases/schemas/tables
    const allTables = await queryMD(`
      SELECT 
        table_catalog,
        table_schema, 
        table_name
      FROM information_schema.tables
      WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    `);
    
    console.log("📋 Tabelas encontradas:", allTables.length);
    
    let schema = "TABELAS E COLUNAS DISPONÍVEIS:\n\n";
    
    for (const table of allTables) {
      const fullName = `${table.table_catalog}.${table.table_schema}.${table.table_name}`;
      
      console.log(`  Descrevendo: ${fullName}`);
      
      // Pega colunas
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
    
    return schema;
    
  } catch (err) {
    console.error("❌ Erro ao pegar schema:", err.message);
    throw err;
  }
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
  try {
    const query = req.body?.query?.trim();
    if (!query) return res.json({ error: "Query vazia" });

    console.log("\n❓ PERGUNTA:", query);

    // 1. Pega schema real
    const schema = await getSchema();
    console.log("✅ Schema montado\n");

    // 2. Claude gera SQL
    const llmSQL = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 500,
      temperature: 0,
      system: "Você é especialista SQL DuckDB. Gere APENAS a query SQL, sem explicações. Use nomes completos de tabelas (database.schema.table).",
      messages: [{ 
        role: "user", 
        content: `${schema}\n\nREGRAS:\n- Para contar EMPRESAS únicas use: COUNT(DISTINCT cnpj_basico)\n- Para contar ESTABELECIMENTOS use: COUNT(*)\n- Empresas ativas: WHERE situacao_cadastral = 'ATIVA'\n- SEMPRE use LIMIT se não for agregação\n\nPERGUNTA: "${query}"\n\nSQL:` 
      }]
    });

    const sql = cleanSQL(llmSQL.content[0].text);
    console.log("📝 SQL:", sql);

    // 3. Executa
    const rows = await queryMD(sql);
    console.log("📊 Retornou:", rows.length, "linhas");

    // Converte BigInt
    const data = rows.map(row => {
      const clean = {};
      for (const [k, v] of Object.entries(row)) {
        clean[k] = typeof v === 'bigint' ? Number(v) : v;
      }
      return clean;
    });

    // 4. Claude explica
    const llmExplain = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 300,
      temperature: 0.7,
      system: "Você é assistente brasileiro. Seja objetivo e use separadores de milhar.",
      messages: [{ 
        role: "user", 
        content: `Pergunta: "${query}"\n\nSQL: ${sql}\n\nResultado: ${JSON.stringify(data.slice(0, 5))}\n\nExplique em português:` 
      }]
    });

    const answer = llmExplain.content[0].text;
    console.log("✅ RESPOSTA:", answer.slice(0, 80) + "...\n");

    return res.json({ answer, sql, rows: data, row_count: data.length });

  } catch (err) {
    console.error("❌ ERRO:", err.message);
    return res.status(500).json({ error: err.message });
  }
});

app.get("/health", (_, res) => res.json({ ok: true }));

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log(`🚀 API rodando na porta ${PORT}`));
