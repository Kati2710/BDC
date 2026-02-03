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
  // Tabelas disponíveis
  const tables = await queryMD("SHOW TABLES");
  
  let schema = "TABELAS DISPONÍVEIS:\n\n";
  
  for (const table of tables) {
    const tableName = table.name;
    const fullName = `${table.database}.${table.schema}.${tableName}`;
    
    // Colunas de cada tabela
    const columns = await queryMD(`DESCRIBE ${fullName}`);
    
    schema += `${fullName}:\n`;
    for (const col of columns) {
      schema += `  - ${col.column_name} (${col.column_type})\n`;
    }
    schema += "\n";
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

/* ========================= ROTA ========================= */
app.post("/chat", async (req, res) => {
  try {
    const query = req.body?.query?.trim();
    if (!query) return res.json({ error: "Query vazia" });

    console.log("\n❓ PERGUNTA:", query);

    // 1. Pega schema real do MotherDuck
    const schema = await getSchema();
    console.log("📋 Schema carregado");

    // 2. Claude gera SQL vendo o schema REAL
    const llmSQL = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 500,
      temperature: 0,
      system: "Você é especialista SQL DuckDB. Gere APENAS a query SQL, sem explicações.",
      messages: [{ 
        role: "user", 
        content: `${schema}\n\nPERGUNTA DO USUÁRIO: "${query}"\n\nGere a SQL:` 
      }]
    });

    const sql = cleanSQL(llmSQL.content[0].text);
    console.log("📝 SQL:", sql);

    // 3. Executa no MotherDuck
    const rows = await queryMD(sql);
    console.log("📊 Retornou:", rows.length, "linhas");

    // Converte BigInt pra JSON
    const data = rows.map(row => {
      const clean = {};
      for (const [k, v] of Object.entries(row)) {
        clean[k] = typeof v === 'bigint' ? Number(v) : v;
      }
      return clean;
    });

    // 4. Claude explica o resultado
    const llmExplain = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 300,
      temperature: 0.7,
      system: "Você é assistente brasileiro. Explique o resultado de forma clara e objetiva.",
      messages: [{ 
        role: "user", 
        content: `Pergunta: "${query}"\n\nSQL executada: ${sql}\n\nResultado (primeiras linhas): ${JSON.stringify(data.slice(0, 5))}\n\nExplique o resultado em português:` 
      }]
    });

    const answer = llmExplain.content[0].text;
    console.log("✅ RESPOSTA:", answer.slice(0, 80) + "...\n");

    return res.json({ 
      answer, 
      sql, 
      rows: data,
      row_count: data.length 
    });

  } catch (err) {
    console.error("❌ ERRO:", err.message);
    return res.status(500).json({ error: err.message });
  }
});

app.get("/health", (_, res) => res.json({ ok: true, timestamp: new Date() }));

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log(`🚀 API rodando na porta ${PORT}`));
