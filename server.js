import express from "express";
import cors from "cors";
import duckdb from "duckdb";
import Anthropic from "@anthropic-ai/sdk";

const app = express();

/* ========================= CORS ========================= */
app.use(cors({
  origin: (origin, cb) => cb(null, true),
  methods: ["GET", "POST"],
  allowedHeaders: ["Content-Type"]
}));

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

/* ========================= CLAUDE ========================= */
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || "";
const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

const SCHEMA = `
TABELAS:

1. chat_rfb.main.empresas - 66M estabelecimentos brasileiros
Colunas: cnpj_basico, cnpj, razao_social, nome_fantasia, situacao_cadastral, uf, municipio, cnae_fiscal, cnae_descricao, porte, opcao_mei, opcao_simples, capital_social, data_inicio_atividade

2. PortaldaTransparencia.main._ceis_corrigido - 22K empresas punidas (CEIS)
Colunas: "CPF OU CNPJ DO SANCIONADO", "NOME DO SANCIONADO", "CATEGORIA DA SANÇÃO", "DATA INÍCIO SANÇÃO", "DATA FINAL SANÇÃO"

3. PortaldaTransparencia.main._cnep_corrigido - 1.5K empresas punidas (CNEP)
Colunas: "CPF OU CNPJ DO SANCIONADO", "NOME DO SANCIONADO", "CATEGORIA DA SANÇÃO", "VALOR DA MULTA"

JOIN: Use REPLACE(REPLACE(REPLACE(ceis."CPF OU CNPJ DO SANCIONADO", '.', ''), '-', ''), '/', '') = e.cnpj

REGRAS:
- Empresas únicas: COUNT(DISTINCT cnpj_basico)
- Estabelecimentos: COUNT(*)
- SEMPRE use LIMIT se não for agregação
- Aspas duplas em colunas com espaços
`;

function cleanSQL(sql) {
  let s = sql.replace(/```sql|```/gi, "").trim();
  s = s.replace(/;+$/, "");
  
  if (!/select/i.test(s)) throw new Error("SQL inválida");
  if (/insert|update|delete|drop|alter|create/i.test(s)) throw new Error("SQL bloqueada");
  
  return s;
}

/* ========================= ROTA ========================= */
app.post("/chat", async (req, res) => {
  try {
    const query = req.body?.query?.trim();
    if (!query) return res.json({ error: "Query vazia" });

    console.log("❓ Query:", query);

    // 1. Claude gera SQL
    const llmSQL = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 400,
      temperature: 0,
      system: "Você é especialista SQL. Gere APENAS a SQL, nada mais.",
      messages: [{ 
        role: "user", 
        content: `${SCHEMA}\n\nPergunta: "${query}"\n\nSQL:` 
      }]
    });

    const sql = cleanSQL(llmSQL.content[0].text);
    console.log("📝 SQL:", sql);

    // 2. Roda no MotherDuck
    const rows = await queryMD(sql);
    console.log("📊 Linhas:", rows.length);

    // Converte BigInt
    const data = rows.map(row => {
      const clean = {};
      for (const [k, v] of Object.entries(row)) {
        clean[k] = typeof v === 'bigint' ? Number(v) : v;
      }
      return clean;
    });

    // 3. Claude explica
    const llmExplain = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 300,
      temperature: 0.7,
      system: "Seja objetivo e use português.",
      messages: [{ 
        role: "user", 
        content: `Pergunta: ${query}\nSQL: ${sql}\nDados: ${JSON.stringify(data.slice(0, 3))}\n\nExplique:` 
      }]
    });

    const answer = llmExplain.content[0].text;
    console.log("✅ Resposta:", answer.slice(0, 50));

    return res.json({ answer, sql, rows: data });

  } catch (err) {
    console.error("❌ Erro:", err.message);
    return res.status(500).json({ error: err.message });
  }
});

app.get("/health", (_, res) => res.json({ ok: true }));

/* ========================= START ========================= */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log(`🚀 API rodando na porta ${PORT}`));
