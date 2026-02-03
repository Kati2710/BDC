import express from "express";
import cors from "cors";
import duckdb from "duckdb";
import Anthropic from "@anthropic-ai/sdk";

const app = express();

/* ========================= CORS ========================= */
const allowedOrigins = new Set([
  "https://brazildatacorp.com",
  "https://www.brazildatacorp.com",
  "http://localhost:5500",
  "http://127.0.0.1:5500",
  "http://localhost:3000",
  "http://127.0.0.1:3000",
  "null"
]);

app.use(cors({
  origin: (origin, cb) => {
    if (!origin) return cb(null, true);
    return cb(null, allowedOrigins.has(origin));
  },
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Accept"],
  optionsSuccessStatus: 204
}));

app.use((req, res, next) => {
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

app.use(express.json({ limit: "256kb" }));

/* ========================= MOTHERDUCK ========================= */
const MD_DB = "md:chat_rfb";
const MD_TOKEN = process.env.MOTHERDUCK_TOKEN;

const db = new duckdb.Database(MD_DB, { motherduck_token: MD_TOKEN });

function queryAll(sql, params = []) {
  return new Promise((resolve, reject) => {
    const conn = db.connect();
    conn.all(sql, params, (err, rows) => {
      conn.close();
      if (err) return reject(err);
      resolve(rows);
    });
  });
}

/* ========================= CLAUDE ========================= */
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

const SCHEMA_HINT = `
Base: chat_rfb.main.empresas
Colunas disponíveis:
cnpj_basico, razao_social, natureza_juridica_codigo, natureza_juridica,
qualificacao_responsavel_codigo, qualificacao_responsavel, capital_social,
porte_codigo, porte, ente_federativo, cnpj, matriz_filial_codigo, matriz_filial,
nome_fantasia, situacao_cadastral_codigo, situacao_cadastral,
data_situacao_cadastral, motivo_situacao_codigo, motivo_situacao,
data_inicio_atividade, cnae_fiscal, cnae_descricao, cnaes_secundarios,
tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf,
municipio_codigo, municipio, ddd, telefone, email, situacao_especial,
opcao_simples, data_opcao_simples, data_exclusao_simples, opcao_mei,
data_opcao_mei, data_exclusao_mei
`;

function sanitizeSQL(sql) {
  const s = String(sql || "").trim();

  // Só permite SELECT
  if (!/^select\b/i.test(s)) throw new Error("SQL inválida: apenas SELECT é permitido.");

  // Bloqueia comandos perigosos
  const blocked = /\b(insert|update|delete|drop|alter|create|truncate|copy|attach|detach|pragma|call)\b/i;
  if (blocked.test(s)) throw new Error("SQL bloqueada: comando não permitido.");

  // Bloqueia múltiplas statements
  if (s.includes(";")) throw new Error("SQL bloqueada: não use ';'.");

  // Garante LIMIT pra não explodir custo/latência (se não tiver)
  if (!/\blimit\b/i.test(s)) return s + " LIMIT 50";

  return s;
}

async function llmToSQL(userQuery) {
  const resp = await anthropic.messages.create({
    model: "claude-3-5-sonnet-latest",
    max_tokens: 260,
    temperature: 0,
    system:
      "Você converte perguntas em SQL DuckDB. " +
      "Use APENAS a tabela chat_rfb.main.empresas e as colunas fornecidas. " +
      "Gere UMA ÚNICA query SELECT. " +
      "Não use ';'. " +
      "Sempre use LIMIT quando retornar linhas. " +
      "Se for contagem, use COUNT(*). " +
      "Não invente tabelas/colunas.",
    messages: [{
      role: "user",
      content:
        `${SCHEMA_HINT}\n` +
        `Pergunta: ${userQuery}\n\n` +
        "Responda SOMENTE com a SQL (sem markdown, sem explicações)."
    }]
  });

  const sql = resp?.content?.[0]?.text || "";
  return sanitizeSQL(sql);
}

async function llmExplain(userQuery, sql, rows) {
  const resp = await anthropic.messages.create({
    model: "claude-3-5-sonnet-latest",
    max_tokens: 240,
    temperature: 0.6,
    system:
      "Você é um assistente brasileiro, objetivo e amigável. " +
      "Use APENAS os dados retornados. Não invente nada. " +
      "Se rows estiver vazio, diga que não encontrou.",
    messages: [{
      role: "user",
      content:
        `Pergunta: ${userQuery}\n` +
        `SQL executada: ${sql}\n` +
        `Resultado (JSON): ${JSON.stringify(rows)}\n\n` +
        "Explique o resultado de forma curta e útil em pt-BR."
    }]
  });

  return resp?.content?.[0]?.text || "Sem resposta.";
}

/* ========================= ROTAS ========================= */
app.get("/health", (_, res) => {
  res.json({
    ok: true,
    timestamp: new Date().toISOString(),
    motherduck: MD_TOKEN ? "configured" : "missing",
    claude: process.env.ANTHROPIC_API_KEY ? "configured" : "missing"
  });
});

app.post("/chat", async (req, res) => {
  const start = Date.now();
  try {
    const q = String(req.body?.query || "").trim();
    if (!q) return res.json({ answer: "Consulta vazia." });

    // 1) Claude gera SQL
    const sql = await llmToSQL(q);

    // 2) Executa no MotherDuck
    const rows = await queryAll(sql);

    // 3) Claude humaniza resposta
    const answer = await llmExplain(q, sql, rows);

    return res.json({
      answer,
      sql,
      rows,
      duration_ms: Date.now() - start
    });

  } catch (e) {
    console.error("❌ /chat error:", e);
    return res.status(500).json({
      answer: "Erro interno.",
      error: process.env.NODE_ENV === "development" ? String(e.message || e) : undefined
    });
  }
});

/* ========================= START ========================= */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log(`🚀 BDC API on :${PORT}`);
  console.log(`🔐 Motherduck: ${MD_TOKEN ? "✅" : "❌"}`);
  console.log(`🤖 Claude: ${process.env.ANTHROPIC_API_KEY ? "✅" : "❌"}`);
});
