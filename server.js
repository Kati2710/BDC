import express from "express";
import cors from "cors";
import duckdb from "duckdb";
import Anthropic from "@anthropic-ai/sdk";

const app = express();

/* ========================= CONFIG ========================= */
const PORT = process.env.PORT || 10000;

// MotherDuck
const MD_DB = "md:chat_rfb";
const MD_TOKEN = process.env.MOTHERDUCK_TOKEN;

// Claude (Anthropic)
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || "";

// CORS whitelist
const allowedOrigins = new Set([
  "https://brazildatacorp.com",
  "https://www.brazildatacorp.com",
  "http://localhost:5500",
  "http://127.0.0.1:5500",
  "http://localhost:3000",
  "http://127.0.0.1:3000",
  "null" // file://
]);

/* ========================= CORS ========================= */
const corsOptions = {
  origin: (origin, cb) => {
    console.log("🔍 Origin:", origin || "NO ORIGIN");

    // sem Origin: curl/postman
    if (!origin) return cb(null, true);

    if (allowedOrigins.has(origin)) return cb(null, true);

    // bloqueia sem "jogar erro" (evita vários bugs de preflight)
    return cb(null, false);
  },
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Accept"],
  optionsSuccessStatus: 204
};

app.use(cors(corsOptions));

// Preflight sempre responde 204
app.use((req, res, next) => {
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

app.use(express.json({ limit: "256kb" }));

/* ========================= DUCKDB / MOTHERDUCK ========================= */
const db = new duckdb.Database(MD_DB, {
  motherduck_token: MD_TOKEN
});

// ✅ queryAll CORRETO: params como array
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

/* ========================= CLAUDE (HUMANIZER) ========================= */
const anthropic = ANTHROPIC_API_KEY
  ? new Anthropic({ apiKey: ANTHROPIC_API_KEY })
  : null;

async function humanizeAnswer({ query, rows }) {
  // se não tem chave ou não tem resultado, nem chama
  if (!anthropic) return null;
  if (!rows?.length) return null;

  // manda só dados necessários (reduz custo e evita vazamento)
  const compact = rows.slice(0, 5).map((r) => ({
    cnpj_basico: r.cnpj_basico,
    razao_social: r.razao_social,
    natureza_juridica: r.natureza_juridica,
    municipio: r.municipio,
    uf: r.uf
  }));

  const resp = await anthropic.messages.create({
    model: "claude-3-5-sonnet-latest",
    max_tokens: 220,
    temperature: 0.6,
    system:
      "Você é um assistente brasileiro, objetivo e amigável. " +
      "Use APENAS os dados fornecidos. " +
      "Não invente informações, não assuma nada. " +
      "Se algo não constar, diga que não consta.",
    messages: [
      {
        role: "user",
        content:
          `Consulta do usuário: ${query}\n\n` +
          `Resultados (JSON):\n${JSON.stringify(compact, null, 2)}\n\n` +
          "Crie uma resposta humana em pt-BR com:\n" +
          "1) Uma frase dizendo quantos resultados encontrou.\n" +
          "2) Destaque do 1º resultado.\n" +
          "3) Uma sugestão do que pesquisar a seguir (ex.: termo mais específico, cidade/UF, CNPJ completo).\n"
      }
    ]
  });

  return resp?.content?.[0]?.text || null;
}

/* ========================= ROTAS ========================= */
app.get("/health", (_, res) => {
  res.json({
    ok: true,
    timestamp: new Date().toISOString(),
    motherduck: MD_TOKEN ? "configured" : "missing",
    claude: ANTHROPIC_API_KEY ? "configured" : "missing"
  });
});

app.post("/chat", async (req, res) => {
  const startTime = Date.now();

  try {
    console.log("📨 POST /chat");
    console.log("📦 Body:", req.body);

    const q = String(req.body?.query || "").trim();
    if (!q) {
      return res.json({ answer: "Consulta vazia." });
    }

    const digits = q.replace(/\D/g, "");
    let rows = [];

    if (digits.length >= 8) {
      const cnpj = digits.slice(0, 8);
      console.log("🏢 Buscando por CNPJ:", cnpj);

      rows = await queryAll(
        `SELECT * FROM chat_rfb.main.empresas
         WHERE cnpj_basico = ?
         LIMIT 5`,
        [cnpj]
      );
    } else {
      const term = q.toUpperCase();
      console.log("📝 Buscando por razão social:", term);

      rows = await queryAll(
        `SELECT * FROM chat_rfb.main.empresas
         WHERE upper(razao_social) LIKE ?
         LIMIT 5`,
        [`%${term}%`]
      );
    }

    const duration = Date.now() - startTime;

    if (!rows?.length) {
      return res.json({
        answer: "Nenhum resultado encontrado.",
        query: q,
        duration_ms: duration
      });
    }

    // fallback básico
    const r = rows[0];
    const basicAnswer =
      `Encontrei ${rows.length} resultado(s).\n` +
      `Primeiro: ${r.razao_social} (CNPJ: ${r.cnpj_basico})`;

    // ✅ tenta humanizar com Claude (se configurado)
    let answer = basicAnswer;
    try {
      const human = await humanizeAnswer({ query: q, rows });
      if (human) answer = human;
    } catch (e) {
      console.error("⚠️ Claude error (fallback para básico):", e?.message || e);
    }

    return res.json({
      answer,
      rows,
      query: q,
      duration_ms: duration
    });
  } catch (e) {
    console.error("❌ CHAT ERROR:", e);
    return res.status(500).json({
      answer: "Erro interno no chat.",
      error: process.env.NODE_ENV === "development" ? e.message : undefined
    });
  }
});

// erro global
app.use((err, req, res, next) => {
  console.error("❌ Global error:", err);
  res.status(500).json({
    error: "Internal server error",
    message: process.env.NODE_ENV === "development" ? err.message : undefined
  });
});

/* ========================= START ========================= */
app.listen(PORT, () => {
  console.log(`🚀 BDC API rodando na porta ${PORT}`);
  console.log(`📍 Modo: ${process.env.NODE_ENV || "production"}`);
  console.log(`🔐 Motherduck: ${MD_TOKEN ? "✅ configurado" : "❌ faltando"}`);
  console.log(`🤖 Claude: ${ANTHROPIC_API_KEY ? "✅ configurado" : "❌ faltando"}`);
});
