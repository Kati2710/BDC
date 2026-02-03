import express from "express";
import cors from "cors";
import duckdb from "duckdb";

const app = express();

/* ========================= CORS ========================= */
const allowedOrigins = new Set([
  "https://brazildatacorp.com",
  "https://www.brazildatacorp.com",
  "http://localhost:5500",
  "http://127.0.0.1:5500",
  "http://localhost:3000",
  "http://127.0.0.1:3000",
  "null" // file:// protocol
]);

const corsOptions = {
  origin: (origin, cb) => {
    console.log("🔍 Request from origin:", origin || "NO ORIGIN");

    // Permite requests sem Origin (curl, Postman, etc)
    if (!origin) return cb(null, true);

    if (allowedOrigins.has(origin)) {
      console.log("✅ Origin allowed:", origin);
      return cb(null, true);
    }

    console.log("❌ Origin blocked:", origin);
    return cb(new Error(`CORS blocked for origin: ${origin}`));
  },
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: ["Content-Type"],
  credentials: false,
  optionsSuccessStatus: 204
};

app.use(cors(corsOptions));
app.options("*", cors(corsOptions));
app.use(express.json({ limit: "256kb" }));

/* ========================= MOTHERDUCK ========================= */
const MD_TOKEN = process.env.MOTHERDUCK_TOKEN; // configure no Render
const MD_DB = "md:chat_rfb";

const db = new duckdb.Database(MD_DB, {
  motherduck_token: MD_TOKEN
});

/**
 * queryAll(sql, params)
 * - DuckDB Node aceita params como ARRAY: conn.all(sql, params, cb)
 * - Isso evita bug/ambiguidade com spread ...params
 */
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

/* ========================= ROTAS ========================= */
app.get("/health", (_, res) => {
  res.json({ ok: true, timestamp: new Date().toISOString() });
});

app.post("/chat", async (req, res) => {
  const startTime = Date.now();
  console.log("📨 POST /chat received");

  try {
    const q = String(req.body?.query || "").trim();
    console.log("🔎 Query:", q);

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
    console.log(`✅ Query executada em ${duration}ms, ${rows?.length || 0} resultados`);

    if (!rows?.length) {
      return res.json({
        answer: "Nenhum resultado encontrado.",
        query: q,
        duration_ms: duration
      });
    }

    const r = rows[0];
    return res.json({
      answer: `Encontrei ${rows.length} resultado(s).\nPrimeiro: ${r.razao_social} (CNPJ: ${r.cnpj_basico})`,
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

// Middleware de erro global
app.use((err, req, res, next) => {
  console.error("❌ Global error:", err);
  res.status(500).json({
    error: "Internal server error",
    message: process.env.NODE_ENV === "development" ? err.message : undefined
  });
});

/* ========================= START ========================= */
const PORT = process.env.PORT || 10000;

app.listen(PORT, () => {
  console.log(`🚀 BDC API rodando na porta ${PORT}`);
  console.log(`📍 Modo: ${process.env.NODE_ENV || "production"}`);
  console.log(`🔐 Motherduck: ${MD_TOKEN ? "✅ configurado" : "❌ faltando"}`);
});
