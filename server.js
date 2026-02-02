import express from "express";
import cors from "cors";
import duckdb from "duckdb";

const app = express();

/* =========================
   CORS (OBRIGATÓRIO) - ROBUSTO
   ========================= */
const allowedOrigins = new Set([
  "https://brazildatacorp.com",
  "https://www.brazildatacorp.com",
  "http://localhost:5500",
  "http://127.0.0.1:5500",
]);

const corsOptions = {
  origin: (origin, cb) => {
    // Permite requests sem Origin (ex: curl/powershell) e origens whitelisted
    if (!origin) return cb(null, true);
    if (allowedOrigins.has(origin)) return cb(null, true);
    return cb(new Error(`CORS blocked for origin: ${origin}`));
  },
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: ["Content-Type"],
  credentials: false,
  optionsSuccessStatus: 204, // importante p/ browsers
};

// 1) CORS para todas as rotas
app.use(cors(corsOptions));

// 2) Preflight explícito (garante que o OPTIONS responda com CORS SEMPRE)
app.options("/chat", cors(corsOptions), (req, res) => {
  // cors() já escreve os headers; aqui garantimos o status e fim
  return res.sendStatus(204);
});

// Se quiser liberar preflight para qualquer rota futura, pode usar também:
// app.options("*", cors(corsOptions));

app.use(express.json({ limit: "256kb" }));

/* =========================
   MOTHERDUCK
   ========================= */
const MD_TOKEN = process.env.MOTHERDUCK_TOKEN;
const MD_DB = "md:chat_rfb";

const db = new duckdb.Database(MD_DB, {
  motherduck_token: MD_TOKEN,
});

function queryAll(sql) {
  return new Promise((resolve, reject) => {
    const conn = db.connect();
    conn.all(sql, (err, rows) => {
      conn.close();
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

/* =========================
   ROTAS
   ========================= */
app.get("/health", (_, res) => {
  res.json({ ok: true });
});

app.post("/chat", async (req, res) => {
  try {
    const q = String(req.body?.query || "").trim();
    if (!q) return res.json({ answer: "Consulta vazia." });

    const digits = q.replace(/\D/g, "");
    let rows;

    if (digits.length >= 8) {
      const cnpj = digits.slice(0, 8);

      // (Melhor: parametrizar. Mas mantendo seu estilo por enquanto)
      rows = await queryAll(`
        SELECT * FROM chat_rfb.main.empresas
        WHERE cnpj_basico = '${cnpj}'
        LIMIT 5
      `);
    } else {
      const term = q.toUpperCase().replace(/'/g, "''");
      rows = await queryAll(`
        SELECT * FROM chat_rfb.main.empresas
        WHERE upper(razao_social) LIKE '%${term}%'
        LIMIT 5
      `);
    }

    if (!rows?.length) {
      return res.json({ answer: "Nenhum resultado encontrado." });
    }

    const r = rows[0];
    return res.json({
      answer: `Encontrei ${rows.length} resultado(s).\n${r.razao_social}`,
      rows,
    });
  } catch (e) {
    console.error("CHAT ERROR:", e);
    return res.status(500).json({ answer: "Erro interno no chat." });
  }
});

/* ========================= */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log("BDC API rodando na porta", PORT));
