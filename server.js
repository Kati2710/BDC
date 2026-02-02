import express from "express";
import duckdb from "duckdb";

console.log("BOOT: starting server...");
console.log("NODE:", process.version);

process.on("unhandledRejection", (reason) => {
  console.error("UNHANDLED REJECTION:", reason);
});
process.on("uncaughtException", (err) => {
  console.error("UNCAUGHT EXCEPTION:", err);
});

const app = express();
app.use(express.json({ limit: "256kb" }));

const MD_TOKEN = process.env.MOTHERDUCK_TOKEN;
const MD_DB = process.env.MOTHERDUCK_DB || "md:chat_rfb";

console.log("PORT env:", process.env.PORT);
console.log("MD DB:", MD_DB);
console.log("MD TOKEN present:", !!MD_TOKEN);

let db = null;
let dbInitPromise = null;

/** Escape básico para strings em SQL */
function sqlEscape(str) {
  return String(str ?? "").replace(/'/g, "''");
}

/** Apenas dígitos (máx 8) */
function sanitizeCnpjBasico(input) {
  return String(input ?? "").replace(/\D/g, "").slice(0, 8);
}

/**
 * Inicializa DuckDB/MotherDuck só quando necessário.
 * Isso evita crash durante boot.
 */
async function getDb() {
  if (!MD_TOKEN) throw new Error("MOTHERDUCK_TOKEN não definido no Render (Environment).");

  if (db) return db;

  if (!dbInitPromise) {
    dbInitPromise = new Promise((resolve, reject) => {
      try {
        const instance = new duckdb.Database(MD_DB, { motherduck_token: MD_TOKEN });

        // testa conexão com uma query simples
        const conn = instance.connect();
        conn.all("SELECT 1 AS ok;", (err) => {
          try { conn.close(); } catch {}
          if (err) return reject(err);

          console.log("DuckDB/MotherDuck: init OK (lazy)");
          db = instance;
          resolve(db);
        });
      } catch (e) {
        reject(e);
      }
    }).catch((e) => {
      // se falhar, permite tentar de novo depois
      dbInitPromise = null;
      throw e;
    });
  }

  return dbInitPromise;
}

/**
 * Query sem parâmetros (DuckDB Node).
 */
async function queryAll(sql) {
  const instance = await getDb();
  return new Promise((resolve, reject) => {
    const conn = instance.connect();
    conn.all(sql, (err, rows) => {
      try { conn.close(); } catch {}
      if (err) return reject(err);
      resolve(rows);
    });
  });
}

app.get("/health", (_, res) => res.json({ ok: true }));

/**
 * /schema robusto:
 * lista tabelas e descreve "empresas" se existir.
 */
app.get("/schema", async (_, res) => {
  try {
    const tables = await queryAll(`SHOW TABLES FROM chat_rfb.main;`);

    const names = (tables || []).map(t =>
      String(t?.name ?? Object.values(t || {})[0] ?? "").toLowerCase()
    );

    let empresasColumns = [];
    if (names.includes("empresas")) {
      empresasColumns = await queryAll(`DESCRIBE chat_rfb.main.empresas;`);
    }

    res.json({ ok: true, tables, empresasColumns });
  } catch (e) {
    console.error("ERRO /schema:", e);
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

app.post("/chat", async (req, res) => {
  try {
    const qRaw = String(req.body?.query || "").trim();
    if (!qRaw) return res.status(400).json({ answer: "Consulta vazia." });

    const digits = qRaw.replace(/\D/g, "");
    const isCnpj = digits.length >= 8;

    let rows = [];

    if (isCnpj) {
      const cnpj_basico = sanitizeCnpjBasico(digits);
      rows = await queryAll(`
        SELECT *
        FROM chat_rfb.main.empresas
        WHERE cnpj_basico = '${cnpj_basico}'
        LIMIT 5;
      `);
    } else {
      const q = sqlEscape(qRaw.toUpperCase());
      rows = await queryAll(`
        SELECT *
        FROM chat_rfb.main.empresas
        WHERE upper(razao_social) LIKE '%${q}%'
        LIMIT 5;
      `);
    }

    if (!rows.length) return res.json({ answer: "Nenhum resultado encontrado." });

    const r = rows[0];
    res.json({
      answer:
        `Encontrei ${rows.length} resultado(s).\n` +
        `• ${r.razao_social ?? "—"}\n` +
        `• CNPJ básico: ${r.cnpj_basico ?? "—"}\n` +
        `• UF: ${r.uf ?? "—"}\n`,
      rows
    });
  } catch (e) {
    console.error("ERRO /chat:", e);
    res.status(500).json({
      answer: "Erro consultando MotherDuck.",
      error: String(e?.message || e)
    });
  }
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log("BDC API rodando na porta", PORT));
