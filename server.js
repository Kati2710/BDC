import express from "express";
import duckdb from "duckdb";

process.on("unhandledRejection", (reason) => {
  console.error("UNHANDLED REJECTION:", reason);
});
process.on("uncaughtException", (err) => {
  console.error("UNCAUGHT EXCEPTION:", err);
});

const app = express();
app.use(express.json({ limit: "256kb" }));

const MD_TOKEN = process.env.MOTHERDUCK_TOKEN;
const MD_DB = process.env.MOTHERDUCK_DB || "md:chat_rfb"; // pode manter assim

let db = null;

if (!MD_TOKEN) {
  console.warn("AVISO: MOTHERDUCK_TOKEN não definido. /health funciona, /schema e /chat vão falhar.");
} else {
  try {
    db = new duckdb.Database(MD_DB, { motherduck_token: MD_TOKEN });
    console.log("DuckDB/MotherDuck: init OK");
  } catch (e) {
    console.error("Falha ao inicializar DuckDB:", e);
    db = null;
  }
}

/** Escape básico para string em SQL */
function sqlEscape(str) {
  return String(str ?? "").replace(/'/g, "''");
}

/** Apenas dígitos (máx 8) */
function sanitizeCnpjBasico(input) {
  return String(input ?? "").replace(/\D/g, "").slice(0, 8);
}

/**
 * IMPORTANTE: DuckDB Node aqui roda sem parâmetros.
 * NUNCA usar conn.all(sql, params).
 */
function queryAll(sql) {
  return new Promise((resolve, reject) => {
    if (!db) return reject(new Error("MotherDuck não está configurado (token/DB)."));

    let conn;
    try {
      conn = db.connect();
    } catch (e) {
      return reject(e);
    }

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
 * 1) lista tabelas no schema main
 * 2) descreve a tabela empresas (se existir)
 */
app.get("/schema", async (_, res) => {
  try {
    const tables = await queryAll(`SHOW TABLES FROM chat_rfb.main;`);

    // tenta encontrar "empresas" na lista
    const hasEmpresas = (tables || []).some((t) => {
      const name = t?.name || t?.table_name || Object.values(t || {})[0];
      return String(name).toLowerCase() === "empresas";
    });

    let empresasColumns = [];
    if (hasEmpresas) {
      // DESCRIBE é mais “seguro” do que information_schema em alguns ambientes
      empresasColumns = await queryAll(`DESCRIBE chat_rfb.main.empresas;`);
    }

    res.json({
      ok: true,
      tables,
      empresasColumns
    });
  } catch (e) {
    console.error("ERRO /schema:", e);
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

/**
 * /chat MVP:
 * - se digitar 8+ dígitos: trata como cnpj_basico
 * - senão: busca por razão social (e tenta nome_fantasia se existir)
 *
 * Obs: para não quebrar, a busca por nome usa somente razao_social.
 * Depois que o /schema mostrar as colunas reais, a gente melhora.
 */
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

    if (!rows || rows.length === 0) {
      return res.json({ answer: "Nenhum resultado encontrado." });
    }

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
