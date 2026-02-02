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
const MD_DB = process.env.MOTHERDUCK_DB || "md:chat_rfb";

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

/**
 * Escape básico para strings em SQL (duplica aspas simples).
 * Isso evita quebrar o SQL se alguém digitar: O'Hara
 * (Para produção, o ideal é usar prepared statements compatíveis com o driver.)
 */
function sqlEscape(str) {
  return String(str ?? "").replace(/'/g, "''");
}

/**
 * CNPJ básico seguro: só dígitos, máximo 8.
 */
function sanitizeCnpjBasico(digits) {
  const only = String(digits ?? "").replace(/\D/g, "").slice(0, 8);
  return only;
}

function queryAll(sql) {
  return new Promise((resolve, reject) => {
    if (!db) return reject(new Error("MotherDuck não está configurado (token/DB)."));

    const conn = db.connect();
    conn.all(sql, (err, rows) => {
      try { conn.close(); } catch {}
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

app.get("/health", (_, res) => res.json({ ok: true }));

app.get("/schema", async (_, res) => {
  try {
    const rows = await queryAll(`
      SELECT table_schema, table_name, column_name, data_type
      FROM information_schema.columns
      WHERE table_schema='main' AND table_name IN ('empresas','empresas_chat')
      ORDER BY table_name, ordinal_position
    `);
    res.json({ ok: true, rows });
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

      // DuckDB Node: sem "?" placeholder -> SQL direto
      rows = await queryAll(`
        SELECT *
        FROM chat_rfb.main.empresas
        WHERE cnpj_basico = '${cnpj_basico}'
        LIMIT 5
      `);
    } else {
      const q = sqlEscape(qRaw.toUpperCase());

      // LIKE com escape básico (evita quebrar por aspas)
      // Observação: se suas colunas não existirem, o try/catch vai devolver erro JSON, sem derrubar
      rows = await queryAll(`
        SELECT *
        FROM chat_rfb.main.empresas
        WHERE upper(razao_social) LIKE '%${q}%'
           OR upper(nome_fantasia) LIKE '%${q}%'
           OR upper(nome) LIKE '%${q}%'
        LIMIT 5
      `);
    }

    if (!rows.length) return res.json({ answer: "Nenhum resultado encontrado." });

    const r = rows[0];
    res.json({
      answer:
        `Encontrei ${rows.length} resultado(s).\n` +
        `• ${r.razao_social ?? r.nome ?? "—"}\n` +
        `• CNPJ: ${r.cnpj_basico ?? r.cnpj ?? "—"}\n` +
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
