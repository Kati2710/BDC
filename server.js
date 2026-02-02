import express from "express";
import duckdb from "duckdb";

const app = express();
app.use(express.json());

const MD_TOKEN = process.env.MOTHERDUCK_TOKEN;
const MD_DB = "md:chat_rfb";

if (!MD_TOKEN) {
  console.error("MOTHERDUCK_TOKEN não definido");
  process.exit(1);
}

const db = new duckdb.Database(MD_DB, {
  motherduck_token: MD_TOKEN
});

function query(sql, params = []) {
  return new Promise((resolve, reject) => {
    const conn = db.connect();
    conn.all(sql, params, (err, rows) => {
      conn.close();
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

app.get("/health", (_, res) => {
  res.json({ ok: true });
});

app.get("/schema", async (_, res) => {
  const rows = await query(`
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema='main'
      AND table_name='empresas'
  `);
  res.json(rows);
});

app.post("/chat", async (req, res) => {
  const q = String(req.body.query || "").trim();
  if (!q) return res.json({ answer: "Consulta vazia." });

  const digits = q.replace(/\D/g, "");
  let rows;

  if (digits.length >= 8) {
    rows = await query(
      `SELECT * FROM chat_rfb.main.empresas
       WHERE cnpj_basico = ?
       LIMIT 5`,
      [digits.slice(0, 8)]
    );
  } else {
    rows = await query(
      `SELECT * FROM chat_rfb.main.empresas
       WHERE upper(razao_social) LIKE ?
       LIMIT 5`,
      [`%${q.toUpperCase()}%`]
    );
  }

  if (!rows.length) {
    return res.json({ answer: "Nenhum resultado encontrado." });
  }

  const r = rows[0];
  res.json({
    answer:
      `Empresa encontrada:\n` +
      `• ${r.razao_social}\n` +
      `• CNPJ: ${r.cnpj_basico}\n` +
      `• UF: ${r.uf}\n`
  });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () =>
  console.log("BDC API rodando na porta", PORT)
);
