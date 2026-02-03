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

// Preflight sempre OK
app.use((req, res, next) => {
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

app.use(express.json({ limit: "256kb" }));

/* ========================= MOTHERDUCK ========================= */
const MD_DB = "md:chat_rfb";
const MD_TOKEN = process.env.MOTHERDUCK_TOKEN || "";

const db = new duckdb.Database(MD_DB, { motherduck_token: MD_TOKEN });

// ✅ queryAll correto (params como array)
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
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || "";
const anthropic = ANTHROPIC_API_KEY ? new Anthropic({ apiKey: ANTHROPIC_API_KEY }) : null;

const SCHEMA_HINT = `
Tabela única: chat_rfb.main.empresas
Colunas:
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
  let s = String(sql || "").trim();

  // remove ```...```
  s = s.replace(/```[\s\S]*?```/g, (m) => m.replace(/```sql|```/gi, "").trim());

  // remove ; final
  s = s.replace(/;+\s*$/g, "").trim();

  // pega do primeiro SELECT
  const idx = s.toLowerCase().indexOf("select");
  if (idx === -1) throw new Error("SQL inválida: não encontrei SELECT.");
  s = s.slice(idx).trim();

  // bloqueia comandos perigosos
  const blocked = /\b(insert|update|delete|drop|alter|create|truncate|copy|attach|detach|pragma|call)\b/i;
  if (blocked.test(s)) throw new Error("SQL bloqueada: comando não permitido.");

  // bloqueia múltiplas instruções
  if (s.includes(";")) throw new Error("SQL bloqueada: múltiplas instruções.");

  // LIMIT automático para listas (não agrega)
  const isAggregate =
    /\bcount\s*\(|\bgroup\s+by\b|\bsum\s*\(|\bavg\s*\(|\bmin\s*\(|\bmax\s*\(/i.test(s);

  if (!isAggregate && !/\blimit\b/i.test(s)) s += " LIMIT 50";

  return s;
}

async function llmToSQL(userQuery) {
  if (!anthropic) throw new Error("Claude não configurado (ANTHROPIC_API_KEY ausente).");

  const resp = await anthropic.messages.create({
    model: "claude-3-5-sonnet-latest",
    max_tokens: 260,
    temperature: 0,
    system:
      "Converta perguntas em SQL DuckDB. " +
      "Use APENAS a tabela chat_rfb.main.empresas e as colunas fornecidas. " +
      "Gere UMA ÚNICA query SELECT. " +
      "Não use ';'. " +
      "Para listas, use LIMIT. " +
      "Para contagem, use COUNT(*). " +
      "Não invente tabelas/colunas.",
    messages: [{
      role: "user",
      content:
        `${SCHEMA_HINT}\n\nPergunta: ${userQuery}\n\n` +
        "Responda SOMENTE com a SQL (sem markdown, sem explicações)."
    }]
  });

  const raw = resp?.content?.[0]?.text || "";
  return sanitizeSQL(raw);
}

async function llmExplain(userQuery, sql, rows) {
  if (!anthropic) return null;

  const resp = await anthropic.messages.create({
    model: "claude-3-5-sonnet-latest",
    max_tokens: 240,
    temperature: 0.6,
    system:
      "Você é um assistente brasileiro, objetivo e amigável. " +
      "Use APENAS os dados retornados. Não invente nada. " +
      "Se vier vazio, diga que não encontrou.",
    messages: [{
      role: "user",
      content:
        `Pergunta: ${userQuery}\n` +
        `SQL executada: ${sql}\n` +
        `Resultado (JSON): ${JSON.stringify(rows)}\n\n` +
        "Explique em pt-BR de forma curta e útil."
    }]
  });

  return resp?.content?.[0]?.text || null;
}

/* ========================= FALLBACK (SEM CLAUDE) ========================= */
async function fallbackQuery(userQuery) {
  const q = String(userQuery || "").trim();
  const qUp = q.toUpperCase();

  // ✅ contagem simples (ex.: "quantas empresas ativas existem no Brasil")
  if (qUp.includes("QUANT") && qUp.includes("ATIV")) {
    const sql = `
      SELECT COUNT(*) AS total
      FROM chat_rfb.main.empresas
      WHERE situacao_cadastral = 'ATIVA'
    `;
    const rows = await queryAll(sql);
    return { sql, rows, mode: "count" };
  }

  // fallback antigo (CNPJ ou nome)
  const digits = q.replace(/\D/g, "");
  if (digits.length >= 8) {
    const cnpj = digits.slice(0, 8);
    const sql = `SELECT * FROM chat_rfb.main.empresas WHERE cnpj_basico = ? LIMIT 5`;
    const rows = await queryAll(sql, [cnpj]);
    return { sql, rows, mode: "list" };
  } else {
    const sql = `SELECT * FROM chat_rfb.main.empresas WHERE upper(razao_social) LIKE ? LIMIT 5`;
    const rows = await queryAll(sql, [`%${qUp}%`]);
    return { sql, rows, mode: "list" };
  }
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
  const start = Date.now();
  const debug = { stage: "start" };

  try {
    const q = String(req.body?.query || "").trim();
    if (!q) {
      return res.json({ answer: "Consulta vazia.", debug: { stage: "empty_query" } });
    }

    // 1) tenta Claude -> SQL
    let sql;
    try {
      debug.stage = "llm_to_sql";
      sql = await llmToSQL(q);
      debug.sql = sql;
    } catch (e) {
      // Claude falhou: NÃO quebra, cai no fallback
      debug.stage = "llm_failed_fallback";
      debug.error = String(e?.message || e);

      const fb = await fallbackQuery(q);
      const duration = Date.now() - start;

      // ✅ modo COUNT
      if (fb.mode === "count") {
        const total = Number(fb.rows?.[0]?.total || 0);
        return res.json({
          answer: `Claude indisponível agora (${debug.error}).\nTotal de empresas ATIVAS no Brasil: ${total}.`,
          sql: fb.sql,
          rows: fb.rows,
          duration_ms: duration,
          debug
        });
      }

      // ✅ modo LISTA
      if (!fb.rows?.length) {
        return res.json({
          answer: `Claude indisponível agora (${debug.error}).\nNenhum resultado encontrado no fallback.`,
          sql: fb.sql,
          rows: [],
          duration_ms: duration,
          debug
        });
      }

      return res.json({
        answer:
          `Claude indisponível agora (${debug.error}).\n` +
          `Mostrando resultados diretos.\n` +
          `Primeiro: ${fb.rows[0].razao_social} (CNPJ: ${fb.rows[0].cnpj_basico})`,
        sql: fb.sql,
        rows: fb.rows,
        duration_ms: duration,
        debug
      });
    }

    // 2) executa SQL no MotherDuck
    debug.stage = "run_sql";
    const rows = await queryAll(sql);

    // 3) Claude explica (se falhar, não quebra)
    debug.stage = "llm_explain";
    let answer = null;
    try {
      answer = await llmExplain(q, sql, rows);
    } catch (e) {
      debug.stage = "llm_explain_failed";
      debug.error = String(e?.message || e);
    }

    const duration = Date.now() - start;

    if (!answer) {
      if (!rows?.length) answer = "Nenhum resultado encontrado.";
      else answer = `Resultado obtido (${rows.length} linha(s)).`;
    }

    return res.json({
      answer,
      sql,
      rows,
      duration_ms: duration,
      debug
    });

  } catch (e) {
    debug.stage = debug.stage || "unknown";
    debug.error = String(e?.message || e);

    return res.status(500).json({
      answer: "Erro interno.",
      duration_ms: Date.now() - start,
      debug
    });
  }
});

/* ========================= START ========================= */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log(`🚀 BDC API on :${PORT}`);
  console.log(`🔐 Motherduck: ${MD_TOKEN ? "✅" : "❌"}`);
  console.log(`🤖 Claude: ${ANTHROPIC_API_KEY ? "✅" : "❌"}`);
});
