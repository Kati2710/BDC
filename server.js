// server.js
import express from "express";
import cors from "cors";
import duckdb from "duckdb";
import Anthropic from "@anthropic-ai/sdk";

/* ============================================================
   BDC CHAT API (MotherDuck + Claude)
   ✅ DSN estável: md:chat_rfb (SEM query params)
   ✅ Lazy connect (não conecta no start)
   ✅ Recria Database+Conn ao detectar erro de conexão
   ✅ Fila (serializa queries) p/ evitar concorrência na mesma conn
   ✅ Aliases: empresas -> empresas_janeiro2026 / empresas_chat -> empresas_chat_janeiro2026
   ✅ Auditoria obrigatória para PortaldaTransparencia + audit_sample
   ✅ dataset_meta (fonte + período)
   ✅ Explain em TEXTO PURO (sem markdown)
============================================================ */

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

/* ========================= CONFIG ========================= */
const MD_TOKEN = process.env.MOTHERDUCK_TOKEN || "";
const MD_DB = "md:chat_rfb"; // ✅ manter assim (sem parâmetros)

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

const DEFAULT_PREVIEW_LIMIT = 50;
const MAX_PREVIEW_LIMIT = 200;

const MODEL_SQL = process.env.ANTHROPIC_MODEL_SQL || "claude-3-5-sonnet-20241022";
const MODEL_EXPLAIN = process.env.ANTHROPIC_MODEL_EXPLAIN || "claude-3-5-sonnet-20241022";

/* ========================= TABLE ALIASES ========================= */
const TABLE_ALIASES = {
  "chat_rfb.main.empresas": "chat_rfb.main.empresas_janeiro2026",
  "chat_rfb.main.empresas_chat": "chat_rfb.main.empresas_chat_janeiro2026",
};

function applyTableAliases(sql) {
  let s = sql;
  for (const [from, to] of Object.entries(TABLE_ALIASES)) {
    const rx = new RegExp(`\\b${from.replace(/\./g, "\\.")}\\b`, "gi");
    s = s.replace(rx, to);
  }
  return s;
}

/* ========================= METADADOS DAS BASES ========================= */
const DATASETS_META = {
  receita_federal_janeiro2026: {
    id: "receita_federal_cnpj_janeiro2026",
    fonte: "Receita Federal do Brasil",
    base: "Cadastro Nacional da Pessoa Jurídica (CNPJ)",
    periodo: "Janeiro/2026",
    origem_url: "https://arquivos.receitafederal.gov.br",
    tabelas: [
      "chat_rfb.main.empresas_janeiro2026",
      "chat_rfb.main.empresas_chat_janeiro2026",
    ],
  },
  portal_transparencia: {
    id: "portal_transparencia_sancoes",
    fonte: "Portal da Transparência — CGU",
    base: "Sanções e Acordos Administrativos",
    periodicidade: "Atualização contínua",
    origem_url: "https://portaldatransparencia.gov.br",
    tabelas_prefixo: "PortaldaTransparencia.main.",
  },
};

function detectDatasetMeta(sql) {
  const s = (sql || "").toLowerCase();
  if (s.includes("chat_rfb.main.empresas_janeiro2026") || s.includes("chat_rfb.main.empresas_chat_janeiro2026")) {
    return DATASETS_META.receita_federal_janeiro2026;
  }
  if (s.includes("portaldatransparencia.main.")) {
    return DATASETS_META.portal_transparencia;
  }
  return null;
}

/* ========================= MOTHERDUCK (ROBUSTO) ========================= */
let db = null;
let conn = null;

// fila simples para não rodar duas queries no mesmo conn ao mesmo tempo
let queue = Promise.resolve();

function initDB() {
  db = new duckdb.Database(MD_DB, { motherduck_token: MD_TOKEN });
}

function closeConnSilent() {
  try { conn?.close(); } catch {}
  conn = null;
}

function resetDBAndConn() {
  closeConnSilent();
  db = null;
}

function isConnError(err) {
  const msg = String(err?.message || err || "").toLowerCase();
  return (
    msg.includes("connection was never established") ||
    msg.includes("has been closed already") ||
    msg.includes("connection error") ||
    msg.includes("timeout") ||
    msg.includes("socket")
  );
}

async function ensureConn() {
  if (!db) initDB();
  if (conn) return conn;

  conn = db.connect();

  // teste rápido
  await new Promise((resolve, reject) => {
    conn.all("SELECT 1 AS ok", (err) => (err ? reject(err) : resolve(true)));
  });

  return conn;
}

async function queryMD(sql) {
  // serializa
  queue = queue.then(async () => {
    // 1ª tentativa
    try {
      const c = await ensureConn();
      return await new Promise((resolve, reject) => {
        c.all(sql, (err, rows) => (err ? reject(err) : resolve(rows)));
      });
    } catch (e1) {
      // se for erro de conexão, reseta e tenta de novo
      if (isConnError(e1)) {
        resetDBAndConn();

        // pequeno backoff
        await new Promise((r) => setTimeout(r, 250));

        try {
          const c2 = await ensureConn();
          return await new Promise((resolve, reject) => {
            c2.all(sql, (err, rows) => (err ? reject(err) : resolve(rows)));
          });
        } catch (e2) {
          throw e2;
        }
      }
      throw e1;
    }
  });

  return queue;
}

/* ========================= SCHEMA COM CACHE ========================= */
let cachedSchema = null;
let cacheExpiry = null;
const CACHE_DURATION = 3600000;
const ALLOWED_SCHEMA = "main";

async function getSchema() {
  if (cachedSchema && Date.now() < cacheExpiry) {
    console.log("📦 Schema em CACHE");
    return cachedSchema;
  }

  console.log("🔄 Buscando schema do MotherDuck...");
  const allTables = await queryMD(`
    SELECT table_catalog, table_schema, table_name
    FROM information_schema.tables
    WHERE table_catalog IN ('chat_rfb', 'PortaldaTransparencia')
      AND table_schema = '${ALLOWED_SCHEMA}'
    ORDER BY table_catalog, table_name
  `);

  console.log(`📋 Encontradas ${allTables.length} tabelas relevantes`);

  let schema = "TABELAS E COLUNAS DISPONÍVEIS:\n\n";
  for (const t of allTables) {
    const fullName = `${t.table_catalog}.${t.table_schema}.${t.table_name}`;
    console.log(`  ├─ ${fullName}`);

    const cols = await queryMD(`
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_catalog='${t.table_catalog}'
        AND table_schema='${t.table_schema}'
        AND table_name='${t.table_name}'
      ORDER BY ordinal_position
    `);

    schema += `TABELA: ${fullName}\nColunas (${cols.length}):\n`;
    for (const c of cols) schema += `  • ${c.column_name} (${c.data_type})\n`;
    schema += "\n";
  }

  schema += `
REGRAS:
- Somente SELECT/CTE.
- Use as tabelas canônicas: chat_rfb.main.empresas e chat_rfb.main.empresas_chat (o backend converte para Janeiro/2026).
- Se usar PortaldaTransparencia.*, inclua SEMPRE: _audit_url_download, _audit_data_disponibilizacao_gov, _audit_periodicidade_atualizacao_gov, _audit_arquivo_csv_origem, _audit_linha_csv, _audit_row_hash
- Se não for agregação, use LIMIT.
`;

  cachedSchema = schema;
  cacheExpiry = Date.now() + CACHE_DURATION;
  console.log("✅ Schema em cache por 1 hora\n");
  return schema;
}

/* ========================= SQL SAFETY ========================= */
function stripFences(sql) {
  return sql.replace(/```sql|```/gi, "").trim();
}
function hasMultipleStatements(sql) {
  const s = sql.trim();
  const semi = (s.match(/;/g) || []).length;
  if (semi === 0) return false;
  if (semi === 1 && s.endsWith(";")) return false;
  return true;
}
function isSelectLike(sql) {
  const s = sql.trim().toLowerCase();
  return s.startsWith("select") || s.startsWith("with");
}
const BLOCKED = [
  /\b(insert|update|delete|drop|alter|create|truncate|merge|grant|revoke)\b/i,
  /\bpragma\b/i, /\battach\b/i, /\binstall\b/i, /\bload\b/i,
  /\bcopy\b/i, /\bexport\b/i, /\bcall\b/i, /\bset\b/i,
  /\bread_(csv|parquet|json|ndjson)\b/i,
  /\bsecret\b/i, /\bhttpfs\b/i, /\bs3\b/i, /\bgcs\b/i, /\bazure\b/i,
];
function findBlockedReason(sql) {
  for (const rx of BLOCKED) {
    const m = sql.match(rx);
    if (m) return `Bloqueado: ${rx.source} (match: "${m[0]}")`;
  }
  return null;
}
function cleanSQL(sqlRaw) {
  let s = stripFences(sqlRaw).replace(/\s+/g, " ").trim();
  s = s.replace(/;+$/g, "");
  if (!isSelectLike(s)) throw new Error("SQL inválida: somente SELECT/CTE.");
  if (hasMultipleStatements(s)) throw new Error("SQL inválida: múltiplas statements.");
  if (/--|\/\*/.test(s)) throw new Error("SQL bloqueada: comentários não permitidos.");
  const reason = findBlockedReason(s);
  if (reason) throw new Error(reason);
  return s;
}
function looksAggregated(sql) {
  return /count\(|sum\(|avg\(|min\(|max\(|group by/i.test(sql.toLowerCase());
}
function hasLimit(sql) {
  return /\slimit\s+\d+/i.test(sql);
}
function enforceLimit(sql, limit = 50) {
  if (looksAggregated(sql) || hasLimit(sql)) return sql;
  return `${sql} LIMIT ${limit}`;
}
function toCountQuery(sql) {
  const noLimit = sql.replace(/\slimit\s+\d+/i, "").trim();
  return `SELECT COUNT(*) AS total_rows FROM (${noLimit}) t`;
}
function coerceBigIntRows(rows) {
  return (rows || []).map((row) => {
    const clean = {};
    for (const [k, v] of Object.entries(row)) clean[k] = typeof v === "bigint" ? Number(v) : v;
    return clean;
  });
}

/* ========================= AUDIT ENFORCEMENT ========================= */
const AUDIT_COLS = [
  "_audit_url_download",
  "_audit_data_disponibilizacao_gov",
  "_audit_periodicidade_atualizacao_gov",
  "_audit_arquivo_csv_origem",
  "_audit_linha_csv",
  "_audit_row_hash",
];
function touchesPortal(sql) {
  return /\bPortaldaTransparencia\.main\./i.test(sql);
}
function hasAllAuditCols(sql) {
  return AUDIT_COLS.every((c) => new RegExp(`\\b${c}\\b`, "i").test(sql));
}
function extractAuditSample(rows) {
  if (!Array.isArray(rows) || !rows.length) return null;
  const r = rows.find(x => x && Object.keys(x).some(k => String(k).startsWith("_audit_"))) || rows[0];
  if (!r) return null;
  const hasAny = Object.keys(r).some(k => String(k).startsWith("_audit_"));
  if (!hasAny) return null;
  return AUDIT_COLS.reduce((acc, col) => (acc[col] = r[col] ?? null, acc), {});
}

/* ========================= LLM SQL ========================= */
async function generateSQL({ schema, userQuery, previewLimit }) {
  const system =
    "Você é especialista em SQL DuckDB. Gere APENAS a query SQL (sem explicações, sem markdown, sem comentários). " +
    "Somente SELECT ou WITH ... SELECT. Use nomes completos (catalog.schema.table). " +
    "Use as tabelas canônicas chat_rfb.main.empresas e chat_rfb.main.empresas_chat (o backend converte para Janeiro/2026). " +
    "Se usar PortaldaTransparencia, inclua OBRIGATORIAMENTE no SELECT: " + AUDIT_COLS.join(", ") + ". " +
    "Se não for agregação, inclua LIMIT.";

  const out = await anthropic.messages.create({
    model: MODEL_SQL,
    max_tokens: 700,
    temperature: 0,
    system,
    messages: [{
      role: "user",
      content:
        `${schema}\n\nPERGUNTA: "${userQuery}"\n\n` +
        `Gere SQL válida (somente SELECT/CTE). Se não for agregação, use LIMIT ${previewLimit}.`
    }]
  });

  return out.content?.[0]?.text ?? "";
}

/* ========================= ROTA /chat ========================= */
app.post("/chat", async (req, res) => {
  const start = Date.now();

  try {
    const userQuery = (req.body?.query || "").trim();
    const wantTotal = Boolean(req.body?.include_total);
    let previewLimit = Number(req.body?.limit ?? DEFAULT_PREVIEW_LIMIT);

    if (!userQuery) return res.json({ error: "Query vazia" });
    if (!Number.isFinite(previewLimit) || previewLimit <= 0) previewLimit = DEFAULT_PREVIEW_LIMIT;
    previewLimit = Math.min(previewLimit, MAX_PREVIEW_LIMIT);

    console.log("\n" + "=".repeat(60));
    console.log("❓ PERGUNTA:", userQuery);
    console.log("=".repeat(60));

    const schema = await getSchema();

    // SQL
    let rawSql = await generateSQL({ schema, userQuery, previewLimit });
    let sql = enforceLimit(cleanSQL(rawSql), previewLimit);
    sql = applyTableAliases(sql);

    // Auditoria obrigatória no Portal (1 regeneração)
    if (touchesPortal(sql) && !hasAllAuditCols(sql)) {
      rawSql = await generateSQL({
        schema,
        userQuery: userQuery + " (INCLUA TODAS as colunas _audit_*)",
        previewLimit
      });
      sql = applyTableAliases(enforceLimit(cleanSQL(rawSql), previewLimit));
      if (!hasAllAuditCols(sql)) throw new Error("Portal exige colunas _audit_* no SELECT.");
    }

    console.log("📝 SQL:", sql.slice(0, 240) + (sql.length > 240 ? "..." : ""));

    // Exec preview
    const rowsRaw = await queryMD(sql);
    const rows = coerceBigIntRows(rowsRaw);

    const audit_sample = extractAuditSample(rows);
    const dataset_meta = detectDatasetMeta(sql);

    // Total opcional
    let totalRows = null;
    if (wantTotal) {
      try {
        const countSql = toCountQuery(sql);
        const totalRes = await queryMD(countSql);
        const t = totalRes?.[0]?.total_rows;
        totalRows = typeof t === "bigint" ? Number(t) : t ?? null;
      } catch {}
    }

    // Explain (texto puro)
    const exp = await anthropic.messages.create({
      model: MODEL_EXPLAIN,
      max_tokens: 260,
      temperature: 0.35,
      system:
        "Responda em TEXTO PURO (sem markdown). Máximo 6 linhas. " +
        "Use separador de milhar (1.234.567). " +
        "Se houver _audit_*, diga: 'Rastreabilidade disponível (URL, arquivo, linha e hash)'.",
      messages: [{
        role: "user",
        content:
          `Pergunta: "${userQuery}"\n` +
          `SQL: ${sql}\n` +
          (dataset_meta ? `Dataset: ${JSON.stringify(dataset_meta)}\n` : "") +
          (audit_sample ? `Audit: ${JSON.stringify(audit_sample)}\n` : "") +
          `Preview: ${JSON.stringify(rows.slice(0, 5))}\n` +
          `Responda:`
      }]
    });

    const answer = (exp.content?.[0]?.text ?? "").trim();
    const duration = Date.now() - start;

    return res.json({
      answer,
      sql,
      rows_preview: rows,
      preview_count: rows.length,
      total_rows: totalRows,
      audit_sample,
      audit_required: touchesPortal(sql),
      dataset_meta,
      duration_ms: duration
    });

  } catch (err) {
    const duration = Date.now() - start;
    console.error("❌ ERRO:", err?.message || err);
    return res.status(500).json({
      error: err?.message || "Erro desconhecido",
      duration_ms: duration
    });
  }
});

/* ========================= HEALTH ========================= */
app.get("/health", async (_, res) => {
  try {
    await ensureConn(); // tenta conectar sob demanda
    res.json({
      ok: true,
      timestamp: new Date().toISOString(),
      motherduck_token: MD_TOKEN ? "configured" : "missing",
      anthropic_key: process.env.ANTHROPIC_API_KEY ? "configured" : "missing",
      models: { sql: MODEL_SQL, explain: MODEL_EXPLAIN },
      aliases: TABLE_ALIASES,
      dataset_receita_federal: DATASETS_META.receita_federal_janeiro2026
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

/* ========================= START ========================= */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log("╔════════════════════════════════════════╗");
  console.log("║     CHAT-RFB API RODANDO (BDC)        ║");
  console.log("╚════════════════════════════════════════╝");
  console.log(`📡 Porta: ${PORT}`);
  console.log(`🔐 MotherDuck token: ${MD_TOKEN ? "✅" : "❌"}`);
  console.log(`🤖 Claude key: ${process.env.ANTHROPIC_API_KEY ? "✅" : "❌"}`);
  console.log(`🧠 Models: ${MODEL_SQL} / ${MODEL_EXPLAIN}`);
  console.log(`🧷 Aliases:`, TABLE_ALIASES);
  console.log("🟡 MotherDuck: lazy connect (vai conectar na primeira query/health)\n");
});
