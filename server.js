// server.js
import express from "express";
import cors from "cors";
import Anthropic from "@anthropic-ai/sdk";
import { DuckDBInstance } from "@duckdb/node-api";

/* ============================================================
   BDC — CHAT-RFB API (MotherDuck + Claude)
   - MotherDuck via @duckdb/node-api (mais estável)
   - Conexão única + auto-reconnect + retry
   - Cache de schema (1h)
   - Somente SELECT/CTE
   - Aliases: empresas -> empresas_janeiro2026
   - dataset_meta: Receita Federal (Jan/2026) + Portal da Transparência
   - Auditoria obrigatória em PortaldaTransparencia
   - Resposta em texto puro (sem markdown)
============================================================ */

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

/* ========================= CONFIG ========================= */
const PORT = process.env.PORT || 10000;

const MD_TOKEN = process.env.MOTHERDUCK_TOKEN || "";
const MD_DBNAME = process.env.MOTHERDUCK_DB || "chat_rfb";

// TTL = 0s evita cache problemático de instância
const MD_PATH =
  `md:${MD_DBNAME}?motherduck_token=${encodeURIComponent(MD_TOKEN)}` +
  `&dbinstance_inactivity_ttl=0s`;

const MODEL_SQL = process.env.ANTHROPIC_MODEL_SQL || "claude-3-5-sonnet-20241022";
const MODEL_EXPLAIN = process.env.ANTHROPIC_MODEL_EXPLAIN || "claude-3-5-sonnet-20241022";

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

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

/* ========================= DATASET META ========================= */
const DATASETS_META = {
  receita_federal_janeiro2026: {
    id: "receita_federal_cnpj_janeiro2026",
    fonte: "Receita Federal do Brasil",
    base: "CNPJ — Cadastro Nacional da Pessoa Jurídica",
    periodo: "Janeiro/2026",
    origem_url: "https://arquivos.receitafederal.gov.br",
    tabelas: ["chat_rfb.main.empresas_janeiro2026", "chat_rfb.main.empresas_chat_janeiro2026"],
  },
  portal_transparencia: {
    id: "portal_transparencia_sancoes",
    fonte: "Portal da Transparência — CGU",
    base: "Sanções + Acordos Administrativos",
    periodicidade: "Atualização contínua",
    origem_url: "https://portaldatransparencia.gov.br",
    tabelas_prefix: "PortaldaTransparencia.main.",
  },
};

function detectDatasetMeta(sql) {
  const s = (sql || "").toLowerCase();
  if (s.includes("empresas_janeiro2026") || s.includes("empresas_chat_janeiro2026")) {
    return DATASETS_META.receita_federal_janeiro2026;
  }
  if (s.includes("portaldatransparencia.")) {
    return DATASETS_META.portal_transparencia;
  }
  return null;
}

/* ========================= AUDITORIA ========================= */
const AUDIT_COLS = [
  "_audit_url_download",
  "_audit_data_disponibilizacao_gov",
  "_audit_periodicidade_atualizacao_gov",
  "_audit_arquivo_csv_origem",
  "_audit_linha_csv",
  "_audit_row_hash",
];

function touchesPortal(sql) {
  return /\bPortaldaTransparencia\./i.test(sql);
}
function hasAllAuditCols(sql) {
  return AUDIT_COLS.every(c => new RegExp(`\\b${c}\\b`, "i").test(sql));
}
function extractAuditSample(rows) {
  if (!Array.isArray(rows) || !rows.length) return null;
  const r = rows.find(x => x && Object.keys(x).some(k => k.startsWith("_audit_"))) || rows[0];
  if (!r || typeof r !== "object") return null;
  const hasAny = Object.keys(r).some(k => k.startsWith("_audit_"));
  if (!hasAny) return null;
  return AUDIT_COLS.reduce((acc, col) => {
    acc[col] = r[col] ?? null;
    return acc;
  }, {});
}

/* ========================= SQL SECURITY ========================= */
function stripFences(sql) {
  return (sql || "").replace(/```sql|```/gi, "").trim();
}
function hasMultipleStatements(sql) {
  const s = sql.trim();
  const semiCount = (s.match(/;/g) || []).length;
  if (semiCount === 0) return false;
  if (semiCount === 1 && s.endsWith(";")) return false;
  return true;
}
function isSelectLike(sql) {
  const s = sql.trim().toLowerCase();
  return s.startsWith("select") || s.startsWith("with");
}

const BLOCKED_SQL_PATTERNS = [
  /\b(insert|update|delete|drop|alter|create|truncate|merge|grant|revoke)\b/i,
  /\bpragma\b/i,
  /\battach\b/i,
  /\bdetach\b/i,
  /\binstall\b/i,
  /\bload\b/i,
  /\bcopy\b/i,
  /\bexport\b/i,
  /\bcall\b/i,
  /\bset\b/i,
  /\bcreate\s+secret\b/i,
  /\bsecret\b/i,
  /\bhttpfs\b/i,
  /\bs3\b/i,
  /\bgcs\b/i,
  /\bazure\b/i,
  /\bread_(csv|parquet|json|ndjson)\b/i,
];

function findBlockedReason(sql) {
  for (const rx of BLOCKED_SQL_PATTERNS) {
    const m = sql.match(rx);
    if (m) return `Padrão bloqueado: ${rx.source} (match: "${m[0]}")`;
  }
  return null;
}

function cleanSQL(sqlRaw) {
  let s = stripFences(sqlRaw).replace(/\s+/g, " ").trim();
  s = s.replace(/;+$/g, "");
  if (!isSelectLike(s)) throw new Error("SQL inválida: somente SELECT/CTE permitido.");
  if (hasMultipleStatements(s)) throw new Error("SQL inválida: múltiplas statements.");
  if (/--|\/\*/.test(s)) throw new Error("SQL bloqueada: comentários não permitidos.");
  const reason = findBlockedReason(s);
  if (reason) throw new Error(`SQL bloqueada: ${reason}`);
  return s;
}

function looksAggregated(sql) {
  const s = sql.toLowerCase();
  return /count\(|sum\(|avg\(|min\(|max\(|group by/i.test(s);
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

/* ========================= TEXT SANITIZER (remove markdown) ========================= */
function toPlainText(s) {
  let x = String(s || "");
  x = x.replace(/```[\s\S]*?```/g, "");          // remove code blocks
  x = x.replace(/^#{1,6}\s+/gm, "");            // remove headings
  x = x.replace(/\*\*(.*?)\*\*/g, "$1");        // bold
  x = x.replace(/\*(.*?)\*/g, "$1");            // italic
  x = x.replace(/^\s*---\s*$/gm, "");           // hr
  x = x.replace(/\n{3,}/g, "\n\n").trim();
  return x;
}

/* ========================= MOTHERDUCK CONNECTION (stable) ========================= */
let mdInstance = null;
let mdConn = null;
let mdConnReady = false;

async function connectMotherDuck() {
  if (!MD_TOKEN) throw new Error("MOTHERDUCK_TOKEN ausente.");
  mdInstance = await DuckDBInstance.create(MD_PATH);
  mdConn = await mdInstance.connect();
  mdConnReady = true;
  return true;
}

function isConnError(err) {
  const msg = String(err?.message || err || "");
  return (
    msg.includes("Connection was never established") ||
    msg.includes("has been closed already") ||
    msg.includes("Connection Error") ||
    msg.includes("Socket") ||
    msg.includes("ECONNRESET") ||
    msg.includes("ETIMEDOUT")
  );
}

async function ensureConn() {
  if (mdConnReady && mdConn) return;
  await connectMotherDuck();
}

async function queryMD(sql, attempt = 1) {
  const MAX = 3;
  try {
    await ensureConn();
    return await mdConn.all(sql);
  } catch (err) {
    if (attempt < MAX && isConnError(err)) {
      console.warn(`⚠️ MotherDuck conn falhou (tentativa ${attempt}/${MAX}). Reconnect...`);
      try {
        mdConnReady = false;
        mdConn = null;
        mdInstance = null;
        await connectMotherDuck();
      } catch (e) {
        console.warn("⚠️ Reconnect falhou:", e?.message || e);
      }
      await new Promise(r => setTimeout(r, 250 * attempt));
      return queryMD(sql, attempt + 1);
    }
    throw err;
  }
}

/* ========================= SCHEMA CACHE ========================= */
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
    const full = `${t.table_catalog}.${t.table_schema}.${t.table_name}`;
    const cols = await queryMD(`
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_catalog = '${t.table_catalog}'
        AND table_schema = '${t.table_schema}'
        AND table_name = '${t.table_name}'
      ORDER BY ordinal_position
    `);

    schema += `TABELA: ${full}\nColunas (${cols.length}):\n`;
    for (const c of cols) schema += `  • ${c.column_name} (${c.data_type})\n`;
    schema += "\n";
  }

  schema += `
═══════════════════════════════════════════════════════════════
REGRAS CRÍTICAS PARA GERAR SQL:
1) Somente SELECT ou WITH ... SELECT.
2) Tabelas canônicas (RFB): chat_rfb.main.empresas e chat_rfb.main.empresas_chat (o backend aplica alias para *_janeiro2026).
3) Portal da Transparência: se usar PortaldaTransparencia.*, inclua SEMPRE no SELECT:
   ${AUDIT_COLS.join(", ")}
4) PERFORMANCE: se não for agregação, usar LIMIT.
`;

  cachedSchema = schema;
  cacheExpiry = Date.now() + CACHE_DURATION;
  console.log("✅ Schema em cache por 1 hora\n");
  return schema;
}

/* ========================= LLM SQL GENERATION ========================= */
async function generateSQL({ schema, userQuery, previewLimit }) {
  const llmSQL = await anthropic.messages.create({
    model: MODEL_SQL,
    max_tokens: 700,
    temperature: 0,
    system:
      "Você é especialista em SQL DuckDB. Gere APENAS a query SQL (sem explicações, sem markdown, sem comentários). " +
      "Somente SELECT/CTE. Use nomes completos (catalog.schema.table). " +
      "Se usar PortaldaTransparencia.*, inclua obrigatoriamente: " + AUDIT_COLS.join(", ") + ".",
    messages: [
      {
        role: "user",
        content:
          `${schema}\n\n` +
          `PERGUNTA DO USUÁRIO: "${userQuery}"\n\n` +
          `Gere SQL válida. Se não for agregação, use LIMIT ${previewLimit}.`,
      },
    ],
  });

  return llmSQL.content?.[0]?.text ?? "";
}

/* ========================= ROUTES ========================= */
const DEFAULT_PREVIEW_LIMIT = 50;
const MAX_PREVIEW_LIMIT = 200;

app.post("/chat", async (req, res) => {
  const startTime = Date.now();
  try {
    const userQuery = String(req.body?.query || "").trim();
    const wantTotal = Boolean(req.body?.include_total);
    let previewLimit = Number(req.body?.limit ?? DEFAULT_PREVIEW_LIMIT);

    if (!userQuery) return res.json({ error: "Query vazia" });
    if (!Number.isFinite(previewLimit) || previewLimit <= 0) previewLimit = DEFAULT_PREVIEW_LIMIT;
    previewLimit = Math.min(previewLimit, MAX_PREVIEW_LIMIT);

    console.log("\n" + "=".repeat(60));
    console.log("❓ PERGUNTA:", userQuery);
    console.log("=".repeat(60));

    const schema = await getSchema();

    // 1) SQL
    let rawSql = await generateSQL({ schema, userQuery, previewLimit });
    let sql = enforceLimit(cleanSQL(rawSql), previewLimit);
    sql = applyTableAliases(sql);

    // 2) Enforce auditoria Portal
    if (touchesPortal(sql) && !hasAllAuditCols(sql)) {
      rawSql = await generateSQL({
        schema,
        userQuery: userQuery + " (INCLUA TODAS AS COLUNAS _audit_*)",
        previewLimit,
      });
      sql = applyTableAliases(enforceLimit(cleanSQL(rawSql), previewLimit));
      if (!hasAllAuditCols(sql)) {
        throw new Error("Consulta ao Portal exige colunas _audit_* no SELECT.");
      }
    }

    console.log("📝 SQL:", sql.slice(0, 240) + (sql.length > 240 ? "..." : ""));

    // 3) Preview
    const rows_preview = await queryMD(sql);
    const audit_sample = extractAuditSample(rows_preview);
    const dataset_meta = detectDatasetMeta(sql);

    // 4) Total opcional
    let total_rows = null;
    if (wantTotal) {
      try {
        const countSql = toCountQuery(sql);
        const totalRes = await queryMD(countSql);
        total_rows = Number(totalRes?.[0]?.total_rows ?? 0);
      } catch {
        total_rows = null;
      }
    }

    // 5) Explain (texto puro)
    const llmExplain = await anthropic.messages.create({
      model: MODEL_EXPLAIN,
      max_tokens: 240,
      temperature: 0.4,
      system:
        "Você é assistente brasileiro de inteligência empresarial. " +
        "Responda em TEXTO PURO, sem markdown, sem títulos, no máximo 6 linhas. " +
        "Use separador de milhar (1.234.567). " +
        "Se houver rastreabilidade (_audit_*), diga 'Rastreabilidade disponível'. " +
        "Sugira no máximo 2 próximos filtros.",
      messages: [
        {
          role: "user",
          content:
            `Pergunta: "${userQuery}"\n` +
            `SQL: ${sql}\n` +
            (dataset_meta ? `Dataset: ${JSON.stringify(dataset_meta)}\n` : "") +
            (audit_sample ? `Audit: ${JSON.stringify(audit_sample)}\n` : "") +
            `Preview (até 5 linhas): ${JSON.stringify(rows_preview.slice(0, 5))}\n` +
            `Responda agora (texto puro):`,
        },
      ],
    });

    const answer = toPlainText(llmExplain.content?.[0]?.text ?? "");
    const duration_ms = Date.now() - startTime;

    return res.json({
      answer,
      sql,
      rows_preview,
      preview_count: rows_preview.length,
      total_rows,
      audit_sample,
      audit_required: touchesPortal(sql),
      dataset_meta,
      duration_ms,
    });
  } catch (err) {
    const duration_ms = Date.now() - startTime;
    console.error("❌ ERRO:", err?.message || err);
    return res.status(500).json({
      error: err?.message || "Erro desconhecido",
      duration_ms,
    });
  }
});

app.get("/health", async (_, res) => {
  try {
    // força um ping simples
    const rows = await queryMD("SELECT 1 AS ok");
    res.json({
      ok: true,
      ping: rows?.[0]?.ok ?? 1,
      timestamp: new Date().toISOString(),
      md_path: MD_PATH.replace(/motherduck_token=[^&]+/i, "motherduck_token=***"),
      models: { sql: MODEL_SQL, explain: MODEL_EXPLAIN },
      canonical_tables: TABLE_ALIASES,
      cache: cachedSchema ? "active" : "empty",
    });
  } catch (e) {
    res.status(500).json({
      ok: false,
      error: String(e?.message || e),
      timestamp: new Date().toISOString(),
    });
  }
});

app.post("/clear-cache", (_, res) => {
  cachedSchema = null;
  cacheExpiry = null;
  console.log("🗑️ Cache limpo!");
  res.json({ ok: true, message: "Cache limpo" });
});

/* ========================= START ========================= */
app.listen(PORT, () => {
  console.log("╔════════════════════════════════════════╗");
  console.log("║     CHAT-RFB API RODANDO (BDC)        ║");
  console.log("╚════════════════════════════════════════╝");
  console.log(`📡 Porta: ${PORT}`);
  console.log(`🔐 MotherDuck token: ${MD_TOKEN ? "✅" : "❌"}`);
  console.log(`🤖 Claude key: ${process.env.ANTHROPIC_API_KEY ? "✅" : "❌"}`);
  console.log(`🧠 Models: ${MODEL_SQL} / ${MODEL_EXPLAIN}`);
  console.log(`🧠 MotherDuck path: ${MD_PATH.replace(/motherduck_token=[^&]+/i, "motherduck_token=***")}`);
  console.log(`🧷 Aliases:`, TABLE_ALIASES);
});
