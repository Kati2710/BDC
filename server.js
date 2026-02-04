// server.js
import express from "express";
import cors from "cors";
import Anthropic from "@anthropic-ai/sdk";
import { DuckDBInstance } from "@duckdb/node-api";

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

/* ========================= CONFIG ========================= */
const PORT = process.env.PORT || 10000;
const MD_TOKEN = process.env.MOTHERDUCK_TOKEN || "";
if (!MD_TOKEN) {
  console.error("ERRO CRÍTICO: MOTHERDUCK_TOKEN não está definido no ambiente.");
}

const MD_DBNAME = process.env.MOTHERDUCK_DB || "chat_rfb";
const MD_PATH = `md:${MD_DBNAME}?motherduck_token=${encodeURIComponent(MD_TOKEN)}&dbinstance_inactivity_ttl=0s`;

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
    origem_url: "https://arquivos.receitafederal.gov.br"
  },
  portal_transparencia: {
    id: "portal_transparencia_sancoes",
    fonte: "Portal da Transparência — CGU",
    base: "Sanções + Acordos Administrativos",
    origem_url: "https://portaldatransparencia.gov.br"
  }
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

/* ========================= MOTHERDUCK CONNECTION (mais robusta) ========================= */
let mdInstance = null;
let mdConn = null;
let mdConnReady = false;

async function connectMotherDuck() {
  if (!MD_TOKEN) throw new Error("MOTHERDUCK_TOKEN ausente.");
  console.log("Tentando conectar ao MotherDuck via HTTP...");
  mdInstance = await DuckDBInstance.create(MD_PATH);
  mdConn = await mdInstance.connect();
  mdConnReady = true;
  console.log("Conexão MotherDuck estabelecida com sucesso.");
}

function isConnError(err) {
  const msg = String(err?.message || err || "").toLowerCase();
  return (
    msg.includes("connection was never established") ||
    msg.includes("has been closed already") ||
    msg.includes("connection error") ||
    msg.includes("econnreset") ||
    msg.includes("etimedout") ||
    msg.includes("timeout") ||
    msg.includes("network")
  );
}

async function ensureConn() {
  if (mdConnReady && mdConn) return;
  console.log("Reconectando MotherDuck...");
  mdConnReady = false;
  mdConn = null;
  mdInstance = null;
  await connectMotherDuck();
}

async function queryMD(sql, attempt = 1) {
  const MAX_ATTEMPTS = 3;
  try {
    await ensureConn();
    return await mdConn.all(sql);
  } catch (err) {
    if (attempt < MAX_ATTEMPTS && isConnError(err)) {
      console.warn(`MotherDuck falhou (tentativa ${attempt}/${MAX_ATTEMPTS}): ${err.message}`);
      await new Promise(r => setTimeout(r, 500 * attempt));
      return queryMD(sql, attempt + 1);
    }
    console.error("Falha definitiva na query MotherDuck:", err);
    throw err;
  }
}

/* ========================= SCHEMA CACHE ========================= */
let cachedSchema = null;
let cacheExpiry = null;
const CACHE_DURATION = 3600000; // 1 hora

const ALLOWED_SCHEMA = "main";

async function getSchema() {
  if (cachedSchema && Date.now() < cacheExpiry) {
    console.log("Schema retornado do cache");
    return cachedSchema;
  }

  console.log("Buscando schema do MotherDuck...");
  const allTables = await queryMD(`
    SELECT table_catalog, table_schema, table_name
    FROM information_schema.tables
    WHERE table_catalog IN ('chat_rfb', 'PortaldaTransparencia')
      AND table_schema = '${ALLOWED_SCHEMA}'
    ORDER BY table_catalog, table_name
  `);

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
    for (const c of cols) {
      schema += ` • ${c.column_name} (${c.data_type})\n`;
    }
    schema += "\n";
  }

  schema += `
REGRAS IMPORTANTES:
- Somente SELECT ou WITH ... SELECT. Nunca INSERT, UPDATE, DELETE, DROP, CREATE, PRAGMA, INSTALL, LOAD, COPY, etc.
- Para PortaldaTransparencia.*, inclua SEMPRE: ${AUDIT_COLS.join(", ")}
- Use tabelas canônicas: chat_rfb.main.empresas / chat_rfb.main.empresas_chat (backend aplica alias para *_janeiro2026)
- Campos JSON: use json_extract_string(empresa, '$.razao_social'), estabelecimentos[1].uf, len(socios), etc.
`;

  cachedSchema = schema;
  cacheExpiry = Date.now() + CACHE_DURATION;
  console.log("Schema cacheado por 1 hora");
  return schema;
}

/* ========================= LLM SQL ========================= */
async function generateSQL({ schema, userQuery, previewLimit }) {
  const llmSQL = await anthropic.messages.create({
    model: MODEL_SQL,
    max_tokens: 700,
    temperature: 0,
    system:
      "Você é especialista em SQL DuckDB para a base CNPJ da Receita Federal. " +
      "Gere APENAS a query SQL (sem texto extra, sem markdown, sem comentários). " +
      "Use nomes completos (chat_rfb.main.empresas). " +
      "Somente SELECT ou WITH ... SELECT. " +
      "Para campos JSON use json_extract_string() ou indexação [1]. " +
      "Contagem de arrays: SUM(len(estabelecimentos)) ou SUM(len(socios)). " +
      "Se usar PortaldaTransparencia.*, inclua obrigatoriamente: " + AUDIT_COLS.join(", "),
    messages: [
      {
        role: "user",
        content:
          `${schema}\n\nPERGUNTA DO USUÁRIO: "${userQuery}"\n\n` +
          `Gere SQL válida. Se não for agregação, inclua LIMIT ${previewLimit}.`,
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
    previewLimit = Math.min(Math.max(previewLimit, 1), MAX_PREVIEW_LIMIT);

    console.log("Pergunta recebida:", userQuery);

    const schema = await getSchema();
    let rawSql = await generateSQL({ schema, userQuery, previewLimit });
    let sql = applyTableAliases(enforceLimit(cleanSQL(rawSql), previewLimit));

    if (touchesPortal(sql) && !hasAllAuditCols(sql)) {
      rawSql = await generateSQL({
        schema,
        userQuery: userQuery + " (OBRIGATÓRIO: inclua TODAS as colunas _audit_*)",
        previewLimit,
      });
      sql = applyTableAliases(enforceLimit(cleanSQL(rawSql), previewLimit));
      if (!hasAllAuditCols(sql)) throw new Error("Consulta ao Portal exige colunas _audit_*");
    }

    console.log("SQL executada:", sql.substring(0, 200) + (sql.length > 200 ? "..." : ""));

    const rows_preview = await queryMD(sql);
    const audit_sample = extractAuditSample(rows_preview);
    const dataset_meta = detectDatasetMeta(sql);

    let total_rows = null;
    if (wantTotal) {
      try {
        const totalRes = await queryMD(toCountQuery(sql));
        total_rows = Number(totalRes?.[0]?.total_rows ?? 0);
      } catch (e) {
        console.warn("Falha ao contar total:", e.message);
      }
    }

    const llmExplain = await anthropic.messages.create({
      model: MODEL_EXPLAIN,
      max_tokens: 240,
      temperature: 0.4,
      system:
        "Você é assistente brasileiro de inteligência empresarial. " +
        "Responda em TEXTO PURO, no máximo 6 linhas. " +
        "Use separador de milhar (1.234.567). " +
        "Se houver _audit_*, mencione 'Rastreabilidade disponível'. " +
        "Sugira no máximo 2 filtros úteis.",
      messages: [
        {
          role: "user",
          content:
            `Pergunta: "${userQuery}"\nSQL: ${sql}\n` +
            (dataset_meta ? `Dataset: ${JSON.stringify(dataset_meta)}\n` : "") +
            (audit_sample ? `Audit: ${JSON.stringify(audit_sample)}\n` : "") +
            `Preview (primeiras 5 linhas): ${JSON.stringify(rows_preview.slice(0, 5))}\n` +
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
    console.error("Erro na rota /chat:", err);
    return res.status(500).json({ error: String(err?.message || err), duration_ms });
  }
});

app.get("/health", async (_, res) => {
  try {
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
    console.error("Erro no /health:", e);
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

app.listen(PORT, () => {
  console.log("╔════════════════════════════════════════╗");
  console.log("║ CHAT-RFB API RODANDO (BDC) ║");
  console.log("╚════════════════════════════════════════╝");
  console.log(`📡 Porta: ${PORT}`);
  console.log(`🔐 MotherDuck token: ${MD_TOKEN ? "✅ configurado" : "❌ AUSENTE"}`);
  console.log(`🤖 Claude key: ${process.env.ANTHROPIC_API_KEY ? "✅" : "❌"}`);
  console.log(`🧠 Models: ${MODEL_SQL} / ${MODEL_EXPLAIN}`);
});
