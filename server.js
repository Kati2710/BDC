import express from "express";
import cors from "cors";
import duckdb from "duckdb";
import Anthropic from "@anthropic-ai/sdk";

/* ============================================================
   CHAT-RFB API (MotherDuck + Claude) — FIX DEFINITIVO
   ✅ Token no connection string (?motherduck_token=...)
   ✅ dbinstance_inactivity_ttl=0s (evita cache problemático)
   ✅ UMA conexão conn aberta e reutilizada (sem connect/close por query)
   ✅ Aliases canônicos -> versionados (janeiro2026)
   ✅ Auditoria obrigatória PortaldaTransparencia (_audit_*)
   ✅ dataset_meta e audit_sample
   ✅ Explain em TEXTO PURO (sem markdown)
============================================================ */

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

/* ========================= CONFIG ========================= */
const PORT = process.env.PORT || 10000;
const MD_TOKEN = (process.env.MOTHERDUCK_TOKEN || "").trim();
const MD_DBNAME = "chat_rfb"; // seu md:chat_rfb

const MODEL_SQL = process.env.ANTHROPIC_MODEL_SQL || "claude-3-5-sonnet-20241022";
const MODEL_EXPLAIN = process.env.ANTHROPIC_MODEL_EXPLAIN || "claude-3-5-sonnet-20241022";
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

const DEFAULT_PREVIEW_LIMIT = 50;
const MAX_PREVIEW_LIMIT = 200;

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
    base: "Cadastro Nacional da Pessoa Jurídica (CNPJ)",
    periodo: "Janeiro/2026",
    origem_url: "https://arquivos.receitafederal.gov.br",
    tabelas_match: [
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
    tabelas_match: ["PortaldaTransparencia.main."],
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

/* ========================= MOTHERDUCK (FIX) ========================= */
// 🔥 FIX: token na connection string + desliga cache de instância
// Isso evita o erro "Connection was never established or has been closed already"
const mdPath = `md:${MD_DBNAME}?dbinstance_inactivity_ttl=0s&motherduck_token=${encodeURIComponent(MD_TOKEN)}`;

// ✅ 1 Database, ✅ 1 Connection reutilizada
const db = new duckdb.Database(mdPath);
const conn = db.connect();

function queryMD(sql) {
  return new Promise((resolve, reject) => {
    conn.all(sql, (err, rows) => {
      if (err) return reject(err);
      resolve(rows || []);
    });
  });
}

function coerceBigIntRows(rows) {
  return (rows || []).map((row) => {
    const clean = {};
    for (const [k, v] of Object.entries(row)) {
      clean[k] = typeof v === "bigint" ? Number(v) : v;
    }
    return clean;
  });
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

  for (const table of allTables) {
    const fullName = `${table.table_catalog}.${table.table_schema}.${table.table_name}`;
    console.log(`  ├─ ${fullName}`);

    const columns = await queryMD(`
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_catalog = '${table.table_catalog}'
        AND table_schema = '${table.table_schema}'
        AND table_name = '${table.table_name}'
      ORDER BY ordinal_position
    `);

    schema += `TABELA: ${fullName}\n`;
    schema += `Colunas (${columns.length}):\n`;
    for (const col of columns) {
      schema += `  • ${col.column_name} (${col.data_type})\n`;
    }
    schema += "\n";
  }

  schema += `
═══════════════════════════════════════════════════════════════
REGRAS CRÍTICAS PARA GERAR SQL:
═══════════════════════════════════════════════════════════════
1) SOMENTE SELECT / WITH...SELECT (sem PRAGMA/ATTACH/INSTALL/LOAD/COPY/EXPORT/CALL/SET/read_*)
2) RFB: use canônicas chat_rfb.main.empresas e chat_rfb.main.empresas_chat (backend traduz para *_janeiro2026)
3) Contagem empresas únicas: COUNT(DISTINCT cnpj_basico)
4) Filtros:
   - Ativas: situacao_cadastral = 'ATIVA'
   - UF: uf = 'SP'
   - MEI: opcao_mei = 'S'
   - Simples: opcao_simples = 'S'
5) Portal: incluir SEMPRE no SELECT:
   _audit_url_download, _audit_data_disponibilizacao_gov, _audit_periodicidade_atualizacao_gov,
   _audit_arquivo_csv_origem, _audit_linha_csv, _audit_row_hash
6) Performance: se NÃO for agregação, use LIMIT e evite SELECT *
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
  /\bpragma\b/i, /\battach\b/i, /\bdetach\b/i, /\binstall\b/i, /\bload\b/i,
  /\bcopy\b/i, /\bexport\b/i, /\bcall\b/i, /\bset\b/i,
  /\bcreate\s+secret\b/i, /\bsecret\b/i, /\bhttpfs\b/i,
  /\bs3\b/i, /\bgcs\b/i, /\bazure\b/i,
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
  return /count\(|sum\(|avg\(|min\(|max\(|group by/i.test(sql.toLowerCase());
}
function hasLimit(sql) {
  return /\slimit\s+\d+/i.test(sql);
}
function enforceLimit(sql, limit) {
  if (looksAggregated(sql) || hasLimit(sql)) return sql;
  return `${sql} LIMIT ${limit}`;
}
function toCountQuery(sql) {
  const noLimit = sql.replace(/\slimit\s+\d+/i, "").trim();
  return `SELECT COUNT(*) AS total_rows FROM (${noLimit}) t`;
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
  return /\bPortaldaTransparencia\./i.test(sql);
}
function hasAllAuditCols(sql) {
  return AUDIT_COLS.every((c) => new RegExp(`\\b${c}\\b`, "i").test(sql));
}
function extractAuditSample(rows) {
  if (!Array.isArray(rows) || !rows.length) return null;
  const r = rows.find((x) => x && Object.keys(x).some((k) => k.startsWith("_audit_"))) || rows[0];
  if (!r || typeof r !== "object") return null;
  const hasAny = Object.keys(r).some((k) => k.startsWith("_audit_"));
  if (!hasAny) return null;
  return AUDIT_COLS.reduce((acc, col) => {
    acc[col] = r[col] ?? null;
    return acc;
  }, {});
}

/* ========================= LLM SQL ========================= */
async function generateSQL({ schema, userQuery, previewLimit, auditRequired }) {
  const baseSystem =
    "Você é especialista em SQL DuckDB. Gere APENAS a query SQL (sem explicações, sem markdown, sem comentários). " +
    "Somente SELECT/CTE. Use nomes completos (catalog.schema.table). " +
    "Se não for agregação, inclua LIMIT. Evite SELECT *.";

  const auditRule = auditRequired
    ? "Se usar PortaldaTransparencia, inclua OBRIGATORIAMENTE no SELECT: " + AUDIT_COLS.join(", ")
    : "";

  const canonicalHint =
    "RFB: use canônicas chat_rfb.main.empresas e chat_rfb.main.empresas_chat (backend traduz para *_janeiro2026).";

  const llmSQL = await anthropic.messages.create({
    model: MODEL_SQL,
    max_tokens: 700,
    temperature: 0,
    system: `${baseSystem}\n${auditRule}\n${canonicalHint}`,
    messages: [
      {
        role: "user",
        content:
          `${schema}\n\nPERGUNTA DO USUÁRIO: "${userQuery}"\n\n` +
          `Gere SQL válida (somente SELECT/CTE). Se não for agregação, use LIMIT ${previewLimit}.`,
      },
    ],
  });

  return llmSQL.content?.[0]?.text ?? "";
}

/* ========================= ROUTE /chat ========================= */
app.post("/chat", async (req, res) => {
  const startTime = Date.now();

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

    console.log("🤖 Gerando SQL...");
    let rawSql = await generateSQL({ schema, userQuery, previewLimit, auditRequired: true });
    let sql = enforceLimit(cleanSQL(rawSql), previewLimit);

    // canonical -> versionado
    sql = applyTableAliases(sql);

    // Auditoria obrigatória
    if (touchesPortal(sql) && !hasAllAuditCols(sql)) {
      console.log("⚠️ Faltou _audit_* no Portal → regenerando...");
      rawSql = await generateSQL({
        schema,
        userQuery: userQuery + " (OBRIGATÓRIO: inclua TODAS as colunas _audit_*)",
        previewLimit,
        auditRequired: true,
      });
      sql = applyTableAliases(enforceLimit(cleanSQL(rawSql), previewLimit));
      if (touchesPortal(sql) && !hasAllAuditCols(sql)) {
        throw new Error("Consulta ao Portal exige colunas _audit_* no SELECT.");
      }
    }

    console.log("📝 SQL:", sql.slice(0, 220) + (sql.length > 220 ? "..." : ""));

    console.log("⚡ Executando preview...");
    const previewRowsRaw = await queryMD(sql);
    const previewRows = coerceBigIntRows(previewRowsRaw);

    const audit_sample = extractAuditSample(previewRows);
    const dataset_meta = detectDatasetMeta(sql);

    let totalRows = null;
    if (wantTotal) {
      try {
        const countSql = toCountQuery(sql);
        const totalRes = await queryMD(countSql);
        const total = totalRes?.[0]?.total_rows;
        totalRows = typeof total === "bigint" ? Number(total) : total ?? null;
      } catch (e) {
        console.warn("COUNT falhou:", e.message);
      }
    }

    console.log("💬 Explicando resultado...");
    const llmExplain = await anthropic.messages.create({
      model: MODEL_EXPLAIN,
      max_tokens: 300,
      temperature: 0.4,
      system:
        "Você é assistente brasileiro de inteligência empresarial. " +
        "Responda em TEXTO PURO (sem Markdown). No máximo 6 linhas. " +
        "Use separadores de milhar (1.234.567). " +
        "Se houver _audit_*, diga: 'Rastreabilidade disponível (URL/arquivo/linha/hash)'. " +
        "Sugira no máximo 2 filtros úteis. Não invente dados.",
      messages: [
        {
          role: "user",
          content:
            `Pergunta: "${userQuery}"\n\nSQL: ${sql}\n\n` +
            (dataset_meta ? `Dataset: ${JSON.stringify(dataset_meta)}\n\n` : "") +
            (audit_sample ? `Audit: ${JSON.stringify(audit_sample)}\n\n` : "") +
            `Preview (primeiras 5 linhas):\n${JSON.stringify(previewRows.slice(0, 5), null, 2)}\n\n` +
            `Responda agora (texto puro):`,
        },
      ],
    });

    const answer = llmExplain.content?.[0]?.text ?? "";
    const duration = Date.now() - startTime;

    return res.json({
      answer,
      sql,
      rows_preview: previewRows,
      preview_count: previewRows.length,
      total_rows: totalRows,
      audit_sample,
      audit_required: touchesPortal(sql),
      dataset_meta,
      duration_ms: duration,
    });
  } catch (err) {
    const duration = Date.now() - startTime;
    console.error("❌ ERRO:", err?.message || err);
    return res.status(500).json({
      error: err?.message || "Erro desconhecido",
      duration_ms: duration,
    });
  }
});

/* ========================= HEALTH ========================= */
app.get("/health", async (_, res) => {
  try {
    // ping simples no MotherDuck
    const ping = await queryMD("SELECT 1 AS ok");
    res.json({
      ok: true,
      ping: ping?.[0]?.ok ?? null,
      timestamp: new Date().toISOString(),
      cache: cachedSchema ? "active" : "empty",
      motherduck_token: MD_TOKEN ? "configured" : "missing",
      anthropic_key: process.env.ANTHROPIC_API_KEY ? "configured" : "missing",
      models: { sql: MODEL_SQL, explain: MODEL_EXPLAIN },
      canonical_tables: TABLE_ALIASES,
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
  console.log("║     CHAT-RFB API RODANDO (FIX MD)      ║");
  console.log("╚════════════════════════════════════════╝");
  console.log(`📡 Porta: ${PORT}`);
  console.log(`🔐 MotherDuck token: ${MD_TOKEN ? "✅" : "❌"}`);
  console.log(`🧠 MotherDuck path: md:${MD_DBNAME}?dbinstance_inactivity_ttl=0s&motherduck_token=***`);
  console.log(`🤖 Claude: ${process.env.ANTHROPIC_API_KEY ? "✅" : "❌"}`);
  console.log(`🧠 Models: ${MODEL_SQL} / ${MODEL_EXPLAIN}`);
});
