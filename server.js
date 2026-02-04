 // server.js
import express from "express";
import cors from "cors";
import duckdb from "duckdb";
import Anthropic from "@anthropic-ai/sdk";

/* ============================================================
   CHAT-RFB API (MotherDuck + Claude) — BDC
   ✅ Conexão MotherDuck PERSISTENTE (não fecha por query)
   ✅ Auto-reconnect se cair
   ✅ Cache do schema (1h)
   ✅ Somente SELECT/CTE (seguro) + bloqueios regex
   ✅ LIMIT automático quando não for agregação
   ✅ Aliases: empresas -> empresas_janeiro2026
   ✅ Auditoria obrigatória em PortaldaTransparencia + audit_sample
   ✅ dataset_meta (fonte + período)
   ✅ Explain SEM Markdown (texto puro)
============================================================ */

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

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
    periodicidade: "Atualização contínua (ex: a cada 4 horas em Sanções)",
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

/* ========================= MOTHERDUCK (PERSISTENTE) ========================= */
const MD_TOKEN = process.env.MOTHERDUCK_TOKEN || "";

// Dica: dbinstance_inactivity_ttl=0s ajuda a evitar estados ruins no cache.
const MD_DB = "md:chat_rfb?dbinstance_inactivity_ttl=0s";

const db = new duckdb.Database(MD_DB, {
  motherduck_token: MD_TOKEN,
});

// Conexão global reutilizada
let mdConn = null;

// fila simples para serializar queries (evita concorrência em cima do mesmo conn)
let mdQueue = Promise.resolve();

function isConnError(err) {
  const msg = String(err?.message || err || "").toLowerCase();
  return (
    msg.includes("connection was never established") ||
    msg.includes("has been closed already") ||
    msg.includes("connection error") ||
    msg.includes("socket") ||
    msg.includes("timeout")
  );
}

async function ensureConn() {
  if (mdConn) return mdConn;

  mdConn = db.connect();
  // teste rápido para garantir que está viva
  await new Promise((resolve, reject) => {
    mdConn.all("SELECT 1 AS ok", (err) => {
      if (err) return reject(err);
      resolve(true);
    });
  });

  return mdConn;
}

async function reconnectConn() {
  try {
    if (mdConn) {
      // em geral NÃO fechamos sempre, mas ao reconectar, fechamos a quebrada
      mdConn.close();
    }
  } catch {}
  mdConn = null;
  return ensureConn();
}

function queryMD(sql) {
  // serializa para evitar duas reqs simultâneas no mesmo conn
  mdQueue = mdQueue.then(async () => {
    try {
      const conn = await ensureConn();
      return await new Promise((resolve, reject) => {
        conn.all(sql, (err, rows) => {
          if (err) return reject(err);
          resolve(rows);
        });
      });
    } catch (err) {
      if (isConnError(err)) {
        // tenta 1 reconexão
        const conn = await reconnectConn();
        return await new Promise((resolve, reject) => {
          conn.all(sql, (e2, rows2) => {
            if (e2) return reject(e2);
            resolve(rows2);
          });
        });
      }
      throw err;
    }
  });

  return mdQueue;
}

/* ========================= SCHEMA COM CACHE ========================= */
let cachedSchema = null;
let cacheExpiry = null;
const CACHE_DURATION = 3600000; // 1 hora

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
REGRAS CRÍTICAS PARA GERAR SQL (DuckDB):
═══════════════════════════════════════════════════════════════

0) TABELAS CANÔNICAS (use estas no prompt; o backend converte para Janeiro/2026):
   - chat_rfb.main.empresas  -> chat_rfb.main.empresas_janeiro2026
   - chat_rfb.main.empresas_chat -> chat_rfb.main.empresas_chat_janeiro2026

1) PERMISSÃO:
   - Somente SELECT (ou WITH ... SELECT)
   - NÃO use: PRAGMA / ATTACH / INSTALL / LOAD / COPY / EXPORT / CALL / SET
   - NÃO use funções read_* (read_csv/read_parquet/read_json/read_ndjson)

2) RFB — filtros comuns:
   - Ativas: WHERE situacao_cadastral = 'ATIVA'
   - Por UF: WHERE uf = 'SP'
   - MEI: WHERE opcao_mei = 'S'
   - Simples: WHERE opcao_simples = 'S'

3) JOIN COMPLIANCE:
   - CNPJ como string: CAST("CPF OU CNPJ DO SANCIONADO" AS VARCHAR)
   - Comparar com: CAST(e.cnpj AS VARCHAR)

4) COLUNAS COM ESPAÇOS:
   - Sempre use aspas duplas: "NOME DO SANCIONADO"

5) AUDITORIA (Portal da Transparência):
   - Ao consultar PortaldaTransparencia.*, inclua SEMPRE:
     _audit_url_download, _audit_data_disponibilizacao_gov, _audit_periodicidade_atualizacao_gov,
     _audit_arquivo_csv_origem, _audit_linha_csv, _audit_row_hash

6) PERFORMANCE:
   - Se NÃO for agregação, use LIMIT (padrão 50)
   - Evite SELECT * em tabelas grandes
`;

  cachedSchema = schema;
  cacheExpiry = Date.now() + CACHE_DURATION;
  console.log("✅ Schema em cache por 1 hora\n");

  return schema;
}

/* ========================= CLAUDE ========================= */
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

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

function coerceBigIntRows(rows) {
  return rows.map((row) => {
    const clean = {};
    for (const [k, v] of Object.entries(row)) {
      clean[k] = typeof v === "bigint" ? Number(v) : v;
    }
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
  const r =
    rows.find((x) => x && Object.keys(x).some((k) => String(k).startsWith("_audit_"))) ||
    rows[0];

  if (!r || typeof r !== "object") return null;
  const hasAny = Object.keys(r).some((k) => String(k).startsWith("_audit_"));
  if (!hasAny) return null;

  return AUDIT_COLS.reduce((acc, col) => {
    acc[col] = r[col] ?? null;
    return acc;
  }, {});
}

/* ========================= CONFIG ========================= */
const DEFAULT_PREVIEW_LIMIT = 50;
const MAX_PREVIEW_LIMIT = 200;

const MODEL_SQL = process.env.ANTHROPIC_MODEL_SQL || "claude-3-5-sonnet-20241022";
const MODEL_EXPLAIN = process.env.ANTHROPIC_MODEL_EXPLAIN || "claude-3-5-sonnet-20241022";

async function generateSQL({ schema, userQuery, previewLimit }) {
  const system =
    "Você é especialista em SQL DuckDB. Gere APENAS a query SQL (sem explicações, sem markdown, sem comentários). " +
    "Somente SELECT ou WITH ... SELECT. Use nomes completos (catalog.schema.table). " +
    "IMPORTANTE: use as tabelas canônicas chat_rfb.main.empresas e chat_rfb.main.empresas_chat (o backend converte para Janeiro/2026). " +
    "Se a consulta usar PortaldaTransparencia, inclua OBRIGATORIAMENTE no SELECT: " +
    AUDIT_COLS.join(", ") +
    ". " +
    "Se não for agregação, sempre inclua LIMIT.";

  const llmSQL = await anthropic.messages.create({
    model: MODEL_SQL,
    max_tokens: 700,
    temperature: 0,
    system,
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

/* ========================= ROTA PRINCIPAL ========================= */
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

    // 1) SQL
    let rawSql = await generateSQL({ schema, userQuery, previewLimit });
    let sql = enforceLimit(cleanSQL(rawSql), previewLimit);

    // 2) aplica aliases (Janeiro/2026)
    sql = applyTableAliases(sql);

    // 3) Se tocar Portal, garantir audit cols (se não tiver, tenta regenerar 1x)
    if (touchesPortal(sql) && !hasAllAuditCols(sql)) {
      console.log("⚠️ Faltou _audit_* no Portal → regenerando 1x...");
      rawSql = await generateSQL({
        schema,
        userQuery: userQuery + " (OBRIGATÓRIO: inclua TODAS as colunas _audit_*)",
        previewLimit,
      });
      sql = applyTableAliases(enforceLimit(cleanSQL(rawSql), previewLimit));
      if (!hasAllAuditCols(sql)) {
        throw new Error("Consulta ao Portal exige colunas _audit_* no SELECT.");
      }
    }

    console.log("📝 SQL:", sql.slice(0, 240) + (sql.length > 240 ? "..." : ""));

    // 4) Preview
    console.log("⚡ Executando preview...");
    const previewRowsRaw = await queryMD(sql);
    const previewRows = coerceBigIntRows(previewRowsRaw);
    console.log(`📊 Preview: ${previewRows.length} linha(s)`);

    const audit_sample = extractAuditSample(previewRows);
    const dataset_meta = detectDatasetMeta(sql);

    // 5) Total opcional
    let totalRows = null;
    if (wantTotal) {
      try {
        const countSql = toCountQuery(sql);
        const totalRes = await queryMD(countSql);
        const total = totalRes?.[0]?.total_rows;
        totalRows = typeof total === "bigint" ? Number(total) : total ?? null;
      } catch (e) {
        console.warn("⚠️ COUNT falhou:", e.message);
      }
    }

    // 6) Explain (texto puro, sem markdown)
    console.log("💬 Explicando resultado...");
    const llmExplain = await anthropic.messages.create({
      model: MODEL_EXPLAIN,
      max_tokens: 260,
      temperature: 0.35,
      system:
        "Você é assistente brasileiro de inteligência empresarial. " +
        "Responda em TEXTO PURO (sem markdown, sem #, sem listas numeradas longas). " +
        "No máximo 6 linhas. Use separadores de milhar (1.234.567). " +
        "Se houver _audit_*, diga: 'Rastreabilidade disponível (URL, arquivo, linha e hash)'. " +
        "Sugira no máximo 2 próximos filtros curtos.",
      messages: [
        {
          role: "user",
          content:
            `Pergunta: "${userQuery}"\n` +
            `SQL: ${sql}\n` +
            (dataset_meta ? `Dataset: ${JSON.stringify(dataset_meta)}\n` : "") +
            (audit_sample ? `Audit: ${JSON.stringify(audit_sample)}\n` : "") +
            `Preview (até 5): ${JSON.stringify(previewRows.slice(0, 5))}\n` +
            `Responda agora:`,
        },
      ],
    });

    const answer = (llmExplain.content?.[0]?.text ?? "").trim();
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

/* ========================= HEALTH / CACHE ========================= */
app.get("/health", async (_, res) => {
  try {
    await ensureConn();
    res.json({
      ok: true,
      timestamp: new Date().toISOString(),
      cache: cachedSchema ? "active" : "empty",
      motherduck_token: MD_TOKEN ? "configured" : "missing",
      anthropic_key: process.env.ANTHROPIC_API_KEY ? "configured" : "missing",
      models: { sql: MODEL_SQL, explain: MODEL_EXPLAIN },
      canonical_tables: TABLE_ALIASES,
      dataset_receita_federal: DATASETS_META.receita_federal_janeiro2026,
    });
  } catch (e) {
    res.status(500).json({
      ok: false,
      error: String(e?.message || e),
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
const PORT = process.env.PORT || 10000;
app.listen(PORT, async () => {
  console.log("╔════════════════════════════════════════╗");
  console.log("║     CHAT-RFB API RODANDO (BDC)        ║");
  console.log("╚════════════════════════════════════════╝");
  console.log(`📡 Porta: ${PORT}`);
  console.log(`🔐 MotherDuck token: ${MD_TOKEN ? "✅" : "❌"}`);
  console.log(`🤖 Claude key: ${process.env.ANTHROPIC_API_KEY ? "✅" : "❌"}`);
  console.log(`🧠 Models: ${MODEL_SQL} / ${MODEL_EXPLAIN}`);
  console.log(`🧷 Aliases:`, TABLE_ALIASES);
  console.log("");

  try {
    await ensureConn();
    console.log("🟢 MotherDuck: conexão OK (persistente)");
  } catch (e) {
    console.log("🔴 MotherDuck: falhou ao conectar no start:", e.message);
  }
});
