// server.js
import express from "express";
import cors from "cors";
import duckdb from "duckdb";
import Anthropic from "@anthropic-ai/sdk";

/* ============================================================
   CHAT-RFB API (MotherDuck + Claude)
   - Cache de schema (1h)
   - Somente SELECT/CTE (seguro)
   - Regex anti-comandos perigosos + motivo exato
   - LIMIT automático (quando não for agregação)
   - Retorno: preview + (opcional) total_rows
   - ✅ Auditoria obrigatória para PortaldaTransparencia.* (_audit_*)
   - ✅ audit_sample padronizado para o front
   - ✅ dataset_meta (fonte + período Janeiro/2026 + origem oficial)
   - ✅ EXPLAIN em TEXTO PURO (sem Markdown)
   - ✅ Aliases: chat_rfb.main.empresas -> chat_rfb.main.empresas_janeiro2026
              chat_rfb.main.empresas_chat -> chat_rfb.main.empresas_chat_janeiro2026
============================================================ */

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

/* ========================= TABLE ALIASES (CANÔNICOS) =========================
   O LLM pode usar "chat_rfb.main.empresas" (canônico), e o backend traduz
   para a tabela versionada mais recente (ex.: empresas_janeiro2026).
*/
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
    tabelas_match: [
      "chat_rfb.main.empresas_janeiro2026",
      "chat_rfb.main.empresas_chat_janeiro2026",
    ],
  },
  portal_transparencia: {
    id: "portal_transparencia_sancoes",
    fonte: "Portal da Transparência — CGU",
    base: "Sanções e Acordos Administrativos",
    periodicidade: "Atualização contínua (conforme publicação oficial)",
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

/* ========================= MOTHERDUCK ========================= */
const MD_DB = "md:chat_rfb";
const MD_TOKEN = process.env.MOTHERDUCK_TOKEN || "";
const db = new duckdb.Database(MD_DB, { motherduck_token: MD_TOKEN });

function queryMD(sql) {
  return new Promise((resolve, reject) => {
    const conn = db.connect();
    conn.all(sql, (err, rows) => {
      conn.close();
      if (err) return reject(err);
      resolve(rows);
    });
  });
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

1. PERMISSÃO:
   - Somente SELECT (ou WITH ... SELECT)
   - NÃO use: PRAGMA / ATTACH / INSTALL / LOAD / COPY / EXPORT / CALL / SET
   - NÃO use read_* (read_csv/read_parquet/read_json/read_ndjson)

2. TABELAS CANÔNICAS (IMPORTANTE):
   - Para RFB, prefira estas tabelas CANÔNICAS:
     • chat_rfb.main.empresas
     • chat_rfb.main.empresas_chat
   - O backend traduz automaticamente para as tabelas versionadas:
     • chat_rfb.main.empresas_janeiro2026
     • chat_rfb.main.empresas_chat_janeiro2026

3. CONTAGEM:
   - Empresas ÚNICAS: COUNT(DISTINCT cnpj_basico)
   - Estabelecimentos: COUNT(*)

4. FILTROS RFB:
   - Ativas: WHERE situacao_cadastral = 'ATIVA'
   - Por UF: WHERE uf = 'SP'
   - MEI: WHERE opcao_mei = 'S'
   - Simples: WHERE opcao_simples = 'S'

5. JOIN COMPLIANCE:
   - CAST("CPF OU CNPJ DO SANCIONADO" AS VARCHAR) = CAST(e.cnpj AS VARCHAR)

6. COLUNAS COM ESPAÇOS:
   - Sempre use aspas duplas

7. AUDITORIA (Portal da Transparência):
   - Se consultar PortaldaTransparencia, inclua no SELECT:
     _audit_url_download,
     _audit_data_disponibilizacao_gov,
     _audit_periodicidade_atualizacao_gov,
     _audit_arquivo_csv_origem,
     _audit_linha_csv,
     _audit_row_hash

8. PERFORMANCE:
   - Se NÃO for agregação, sempre use LIMIT.
   - Evite SELECT *.
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
    if (m) return `Padrão bloqueado: ${rx} (match: "${m[0]}")`;
  }
  return null;
}

function cleanSQL(sqlRaw) {
  let s = stripFences(sqlRaw).replace(/\s+/g, " ").trim();
  s = s.replace(/;+$/g, "");

  if (!isSelectLike(s)) throw new Error("SQL inválida: somente SELECT/CTE é permitido.");
  if (hasMultipleStatements(s)) throw new Error("SQL inválida: múltiplas statements bloqueadas.");
  if (/--|\/\*/.test(s)) throw new Error("SQL bloqueada: comentários não são permitidos.");

  const reason = findBlockedReason(s);
  if (reason) throw new Error(`SQL bloqueada: contém operação/comando não permitido. (${reason})`);

  return s;
}

function looksAggregated(sql) {
  const s = sql.toLowerCase();
  return (
    s.includes(" count(") ||
    s.includes(" sum(") ||
    s.includes(" avg(") ||
    s.includes(" min(") ||
    s.includes(" max(") ||
    s.includes(" group by ")
  );
}

function hasLimit(sql) {
  return /\slimit\s+\d+/i.test(sql);
}

function enforceLimit(sql, limit = 50) {
  if (looksAggregated(sql)) return sql;
  if (hasLimit(sql)) return sql;
  return `${sql} LIMIT ${limit}`;
}

function toCountQuery(sql) {
  const noLimit = sql.replace(/\slimit\s+\d+/i, "").trim();
  return `SELECT COUNT(*) AS total_rows FROM (${noLimit}) t`;
}

function coerceBigIntRows(rows) {
  return rows.map(row => {
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
  return /\bPortaldaTransparencia\./i.test(sql);
}

function hasAllAuditCols(sql) {
  return AUDIT_COLS.every(c => new RegExp(`\\b${c}\\b`, "i").test(sql));
}

function extractAuditSample(rows) {
  if (!Array.isArray(rows) || !rows.length) return null;
  const r = rows.find(x => x && typeof x === "object" && Object.keys(x).some(k => k.startsWith("_audit_"))) || rows[0];
  if (!r || typeof r !== "object") return null;

  const hasAny = Object.keys(r).some(k => k.startsWith("_audit_"));
  if (!hasAny) return null;

  return {
    _audit_url_download: r._audit_url_download ?? null,
    _audit_data_disponibilizacao_gov: r._audit_data_disponibilizacao_gov ?? null,
    _audit_periodicidade_atualizacao_gov: r._audit_periodicidade_atualizacao_gov ?? null,
    _audit_arquivo_csv_origem: r._audit_arquivo_csv_origem ?? null,
    _audit_linha_csv: r._audit_linha_csv ?? null,
    _audit_row_hash: r._audit_row_hash ?? null,
  };
}

/* ========================= CONFIG ========================= */
const DEFAULT_PREVIEW_LIMIT = 50;
const MAX_PREVIEW_LIMIT = 200;

const MODEL_SQL = process.env.ANTHROPIC_MODEL_SQL || "claude-sonnet-4-5-20250929";
const MODEL_EXPLAIN = process.env.ANTHROPIC_MODEL_EXPLAIN || "claude-sonnet-4-5-20250929";

async function generateSQL({ schema, userQuery, previewLimit, auditRequired }) {
  const baseSystem =
    "Você é especialista SQL DuckDB. Gere APENAS a query SQL (sem explicações, sem markdown, sem comentários). " +
    "Use nomes completos (catalog.schema.table). " +
    "Somente SELECT/CTE. " +
    "Se não for agregação, sempre inclua LIMIT. " +
    "Evite SELECT *.";

  const auditRule = auditRequired
    ? (
        "REGRA OBRIGATÓRIA DE AUDITORIA:\n" +
        "- Se consultar qualquer tabela do catálogo 'PortaldaTransparencia', o SELECT deve incluir SEMPRE:\n" +
        `  ${AUDIT_COLS.join(", ")}\n`
      )
    : "";

  const canonicalHint =
    "IMPORTANTE (RFB): use preferencialmente as tabelas canônicas:\n" +
    "- chat_rfb.main.empresas\n" +
    "- chat_rfb.main.empresas_chat\n" +
    "O backend traduz para as tabelas versionadas automaticamente.\n";

  const llmSQL = await anthropic.messages.create({
    model: MODEL_SQL,
    max_tokens: 700,
    temperature: 0,
    system: `${baseSystem}\n${auditRule}`,
    messages: [
      {
        role: "user",
        content:
          `${schema}\n\n${canonicalHint}\n` +
          `PERGUNTA DO USUÁRIO: "${userQuery}"\n\n` +
          `Gere a SQL (somente SELECT/CTE). Se não for agregação, use LIMIT ${previewLimit}.`,
      },
    ],
  });

  return llmSQL.content?.[0]?.text ?? "";
}

/* ========================= ROTA ========================= */
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

    // 1) Schema
    const schema = await getSchema();

    // 2) SQL
    console.log("🤖 Claude gerando SQL...");
    let rawSql = await generateSQL({ schema, userQuery, previewLimit, auditRequired: true });

    let sql = enforceLimit(cleanSQL(rawSql), previewLimit);

    // ✅ aplica alias canônico -> versionado
    sql = applyTableAliases(sql);

    // ✅ Auditoria obrigatória para Portal
    if (touchesPortal(sql) && !hasAllAuditCols(sql)) {
      console.log("⚠️  SQL tocou Portal mas faltou _audit_* → regenerando...");
      rawSql = await generateSQL({
        schema,
        userQuery: userQuery + " (IMPORTANTE: inclua TODAS as colunas _audit_* obrigatórias no SELECT.)",
        previewLimit,
        auditRequired: true,
      });
      sql = applyTableAliases(enforceLimit(cleanSQL(rawSql), previewLimit));

      if (touchesPortal(sql) && !hasAllAuditCols(sql)) {
        throw new Error("SQL inválida: consulta ao Portal exige colunas _audit_* no SELECT (auditoria obrigatória).");
      }
    }

    console.log("📝 SQL gerada:", sql.slice(0, 220) + (sql.length > 220 ? "..." : ""));

    // 3) Preview
    console.log("⚡ Executando preview no MotherDuck...");
    const previewRowsRaw = await queryMD(sql);
    const previewRows = coerceBigIntRows(previewRowsRaw);
    console.log(`📊 Preview: ${previewRows.length} linha(s)`);

    const audit_sample = extractAuditSample(previewRows);
    const dataset_meta = detectDatasetMeta(sql);

    // 3b) Total
    let totalRows = null;
    let countSql = null;

    if (wantTotal) {
      try {
        countSql = cleanSQL(toCountQuery(sql));
        console.log("🧮 Executando COUNT(*)...");
        const totalRes = await queryMD(countSql);
        const total = totalRes?.[0]?.total_rows;
        totalRows = typeof total === "bigint" ? Number(total) : total ?? null;
      } catch (e) {
        console.log("⚠️  Falhou COUNT(*) (seguindo sem total):", e.message);
        totalRows = null;
        countSql = null;
      }
    }

    // 4) Explain (TEXTO PURO)
    console.log("💬 Claude explicando resultado...");
    const llmExplain = await anthropic.messages.create({
      model: MODEL_EXPLAIN,
      max_tokens: 300,
      temperature: 0.4,
      system:
        "Você é assistente brasileiro de inteligência empresarial. " +
        "Responda em TEXTO PURO (sem Markdown, sem #, sem '---', sem listas numeradas). " +
        "Use no máximo 6 linhas. " +
        "Use separadores de milhar (ex: 1.234.567). " +
        "Se houver _audit_* no preview, inclua uma linha: 'Rastreabilidade: disponível (URL/arquivo/linha/hash)'. " +
        "Sugira no máximo 2 filtros curtos. " +
        "Não invente dados além do preview.",
      messages: [
        {
          role: "user",
          content:
            `Pergunta: "${userQuery}"\n\n` +
            `SQL executada (preview):\n${sql}\n\n` +
            (dataset_meta ? `Dataset meta:\n${JSON.stringify(dataset_meta, null, 2)}\n\n` : "") +
            (audit_sample ? `Audit sample:\n${JSON.stringify(audit_sample, null, 2)}\n\n` : "") +
            `Preview (primeiras 5 linhas):\n${JSON.stringify(previewRows.slice(0, 5), null, 2)}\n\n` +
            `Responda agora (texto puro):`,
        },
      ],
    });

    const answer = llmExplain.content?.[0]?.text ?? "";
    const duration = Date.now() - startTime;

    console.log("✅ CONCLUÍDO em", duration, "ms");

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
app.get("/health", (_, res) => {
  res.json({
    ok: true,
    timestamp: new Date().toISOString(),
    cache: cachedSchema ? "active" : "empty",
    motherduck_token: MD_TOKEN ? "configured" : "missing",
    anthropic_key: process.env.ANTHROPIC_API_KEY ? "configured" : "missing",
    models: { sql: MODEL_SQL, explain: MODEL_EXPLAIN },
    canonical_tables: TABLE_ALIASES,
  });
});

app.post("/clear-cache", (_, res) => {
  cachedSchema = null;
  cacheExpiry = null;
  console.log("🗑️  Cache limpo!");
  res.json({ ok: true, message: "Cache limpo com sucesso" });
});

/* ========================= START ========================= */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log("╔════════════════════════════════════════╗");
  console.log("║   🚀 CHAT-RFB API RODANDO             ║");
  console.log("╚════════════════════════════════════════╝");
  console.log(`📡 Porta: ${PORT}`);
  console.log(`🔐 MotherDuck: ${MD_TOKEN ? "✅ Configurado" : "❌ Faltando"}`);
  console.log(`🤖 Claude: ${process.env.ANTHROPIC_API_KEY ? "✅ Configurado" : "❌ Faltando"}`);
  console.log(`🧠 Models: SQL=${MODEL_SQL} | EXPLAIN=${MODEL_EXPLAIN}`);
  console.log("🧩 Canonical tables:", TABLE_ALIASES);
  console.log("");
});
