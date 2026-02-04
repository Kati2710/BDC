// server.js
import express from "express";
import cors from "cors";
import duckdb from "duckdb";
import Anthropic from "@anthropic-ai/sdk";

/* ============================================================
   CHAT-RFB API (MotherDuck + Claude)
   - Cache de schema (1h) com estrutura JSON detalhada
   - Somente SELECT/CTE (seguro)
   - Regex anti-comandos perigosos + motivo exato
   - LIMIT automático (quando não for agregação)
   - Retorno: preview + (opcional) total_rows
   - Auditoria obrigatória para PortaldaTransparencia.*
   - audit_sample padronizado
   - dataset_meta (fonte + período Janeiro/2026)
   - EXPLAIN em TEXTO PURO
   - Aliases: chat_rfb.main.empresas → chat_rfb.main.empresas_janeiro2026
   - CONTAGEM CORRETA DE ARRAYS + ACESSO A CAMPOS ANINHADOS
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

/* ========================= SCHEMA COM CACHE + ESTRUTURA JSON ========================= */
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
    console.log(` ├─ ${fullName}`);

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
      schema += ` • ${col.column_name} (${col.data_type})\n`;

      // Descrição detalhada para campos JSON da RFB
      const colName = col.column_name.toLowerCase();
      if (["empresa", "estabelecimentos", "socios", "simples"].includes(colName)) {
        schema += `   → Estrutura JSON principal (use json_extract_string ou indexação):\n`;

        if (colName === "empresa") {
          schema += `     • cnpj_basico, razao_social, natureza_juridica, natureza_juridica_codigo,\n`;
          schema += `     • qualificacao_responsavel, capital_social (DOUBLE), porte, porte_codigo, ente_federativo\n`;
          schema += `     Exemplo: json_extract_string(empresa, '$.razao_social')\n`;
        } else if (colName === "estabelecimentos") {
          schema += `     • ARRAY de objetos → len(estabelecimentos) para contar\n`;
          schema += `     • Campos principais: cnpj_completo, matriz_filial, situacao_cadastral,\n`;
          schema += `     • data_situacao_cadastral, cnae_principal, cnaes_secundarios_codigos (array),\n`;
          schema += `     • logradouro, numero, bairro, cep, uf, municipio, telefone_1\n`;
          schema += `     Exemplo: estabelecimentos[1].cnpj_completo, estabelecimentos[1].uf\n`;
        } else if (colName === "socios") {
          schema += `     • ARRAY de sócios → len(socios) para contar\n`;
          schema += `     • Campos típicos: cpf_cnpj_socio, nome_socio, qualificacao_socio,\n`;
          schema += `     • data_entrada_sociedade, representante_legal\n`;
          schema += `     Exemplo: socios[1].nome_socio, SUM(len(socios)) AS total_socios\n`;
        } else if (colName === "simples") {
          schema += `     • opcao_simples ('S'/'N'), data_opcao_simples, data_exclusao_simples,\n`;
          schema += `     • opcao_mei ('S'/'N'), data_opcao_mei, data_exclusao_mei\n`;
          schema += `     Exemplo: simples.opcao_mei = 'S' para filtrar MEI\n`;
        }
      }
    }
    schema += "\n";
  }

  schema += `
═══════════════════════════════════════════════════════════════
REGRAS CRÍTICAS PARA GERAR SQL:
═══════════════════════════════════════════════════════════════
1. PERMISSÃO: SOMENTE SELECT ou WITH ... SELECT. NUNCA INSERT/UPDATE/DELETE/DROP/ALTER/PRAGMA/ATTACH/INSTALL/LOAD/COPY/EXPORT/CALL/SET/read_*
2. TABELAS CANÔNICAS (RFB): use chat_rfb.main.empresas ou chat_rfb.main.empresas_chat (backend traduz para versionada)
3. CONTAGEM (MUITO IMPORTANTE - ARRAYS):
   - Total empresas: COUNT(*)
   - Total estabelecimentos: SUM(len(estabelecimentos))
   - Total sócios: SUM(len(socios))
   - Total CNAEs secundários: SUM(len(estabelecimentos[1].cnaes_secundarios_codigos)) ou similar
   - NUNCA use COUNT(estabelecimentos) ou COUNT(socios) → ERRADO!
4. ACESSO A CAMPOS ANINHADOS:
   - json_extract_string(coluna, '$.chave') para texto
   - CAST(json_extract_string(empresa, '$.capital_social') AS DOUBLE) para números
   - Arrays: estabelecimentos[1].chave, socios[1].nome_socio
   - UF principal: estabelecimentos[1].uf
   - Razão social: json_extract_string(empresa, '$.razao_social')
   - MEI/Simples: simples.opcao_mei = 'S', simples.opcao_simples = 'S'
5. FILTROS COMUNS:
   - Ativas: estabelecimentos[1].situacao_cadastral = 'ATIVA'
   - Por UF: estabelecimentos[1].uf = 'MT'
   - Porte: json_extract_string(empresa, '$.porte') = 'MICRO EMPRESA'
6. PERFORMANCE: Evite SELECT *. Use LIMIT se não for agregação.
7. AUDITORIA (Portal da Transparencia): inclua SEMPRE _audit_url_download, _audit_data_disponibilizacao_gov, _audit_periodicidade_atualizacao_gov, _audit_arquivo_csv_origem, _audit_linha_csv, _audit_row_hash
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
  const r = rows.find(x => x && Object.keys(x).some(k => k.startsWith("_audit_"))) || rows[0];
  if (!r || typeof r !== "object") return null;
  const hasAny = Object.keys(r).some(k => k.startsWith("_audit_"));
  if (!hasAny) return null;
  return AUDIT_COLS.reduce((acc, col) => {
    acc[col] = r[col] ?? null;
    return acc;
  }, {});
}

/* ========================= CONFIG & PROMPT ========================= */
const DEFAULT_PREVIEW_LIMIT = 50;
const MAX_PREVIEW_LIMIT = 200;
const MODEL_SQL = process.env.ANTHROPIC_MODEL_SQL || "claude-3-5-sonnet-20241022";
const MODEL_EXPLAIN = process.env.ANTHROPIC_MODEL_EXPLAIN || "claude-3-5-sonnet-20241022";

async function generateSQL({ schema, userQuery, previewLimit, auditRequired }) {
  const baseSystem = 
    "Você é especialista em SQL DuckDB para a base da Receita Federal. " +
    "Gere APENAS a query SQL (sem explicações, sem markdown, sem comentários). " +
    "Use nomes completos (chat_rfb.main.empresas). " +
    "Somente SELECT ou WITH ... SELECT. " +
    "Sempre use json_extract_string para campos dentro de empresa/simples. " +
    "Para arrays use [1] para principal ou len() para contar. " +
    "Se for número use CAST(... AS DOUBLE/INTEGER). " +
    "\n\n⚠️ REGRAS DE CONTAGEM:\n" +
    "- Total empresas: COUNT(*)\n" +
    "- Total estabelecimentos/sócios: SUM(len(estabelecimentos)) ou SUM(len(socios))\n" +
    "- NUNCA COUNT(estabelecimentos) ou COUNT(socios) → ERRADO!\n" +
    "\nExemplos corretos:\n" +
    "SELECT json_extract_string(empresa, '$.razao_social') AS razao\n" +
    "SELECT estabelecimentos[1].uf AS uf_principal\n" +
    "SELECT SUM(len(socios)) AS total_socios\n";

  const auditRule = auditRequired
    ? "Se usar PortaldaTransparencia, inclua OBRIGATORIAMENTE: " + AUDIT_COLS.join(", ") + "\n"
    : "";

  const canonicalHint =
    "Use preferencialmente chat_rfb.main.empresas (backend traduz automaticamente).\n";

  const llmSQL = await anthropic.messages.create({
    model: MODEL_SQL,
    max_tokens: 700,
    temperature: 0,
    system: `${baseSystem}\n${auditRule}\n${canonicalHint}`,
    messages: [
      {
        role: "user",
        content: `${schema}\n\nPERGUNTA DO USUÁRIO: "${userQuery}"\n\nGere SQL válida (somente SELECT/CTE). Se não for agregação, use LIMIT ${previewLimit}.`,
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
    previewLimit = Math.min(Math.max(previewLimit, 1), MAX_PREVIEW_LIMIT);

    console.log("\n" + "=".repeat(60));
    console.log("❓ PERGUNTA:", userQuery);
    console.log("=".repeat(60));

    const schema = await getSchema();

    console.log("🤖 Gerando SQL...");
    let rawSql = await generateSQL({ schema, userQuery, previewLimit, auditRequired: true });
    let sql = enforceLimit(cleanSQL(rawSql), previewLimit);
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
      if (!hasAllAuditCols(sql)) {
        throw new Error("Consulta ao Portal exige colunas _audit_* no SELECT.");
      }
    }

    console.log("📝 SQL:", sql.slice(0, 220) + (sql.length > 220 ? "..." : ""));

    // Preview
    console.log("⚡ Executando preview...");
    const previewRowsRaw = await queryMD(sql);
    const previewRows = coerceBigIntRows(previewRowsRaw);
    console.log(`📊 Preview: ${previewRows.length} linhas`);

    const audit_sample = extractAuditSample(previewRows);
    const dataset_meta = detectDatasetMeta(sql);

    // Total (opcional)
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

    // Explain em texto puro
    console.log("💬 Explicando resultado...");
    const llmExplain = await anthropic.messages.create({
      model: MODEL_EXPLAIN,
      max_tokens: 300,
      temperature: 0.4,
      system:
        "Você é assistente brasileiro de inteligência empresarial. " +
        "Responda em TEXTO PURO, no máximo 6 linhas. " +
        "Use separadores de milhar (1.234.567). " +
        "Se houver _audit_*, mencione 'Rastreabilidade disponível'. " +
        "Sugira no máximo 2 filtros úteis. " +
        "Não invente dados.",
      messages: [
        {
          role: "user",
          content:
            `Pergunta: "${userQuery}"\n\n` +
            `SQL: ${sql}\n\n` +
            (dataset_meta ? `Dataset: ${JSON.stringify(dataset_meta, null, 2)}\n\n` : "") +
            (audit_sample ? `Audit: ${JSON.stringify(audit_sample, null, 2)}\n\n` : "") +
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

/* ========================= HEALTH & CACHE ========================= */
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
  console.log("🗑️ Cache limpo!");
  res.json({ ok: true, message: "Cache limpo" });
});

/* ========================= START ========================= */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log("╔════════════════════════════════════════╗");
  console.log("║     CHAT-RFB API RODANDO (corrigida)   ║");
  console.log("╚════════════════════════════════════════╝");
  console.log(`📡 Porta: ${PORT}`);
  console.log(`🔐 MotherDuck: ${MD_TOKEN ? "✅" : "❌"}`);
  console.log(`🤖 Claude: ${process.env.ANTHROPIC_API_KEY ? "✅" : "❌"}`);
  console.log(`🧠 Models: ${MODEL_SQL} / ${MODEL_EXPLAIN}`);
});
