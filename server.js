import express from "express";
import cors from "cors";
import duckdb from "duckdb";
import Anthropic from "@anthropic-ai/sdk";

/* ============================================================
   CHAT-RFB API (MotherDuck + Claude)
   - Cache de schema (1h)
   - Geração de SQL segura (somente SELECT)
   - Bloqueios anti-exfiltração / comandos perigosos
   - LIMIT automático (quando não for agregação)
   - Retorno com preview (evita JSON gigante)
   - Opcional: total_rows via COUNT(*) (sem estourar resposta)
============================================================ */

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

/* ========================= MOTHERDUCK ========================= */
const MD_DB = "md:chat_rfb";
const MD_TOKEN = process.env.MOTHERDUCK_TOKEN || "";

// ⚠️ Se você rodar local sem token, vai falhar ao acessar md:
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
const CACHE_DURATION = 3600000; // 1h

// Escolha “catálogos” relevantes
const ALLOWED_CATALOGS = new Set(["chat_rfb", "PortaldaTransparencia"]);
const ALLOWED_SCHEMA = "main";

// Se você quiser reduzir contexto, coloque “tabelas prioritárias” aqui:
const PRIORITY_TABLES = null; // ex: new Set(["empresas", "_ceis_auditado_limpo"])

async function getSchema() {
  if (cachedSchema && Date.now() < cacheExpiry) {
    console.log("📦 Schema em CACHE");
    return cachedSchema;
  }

  console.log("🔄 Buscando schema do MotherDuck...");

  // Busca tabelas relevantes
  const allTables = await queryMD(`
    SELECT table_catalog, table_schema, table_name
    FROM information_schema.tables
    WHERE table_catalog IN ('chat_rfb', 'PortaldaTransparencia')
      AND table_schema = '${ALLOWED_SCHEMA}'
    ORDER BY table_catalog, table_name
  `);

  let tables = allTables;

  // Se quiser restringir a um subconjunto de tabelas “prioritárias”
  if (PRIORITY_TABLES instanceof Set) {
    tables = allTables.filter(t => PRIORITY_TABLES.has(t.table_name));
  }

  console.log(`📋 Encontradas ${tables.length} tabelas relevantes`);

  let schema = "TABELAS E COLUNAS DISPONÍVEIS:\n\n";

  // ⚡ Evita loop com trocentas conexões: usa queryMD mesmo, mas é ok.
  // (Se quiser ultra-perf, dá pra puxar columns de uma vez e agrupar.)
  for (const t of tables) {
    const fullName = `${t.table_catalog}.${t.table_schema}.${t.table_name}`;
    console.log(`  ├─ ${fullName}`);

    const columns = await queryMD(`
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_catalog = '${t.table_catalog}'
        AND table_schema = '${t.table_schema}'
        AND table_name = '${t.table_name}'
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
   - Somente SELECT (sem INSERT/UPDATE/DELETE/DDL)
   - Não use PRAGMA / ATTACH / INSTALL / LOAD / COPY / EXPORT / CALL / SET

2. CONTAGEM:
   - Empresas ÚNICAS: COUNT(DISTINCT cnpj_basico)
   - Estabelecimentos: COUNT(*)

3. FILTROS COMUNS RFB:
   - Ativas: WHERE situacao_cadastral = 'ATIVA'
   - Por estado: WHERE uf = 'SP'
   - MEI: WHERE opcao_mei = 'S'
   - Simples: WHERE opcao_simples = 'S'

4. JOIN COMPLIANCE (CEIS/CNEP + RFB):
   - CNPJ como string: CAST("CPF OU CNPJ DO SANCIONADO" AS VARCHAR)
   - Comparar com: CAST(e.cnpj AS VARCHAR)

5. COLUNAS COM ESPAÇOS:
   - SEMPRE use aspas duplas: "NOME DO SANCIONADO"

6. AUDITORIA:
   - URL de origem: _audit_url_download
   - Data gov: _audit_data_disponibilizacao_gov
   - Periodicidade: _audit_periodicidade_atualizacao_gov
   - Linha CSV: _audit_linha_csv
   - Hash linha: _audit_row_hash

7. PERFORMANCE:
   - Se a query NÃO for agregação, use LIMIT 50 (ou menor se o usuário pedir).
   - Evite SELECT * em tabelas grandes.

8. TECNOLOGIA (CNAEs sem hífen):
   - 6201501, 6201502, 6202300, 6203100, 6204000, 6209100

EXEMPLOS:

-- Empresas ativas de SP com sanções (COM AUDITORIA):
SELECT 
  e.razao_social, 
  e.uf, 
  c."CATEGORIA DA SANÇÃO",
  c._audit_url_download,
  c._audit_data_disponibilizacao_gov,
  c._audit_linha_csv
FROM chat_rfb.main.empresas e
INNER JOIN PortaldaTransparencia.main._ceis_auditado_limpo c
  ON CAST(c."CPF OU CNPJ DO SANCIONADO" AS VARCHAR) = CAST(e.cnpj AS VARCHAR)
WHERE e.situacao_cadastral = 'ATIVA' AND e.uf = 'SP'
LIMIT 50;
`;

  cachedSchema = schema;
  cacheExpiry = Date.now() + CACHE_DURATION;
  console.log("✅ Schema em cache por 1 hora\n");

  return schema;
}

/* ========================= CLAUDE ========================= */
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

/**
 * Segurança SQL:
 * - remove code fences
 * - bloqueia múltiplas statements
 * - permite somente SELECT / WITH ... SELECT
 * - bloqueia comandos perigosos de DuckDB/MotherDuck
 */
const BLOCKED_SQL_TOKENS = [
  // DDL/DML
  "insert", "update", "delete", "drop", "alter", "create", "truncate", "merge", "grant", "revoke",
  // DuckDB/MotherDuck comandos perigosos / exfil
  "pragma", "attach", "detach", "install", "load", "copy", "export", "call", "set",
  "create secret", "secret", "httpfs", "s3", "gcs", "azure",
  "read_csv", "read_parquet", "read_json", "read_ndjson",
];

function stripFences(sql) {
  return sql.replace(/```sql|```/gi, "").trim();
}

function hasMultipleStatements(sql) {
  // impede "SELECT ...; SELECT ...;" e afins
  // (permitimos no máximo 1 ";" no final)
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

function containsBlockedTokens(sql) {
  const s = sql.toLowerCase();
  return BLOCKED_SQL_TOKENS.some(tok => s.includes(tok));
}

function cleanSQL(sqlRaw) {
  let s = stripFences(sqlRaw).replace(/\s+/g, " ").trim();

  // remove ; final (se existir)
  s = s.replace(/;+$/g, "");

  if (!isSelectLike(s)) throw new Error("SQL inválida: somente SELECT/CTE é permitido.");
  if (hasMultipleStatements(s)) throw new Error("SQL inválida: múltiplas statements bloqueadas.");
  if (containsBlockedTokens(s)) throw new Error("SQL bloqueada: contém operação/comando não permitido.");
  if (/--|\/\*/.test(s)) {
    // comentários podem esconder payload; opcional bloquear
    // se quiser permitir, remova esta regra
    throw new Error("SQL bloqueada: comentários não são permitidos.");
  }

  return s;
}

/**
 * Detecta se a query é “agregação” para decidir LIMIT automático.
 * Heurística simples: presença de COUNT/SUM/AVG/MIN/MAX/GROUP BY/DISTINCT agregado
 */
function looksAggregated(sql) {
  const s = sql.toLowerCase();
  return (
    s.includes(" count(") ||
    s.includes(" sum(") ||
    s.includes(" avg(") ||
    s.includes(" min(") ||
    s.includes(" max(") ||
    s.includes(" group by ") ||
    // DISTINCT sozinho às vezes ainda retorna muitas linhas; não contar como agregação:
    false
  );
}

function hasLimit(sql) {
  return /\slimit\s+\d+/i.test(sql);
}

function enforceLimit(sql, limit = 50) {
  if (looksAggregated(sql)) return sql; // agregação normalmente retorna pouco
  if (hasLimit(sql)) return sql;
  return `${sql} LIMIT ${limit}`;
}

/**
 * Transformar SELECT em COUNT(*) para total_rows (sem LIMIT).
 * Se falhar (CTE complexo), a API segue sem total.
 */
function toCountQuery(sql) {
  // remove LIMIT se existir
  const noLimit = sql.replace(/\slimit\s+\d+/i, "").trim();

  // se for CTE, envelopa
  if (noLimit.toLowerCase().startsWith("with")) {
    return `SELECT COUNT(*) AS total_rows FROM (${noLimit}) t`;
  }

  // caso normal
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

/* ========================= CONFIG API ========================= */
const DEFAULT_PREVIEW_LIMIT = 50;
const MAX_PREVIEW_LIMIT = 200; // evita abuso
const MODEL_SQL = process.env.ANTHROPIC_MODEL_SQL || "claude-sonnet-4-5-20250929";
const MODEL_EXPLAIN = process.env.ANTHROPIC_MODEL_EXPLAIN || "claude-sonnet-4-5-20250929";

/* ========================= ROTA PRINCIPAL ========================= */
app.post("/chat", async (req, res) => {
  const startTime = Date.now();

  try {
    const query = (req.body?.query || "").trim();
    const wantTotal = Boolean(req.body?.include_total); // opcional: true/false
    let previewLimit = Number(req.body?.limit ?? DEFAULT_PREVIEW_LIMIT);

    if (!query) return res.json({ error: "Query vazia" });
    if (!Number.isFinite(previewLimit) || previewLimit <= 0) previewLimit = DEFAULT_PREVIEW_LIMIT;
    previewLimit = Math.min(previewLimit, MAX_PREVIEW_LIMIT);

    console.log("\n" + "=".repeat(60));
    console.log("❓ PERGUNTA:", query);
    console.log("=".repeat(60));

    // 1) Schema
    const schema = await getSchema();

    // 2) Claude gera SQL
    console.log("🤖 Claude gerando SQL...");
    const llmSQL = await anthropic.messages.create({
      model: MODEL_SQL,
      max_tokens: 700,
      temperature: 0,
      system:
        "Você é especialista SQL DuckDB. Gere APENAS a query SQL (sem explicações, sem markdown, sem comentários). " +
        "Use nomes completos (catalog.schema.table). " +
        "Use CAST para conversão de tipos quando necessário. " +
        "Se NÃO for agregação, sempre inclua LIMIT.",
      messages: [
        {
          role: "user",
          content:
            `${schema}\n\n` +
            `PERGUNTA DO USUÁRIO: "${query}"\n\n` +
            `Gere a SQL (somente SELECT/CTE). Se não for agregação, use LIMIT ${previewLimit}.`,
        },
      ],
    });

    const rawSql = llmSQL.content?.[0]?.text ?? "";
    let sql = cleanSQL(rawSql);
    sql = enforceLimit(sql, previewLimit);

    console.log("📝 SQL gerada:", sql.slice(0, 200) + (sql.length > 200 ? "..." : ""));

    // 3) Executa preview
    console.log("⚡ Executando preview no MotherDuck...");
    const previewRowsRaw = await queryMD(sql);
    const previewRows = coerceBigIntRows(previewRowsRaw);
    console.log(`📊 Preview: ${previewRows.length} linha(s)`);

    // 3b) (Opcional) total_rows
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
        console.log("⚠️  Falhou COUNT(*) (ok, seguindo sem total):", e.message);
        totalRows = null;
        countSql = null;
      }
    }

    // 4) Claude explica (com base só nas primeiras linhas)
    console.log("💬 Claude explicando resultado...");
    const llmExplain = await anthropic.messages.create({
      model: MODEL_EXPLAIN,
      max_tokens: 450,
      temperature: 0.6,
      system:
        "Você é assistente brasileiro de inteligência empresarial. " +
        "Seja claro, objetivo e use separadores de milhar (ex: 1.234.567). " +
        "Se houver colunas _audit_*, mencione rastreabilidade e origem quando relevante. " +
        "Não invente dados além do que está no preview.",
      messages: [
        {
          role: "user",
          content:
            `Pergunta: "${query}"\n\n` +
            `SQL executada (preview):\n${sql}\n\n` +
            (countSql ? `SQL de total (COUNT):\n${countSql}\n\n` : "") +
            (totalRows !== null ? `Total de linhas (estimado pelo COUNT): ${totalRows}\n\n` : "") +
            `Resultado (preview até ${previewLimit} linhas; aqui vão as 5 primeiras):\n` +
            `${JSON.stringify(previewRows.slice(0, 5), null, 2)}\n\n` +
            `Explique o resultado em português (curto e direto) e, se fizer sentido, sugira 1-2 próximos filtros úteis:`,
        },
      ],
    });

    const answer = llmExplain.content?.[0]?.text ?? "";
    const duration = Date.now() - startTime;

    console.log("✅ CONCLUÍDO em", duration, "ms");
    console.log("📤 Resposta:", answer.slice(0, 120) + (answer.length > 120 ? "..." : ""), "\n");

    return res.json({
      answer,
      sql,
      rows_preview: previewRows,         // ✅ só preview
      preview_count: previewRows.length, // ✅ tamanho do preview
      total_rows: totalRows,             // ✅ se include_total=true e deu certo
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
  console.log("");
});
