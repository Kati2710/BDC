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

app.use((req, res, next) => {
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

app.use(express.json({ limit: "256kb" }));

/* ========================= MOTHERDUCK ========================= */
const MD_DB = "md:chat_rfb";
const MD_TOKEN = process.env.MOTHERDUCK_TOKEN || "";

const db = new duckdb.Database(MD_DB, { motherduck_token: MD_TOKEN });

function queryAll(sql, params = []) {
  return new Promise((resolve, reject) => {
    const conn = db.connect();
    // Se params é vazio, não passa nada
    if (!params || params.length === 0) {
      conn.all(sql, (err, rows) => {
        conn.close();
        if (err) return reject(err);
        resolve(rows);
      });
    } else {
      conn.all(sql, params, (err, rows) => {
        conn.close();
        if (err) return reject(err);
        resolve(rows);
      });
    }
  });
}

/* ========================= CACHE ========================= */
const queryCache = new Map();
const CACHE_TTL = 10 * 60 * 1000; // 10 minutos

async function cachedQueryAll(sql, params = []) {
  const cacheKey = JSON.stringify({ sql, params });
  
  if (queryCache.has(cacheKey)) {
    const { data, timestamp } = queryCache.get(cacheKey);
    if (Date.now() - timestamp < CACHE_TTL) {
      console.log("📦 Cache HIT");
      return data;
    }
    queryCache.delete(cacheKey);
  }

  const data = (!params || params.length === 0) 
    ? await queryAll(sql) 
    : await queryAll(sql, params);
    
  queryCache.set(cacheKey, { data, timestamp: Date.now() });
  console.log("💾 Cache MISS - salvando");
  return data;
}

/* ========================= CLAUDE ========================= */
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || "";
const anthropic = ANTHROPIC_API_KEY ? new Anthropic({ apiKey: ANTHROPIC_API_KEY }) : null;

const SCHEMA_HINT = `
Tabela: chat_rfb.main.empresas (empresas brasileiras da Receita Federal)

Colunas principais:
- cnpj_basico: 8 primeiros dígitos do CNPJ (ex: "33000167")
- razao_social: nome oficial da empresa em MAIÚSCULAS (ex: "PETROBRAS S.A.")
- situacao_cadastral: situação atual - VALORES: "ATIVA", "SUSPENSA", "BAIXADA", "NULA", "INAPTA"
- uf: sigla do estado (ex: "SP", "RJ", "MG", "RS")
- municipio: nome da cidade em MAIÚSCULAS (ex: "SAO PAULO", "RIO DE JANEIRO")
- cnae_fiscal: código CNAE principal da atividade
- cnae_descricao: descrição da atividade econômica
- porte: tamanho da empresa - VALORES: "ME", "EPP", "DEMAIS"
- opcao_mei: se é MEI - VALORES: "S" (sim) ou "N" (não)
- natureza_juridica: tipo societário (ex: "SOCIEDADE ANONIMA FECHADA", "SOCIEDADE LIMITADA")
- capital_social: capital social em reais
- nome_fantasia: nome fantasia (pode ser NULL)
- data_inicio_atividade: data de abertura
- email: email de contato (pode ser NULL)

REGRAS CRÍTICAS:
1. NÃO existe coluna 'pais' ou 'país' - TODAS as empresas já são do Brasil
2. Para empresas ativas use: WHERE situacao_cadastral = 'ATIVA'
3. Para MEI use: WHERE opcao_mei = 'S'
4. Nomes estão em MAIÚSCULAS sem acentos (use UPPER() para comparar)
5. Para buscar nome: WHERE razao_social LIKE '%TERMO%' ou WHERE nome_fantasia LIKE '%TERMO%'
6. NUNCA invente colunas que não existem no schema acima
`;

function sanitizeSQL(sql) {
  let s = String(sql || "").trim();

  // Remove markdown
  s = s.replace(/```[\s\S]*?```/g, (m) => m.replace(/```sql|```/gi, "").trim());
  
  // Remove ; final
  s = s.replace(/;+\s*$/g, "").trim();

  // Pega do primeiro SELECT
  const idx = s.toLowerCase().indexOf("select");
  if (idx === -1) throw new Error("SQL inválida: não encontrei SELECT.");
  s = s.slice(idx).trim();

  // Bloqueia comandos perigosos
  const blocked = /\b(insert|update|delete|drop|alter|create|truncate|copy|attach|detach|pragma|call)\b/i;
  if (blocked.test(s)) throw new Error("SQL bloqueada: comando não permitido.");

  // Bloqueia múltiplas instruções
  if (s.includes(";")) throw new Error("SQL bloqueada: múltiplas instruções.");

  // LIMIT automático para listas (não agregações)
  const isAggregate = /\bcount\s*\(|\bgroup\s+by\b|\bsum\s*\(|\bavg\s*\(|\bmin\s*\(|\bmax\s*\(/i.test(s);
  if (!isAggregate && !/\blimit\b/i.test(s)) s += " LIMIT 50";

  return s;
}

async function llmToSQL(userQuery) {
  if (!anthropic) throw new Error("Claude não configurado (ANTHROPIC_API_KEY ausente).");

  const resp = await anthropic.messages.create({
    model: "claude-3-5-sonnet-latest",
    max_tokens: 300,
    temperature: 0,
    system:
      "Você é um especialista em SQL DuckDB para dados da Receita Federal brasileira. " +
      "Gere queries PRECISAS e OTIMIZADAS usando APENAS a tabela e colunas fornecidas. " +
      "NUNCA invente colunas. NUNCA use colunas que não existem no schema. " +
      "Para contagens use COUNT(*). Para listas use LIMIT. " +
      "Responda APENAS a SQL, sem explicações, sem markdown.",
    messages: [{
      role: "user",
      content:
        `${SCHEMA_HINT}\n\n` +
        `Pergunta do usuário: "${userQuery}"\n\n` +
        "Gere SOMENTE a SQL DuckDB (query única, sem markdown, sem explicação):"
    }]
  });

  const raw = resp?.content?.[0]?.text || "";
  return sanitizeSQL(raw);
}

async function llmExplain(userQuery, sql, rows) {
  if (!anthropic) return null;

  const resp = await anthropic.messages.create({
    model: "claude-3-5-sonnet-latest",
    max_tokens: 280,
    temperature: 0.7,
    system:
      "Você é um assistente brasileiro experiente em dados empresariais. " +
      "Seja objetivo, amigável e preciso. " +
      "Use APENAS os dados fornecidos - não invente informações. " +
      "Se o resultado for vazio, informe de forma clara. " +
      "Para números grandes, use separadores de milhar.",
    messages: [{
      role: "user",
      content:
        `Pergunta do usuário: ${userQuery}\n` +
        `SQL executada: ${sql}\n` +
        `Resultado (primeiras linhas): ${JSON.stringify(rows.slice(0, 3))}\n` +
        `Total de linhas: ${rows.length}\n\n` +
        "Explique o resultado em português brasileiro de forma útil e concisa."
    }]
  });

  return resp?.content?.[0]?.text || null;
}

/* ========================= FALLBACK ========================= */
async function fallbackQuery(userQuery) {
  const q = String(userQuery || "").trim();
  const qUp = q.toUpperCase();

  // 1) MEI
  if ((qUp.includes("QUANT") || qUp.includes("TOTAL")) && qUp.includes("MEI")) {
    const sql = `
      SELECT COUNT(*) AS total
      FROM chat_rfb.main.empresas
      WHERE opcao_mei = 'S'
    `;
    const rows = await cachedQueryAll(sql);
    return { sql, rows, mode: "count" };
  }

  // 2) Empresas ativas
  if ((qUp.includes("QUANT") || qUp.includes("TOTAL")) && qUp.includes("ATIV")) {
    const sql = `
      SELECT COUNT(*) AS total
      FROM chat_rfb.main.empresas
      WHERE situacao_cadastral = 'ATIVA'
    `;
    const rows = await cachedQueryAll(sql);
    return { sql, rows, mode: "count" };
  }

  // 3) Por porte (ME, EPP)
  if ((qUp.includes("QUANT") || qUp.includes("TOTAL")) && (qUp.includes("MICROEMPRESA") || qUp.includes(" ME "))) {
    const sql = `
      SELECT COUNT(*) AS total
      FROM chat_rfb.main.empresas
      WHERE porte = 'ME'
    `;
    const rows = await cachedQueryAll(sql);
    return { sql, rows, mode: "count" };
  }

  if ((qUp.includes("QUANT") || qUp.includes("TOTAL")) && qUp.includes("EPP")) {
    const sql = `
      SELECT COUNT(*) AS total
      FROM chat_rfb.main.empresas
      WHERE porte = 'EPP'
    `;
    const rows = await cachedQueryAll(sql);
    return { sql, rows, mode: "count" };
  }

  // 4) Por UF
  const ufs = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "PE", "CE", "DF", "GO", "ES", "PA", "AM", "MA", "PB", "RN", "AL", "SE", "PI", "MT", "MS", "AC", "RO", "RR", "AP", "TO"];
  for (const uf of ufs) {
    if (qUp.includes(` ${uf} `) || qUp.includes(` ${uf}?`) || qUp.endsWith(` ${uf}`)) {
      const sql = `
        SELECT COUNT(*) AS total
        FROM chat_rfb.main.empresas
        WHERE uf = ?
      `;
      const rows = await queryAll(sql, [uf]);
      return { sql, rows, mode: "count" };
    }
  }

  // 5) Busca por CNPJ
  const digits = q.replace(/\D/g, "");
  if (digits.length >= 8) {
    const cnpj = digits.slice(0, 8);
    const sql = `SELECT * FROM chat_rfb.main.empresas WHERE cnpj_basico = ? LIMIT 10`;
    const rows = await queryAll(sql, [cnpj]);
    return { sql, rows, mode: "list" };
  }

  // 6) Busca por nome
  const sql = `
    SELECT * FROM chat_rfb.main.empresas 
    WHERE razao_social LIKE ? OR nome_fantasia LIKE ?
    LIMIT 10
  `;
  const rows = await queryAll(sql, [`%${qUp}%`, `%${qUp}%`]);
  return { sql, rows, mode: "list" };
}

/* ========================= ROTAS ========================= */
app.get("/health", (_, res) => {
  res.json({
    ok: true,
    timestamp: new Date().toISOString(),
    motherduck: MD_TOKEN ? "configured" : "missing",
    claude: ANTHROPIC_API_KEY ? "configured" : "missing",
    cache_size: queryCache.size
  });
});

app.post("/chat", async (req, res) => {
  const start = Date.now();
  const debug = { stage: "start" };

  try {
    const q = String(req.body?.query || "").trim();
    if (!q) {
      return res.json({ 
        answer: "Por favor, digite uma consulta.", 
        debug: { stage: "empty_query" } 
      });
    }

    let sql;
    let rows;
    let usedFallback = false;

    // 1) Tenta Claude -> SQL
    try {
      debug.stage = "llm_to_sql";
      sql = await llmToSQL(q);
      debug.sql = sql;

      // 2) Executa SQL (com cache)
      debug.stage = "run_sql";
      rows = await cachedQueryAll(sql);

      // FALLBACK INTELIGENTE: Se retornou vazio em query de contagem
      if (!rows?.length && (q.toLowerCase().includes("quant") || q.toLowerCase().includes("total"))) {
        console.log("⚠️ SQL retornou vazio em query de contagem - tentando fallback");
        const fb = await fallbackQuery(q);
        sql = fb.sql;
        rows = fb.rows;
        usedFallback = true;
        debug.fallback_reason = "empty_count_result";
      }

    } catch (e) {
      // Claude falhou ou SQL inválida - usa fallback
      debug.stage = "llm_failed_using_fallback";
      debug.llm_error = String(e?.message || e);

      const fb = await fallbackQuery(q);
      sql = fb.sql;
      rows = fb.rows;
      usedFallback = true;

      const duration = Date.now() - start;

      // Resposta para COUNT
      if (fb.mode === "count") {
        const total = Number(fb.rows?.[0]?.total || 0);
        return res.json({
          answer: `Total encontrado: ${total.toLocaleString('pt-BR')} empresa(s)`,
          sql: fb.sql,
          rows: fb.rows,
          duration_ms: duration,
          used_fallback: true,
          debug
        });
      }

      // Resposta para LISTA vazia
      if (!fb.rows?.length) {
        return res.json({
          answer: "Nenhuma empresa encontrada com esses critérios.",
          sql: fb.sql,
          rows: [],
          duration_ms: duration,
          used_fallback: true,
          debug
        });
      }

      // Resposta para LISTA com resultados
      const first = fb.rows[0];
      return res.json({
        answer: `Encontrei ${fb.rows.length} empresa(s). Primeira: ${first.razao_social || first.nome_fantasia} (CNPJ: ${first.cnpj_basico})`,
        sql: fb.sql,
        rows: fb.rows,
        duration_ms: duration,
        used_fallback: true,
        debug
      });
    }

    // 3) Claude explica o resultado
    debug.stage = "llm_explain";
    let answer = null;
    
    try {
      if (rows?.length) {
        answer = await llmExplain(q, sql, rows);
      }
    } catch (e) {
      debug.explain_error = String(e?.message || e);
    }

    const duration = Date.now() - start;

    // Fallback para resposta se Claude não explicou
    if (!answer) {
      if (!rows?.length) {
        answer = "Nenhuma empresa encontrada.";
      } else if (rows.length === 1 && rows[0].total !== undefined) {
        // É uma contagem
        const num = Number(rows[0].total).toLocaleString('pt-BR');
        answer = `Total encontrado: ${num} empresa(s).`;
      } else {
        // É uma lista
        answer = `Encontrei ${rows.length} empresa(s).`;
      }
    }

    return res.json({
      answer,
      sql,
      rows,
      duration_ms: duration,
      used_fallback: usedFallback,
      debug
    });

  } catch (e) {
    debug.stage = debug.stage || "unknown_error";
    debug.error = String(e?.message || e);

    return res.status(500).json({
      answer: "Erro interno no servidor. Tente novamente.",
      duration_ms: Date.now() - start,
      debug
    });
  }
});

/* ========================= KEEP-ALIVE ========================= */
setInterval(async () => {
  try {
    await queryAll("SELECT COUNT(*) FROM chat_rfb.main.empresas LIMIT 1");
    console.log("💓 Heartbeat OK - " + new Date().toLocaleTimeString());
  } catch (e) {
    console.error("❌ Heartbeat failed:", e.message);
  }
}, 5 * 60 * 1000);

/* ========================= START ========================= */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log(`🚀 BDC API rodando na porta :${PORT}`);
  console.log(`🔐 Motherduck: ${MD_TOKEN ? "✅ Configurado" : "❌ Ausente"}`);
  console.log(`🤖 Claude: ${ANTHROPIC_API_KEY ? "✅ Configurado" : "❌ Ausente"}`);
  console.log(`📦 Cache: Ativo (TTL: 10min)`);
  console.log(`💓 Keep-alive: Ativo (5min)`);
});
