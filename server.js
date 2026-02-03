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

function queryAll(sql) {
  return new Promise((resolve, reject) => {
    const conn = db.connect();
    conn.all(sql, (err, rows) => {
      conn.close();
      if (err) return reject(err);
      resolve(rows);
    });
  });
}

/* ========================= CACHE ========================= */
const queryCache = new Map();
const CACHE_TTL = 10 * 60 * 1000;

async function cachedQueryAll(sql) {
  if (queryCache.has(sql)) {
    const { data, timestamp } = queryCache.get(sql);
    if (Date.now() - timestamp < CACHE_TTL) {
      console.log("📦 Cache HIT");
      return data;
    }
    queryCache.delete(sql);
  }

  const data = await queryAll(sql);
  queryCache.set(sql, { data, timestamp: Date.now() });
  console.log("💾 Cache MISS");
  return data;
}

/* ========================= CLAUDE ========================= */
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || "";
const anthropic = ANTHROPIC_API_KEY ? new Anthropic({ apiKey: ANTHROPIC_API_KEY }) : null;

const SCHEMA_HINT = `
Tabela: chat_rfb.main.empresas (ESTABELECIMENTOS brasileiros da Receita Federal)

IMPORTANTE: Cada linha é um ESTABELECIMENTO (matriz ou filial), NÃO uma empresa!
- Para contar EMPRESAS únicas: COUNT(DISTINCT cnpj_basico)
- Para contar ESTABELECIMENTOS: COUNT(*)

COLUNAS DISPONÍVEIS:

IDENTIFICAÇÃO:
- cnpj_basico: 8 dígitos que IDENTIFICAM A EMPRESA (ex: "33000167")
- cnpj: CNPJ completo 14 dígitos - IDENTIFICA O ESTABELECIMENTO
- matriz_filial_codigo: 1=MATRIZ, 2=FILIAL
- matriz_filial: "MATRIZ" ou "FILIAL"

DADOS CADASTRAIS:
- razao_social: nome oficial em MAIÚSCULAS (ex: "PETROBRAS S.A.")
- nome_fantasia: nome fantasia (pode ser NULL)
- situacao_cadastral: "ATIVA", "SUSPENSA", "BAIXADA", "NULA", "INAPTA"
- situacao_cadastral_codigo: código numérico da situação
- data_situacao_cadastral: data da situação cadastral
- motivo_situacao: motivo da situação cadastral
- data_inicio_atividade: data de abertura
- situacao_especial: situação especial (pode ser NULL)

CLASSIFICAÇÃO:
- porte: "ME", "EPP", "DEMAIS"
- porte_codigo: 1, 3, 5
- natureza_juridica: tipo societário (ex: "SOCIEDADE ANONIMA FECHADA")
- natureza_juridica_codigo: código numérico
- qualificacao_responsavel: qualificação do responsável
- capital_social: capital social em reais
- ente_federativo: ente federativo responsável

REGIME TRIBUTÁRIO:
- opcao_mei: "S" ou "N" (se é MEI)
- data_opcao_mei: data de opção pelo MEI
- data_exclusao_mei: data de exclusão do MEI
- opcao_simples: "S" ou "N" (se é Simples Nacional)
- data_opcao_simples: data de opção pelo Simples
- data_exclusao_simples: data de exclusão do Simples

ATIVIDADE ECONÔMICA:
- cnae_fiscal: código CNAE principal
- cnae_descricao: descrição da atividade principal
- cnaes_secundarios: lista de CNAEs secundários

LOCALIZAÇÃO:
- uf: sigla do estado (ex: "SP", "RJ", "MG")
- municipio: nome da cidade em MAIÚSCULAS (ex: "SAO PAULO")
- municipio_codigo: código IBGE do município
- cep: CEP do endereço
- bairro: bairro
- tipo_logradouro: tipo de logradouro (Rua, Av, etc)
- logradouro: nome do logradouro
- numero: número
- complemento: complemento do endereço

CONTATO:
- ddd: DDD do telefone
- telefone: número de telefone
- email: email (pode ser NULL)

REGRAS CRÍTICAS:
1. EMPRESAS ≠ ESTABELECIMENTOS! Use COUNT(DISTINCT cnpj_basico) para empresas únicas
2. Para empresas ativas: WHERE situacao_cadastral = 'ATIVA'
3. Para MEI: WHERE opcao_mei = 'S'
4. Para Simples Nacional: WHERE opcao_simples = 'S'
5. Nomes em MAIÚSCULAS sem acentos
6. NUNCA use colunas que não existem nesta lista
`;

function sanitizeSQL(sql) {
  let s = String(sql || "").trim();
  s = s.replace(/```[\s\S]*?```/g, (m) => m.replace(/```sql|```/gi, "").trim());
  s = s.replace(/;+\s*$/g, "").trim();

  const idx = s.toLowerCase().indexOf("select");
  if (idx === -1) throw new Error("SQL inválida");
  s = s.slice(idx).trim();

  const blocked = /\b(insert|update|delete|drop|alter|create|truncate|copy|attach|detach|pragma|call)\b/i;
  if (blocked.test(s)) throw new Error("SQL bloqueada");
  if (s.includes(";")) throw new Error("SQL bloqueada");

  const isAggregate = /\bcount\s*\(|\bgroup\s+by\b|\bsum\s*\(|\bavg\s*\(|\bmin\s*\(|\bmax\s*\(/i.test(s);
  if (!isAggregate && !/\blimit\b/i.test(s)) s += " LIMIT 50";

  return s;
}

async function llmToSQL(userQuery) {
  if (!anthropic) throw new Error("Claude não configurado");

  const resp = await anthropic.messages.create({
    model: "claude-sonnet-4-5-20250929",
    max_tokens: 300,
    temperature: 0,
    system: "Você é um especialista em SQL DuckDB. Gere queries usando APENAS a tabela e colunas fornecidas. Responda APENAS a SQL.",
    messages: [{
      role: "user",
      content: `${SCHEMA_HINT}\n\nPergunta: "${userQuery}"\n\nGere a SQL:`
    }]
  });

  return sanitizeSQL(resp?.content?.[0]?.text || "");
}

async function llmExplain(userQuery, sql, rows) {
  if (!anthropic) return null;

  // Converte BigInt pra Number
  const rowsSerializable = rows.map(row => {
    const newRow = {};
    for (const [key, value] of Object.entries(row)) {
      newRow[key] = typeof value === 'bigint' ? Number(value) : value;
    }
    return newRow;
  });

  const resp = await anthropic.messages.create({
    model: "claude-sonnet-4-5-20250929",
    max_tokens: 280,
    temperature: 0.7,
    system: "Você é um assistente brasileiro. Seja objetivo e use separadores de milhar.",
    messages: [{
      role: "user",
      content: `Pergunta: ${userQuery}\nSQL: ${sql}\nResultado: ${JSON.stringify(rowsSerializable.slice(0, 3))}\n\nExplique em pt-BR:`
    }]
  });

  return resp?.content?.[0]?.text || null;
}

/* ========================= FALLBACK ========================= */
async function fallbackQuery(userQuery) {
  const q = String(userQuery || "").trim();
  const qUp = q.toUpperCase();

  // MEI - distingue EMPRESAS vs ESTABELECIMENTOS
  if (qUp.includes("MEI")) {
    // Se pergunta por EMPRESAS
    if (qUp.includes("EMPRES") && !qUp.includes("ESTABELEC")) {
      const sql = "SELECT COUNT(DISTINCT cnpj_basico) AS total FROM chat_rfb.main.empresas WHERE opcao_mei = 'S'";
      const rows = await cachedQueryAll(sql);
      return { sql, rows, mode: "count" };
    }
    // Se pergunta por ESTABELECIMENTOS ou genérico
    const sql = "SELECT COUNT(*) AS total FROM chat_rfb.main.empresas WHERE opcao_mei = 'S'";
    const rows = await cachedQueryAll(sql);
    return { sql, rows, mode: "count" };
  }

  // Ativas - distingue EMPRESAS vs ESTABELECIMENTOS
  if (qUp.includes("ATIV")) {
    // Se pergunta por EMPRESAS (não estabelecimentos)
    if (qUp.includes("EMPRES") && !qUp.includes("ESTABELEC")) {
      const sql = "SELECT COUNT(DISTINCT cnpj_basico) AS total FROM chat_rfb.main.empresas WHERE situacao_cadastral = 'ATIVA'";
      const rows = await cachedQueryAll(sql);
      return { sql, rows, mode: "count" };
    }
    // Se pergunta por ESTABELECIMENTOS ou genérico
    const sql = "SELECT COUNT(*) AS total FROM chat_rfb.main.empresas WHERE situacao_cadastral = 'ATIVA'";
    const rows = await cachedQueryAll(sql);
    return { sql, rows, mode: "count" };
  }

  // UF
  const ufs = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "PE", "CE", "DF"];
  for (const uf of ufs) {
    if (qUp.includes(` ${uf}`) || qUp.endsWith(uf)) {
      const sql = `SELECT COUNT(*) AS total FROM chat_rfb.main.empresas WHERE uf = '${uf}'`;
      const rows = await queryAll(sql);
      return { sql, rows, mode: "count" };
    }
  }

  // CNPJ
  const digits = q.replace(/\D/g, "");
  if (digits.length >= 8) {
    const cnpj = digits.slice(0, 8);
    const sql = `SELECT * FROM chat_rfb.main.empresas WHERE cnpj_basico = '${cnpj}' LIMIT 10`;
    const rows = await queryAll(sql);
    return { sql, rows, mode: "list" };
  }

  // Nome
  const sql = `SELECT * FROM chat_rfb.main.empresas WHERE razao_social LIKE '%${qUp}%' OR nome_fantasia LIKE '%${qUp}%' LIMIT 10`;
  const rows = await queryAll(sql);
  return { sql, rows, mode: "list" };
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
      return res.json({ answer: "Digite uma consulta.", debug: { stage: "empty" } });
    }

    let sql, rows, usedFallback = false;

    try {
      debug.stage = "llm_to_sql";
      sql = await llmToSQL(q);
      debug.sql = sql;
      console.log("✅ Claude gerou SQL:", sql);

      debug.stage = "run_sql";
      rows = await cachedQueryAll(sql);
      console.log(`📊 SQL retornou ${rows?.length || 0} linhas`);

      if (!rows?.length && q.toLowerCase().includes("quant")) {
        console.log("⚠️ Vazio, usando fallback");
        const fb = await fallbackQuery(q);
        sql = fb.sql;
        rows = fb.rows;
        usedFallback = true;
      }

    } catch (e) {
      debug.stage = "llm_failed";
      debug.error = String(e?.message || e);
      console.log("❌ Claude falhou:", e?.message || e);

      const fb = await fallbackQuery(q);
      sql = fb.sql;
      rows = fb.rows;
      usedFallback = true;

      const duration = Date.now() - start;

      if (fb.mode === "count") {
        const total = Number(fb.rows?.[0]?.total || fb.rows?.[0]?.total_empresas_ativas || 0);
        
        // Converte BigInt em rows
        const rowsSerializable = fb.rows.map(row => {
          const newRow = {};
          for (const [key, value] of Object.entries(row)) {
            newRow[key] = typeof value === 'bigint' ? Number(value) : value;
          }
          return newRow;
        });
        
        return res.json({
          answer: `Total: ${total.toLocaleString('pt-BR')} empresa(s)`,
          sql: fb.sql,
          rows: rowsSerializable,
          duration_ms: duration,
          used_fallback: true,
          debug
        });
      }

      if (!fb.rows?.length) {
        return res.json({
          answer: "Nenhuma empresa encontrada.",
          sql: fb.sql,
          rows: [],
          duration_ms: duration,
          used_fallback: true,
          debug
        });
      }

      const first = fb.rows[0];
      
      // Converte BigInt
      const rowsSerializable = fb.rows.map(row => {
        const newRow = {};
        for (const [key, value] of Object.entries(row)) {
          newRow[key] = typeof value === 'bigint' ? Number(value) : value;
        }
        return newRow;
      });
      
      return res.json({
        answer: `Encontrei ${fb.rows.length} empresa(s). Primeira: ${first.razao_social || first.nome_fantasia}`,
        sql: fb.sql,
        rows: rowsSerializable,
        duration_ms: duration,
        used_fallback: true,
        debug
      });
    }

    debug.stage = "llm_explain";
    let answer = null;
    
    try {
      if (rows?.length) {
        answer = await llmExplain(q, sql, rows);
        console.log("✅ Claude explicou:", answer?.slice(0, 50));
      }
    } catch (e) {
      console.log("⚠️ llmExplain falhou:", e?.message || e);
      debug.explain_error = String(e?.message || e);
      // Não quebra - continua sem explicação
    }

    const duration = Date.now() - start;

    if (!answer) {
      if (!rows?.length) {
        answer = "Nenhuma empresa encontrada.";
      } else if (rows.length === 1 && rows[0].total !== undefined) {
        const num = Number(rows[0].total).toLocaleString('pt-BR');
        answer = `Total: ${num} empresa(s).`;
      } else {
        answer = `Encontrei ${rows.length} empresa(s).`;
      }
    }

    console.log("📤 Enviando resposta final, answer length:", answer?.length);

    // Converte BigInt em rows também
    const rowsSerializable = rows.map(row => {
      const newRow = {};
      for (const [key, value] of Object.entries(row)) {
        newRow[key] = typeof value === 'bigint' ? Number(value) : value;
      }
      return newRow;
    });

    return res.json({
      answer,
      sql,
      rows: rowsSerializable,
      duration_ms: duration,
      used_fallback: usedFallback,
      debug
    });

  } catch (e) {
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
  console.log(`🚀 BDC API :${PORT}`);
  console.log(`🔐 Motherduck: ${MD_TOKEN ? "✅" : "❌"}`);
  console.log(`🤖 Claude: ${ANTHROPIC_API_KEY ? "✅" : "❌"}`);
});
