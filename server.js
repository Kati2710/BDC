import express from "express";
import cors from "cors";
import Anthropic from "@anthropic-ai/sdk";

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

const HETZNER_API = process.env.HETZNER_API_BASE || "http://89.167.48.3:5010";
const HETZNER_KEY = process.env.HETZNER_API_KEY || "bdc-sql-api-key-2026-segura";
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

const DB_CATALOG = `
BANCO DE DADOS UNIFICADO — brazildatacorp.duckdb (5 bilhões de linhas)
Todas as colunas são VARCHAR. Use CAST quando precisar de número.

== PROGRAMAS SOCIAIS ==
_bolsafamilia_pagamentos (1.38 bilhão): "MÊS COMPETÊNCIA", "MÊS REFERÊNCIA", UF, "CÓDIGO MUNICÍPIO SIAFI", "NOME MUNICÍPIO", "CPF FAVORECIDO", "NIS FAVORECIDO", "NOME FAVORECIDO", "VALOR PARCELA"
_bolsafamilia_saques (478M): "MÊS COMPETÊNCIA", "MÊS REFERÊNCIA", UF, "CÓDIGO MUNICÍPIO SIAFI", "NOME MUNICÍPIO", "CPF FAVORECIDO", "NIS FAVORECIDO", "NOME FAVORECIDO", "VALOR PARCELA"
_novobolsafamilia (668M): "MÊS COMPETÊNCIA", "MÊS REFERÊNCIA", UF, "CÓDIGO MUNICÍPIO SIAFI", "NOME MUNICÍPIO", "CPF FAVORECIDO", "NIS FAVORECIDO", "NOME FAVORECIDO", "VALOR PARCELA"
_auxilioemergencial (781M): "MÊS DISPONIBILIZAÇÃO", UF, "CÓDIGO MUNICÍPIO IBGE", "NOME MUNICÍPIO", "NIS BENEFICIÁRIO", "CPF BENEFICIÁRIO", "NOME BENEFICIÁRIO", "NIS RESPONSÁVEL", "VALOR BENEFÍCIO"
_auxiliobrasil (279M): "MÊS COMPETÊNCIA", "MÊS REFERÊNCIA", UF, "CÓDIGO MUNICÍPIO SIAFI", "NOME MUNICÍPIO", "CPF FAVORECIDO", "NIS FAVORECIDO", "NOME FAVORECIDO", "VALOR PARCELA"
_bpc (300M): "MÊS COMPETÊNCIA", "MÊS REFERÊNCIA", UF, "CÓDIGO MUNICÍPIO SIAFI", "NOME MUNICÍPIO", "NIS BENEFICIÁRIO", "CPF BENEFICIÁRIO", "NOME BENEFICIÁRIO", "VALOR BENEFÍCIO"
_segurodefeso (39M): "MÊS REFERÊNCIA", UF, "CÓDIGO MUNICÍPIO SIAFI", "NOME MUNICÍPIO", "CPF FAVORECIDO", "NIS FAVORECIDO", "RGP FAVORECIDO", "NOME FAVORECIDO", "VALOR PARCELA"
_garantiasafra (32M): "MÊS REFERÊNCIA", UF, "CÓDIGO MUNICÍPIO SIAFI", "NOME MUNICÍPIO", "NIS FAVORECIDO", "NOME FAVORECIDO", "VALOR PARCELA"
_pedemeia (36M): "MÊS FOLHA", "MÊS REFERÊNCIA", UF, "CÓDIGO MUNICÍPIO SIAFI", "NOME MUNICÍPIO", "NIS BENEFICIÁRIO", "CPF BENEFICIÁRIO", "NOME BENEFICIÁRIO", "VALOR BENEFÍCIO"
_peti (802K): "MÊS REFERÊNCIA", UF, "CÓDIGO SIAFI MUNICÍPIO", "NOME MUNICÍPIO", "NIS FAVORECIDO", "NOME FAVORECIDO", "SITUAÇÃO BENEFÍCIO", "VALOR PARCELA"
_auxilioreconstrucao (425K): "MÊS REFERÊNCIA", UF, "CÓDIGO MUNICÍPIO SIAFI", "NOME MUNICÍPIO", "CPF FAVORECIDO", "NIS FAVORECIDO", "NOME FAVORECIDO", "QUANTIDADE DE PESSOAS NA FAMÍLIA", "VALOR BENEFÍCIO"

== EMPRESAS RECEITA FEDERAL (por UF) ==
_empresas_sp (20M), _empresas_mg (7.5M), _empresas_rj (5.9M), _empresas_rs (4.7M), _empresas_pr (4.7M),
_empresas_ba (3.4M), _empresas_sc (3.3M), _empresas_go (2.5M), _empresas_pe (2M), _empresas_ce (1.9M),
_empresas_df (1.2M), _empresas_es (1.4M), _empresas_mt (1.3M), _empresas_pa (1.4M), _empresas_am (739K),
_empresas_ms (914K), _empresas_ma (1M), _empresas_pb (881K), _empresas_rn (787K), _empresas_pi (593K),
_empresas_al (653K), _empresas_ro (476K), _empresas_to (460K), _empresas_se (457K), _empresas_ex (168K),
_empresas_ap (150K), _empresas_rr (133K), _empresas_ac (157K)
Colunas: cnpj_basico (8 dígitos), razao_social, porte, capital_social, est (STRUCT)
  est contém: est.uf, est.municipio, est.situacao_cadastral, est.bairro, est.cep, est.cnpj_completo
Para buscar por UF específica: WHERE est.uf = 'SP'
Para cruzar com PT via CNPJ: SUBSTRING("CNPJ DO SANCIONADO", 1, 8) = cnpj_basico

== SERVIDORES PÚBLICOS FEDERAIS ==
_servidores_cadastro (18.7M): Id_SERVIDOR_PORTAL, NOME, CPF, MATRICULA, DESCRICAO_CARGO, CLASSE_CARGO, REFERENCIA_CARGO, PADRAO_CARGO, NIVEL_CARGO, SIGLA_FUNCAO, NIVEL_FUNCAO, FUNCAO, CODIGO_ATIVIDADE, ATIVIDADE, OPCAO_PARCIAL, COD_UORG_LOTACAO, UORG_LOTACAO, COD_ORG_LOTACAO, ORG_LOTACAO, COD_ORGSUP_LOTACAO, ORGSUP_LOTACAO, COD_UORG_EXERCICIO, UORG_EXERCICIO, COD_ORG_EXERCICIO, ORG_EXERCICIO, COD_ORGSUP_EXERCICIO, ORGSUP_EXERCICIO, COD_TIPO_VINCULO, TIPO_VINCULO, SITUACAO_VINCULO, DATA_INICIO_AFASTAMENTO, DATA_TERMINO_AFASTAMENTO
_servidores_cadastro__7 (52M — militares): mesmas colunas
_servidores_cadastro__5 (11.8M — aposentados): Id_SERVIDOR_PORTAL, NOME, CPF, MATRICULA, COD_TIPO_APOSENTADORIA, TIPO_APOSENTADORIA, DATA_APOSENTADORIA, DESCRICAO_CARGO
_servidores_remuneracao (18.7M): ANO, MES, Id_SERVIDOR_PORTAL, CPF, NOME, "REMUNERAÇÃO BÁSICA BRUTA (R$)", "REMUNERAÇÃO BÁSICA BRUTA (U$)", "ABATE-TETO (R$)", "GRATIFICAÇÃO DE NATAL (R$)", "REMUNERAÇÃO APÓS DEDUÇÕES OBRIGATÓRIAS (R$)", "VERBAS INDENIZATÓRIAS REGISTRADAS NO SISTEMA DE REMUNERAÇÃO FIXA", "OUTRAS REMUNERAÇÕES TEMPORÁRIAS", "IRRF (R$)", "PSS/RPPS (R$)", "DEMAIS DEDUÇÕES (R$)", "PENSÃO MILITAR (R$)", "FUNDO DE SAÚDE (R$)", "TCU/CGU (R$)", "REMUNERAÇÃO APÓS DEDUÇÕES OBRIGATÓRIAS (R$)"
_servidores_remuneracao__2 (29.7M), _servidores_remuneracao__3 (51.9M), _servidores_remuneracao__4 (237K), _servidores_remuneracao__5 (8.5M): mesmas colunas
_servidores_afastamentos (84K): ANO, MES, Id_SERVIDOR_PORTAL, CPF, NOME, DATA_INICIO_AFASTAMENTO, DATA_FIM_AFASTAMENTO, CODIGO_AFASTAMENTO, AFASTAMENTO
_servidores_afastamentos__2 (7.7M): mesmas colunas
_servidores_observacoes (463K, __2 39K, __3 7.7M, __4 3K, __5 1.2M, __6 17K, __7 918K): ANO, MES, Id_SERVIDOR_PORTAL, NOME, CPF, OBSERVACAO
_servidores_honorarios_jetons_ (45K): ANO, MES, Id_SERVIDOR_PORTAL, CPF, NOME, EMPRESA, VALOR
_servidores_honorariosadvocaticios (1.4M): ANO, MES, Id_SERVIDOR_PORTAL, CPF, NOME, OBSERVACOES, VALOR

== DESPESAS DIÁRIAS ==
_despesasdiarias_despesas_empenho (31M): "Id Empenho", "Código Empenho", "Data Emissão", "Tipo Empenho", "Código Órgão Superior", "Órgão Superior", "Código Órgão", "Órgão", "Código Unidade Gestora", "Unidade Gestora", "Código Função", "Função", "Código Subfunção", "Subfunção", "Valor Empenhado (R$)", "CNPJ Credor", "Nome Credor"
_despesasdiarias_despesas_pagamento (102M): "Código Pagamento", "Data Emissão", "Código Órgão Superior", "Órgão Superior", "Código Órgão", "Órgão", "CNPJ Favorecido", "Nome Favorecido", "Valor Bruto (R$)", "Valor Líquido (R$)"
_despesasdiarias_despesas_pagamento_favorecidosfinais (130M): "Código Pagamento", "Data Emissão", "Código Favorecido", "Favorecido", "Valor do Pagamento em R$"
_despesasdiarias_despesas_liquidacao_empenhosimpactados (76M): "Código Liquidação", "Código Empenho", "Valor Liquidado (R$)"
_despesas_favorecidos (114M): "Código Favorecido", "Nome Favorecido", "Sigla UF", "Nome Município", "Código Órgão Superior", "Nome Órgão Superior", "Valor Recebido (R$)"

== VIAGENS A SERVIÇO ==
_viagens_viagem (9.4M): "Identificador do processo de viagem", "Número da Proposta (PCDP)", "Situação", "Viagem Urgente", "Código do órgão superior", "Nome do órgão superior", "Código órgão solicitante", "Nome órgão solicitante", "CPF Viajante", "Nome", "Cargo"
_viagens_passagem (4.9M): "Identificador do processo de viagem", "Meio de transporte", "País - Origem ida", "UF - Origem ida", "Cidade - Origem ida", "País - Destino ida", "UF - Destino ida", "Cidade - Destino ida", "Valor da passagem"
_viagens_pagamento (16M): "Identificador do processo de viagem", "Código do órgão superior", "Nome do órgão superior", "Valor diárias (R$)", "Valor passagens (R$)", "Valor outros gastos (R$)", "Valor total (R$)"
_viagens_trecho (20M): "Identificador do processo de viagem", "Sequência Trecho", "Origem - Data", "Origem - País", "Origem - UF", "Origem - Cidade", "Destino - Data", "Destino - País", "Destino - UF", "Destino - Cidade"

== SANÇÕES E IMPEDIMENTOS ==
_ceis (22K): "CADASTRO", "CPF OU CNPJ DO SANCIONADO", "NOME DO SANCIONADO", "RAZÃO SOCIAL - CADASTRO RECEITA", "CATEGORIA DA SANÇÃO", "DATA INÍCIO SANÇÃO", "DATA FIM SANÇÃO", "ÓRGÃO SANCIONADOR"
_cnep (1.5K): mesmas colunas que _ceis
_cepim (3.5K): "CNPJ ENTIDADE", "NOME ENTIDADE", "NÚMERO CONVÊNIO", "ÓRGÃO CONCEDENTE", "MOTIVO DO IMPEDIMENTO"
_acordos (143): "ID DO ACORDO", "CNPJ DO SANCIONADO", "RAZÃO SOCIAL – CADASTRO RECEITA", "SITUAÇÃO DO ACORDO DE LENIÊNICA", "DATA DE INÍCIO DO ACORDO", "DATA DE FIM DO ACORDO", "ÓRGÃO SANCIONADOR"
_ceaf (4K): "CPF OU CNPJ DO SANCIONADO", "NOME DO SANCIONADO", "CATEGORIA DA SANÇÃO", "DATA INÍCIO SANÇÃO", "DATA FIM SANÇÃO"

== LICITAÇÕES E COMPRAS ==
_licitacoes (1.7M): "Número Licitação", "Código UG", "Nome UG", "Modalidade Compra", "Número Processo", "Objeto", "Situação Licitação", "Valor Licitação"
_compras (4.2M): "Código Órgão", "Nome Órgão", "Número Contrato", "Descrição Item Compra", "Quantidade", "Valor Unitário", "Valor Total"
_convenios (612K): "NÚMERO CONVÊNIO", UF, "NOME MUNICÍPIO", "SITUAÇÃO CONVÊNIO", "OBJETO DO CONVÊNIO", "VALOR GLOBAL", "VALOR REPASSE", "DATA INÍCIO VIGÊNCIA", "DATA FIM VIGÊNCIA"

== OUTROS ==
_pep (70K — Pessoas Expostas Politicamente): CPF, Nome_PEP, Sigla_Função, Descrição_Função, Nome_Órgão, Data_Início_Exercício, Data_Fim_Exercício
_renúnciasfiscais (752K): "Ano-calendário", CNPJ, "Razão Social", "Código CNAE", CNAE, Município, UF, "Modalidade", "Valor Renúncia"
_emendas (69K): "Código da Emenda", "Nome Função", "Tipo de Emenda", "Valor Empenhado", "Valor Pago"
_emendasparlamentarespordocumento (4.4M): "Código da Emenda", "Nome do Autor da Emenda", "Número da emenda", "Valor Empenhado", "Valor Pago", "Tipo de Emenda"
_transferencias (9M): "ANO / MÊS", "TIPO TRANSFERÊNCIA", UF, "NOME MUNICÍPIO", "NOME ÓRGÃO", "VALOR TRANSFERIDO"
_imoveisfuncionais (22K): "NOME PERMISSIONÁRIO", CPF, "CARGO OU FUNÇÃO DE CONFIANÇA", "ÓRGÃO EXERCÍCIO DO PERMISSIONÁRIO", "DATA INÍCIO OCUPAÇÃO"
_cpgf (1.7M — cartão corporativo): "CÓDIGO ÓRGÃO", "NOME ÓRGÃO", "ANO EXTRATO", "MÊS EXTRATO", "CPF PORTADOR", "NOME PORTADOR", "CNPJ ESTABELECIMENTO", "NOME ESTABELECIMENTO", "VALOR TRANSAÇÃO"
_orçamentodadespesa (304K): "EXERCÍCIO", "CÓDIGO ÓRGÃO SUPERIOR", "NOME ÓRGÃO SUPERIOR", "NOME FUNÇÃO", "DOTAÇÃO INICIAL", "DOTAÇÃO ATUALIZADA"
_execuçãodareceita (1.7M): "CÓDIGO ÓRGÃO", "NOME ÓRGÃO", "CATEGORIA ECONÔMICA", "ORIGEM RECEITA", "PREVISÃO INICIAL", "ARRECADAÇÃO REALIZADA"
_notasfiscais (273K): "CHAVE DE ACESSO", "NÚMERO", "NATUREZA DA OPERAÇÃO", "DATA EMISSÃO", "CNPJ EMITENTE", "NOME EMITENTE", "VALOR TOTAL"

== REGRAS IMPORTANTES ==
1. Todas as colunas são VARCHAR — use CAST para somar: CAST("VALOR PARCELA" AS DECIMAL)
2. Valores monetários têm vírgula decimal: use REPLACE("VALOR", ',', '.') antes de CAST
3. Para empresas RFB: acesse campos aninhados com est.uf, est.municipio, est.situacao_cadastral
4. Para cruzar beneficiários com empresas: SUBSTRING(CPF, 1, 11) ou CNPJ com cnpj_basico (8 dígitos)
5. Sempre use LIMIT 100 salvo pedido explícito de agregação
6. Datas no formato YYYYMM (ex: 202401) ou DD/MM/YYYY
7. Para servidores com múltiplas tabelas __2, __3 etc — use UNION ALL se precisar do histórico completo
`;

/* ========================= MAIN HANDLER ========================= */
app.post("/chat", async (req, res) => {
  const start = Date.now();
  const query = (req.body?.query || "").trim();
  
  if (!query) return res.json({ error: "Query vazia" });
  
  try {
    console.log(`\n${"=".repeat(60)}\n❓ "${query}"\n${"=".repeat(60)}`);
    
    // PASSO 1: Claude gera SQL
    console.log("🤖 Claude gerando SQL...");
    
    const sqlGen = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 1000,
      messages: [{
        role: "user",
        content: `Você é analista de dados públicos brasileiros especialista em DuckDB.

${DB_CATALOG}

PERGUNTA DO USUÁRIO:
"${query}"

Responda APENAS com o SQL necessário para responder à pergunta.
Sem explicações, sem markdown, apenas SQL puro.
Use aspas duplas em nomes de colunas com espaços ou acentos.`
      }]
    });
    
    let sql = sqlGen.content.find(b => b.type === "text")?.text.trim() || "";
    sql = sql.replace(/```sql\n?/g, "").replace(/```/g, "").trim();
    
    console.log(`📝 SQL: ${sql.substring(0, 300)}...`);
    
    // PASSO 2: Executa SQL
    console.log("⚡ Executando...");
    
    const response = await fetch(`${HETZNER_API}/query_unified`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-API-Key": HETZNER_KEY
      },
      body: JSON.stringify({ sql }),
      signal: AbortSignal.timeout(120000)
    });
    
    const data = await response.json();
    
    if (!response.ok) {
      throw new Error(data.error || "Query falhou");
    }
    
    console.log(`📊 ${data.row_count || 0} linhas retornadas`);
    
    // PASSO 3: Claude explica
    console.log("💬 Claude explicando...");
    
    const explanation = await anthropic.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 1500,
      messages: [{
        role: "user",
        content: `Pergunta: "${query}"

SQL executado: ${sql}

Resultados (${data.row_count} linhas):
${JSON.stringify(data.rows?.slice(0, 50), null, 2)}

Explique os resultados em português de forma clara e objetiva.
Se houver valores monetários, formate em R$.
Cite a fonte dos dados (Portal da Transparência / Receita Federal).`
      }]
    });
    
    const answer = explanation.content.find(b => b.type === "text")?.text || "Sem resposta";
    
    console.log(`✅ CONCLUÍDO em ${Date.now() - start}ms`);
    
    return res.json({
      answer,
      sql,
      duration_ms: Date.now() - start,
      rows_returned: data.row_count
    });
    
  } catch (err) {
    console.error("❌ ERRO:", err.message);
    return res.status(500).json({ 
      error: err.message, 
      duration_ms: Date.now() - start 
    });
  }
});

app.get("/health", async (_, res) => {
  try {
    const r = await fetch(`${HETZNER_API}/health`, { 
      headers: { "X-API-Key": HETZNER_KEY },
      signal: AbortSignal.timeout(5000)
    });
    res.json({ ok: true, hetzner: r.ok });
  } catch {
    res.json({ ok: true, hetzner: false });
  }
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log("═".repeat(60));
  console.log("🚀 BDC — MOTHERDUCK NO HETZNER");
  console.log("═".repeat(60));
  console.log(`📡 Porta: ${PORT}`);
  console.log(`🧱 API: ${HETZNER_API}`);
  console.log(`🗄️  5 bilhões de linhas | 475 tabelas`);
  console.log(`⚡ 2 chamadas Claude por pergunta`);
  console.log("═".repeat(60));
});
