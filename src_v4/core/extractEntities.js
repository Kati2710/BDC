function extractCnpj(query){const d=String(query||"").replace(/\D/g,"");if(d.length>=14)return d.slice(0,14);return null;}
function extractCpf(query){const d=String(query||"").replace(/\D/g,"");if(d.length===11)return d;return null;}
function extractTopN(query,fallback=10){const m=String(query||"").match(/\b(\d{1,3})\b/);if(!m)return fallback;const n=Number(m[1]);if(!Number.isFinite(n)||n<=0)return fallback;return Math.min(n,100);}

function extractPersonName(query){
  const q=String(query||"").trim();
  const patterns=[
    /viagens?\s+de\s+([A-ZÀ-Ú][a-zA-ZÀ-ú]+(?:\s+[A-ZÀ-Ú][a-zA-ZÀ-ú]+){1,4})/i,
    /ultimas?\s+\d*\s*viagens?\s+de\s+([A-ZÀ-Ú][a-zA-ZÀ-ú]+(?:\s+[A-ZÀ-Ú][a-zA-ZÀ-ú]+){1,4})/i,
    /de\s+([A-ZÀ-Ú][a-zA-ZÀ-ú]+(?:\s+[A-ZÀ-Ú][a-zA-ZÀ-ú]+){1,4})\s+com\b/i,
    /servidor[a]?\s+([A-ZÀ-Ú][a-zA-ZÀ-ú]+(?:\s+[A-ZÀ-Ú][a-zA-ZÀ-ú]+){1,4})/i,
  ];
  for(const pat of patterns){const m=q.match(pat);if(m?.[1])return m[1].trim();}
  return null;
}

function extractCompanyName(query){
  const q=String(query||"").trim();
  const patterns=[
    /acordo\s+de\s+leniencia\s+da\s+(.+?)(?:\s+com|\s+e|\s*$)/i,
    /dados\s+(?:completos\s+)?do\s+acordo\s+de\s+leniencia\s+da\s+(.+?)(?:\s+com|\s+e|\s*$)/i,
    /contratos?\s+(?:da|do|de)\s+([A-ZÀ-Ú][a-zA-ZÀ-ú\d]+(?:\s+[a-zA-ZÀ-ú\d]+){0,4})/i,
    /sancoes?\s+(?:da|do|de)\s+([A-ZÀ-Ú][a-zA-ZÀ-ú\d]+(?:\s+[a-zA-ZÀ-ú\d]+){0,4})/i,
  ];
  for(const pat of patterns){const m=q.match(pat);if(m?.[1])return m[1].trim();}
  return null;
}

function extractAgency(query){
  const q=String(query||"").trim();
  const m=q.match(/(?:servidores?\s+do|imoveis?\s+do)\s+([A-ZÀ-Ú][a-zA-ZÀ-ú]+(?:\s+[a-zA-ZÀ-ú]+){0,5})/i);
  if(m?.[1])return m[1].trim();
  return null;
}

export function extractEntities(query){
  return{cnpj:extractCnpj(query),cpf:extractCpf(query),topN:extractTopN(query,10),personName:extractPersonName(query),companyName:extractCompanyName(query),agency:extractAgency(query)};
}
