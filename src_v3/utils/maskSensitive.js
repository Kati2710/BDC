function maskDocument(value) {
  if (value == null) return value;

  const str = String(value).trim();

  if (/^\d{11}$/.test(str)) {
    return `${str.slice(0, 3)}.***.***-${str.slice(-2)}`;
  }

  if (/^\d{14}$/.test(str)) {
    return `${str.slice(0, 2)}.***.***/****-${str.slice(-2)}`;
  }

  return value;
}

export function maskSensitiveRows(rows = []) {
  return rows.map((row) => {
    const masked = { ...row };

    for (const key of Object.keys(masked)) {
      if (/(cpf|cnpj)/i.test(key)) {
        masked[key] = maskDocument(masked[key]);
      }
    }

    return masked;
  });
}