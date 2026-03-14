#!/bin/bash
# schedule.sh
# Configura o cron do batch de findings no Hetzner
# Uso: bash schedule.sh

BATCH_DIR="/root/bdc-batch"
PYTHON="python3"
LOG_DIR="/var/log"

echo "========================================"
echo "BDC Findings — Configuração de Agendamento"
echo "========================================"

# Cria diretório se não existir
mkdir -p "$BATCH_DIR"

# Instala o cron job — roda todo dia às 03:00
CRON_LINE="0 3 * * * cd $BATCH_DIR && $PYTHON run_findings.py >> $LOG_DIR/bdc_findings_cron.log 2>&1"

# Verifica se já existe
if crontab -l 2>/dev/null | grep -q "run_findings.py"; then
    echo "⚠️  Cron já configurado. Substituindo..."
    crontab -l 2>/dev/null | grep -v "run_findings.py" | crontab -
fi

# Adiciona
(crontab -l 2>/dev/null; echo "$CRON_LINE") | crontab -

echo "✅ Cron configurado:"
crontab -l | grep run_findings

echo ""
echo "Para rodar agora sem esperar o cron:"
echo "  cd $BATCH_DIR && python3 run_findings.py"
echo ""
echo "Para rodar só um finding específico:"
echo "  cd $BATCH_DIR && python3 run_findings.py ceis_x_despesas"
echo ""
echo "Para listar todos os findings:"
echo "  cd $BATCH_DIR && python3 run_findings.py --list"
echo ""
echo "Para acompanhar o log em tempo real:"
echo "  tail -f $LOG_DIR/bdc_findings_cron.log"
