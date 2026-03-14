"""
run_findings.py
Materializa todos os findings definidos em findings_config.py
Uso:
    python3 run_findings.py                    # todos
    python3 run_findings.py ceis_x_despesas    # um especifico
    python3 run_findings.py --list             # lista disponiveis
"""

import sys
import time
import logging
import os
import duckdb
from datetime import datetime
from findings_config import SOURCE_DB, FINDINGS_DB, ANCHORS, TARGETS, FINDINGS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("/var/log/bdc_findings.log"),
    ],
)
log = logging.getLogger("findings")


def build_sql(finding):
    anchor_cfg = ANCHORS[finding["anchor"]]
    target_cfg = TARGETS[finding["target"]]
    out_table  = finding["output_table"]
    rule       = finding["temporal_rule"]

    anchor_key = anchor_cfg["key_col"]
    target_key = target_cfg["key_col"]

    # Monta lista de colunas do SELECT
    select_parts = []
    select_parts.append('  a."' + anchor_key + '"  AS documento')

    for col in anchor_cfg.get("extra_cols", []):
        select_parts.append("  a." + col)

    for col in target_cfg.get("dim_cols", []):
        select_parts.append("  t." + col)

    value_expr  = target_cfg["value_expr"]
    value_alias = target_cfg["value_alias"]
    date_expr   = target_cfg["date_expr"]

    select_parts.append("  COUNT(*)  AS qtd_registros")
    select_parts.append("  ,SUM(" + value_expr + ")  AS " + value_alias + "_total")
    select_parts.append("  ,MIN(" + date_expr + ")   AS primeiro_pagamento")
    select_parts.append("  ,MAX(" + date_expr + ")   AS ultimo_pagamento")

    for col in anchor_cfg.get("audit_cols", []):
        select_parts.append("  ,MAX(a." + col + ")")

    for col in target_cfg.get("audit_cols", []):
        select_parts.append("  ,MAX(t." + col + ")")

    col_list = "\n".join(select_parts)

    # JOIN condition
    join_lines = ['  ON t."' + target_key + '" = a."' + anchor_key + '"']

    date_start = anchor_cfg.get("date_start_col")
    date_end   = anchor_cfg.get("date_end_col")

    if rule == "after_start" and date_start:
        join_lines.append('  AND ' + date_expr + ' >= a."' + date_start + '"')
    elif rule == "during" and date_start and date_end:
        join_lines.append('  AND ' + date_expr + ' >= a."' + date_start + '"')
        join_lines.append('  AND ' + date_expr + ' <= a."' + date_end + '"')

    join_on = "\n".join(join_lines)

    # GROUP BY posicional
    non_agg_count = (
        1
        + len(anchor_cfg.get("extra_cols", []))
        + len(target_cfg.get("dim_cols", []))
    )
    group_by = ", ".join(str(i) for i in range(1, non_agg_count + 1))

    idx_doc   = "idx_" + out_table + "_doc"
    idx_valor = "idx_" + out_table + "_valor"

    parts = [
        "-- " + finding["description"],
        "CREATE OR REPLACE TABLE " + out_table + " AS",
        "SELECT",
        col_list,
        "FROM source." + anchor_cfg["table"] + " a",
        "JOIN source." + target_cfg["table"] + " t",
        join_on,
        "GROUP BY " + group_by,
        "HAVING SUM(" + value_expr + ") > 0",
        "ORDER BY " + value_alias + "_total DESC",
    ]
    sql_create = "\n".join(parts)

    sql_idx1 = "CREATE INDEX IF NOT EXISTS " + idx_doc + " ON " + out_table + "(documento)"
    sql_idx2 = "CREATE INDEX IF NOT EXISTS " + idx_valor + " ON " + out_table + "(" + value_alias + "_total DESC)"

    return [sql_create, sql_idx1, sql_idx2]


def run_finding(finding, con):
    fid = finding["id"]
    log.info("▶  Iniciando: " + fid)
    log.info("   " + finding["description"])

    statements = build_sql(finding)

    t0 = time.time()
    try:
        for stmt in statements:
            log.debug("SQL: " + stmt[:120])
            con.execute(stmt)

        elapsed = time.time() - t0
        out     = finding["output_table"]
        val_col = TARGETS[finding["target"]]["value_alias"] + "_total"
        count   = con.execute("SELECT COUNT(*) FROM " + out).fetchone()[0]
        total   = con.execute("SELECT SUM(" + val_col + ") FROM " + out).fetchone()[0] or 0

        log.info("OK  " + fid + ": " + str(count) + " achados | R$ " + "{:,.2f}".format(total) + " | " + "{:.1f}".format(elapsed) + "s")
        return {"id": fid, "ok": True, "rows": count, "total_valor": total, "elapsed": elapsed}

    except Exception as e:
        elapsed = time.time() - t0
        log.error("ERRO  " + fid + " (" + "{:.1f}".format(elapsed) + "s): " + str(e))
        return {"id": fid, "ok": False, "error": str(e), "elapsed": elapsed}


def main():
    args = sys.argv[1:]

    if "--list" in args:
        print("\nFindings disponíveis:\n")
        for f in sorted(FINDINGS, key=lambda x: x["priority"]):
            print("  [" + str(f["priority"]) + "] " + f["id"])
            print("      " + f["description"] + "\n")
        return

    targets = FINDINGS
    if args:
        ids     = set(args)
        targets = [f for f in FINDINGS if f["id"] in ids]
        if not targets:
            log.error("Nenhum finding encontrado para: " + str(args))
            sys.exit(1)

    targets = sorted(targets, key=lambda x: x["priority"])

    log.info("=" * 60)
    log.info("BDC Findings Batch — " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("Origem : " + SOURCE_DB)
    log.info("Destino: " + FINDINGS_DB)
    log.info("Jobs   : " + str(len(targets)))
    log.info("=" * 60)

    os.makedirs("/mnt/volume/duckdb_tmp", exist_ok=True)

    con = duckdb.connect(FINDINGS_DB)
    con.execute("PRAGMA threads=8")
    con.execute("PRAGMA memory_limit='12GB'")
    con.execute("PRAGMA temp_directory='/mnt/volume/duckdb_tmp'")
    con.execute("ATTACH '" + SOURCE_DB + "' AS source (READ_ONLY)")

    t_total = time.time()
    results = []
    for finding in targets:
        result = run_finding(finding, con)
        results.append(result)

    con.close()

    elapsed_total = time.time() - t_total
    ok  = [r for r in results if r["ok"]]
    err = [r for r in results if not r["ok"]]

    log.info("=" * 60)
    log.info("CONCLUIDO em " + "{:.1f}".format(elapsed_total) + "s")
    log.info("  OK: " + str(len(ok)) + "   ERRO: " + str(len(err)))
    for r in err:
        log.error("  ERRO: " + r["id"] + " — " + str(r.get("error")))
    log.info("=" * 60)

    sys.exit(0 if not err else 1)


if __name__ == "__main__":
    main()