#!/usr/bin/env bash
# Run the whole ELT pipeline:  extract (FCA API -> JSONL)  ->  dbt build (load + transform + test)
#
#   bash run_pipeline.sh                          # daily-style run: refresh FCA_FIRM_LIMIT firms
#   bash run_pipeline.sh --limit 50               # any extract_fca.py options are passed through
#   bash run_pipeline.sh sweep --max-minutes 600  # backfill: sweep FRNs for 10 hours, then dbt
#   bash run_pipeline.sh --skip-extract           # just rebuild dbt from data already on disk
#
# If the extract stops early (API outage, Ctrl+C), dbt still runs on whatever was
# fetched, and the script then exits with the extract's error code.
set -uo pipefail
cd "$(dirname "$0")"
ROOT="$(pwd)"

# prefer the project's virtualenv; absolute paths because we cd into dbt/ later
PY=python3; [ -x "$ROOT/.venv/bin/python" ] && PY="$ROOT/.venv/bin/python"
DBT=dbt;    [ -x "$ROOT/.venv/bin/dbt" ]    && DBT="$ROOT/.venv/bin/dbt"
export DO_NOT_TRACK=1

extract_rc=0
if [ "${1:-}" = "--skip-extract" ]; then
    shift
else
    echo "==> Extract"
    "$PY" extract/extract_fca.py "$@" || extract_rc=$?
fi

echo "==> dbt build (raw -> staging -> snapshot -> intermediate -> marts, with tests)"
cd "$ROOT/dbt/fca_dbt"
"$DBT" build --profiles-dir . || exit $?
echo "==> Done. Warehouse: $ROOT/fca.duckdb"
exit $extract_rc
