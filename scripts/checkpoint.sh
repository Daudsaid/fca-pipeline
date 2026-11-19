#!/usr/bin/env bash
# Mid-run checkpoint. In CI the extractor runs this every FCA_CHECKPOINT_MINUTES:
# it loads everything fetched so far into fca.duckdb (dbt build, incl. tests) and
# uploads the warehouse + extractor state (with the same safety checks as the
# final upload), so a lost runner only loses the work done since the last checkpoint.
set -uo pipefail
cd "$(dirname "$0")/.."
export DO_NOT_TRACK=1
DBT=dbt; [ -x .venv/bin/dbt ] && DBT="$PWD/.venv/bin/dbt"

if ! (cd dbt/fca_dbt && "$DBT" build --profiles-dir . --quiet); then
  echo "checkpoint: dbt build failed - not uploading"
  exit 1
fi
bash scripts/upload_warehouse.sh
