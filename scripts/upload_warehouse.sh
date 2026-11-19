#!/usr/bin/env bash
# Upload the warehouse + state to the "warehouse" release, with two safety nets:
#   1. never go backwards: refuse if the number of known firms is lower than
#      the baseline recorded at download time (set ALLOW_SHRINK=true to override)
#   2. daily backup: the first upload each day also goes to the "backups"
#      release as fca-YYYY-MM-DD.duckdb / extract_state-YYYY-MM-DD.json;
#      backups older than BACKUP_DAYS (default 7) are pruned
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f fca.duckdb ] || { echo "No fca.duckdb to upload"; exit 0; }
now=$(python3 scripts/count_firms.py data/raw/extract_state.json)
baseline=$(cat /tmp/wh/baseline_firms 2>/dev/null || echo 0)
if [ "$now" -lt "$baseline" ] && [ "${ALLOW_SHRINK:-false}" != "true" ]; then
  echo "::error::Refusing to upload: $now known firms is fewer than the $baseline downloaded. Something reset the data."
  exit 1
fi

gh release view warehouse >/dev/null 2>&1 || \
  gh release create warehouse --title "Warehouse (fca.duckdb)" \
    --notes "Latest DuckDB warehouse + extractor state, updated by the pipeline."
gh release upload warehouse fca.duckdb data/raw/extract_state.json --clobber
echo "Uploaded warehouse: $now known firms (baseline $baseline)"

# ---- daily backup
today=$(date -u +%F)
gh release view backups >/dev/null 2>&1 || \
  gh release create backups --title "Warehouse backups" \
    --notes "Daily copies of the warehouse and extractor state (last ${BACKUP_DAYS:-7} days)."
if ! gh release view backups --json assets --jq '.assets[].name' | grep -qx "fca-$today.duckdb"; then
  cp fca.duckdb "/tmp/fca-$today.duckdb"
  cp data/raw/extract_state.json "/tmp/extract_state-$today.json"
  gh release upload backups "/tmp/fca-$today.duckdb" "/tmp/extract_state-$today.json" --clobber
  echo "Daily backup saved: fca-$today.duckdb"
  cutoff=$(date -u -d "-${BACKUP_DAYS:-7} days" +%F)
  gh release view backups --json assets --jq '.assets[].name' | while read -r name; do
    d=$(echo "$name" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' || true)
    if [ -n "$d" ] && [[ "$d" < "$cutoff" ]]; then
      gh release delete-asset backups "$name" -y && echo "Pruned old backup $name"
    fi
  done
fi
