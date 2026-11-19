#!/usr/bin/env bash
# Download the warehouse + extractor state from the "warehouse" release.
#
# Safety: a failed download must NEVER be treated as "start from scratch".
# On 18 Nov 2026 a transient GitHub error ("release not found") made the old
# workflow rebuild from the seed file and overwrite 339k firms. Now:
#   - downloads are retried with backoff
#   - starting from the seed is only allowed with BOOTSTRAP=true (manual run)
#   - the downloaded firm count is saved as a baseline for upload_warehouse.sh
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p /tmp/wh data/raw

for attempt in 1 2 3 4 5; do
  rm -f /tmp/wh/fca.duckdb /tmp/wh/extract_state.json
  if gh release download warehouse -D /tmp/wh -p fca.duckdb -p extract_state.json --clobber; then
    mv /tmp/wh/fca.duckdb . && mv /tmp/wh/extract_state.json data/raw/
    baseline=$(python3 scripts/count_firms.py data/raw/extract_state.json)
    echo "$baseline" > /tmp/wh/baseline_firms
    echo "Downloaded warehouse: $(du -h fca.duckdb | cut -f1), $baseline known firms (baseline for upload)"
    exit 0
  fi
  echo "::warning::Warehouse download failed (attempt $attempt/5); retrying"
  sleep $((attempt * ${RETRY_SLEEP:-30}))
done

if [ "${BOOTSTRAP:-false}" = "true" ]; then
  echo "::warning::BOOTSTRAP=true: starting from the seed data"
  echo 0 > /tmp/wh/baseline_firms
  exit 0
fi
echo "::error::Could not download the warehouse after 5 attempts. Refusing to start from scratch (that would overwrite the real data). Re-run later, or run manually with bootstrap=true only if the warehouse really has to be rebuilt."
exit 1
