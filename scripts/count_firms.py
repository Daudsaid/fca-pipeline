"""Print the number of firms the extractor has fetched, from its state file (0 if missing)."""
import json
import sys
from pathlib import Path

p = Path(sys.argv[1] if len(sys.argv) > 1 else "data/raw/extract_state.json")
try:
    print(sum(1 for t in json.loads(p.read_text()).get("firms", {}).values() if t))
except (OSError, ValueError):
    print(0)
