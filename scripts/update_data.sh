#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${PYTHON:-$ROOT_DIR/.venv/bin/python}"
FROM="${FROM:-latest}"
TO="${TO:-$(date +%Y-%m-%d)}"
ORG_ID="${ORG_ID:-}"
VENDOR_ID="${VENDOR_ID:-}"
TYPE_ID="${TYPE_ID:-}"
FORCE_DOWNLOAD="${FORCE_DOWNLOAD:-}"
SEARCH_FROM="${SEARCH_FROM:-}"
SEARCH_TO="${SEARCH_TO:-}"
PARQUET_PATH="${PARQUET_PATH:-$ROOT_DIR/data/parquet/opnirreikningar.parquet}"

if [ ! -x "$PYTHON" ]; then
  echo "Python not found at $PYTHON. Set PYTHON or create .venv." >&2
  exit 1
fi

args=("$ROOT_DIR/scripts/pipeline.py" "--to" "$TO")
if [ -n "$ORG_ID" ]; then
  args+=("--org-id" "$ORG_ID")
fi
if [ -n "$VENDOR_ID" ]; then
  args+=("--vendor-id" "$VENDOR_ID")
fi
if [ -n "$TYPE_ID" ]; then
  args+=("--type-id" "$TYPE_ID")
fi

if [ "$FROM" = "latest" ]; then
  FROM="$("$PYTHON" - <<PY
import duckdb
from pathlib import Path

path = Path("${PARQUET_PATH}")
if not path.exists():
    print("")
else:
    con = duckdb.connect(database=":memory:")
    try:
        row = con.execute("SELECT MAX(\\"Dags.greiðslu\\") FROM read_parquet(?)", [str(path)]).fetchone()
        value = row[0]
        if value is None:
            print("")
        else:
            print(value.isoformat())
    finally:
        con.close()
PY
)"
fi

if [ -n "$FROM" ] && [ "$FROM" != "latest" ]; then
  args+=("--from" "$FROM")
else
  earliest=("$PYTHON" "$ROOT_DIR/scripts/find_earliest.py" "--print-only")
  if [ -n "$SEARCH_FROM" ]; then
    earliest+=("--search-from" "$SEARCH_FROM")
  fi
  if [ -n "$SEARCH_TO" ]; then
    earliest+=("--search-to" "$SEARCH_TO")
  fi
  if [ -n "$ORG_ID" ]; then
    earliest+=("--org-id" "$ORG_ID")
  fi
  if [ -n "$VENDOR_ID" ]; then
    earliest+=("--vendor-id" "$VENDOR_ID")
  fi
  if [ -n "$TYPE_ID" ]; then
    earliest+=("--type-id" "$TYPE_ID")
  fi
  FROM="$(${earliest[@]})"
  if [ -z "$FROM" ]; then
    echo "Could not determine earliest date. Set FROM=YYYY-MM-DD." >&2
    exit 1
  fi
  args+=("--from" "$FROM")
fi

if [ -n "$FORCE_DOWNLOAD" ]; then
  args+=("--force-download")
fi

"$PYTHON" "${args[@]}"
"$PYTHON" "$ROOT_DIR/scripts/build_anomalies.py"
