#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${PYTHON:-$ROOT_DIR/.venv/bin/python}"
FROM="${FROM:-}"
TO="${TO:-$(date +%Y-%m-%d)}"
ORG_ID="${ORG_ID:-}"
VENDOR_ID="${VENDOR_ID:-}"
TYPE_ID="${TYPE_ID:-}"
FORCE_DOWNLOAD="${FORCE_DOWNLOAD:-}"
SEARCH_FROM="${SEARCH_FROM:-}"
SEARCH_TO="${SEARCH_TO:-}"

if [ ! -x "$PYTHON" ]; then
  echo "Python not found at $PYTHON. Set PYTHON or create .venv." >&2
  exit 1
fi

args=("$ROOT_DIR/scripts/pipeline.py" "--to" "$TO" "--org-id" "$ORG_ID" "--vendor-id" "$VENDOR_ID" "--type-id" "$TYPE_ID")

if [ -n "$FROM" ]; then
  args+=("--from" "$FROM")
else
  earliest=("$PYTHON" "$ROOT_DIR/scripts/find_earliest.py" "--print-only")
  if [ -n "$SEARCH_FROM" ]; then
    earliest+=("--search-from" "$SEARCH_FROM")
  fi
  if [ -n "$SEARCH_TO" ]; then
    earliest+=("--search-to" "$SEARCH_TO")
  fi
  earliest+=("--org-id" "$ORG_ID" "--vendor-id" "$VENDOR_ID" "--type-id" "$TYPE_ID")
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

exec "$PYTHON" "${args[@]}"
