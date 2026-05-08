#!/bin/sh
# Mirror idac-workflow-orchestration into publishers/workflow-orchestration/
# (flat: dags/, requirements.txt, startup_script.sh).
# Override repo root with WORKFLOW_ORCHESTRATION_ROOT.

set -eu

ROOT="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
SRC="${WORKFLOW_ORCHESTRATION_ROOT:-$ROOT/../idac-workflow-orchestration}"
BASE="$ROOT/publishers/workflow-orchestration"

if [ ! -d "$SRC/dags" ]; then
  echo "sync-workflow-orchestration: missing dags in $SRC" >&2
  exit 1
fi

if [ ! -f "$SRC/requirements.txt" ]; then
  echo "sync-workflow-orchestration: missing requirements.txt in $SRC" >&2
  exit 1
fi

if [ ! -f "$SRC/internal_scripts/startup_script.sh" ]; then
  echo "sync-workflow-orchestration: missing internal_scripts/startup_script.sh in $SRC" >&2
  exit 1
fi

rm -rf "$BASE/staging"
rm -rf "$BASE/dags" "$BASE/requirements.txt" "$BASE/startup_script.sh"
cp -a "$SRC/dags" "$BASE/"
cp -a "$SRC/requirements.txt" "$BASE/"
cp -a "$SRC/internal_scripts/startup_script.sh" "$BASE/startup_script.sh"

echo "sync-workflow-orchestration: $SRC -> $BASE/ (dags/, requirements.txt, startup_script.sh)"
