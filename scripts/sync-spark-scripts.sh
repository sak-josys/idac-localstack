#!/bin/sh

set -eu

ROOT="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
SRC="${WORKFLOW_ORCHESTRATION_ROOT:-$ROOT/../idac-workflow-orchestration}"
BASE="$ROOT/publishers/workflow-orchestration/pyScripts"

for sub in churn_deletion delta_optimization vacuum_delta; do
  if [ ! -d "$SRC/scripts/$sub" ]; then
    echo "sync-spark-scripts: missing $SRC/scripts/$sub" >&2
    exit 1
  fi
done

mkdir -p "$BASE"
rm -rf "$BASE/churn_deletion" "$BASE/delta_optimization" "$BASE/vacuum_delta"
cp -a "$SRC/scripts/churn_deletion"     "$BASE/"
cp -a "$SRC/scripts/delta_optimization" "$BASE/"
cp -a "$SRC/scripts/vacuum_delta"       "$BASE/"

echo "sync-spark-scripts: $SRC/scripts/{churn_deletion,delta_optimization,vacuum_delta} -> $BASE/"
