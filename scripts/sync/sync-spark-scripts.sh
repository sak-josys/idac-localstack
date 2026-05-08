#!/bin/sh
# Mirror upstream PySpark scripts into publishers/workflow-orchestration/pyScripts/.

set -eu

ROOT="$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)"
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

# Local-only patch: upstream pins s3.amazonaws.com inside SparkSession.builder,
# which overrides spark-submit --conf at runtime. Rewrite the synced copy to
# point at LocalStack. Upstream source is left untouched.
S3_ENDPOINT="${LOCALSTACK_S3_ENDPOINT:-http://localstack:4566}"
find "$BASE" -type f -name '*.py' -exec sed -i.bak \
  "s|\"spark.hadoop.fs.s3a.endpoint\", \"s3.amazonaws.com\"|\"spark.hadoop.fs.s3a.endpoint\", \"${S3_ENDPOINT}\"|g" \
  {} +
find "$BASE" -type f -name '*.py.bak' -delete

echo "sync-spark-scripts: $SRC/scripts/{churn_deletion,delta_optimization,vacuum_delta} -> $BASE/ (s3a.endpoint -> ${S3_ENDPOINT})"
