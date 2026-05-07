#!/usr/bin/env bash
# CI upload-only mirror: idac-dp-config (Airflow_configs/) + idac-workflow-orchestration (Airflow Orchestration/).
# No tests, CodeArtifact, deployments, or EMR/Spark script uploads (upload_scripts.sh) — add those later if needed.
#
# Host:  export AWS_ENDPOINT_URL=http://localhost:4566  (default below)
# Compose: AWS_ENDPOINT_URL=http://localstack:4566
#
# With docker compose, set SKIP_SYNC=1 so sync-* services remain the single source of copy-from-upstream.

set -euo pipefail

ROOT="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"
export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:4566}"
export OPENLINEAGE_API_KEY="${OPENLINEAGE_API_KEY:-local-dev}"

if [ "${SKIP_SYNC:-0}" != "1" ]; then
  sh "$ROOT/scripts/sync-dp-config.sh"
  sh "$ROOT/scripts/sync-workflow-orchestration.sh"
fi

echo "=== publish-airflow-to-localstack: dp-config (Airflow_configs/) ==="
( cd "$ROOT/publishers/dp-config" && bash scripts/run-publish-to-localstack.sh )

echo "=== publish-airflow-to-localstack: workflow-orchestration (Airflow Orchestration/) ==="
( cd "$ROOT/publishers/workflow-orchestration" && bash scripts/run-publish-to-localstack.sh )

echo "=== publish-airflow-to-localstack: done (idac-airflow-bucket-local) ==="
