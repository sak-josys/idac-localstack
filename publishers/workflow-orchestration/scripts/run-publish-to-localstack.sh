#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(pwd)"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"
export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localstack:4566}"

BUCKET="idac-airflow-bucket-local"

cd "${ROOT}"

if [ ! -d "${ROOT}/dags" ]; then
  echo "publish-workflow: missing ${ROOT}/dags — run scripts/sync/sync-workflow-orchestration.sh first." >&2
  exit 1
fi
if [ ! -f "${ROOT}/requirements.txt" ] || [ ! -f "${ROOT}/startup_script.sh" ]; then
  echo "publish-workflow: missing requirements.txt or startup_script.sh in ${ROOT}" >&2
  exit 1
fi

if ! aws s3 ls "s3://${BUCKET}" >/dev/null 2>&1; then
  aws s3 mb "s3://${BUCKET}"
fi

# CI expects this package folder name inside tmp/; ephemeral.
rm -rf tmp
mkdir -p tmp/josys-workflow-orchestration-0.1
cp -a "${ROOT}/dags" tmp/josys-workflow-orchestration-0.1/
cp -a "${ROOT}/requirements.txt" tmp/josys-workflow-orchestration-0.1/
cp -a "${ROOT}/startup_script.sh" tmp/josys-workflow-orchestration-0.1/

echo "publish-workflow: publishing -> ${BUCKET}/Airflow Orchestration/"

sh "${SCRIPT_DIR}/publish_airflow_orch_to_s3.sh"

echo "publish-workflow: complete"
