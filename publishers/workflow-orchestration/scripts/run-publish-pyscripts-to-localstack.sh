#!/usr/bin/env bash
# Entrypoint invoked by the publish-spark-scripts compose service.
# Sets AWS env, ensures the EMR bucket exists, runs publish_pyscripts_to_s3.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"
export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localstack:4566}"

cd "${ROOT}"

if [ ! -d "${ROOT}/pyScripts/churn_deletion" ] \
  || [ ! -d "${ROOT}/pyScripts/delta_optimization" ] \
  || [ ! -d "${ROOT}/pyScripts/vacuum_delta" ]; then
  echo "publish-spark-scripts: pyScripts/ not synced — run scripts/sync-spark-scripts.sh first." >&2
  exit 1
fi

echo "publish-spark-scripts: publishing -> idac-emr-bucket-local/local/migration_transformation/artifacts/scripts/"
sh "${SCRIPT_DIR}/publish_pyscripts_to_s3.sh"
echo "publish-spark-scripts: complete"
