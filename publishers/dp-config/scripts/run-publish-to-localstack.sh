#!/usr/bin/env bash
# Stage configs from airflow/configs (after sync-dp-config.sh), upload to LocalStack S3.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(pwd)"
CONFIG_SRC="${CONFIG_SRC:-${ROOT}/airflow/configs}"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"
export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localstack:4566}"

BUCKET="idac-airflow-bucket-local"

cd "${ROOT}"

if [ ! -d "${CONFIG_SRC}" ]; then
  echo "publish-dp-config: missing ${CONFIG_SRC} — run scripts/sync/sync-dp-config.sh first." >&2
  exit 1
fi

if [ -z "$(find "${CONFIG_SRC}" -type f 2>/dev/null | head -1)" ]; then
  echo "publish-dp-config: no files under ${CONFIG_SRC}" >&2
  exit 1
fi

if ! aws s3 ls "s3://${BUCKET}" >/dev/null 2>&1; then
  aws s3 mb "s3://${BUCKET}"
fi

rm -rf zip_tmp
mkdir -p zip_tmp/airflow
cp -a "${CONFIG_SRC}/." zip_tmp/airflow/configs/

echo "publish-dp-config: staging from ${CONFIG_SRC}"

sh "${SCRIPT_DIR}/publish_airflow_configs_to_s3.sh"

rm -rf zip_tmp

echo "publish-dp-config: done -> s3://${BUCKET}/Airflow_configs/"
