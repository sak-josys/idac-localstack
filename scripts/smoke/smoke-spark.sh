#!/usr/bin/env bash
# Smoke-test the local Spark cluster: hello.py (cluster works) +
# s3_check.py (s3a:// reaches LocalStack via baked-in hadoop-aws JARs).
# Requires spark-master + spark-worker up, and publish-airflow to have run.

set -euo pipefail

ROOT="$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

# Same image as master/worker so spark-submit client matches cluster versions.
IMAGE="idac-localstack-spark:latest"
NETWORK="idac-localstack"

run_smoke() {
    name="$1"
    script="$2"
    shift 2

    echo
    echo "=== smoke-spark: $name ==="
    docker run --rm \
        --network "$NETWORK" \
        -v "$ROOT:/workspace" \
        -e AWS_ACCESS_KEY_ID=test \
        -e AWS_SECRET_ACCESS_KEY=test \
        -e AWS_DEFAULT_REGION=ap-northeast-1 \
        "$IMAGE" \
        /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --conf spark.hadoop.fs.s3a.endpoint=http://localstack:4566 \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
            --conf spark.hadoop.fs.s3a.access.key=test \
            --conf spark.hadoop.fs.s3a.secret.key=test \
            "$@" \
            "$script"
}

run_smoke "hello"    /workspace/spark/smoke/hello.py
run_smoke "s3_check" /workspace/spark/smoke/s3_check.py

echo
echo "=== smoke-spark: all checks passed ==="
