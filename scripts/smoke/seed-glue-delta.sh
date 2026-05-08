#!/usr/bin/env bash
# Seed local_silver_db.okta_auditlog as a minimal Delta table fixture so
# local_vacuum_delta_dag has something to operate on:
#   {"vacuum_delta_tables":["local_silver_db.okta_auditlog"]}

set -euo pipefail

NETWORK="${NETWORK:-idac-localstack}"
AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localstack:4566}"
AWS_REGION="${AWS_REGION:-ap-northeast-1}"

DATA_BUCKET="${DATA_BUCKET:-idac-data-local}"
DATABASE_NAME="${DATABASE_NAME:-local_silver_db}"
TABLE_NAME="${TABLE_NAME:-okta_auditlog}"
TABLE_PREFIX="${TABLE_PREFIX:-local/silver/okta_auditlog}"
S3_LOCATION="s3://${DATA_BUCKET}/${TABLE_PREFIX}"
S3A_LOCATION="s3a://${DATA_BUCKET}/${TABLE_PREFIX}"

awscli() {
    docker run --rm \
        --network "$NETWORK" \
        -e AWS_ACCESS_KEY_ID=test \
        -e AWS_SECRET_ACCESS_KEY=test \
        -e AWS_DEFAULT_REGION="$AWS_REGION" \
        amazon/aws-cli:latest \
        --endpoint-url "$AWS_ENDPOINT_URL" \
        --no-cli-pager \
        "$@"
}

echo "==> Seed target"
echo "    bucket : ${DATA_BUCKET}"
echo "    glue   : ${DATABASE_NAME}.${TABLE_NAME}"
echo "    path   : ${S3_LOCATION}"

echo
echo "==> Ensure data bucket exists"
if ! awscli s3 ls "s3://${DATA_BUCKET}" >/dev/null 2>&1; then
    awscli s3 mb "s3://${DATA_BUCKET}"
fi

echo
echo "==> Write minimal Delta table via Spark"
docker exec \
    -e AWS_ACCESS_KEY_ID=test \
    -e AWS_SECRET_ACCESS_KEY=test \
    -e AWS_DEFAULT_REGION="$AWS_REGION" \
    idac-localstack-spark-master-1 \
    /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        --conf spark.hadoop.fs.s3a.endpoint="$AWS_ENDPOINT_URL" \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
        --conf spark.hadoop.fs.s3a.access.key=test \
        --conf spark.hadoop.fs.s3a.secret.key=test \
        /workspace/spark/seed_delta_table.py \
        --path "$S3A_LOCATION" \
        --s3-endpoint "$AWS_ENDPOINT_URL"

echo
echo "==> Ensure Glue database exists"
if ! awscli glue get-database --name "$DATABASE_NAME" >/dev/null 2>&1; then
    awscli glue create-database \
        --database-input "{\"Name\":\"${DATABASE_NAME}\"}"
fi

echo
echo "==> Upsert Glue table metadata"
TABLE_INPUT=$(python3 - <<PY
import json

print(json.dumps({
    "Name": "${TABLE_NAME}",
    "TableType": "EXTERNAL_TABLE",
    "Parameters": {
        "classification": "delta",
        "table_type": "delta",
    },
    "StorageDescriptor": {
        "Columns": [
            {"Name": "id", "Type": "string"},
            {"Name": "env", "Type": "string"},
        ],
        "Location": "${S3_LOCATION}",
        "InputFormat": "org.apache.hadoop.mapred.SequenceFileInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
        "SerdeInfo": {
            "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            "Parameters": {},
        },
    },
}))
PY
)

if awscli glue get-table --database-name "$DATABASE_NAME" --name "$TABLE_NAME" >/dev/null 2>&1; then
    awscli glue update-table \
        --database-name "$DATABASE_NAME" \
        --table-input "$TABLE_INPUT"
else
    awscli glue create-table \
        --database-name "$DATABASE_NAME" \
        --table-input "$TABLE_INPUT"
fi

echo
echo "==> Verify"
awscli glue get-table \
    --database-name "$DATABASE_NAME" \
    --name "$TABLE_NAME" \
    --query 'Table.StorageDescriptor.Location'
awscli s3 ls "s3://${DATA_BUCKET}/${TABLE_PREFIX}/" --recursive | head -20

echo
echo "==> seed-glue-delta: complete"
