#!/usr/bin/env bash
# Seed synthetic 'COMPLETED' rows into local_common_db.auditlog so DAGs that
# gate on prior-run success (sidsum, silver, gold) can proceed past their
# first run on a fresh local stack.
#
# Two-phase flow per seed triple (jobId, jobName, tenantId):
#   1) skip if Glue already has the partition (idempotent re-run)
#   2) PySpark writes one parquet row at the partition's S3 path,
#      then `aws glue create-partition` registers it so Athena can find it.
#
# The script self-skips unless BOOTSTRAP=true, matching the rest of the
# bootstrap chain (PERSISTENCE=1 keeps Glue/S3 across restarts so we don't
# reseed on every `docker compose up`).

set -euo pipefail

if [ "${BOOTSTRAP:-false}" != "true" ]; then
    echo "==> BOOTSTRAP != true, skipping auditlog seed (assumed persisted from previous run)"
    exit 0
fi

LOCAL_ENDPOINT="${AWS_ENDPOINT_URL:-http://localstack:4566}"
DATA_BUCKET="${LOCAL_DATA_BUCKET:-idac-data-local}"
COMMON_DB="${LOCAL_COMMON_DB:-local_common_db}"
ENV_TAG="${TARGET_ENV:-local}"

# Default seed list = the rows needed by sidsum's gate queries on a fresh stack.
# Add more triples as new gating queries surface (semicolon-separated):
#   "jobId1:jobName1:tenantId1;jobId2:jobName2:tenantId2"
AUDITLOG_SEEDS="${AUDITLOG_SEEDS:-browser_extension_silver_transformation:browser_extension_silver_transformation:ALL_DEFAULT}"

if [ -z "$AUDITLOG_SEEDS" ]; then
    echo "==> AUDITLOG_SEEDS is empty — nothing to seed."
    exit 0
fi

# s3a:// for Spark's Hadoop FS (the JARs are baked into the spark image).
TABLE_PATH_S3A="s3a://${DATA_BUCKET}/${ENV_TAG}/common/auditlog/"
TABLE_PATH_S3="s3://${DATA_BUCKET}/${ENV_TAG}/common/auditlog/"

local_aws() {
    AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}" \
    AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}" \
    AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}" \
    aws --endpoint-url "$LOCAL_ENDPOINT" --no-cli-pager "$@"
}

echo "==> Auditlog seed"
echo "    table   : ${TABLE_PATH_S3}"
echo "    glue db : ${COMMON_DB}"
echo "    seeds   : ${AUDITLOG_SEEDS}"

# Phase 1: filter out triples whose Glue partition already exists.
PENDING=()
IFS=';' read -r -a SEED_LIST <<< "$AUDITLOG_SEEDS"
for entry in "${SEED_LIST[@]+"${SEED_LIST[@]}"}"; do
    entry="${entry//[[:space:]]/}"
    [ -z "$entry" ] && continue

    IFS=':' read -r JID JNAME TID <<< "$entry"
    if [ -z "$JID" ] || [ -z "$JNAME" ] || [ -z "$TID" ]; then
        echo "ERROR: malformed AUDITLOG_SEEDS entry '$entry' (need jobId:jobName:tenantId)" >&2
        exit 2
    fi

    if local_aws glue get-partition \
            --database-name "$COMMON_DB" \
            --table-name auditlog \
            --partition-values "$JID" "$JNAME" "$TID" \
            >/dev/null 2>&1; then
        echo "    [skip] partition already registered: jobId=${JID} jobName=${JNAME} tenantId=${TID}"
    else
        echo "    [pending] jobId=${JID} jobName=${JNAME} tenantId=${TID}"
        PENDING+=("${JID}:${JNAME}:${TID}")
    fi
done

if [ "${#PENDING[@]}" -eq 0 ]; then
    echo "==> All requested partitions already present. Nothing to do."
    exit 0
fi

# Phase 2a: write parquet rows via Spark (local mode — same shape as setup-glue).
PENDING_JOINED=$(IFS=';'; echo "${PENDING[*]}")
echo
echo "==> Writing ${#PENDING[@]} pending row(s) via spark-submit"
echo "    pending: ${PENDING_JOINED}"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"
export AUDITLOG_TABLE_PATH="$TABLE_PATH_S3A"
export AUDITLOG_SEEDS_PENDING="$PENDING_JOINED"

# SimpleAWSCredentialsProvider — the default chain tries IMDS, which doesn't
# exist locally. s3a path-style + LocalStack endpoint match setup-glue exactly.
/opt/spark/bin/spark-submit \
    --master "local[1]" \
    --conf "spark.hadoop.fs.s3a.endpoint=${LOCAL_ENDPOINT}" \
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
    --conf "spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}" \
    --conf "spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}" \
    /workspace/scripts/bootstrap/seed-auditlog/seed_auditlog.py

# Phase 2b: register Glue partitions for the freshly-written rows.
# Storage descriptor mirrors what idac-dp-config/ddl/glueDDL.py registers for
# the auditlog table's placeholder partition (parquet input/output formats +
# Hive parquet SerDe). Mismatching SerDe makes Athena read 0 rows silently.
echo
echo "==> Registering ${#PENDING[@]} Glue partition(s)"
for entry in "${PENDING[@]}"; do
    IFS=':' read -r JID JNAME TID <<< "$entry"
    PARTITION_LOCATION="${TABLE_PATH_S3}jobId=${JID}/jobName=${JNAME}/tenantId=${TID}/"

    # Heredoc instead of jq because the spark image doesn't ship jq, and the
    # values we interpolate are AWS-safe ASCII (jobId/jobName/tenantId).
    PARTITION_INPUT=$(cat <<EOF
{
  "Values": ["${JID}", "${JNAME}", "${TID}"],
  "StorageDescriptor": {
    "Location": "${PARTITION_LOCATION}",
    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
    "Compressed": false,
    "SerdeInfo": {
      "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
  }
}
EOF
)

    if local_aws glue create-partition \
            --database-name "$COMMON_DB" \
            --table-name auditlog \
            --partition-input "$PARTITION_INPUT" >/dev/null 2>&1; then
        echo "    [registered] jobId=${JID} jobName=${JNAME} tenantId=${TID}"
    else
        # Could be a race (another seeder beat us to it) or a real failure;
        # re-check via get-partition to disambiguate.
        if local_aws glue get-partition \
                --database-name "$COMMON_DB" \
                --table-name auditlog \
                --partition-values "$JID" "$JNAME" "$TID" \
                >/dev/null 2>&1; then
            echo "    [exists] jobId=${JID} jobName=${JNAME} tenantId=${TID} (registered concurrently)"
        else
            echo "ERROR: failed to register partition jobId=${JID} jobName=${JNAME} tenantId=${TID}" >&2
            exit 1
        fi
    fi
done

echo
echo "==> Auditlog seed complete."
