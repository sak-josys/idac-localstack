#!/usr/bin/env bash
# Local equivalent of idac-dp-config/ddl/execute_ddl.sh, pointed at LocalStack.
# Resolves dp-config DDL JSONs, uploads them to s3://idac-emr-bucket-local/local/
# ddl/configs/, then runs ONE spark-submit (local mode) batch-registering every
# Glue DB + table in a single SparkSession.

set -euo pipefail

if [ "${BOOTSTRAP:-false}" != "true" ]; then
    echo "==> BOOTSTRAP != true, skipping Glue catalog setup (catalog persisted from previous run)"
    exit 0
fi

DP_CONFIG_ROOT="${DP_CONFIG_ROOT:-/dp-config}"
DDL_CONFIGS_DIR="${DP_CONFIG_ROOT}/ddl/configs"
DDL_SCRIPT="${DP_CONFIG_ROOT}/ddl/glueDDL.py"
STAGING_DIR="/tmp/setup-glue"

LOCAL_ENDPOINT="${AWS_ENDPOINT_URL:-http://localstack:4566}"
EMR_BUCKET="${EMR_BUCKET:-idac-emr-bucket-local}"
ENV="${TARGET_ENV:-local}"
BUCKET_SUFFIX="${BUCKET_SUFFIX:-local}"

SECTIONS_DEFAULT=(bronze silver gold common)
SECTIONS=("${SECTIONS_DEFAULT[@]}")
if [ -n "${GLUE_SECTIONS:-}" ]; then
    # shellcheck disable=SC2206
    SECTIONS=(${GLUE_SECTIONS})
fi

if [ ! -f "$DDL_SCRIPT" ]; then
    echo "ERROR: ${DDL_SCRIPT} not found." >&2
    echo "       Mount idac-dp-config repo at ${DP_CONFIG_ROOT} (see compose)." >&2
    exit 1
fi

if [ ! -d "$DDL_CONFIGS_DIR" ]; then
    echo "ERROR: ${DDL_CONFIGS_DIR} not found." >&2
    exit 1
fi

local_aws() {
    AWS_ACCESS_KEY_ID=test \
    AWS_SECRET_ACCESS_KEY=test \
    AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}" \
    aws --endpoint-url "$LOCAL_ENDPOINT" --no-cli-pager "$@"
}

if ! local_aws s3 ls "s3://${EMR_BUCKET}" >/dev/null 2>&1; then
    echo "==> Creating s3://${EMR_BUCKET}/ ..."
    local_aws s3 mb "s3://${EMR_BUCKET}"
fi

echo "==> Glue setup: env=${ENV}, bucket_suffix=${BUCKET_SUFFIX}"
echo "    dp-config root : ${DP_CONFIG_ROOT}"
echo "    sections       : ${SECTIONS[*]}"

rm -rf "$STAGING_DIR" && mkdir -p "$STAGING_DIR"

# Resolve {env} / {bucket_suffix} placeholders and upload to S3. Configs only
# use those two placeholders, so sed is sufficient (avoids needing jq).
declare -a S3_CONFIG_PATHS=()

for section in "${SECTIONS[@]}"; do
    section_dir="${DDL_CONFIGS_DIR}/${section}"
    if [ ! -d "$section_dir" ]; then
        echo "    [skip] no ${section}/ directory under ${DDL_CONFIGS_DIR}"
        continue
    fi

    mkdir -p "${STAGING_DIR}/${section}"
    for src_cfg in "$section_dir"/*.json; do
        [ -e "$src_cfg" ] || continue
        name=$(basename "$src_cfg")

        # GLUE_TABLES whitelist (basename without .json) for surgical iteration.
        if [ -n "${GLUE_TABLES:-}" ]; then
            table_no_ext="${name%.json}"
            case " ${GLUE_TABLES} " in
                *" ${table_no_ext} "*) ;;
                *) echo "    [${section}] skip ${name} (not in GLUE_TABLES)"
                   continue ;;
            esac
        fi

        staged="${STAGING_DIR}/${section}/${name}"

        sed \
            -e "s|{env}|${ENV}|g" \
            -e "s|{bucket_suffix}|${BUCKET_SUFFIX}|g" \
            "$src_cfg" > "$staged"

        s3_key="${ENV}/ddl/configs/${section}/${name}"
        s3_path="s3://${EMR_BUCKET}/${s3_key}"

        local_aws s3 cp "$staged" "$s3_path" --only-show-errors
        echo "    [${section}] uploaded ${name} -> ${s3_path}"

        S3_CONFIG_PATHS+=("$s3_path")
    done
done

if [ "${#S3_CONFIG_PATHS[@]}" -eq 0 ]; then
    echo "==> No configs found under ${DDL_CONFIGS_DIR} for sections: ${SECTIONS[*]}"
    exit 0
fi

echo
echo "==> Running batch_register.py for ${#S3_CONFIG_PATHS[@]} config(s) ..."
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"
export AWS_REGION="${AWS_REGION:-$AWS_DEFAULT_REGION}"
export AWS_ENDPOINT_URL="$LOCAL_ENDPOINT"
export DP_CONFIG_DDL="${DP_CONFIG_ROOT}/ddl"

# SimpleAWSCredentialsProvider so the dummy test/test creds get picked up
# (default chain would try IMDS, which doesn't exist locally).
/opt/spark/bin/spark-submit \
    --master "local[2]" \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --conf "spark.hadoop.fs.s3a.endpoint=${LOCAL_ENDPOINT}" \
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
    --conf "spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}" \
    --conf "spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}" \
    /workspace/scripts/bootstrap/setup-glue/batch_register.py \
    --region "$AWS_REGION" \
    --config-paths "${S3_CONFIG_PATHS[@]}"

echo
echo "==> Glue setup complete."
