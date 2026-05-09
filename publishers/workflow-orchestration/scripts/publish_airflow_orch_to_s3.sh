#!/bin/sh
# Mirrors CI's S3 layout: stages under CodeArtifact/, copies into "Airflow Orchestration/",
# tears down. Expects tmp/josys-workflow-orchestration-0.1/{dags,requirements.txt,startup_script.sh}.

set -eu

ENV="local"
Bucket_Suffix="local"
AWS_REGION="ap-northeast-1"
OPENLINEAGE_API_KEY="${OPENLINEAGE_API_KEY:-local-dev}"

S3_BUCKET="idac-airflow-bucket-${Bucket_Suffix}"
DIR="tmp"
PACKAGE_NAME="josys-workflow-orchestration"
LOCAL_PKG="${DIR}/${PACKAGE_NAME}-0.1"
S3_UPLOAD_PATH="s3://${S3_BUCKET}/CodeArtifact/airflow/${PACKAGE_NAME}-0.1/"
DAGS_PATH="Airflow Orchestration/"

error_exit() {
    echo "$1" 1>&2
    exit 1
}

aws s3 ls "s3://${S3_BUCKET}" > /dev/null 2>&1 \
    || error_exit "S3 bucket does not exist: ${S3_BUCKET}"

[ -d "$DIR" ] || error_exit "Temporary directory $DIR does not exist."
[ -d "$LOCAL_PKG" ] || error_exit "Expected package at $LOCAL_PKG"

apply_orchestration_placeholders() {
    _f="$1"
    if [ ! -f "$_f" ]; then
        echo "Warning: Skipping missing file '$_f'"
        return 0
    fi
    sed -e "s/{bucket_suffix}/$Bucket_Suffix/g; s/{openlineage_api_key}/$OPENLINEAGE_API_KEY/g; s/{aws_region}/$AWS_REGION/g" "$_f" > "$_f.__tmp__" \
        && mv "$_f.__tmp__" "$_f" || error_exit "sed failed for $_f"
}

[ -f "$LOCAL_PKG/dags/idac_airflow/dynamic_dag_generator.py" ] \
    || error_exit "Missing $LOCAL_PKG/dags/idac_airflow/dynamic_dag_generator.py"

apply_orchestration_placeholders "$LOCAL_PKG/dags/idac_airflow/dynamic_dag_generator.py"
apply_orchestration_placeholders "$LOCAL_PKG/dags/idac_airflow/control_event_automation_dag.py"
apply_orchestration_placeholders "$LOCAL_PKG/startup_script.sh"

RUNTIME_DAGS_DIR="../../airflow/dags"
mkdir -p "$RUNTIME_DAGS_DIR"
find "$RUNTIME_DAGS_DIR" -mindepth 1 -exec rm -rf {} +
cp -a "$LOCAL_PKG/dags/idac_airflow"/. "$RUNTIME_DAGS_DIR/"

aws s3 sync "$LOCAL_PKG" "$S3_UPLOAD_PATH" \
  --exclude "__pycache__/*" \
  --exclude "*/__pycache__/*" \
  || error_exit "Failed to upload to S3."

echo "Staged on S3: $S3_UPLOAD_PATH"

# LocalStack errors on recursive rm of an empty prefix on first deploy.
aws s3 rm "s3://${S3_BUCKET}/${DAGS_PATH}" --recursive 2>/dev/null || true

aws s3 cp "s3://${S3_BUCKET}/CodeArtifact/airflow/${PACKAGE_NAME}-0.1/dags/idac_airflow" "s3://${S3_BUCKET}/${DAGS_PATH}" --recursive --exclude "resources/*" || error_exit "Failed to copy dags to $DAGS_PATH"
aws s3 cp "s3://${S3_BUCKET}/CodeArtifact/airflow/${PACKAGE_NAME}-0.1/requirements.txt" "s3://${S3_BUCKET}/${DAGS_PATH}" || error_exit "Failed to copy requirements.txt"
aws s3 cp "s3://${S3_BUCKET}/CodeArtifact/airflow/${PACKAGE_NAME}-0.1/startup_script.sh" "s3://${S3_BUCKET}/${DAGS_PATH}" || error_exit "Failed to copy startup_script.sh"

rm -rf "$DIR" || error_exit "Failed to remove tmp"

# Best-effort cleanup of staging prefix.
aws s3 rm "s3://${S3_BUCKET}/CodeArtifact/airflow/" --recursive 2>/dev/null || true

echo "publish-workflow-orchestration: done -> s3://${S3_BUCKET}/${DAGS_PATH}"
