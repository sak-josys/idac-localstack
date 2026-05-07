#!/bin/sh
# Local-only mirror of idac-workflow-orchestration/.github/scripts/upload_scripts.sh.
# Hardcoded for LocalStack: ENV=local, Bucket_Suffix=local. Run from
# publishers/workflow-orchestration/ (so the pyScripts/ subdirs resolve).

set -eu

echo "Running the Script: $0"

ENV="local"
Bucket_Suffix="local"

S3_BUCKET="idac-emr-bucket-${Bucket_Suffix}"
SCRIPTS_PATH="s3://${S3_BUCKET}/${ENV}/migration_transformation/artifacts/scripts/"
LOCAL_OPTIMIZE_SCRIPTS_DIR="pyScripts/delta_optimization"
LOCAL_CHURN_SCRIPTS_DIR="pyScripts/churn_deletion"
LOCAL_VACUUM_SCRIPTS_DIR="pyScripts/vacuum_delta"

error_exit() {
    echo "Error: $1" >&2
    exit 1
}

upload_script() {
    _src="$1"
    _name=$(basename "$_src")
    if [ ! -f "$_src" ]; then
        error_exit "Missing file: $_src"
    fi
    echo "Uploading ${_name} to $SCRIPTS_PATH..."
    aws s3 cp "$_src" "$SCRIPTS_PATH" || error_exit "Failed to upload ${_name}"
}

echo "============================================"
echo "Environment: $ENV"
echo "Bucket Suffix: $Bucket_Suffix"
echo "Target S3 Path: $SCRIPTS_PATH"
echo "============================================"

# CI assumes the bucket pre-exists; LocalStack starts empty, so create on demand.
if ! aws s3 ls "s3://${S3_BUCKET}" >/dev/null 2>&1; then
    echo "Bucket '$S3_BUCKET' not found, creating..."
    aws s3 mb "s3://${S3_BUCKET}" || error_exit "Failed to create bucket '$S3_BUCKET'"
fi

upload_script "$LOCAL_OPTIMIZE_SCRIPTS_DIR/optimize_delta_table.py"
upload_script "$LOCAL_CHURN_SCRIPTS_DIR/churn_delete_customer_data.py"
upload_script "$LOCAL_CHURN_SCRIPTS_DIR/churn_org_recovery.py"
upload_script "$LOCAL_VACUUM_SCRIPTS_DIR/vacuum_delta.py"

echo "============================================"
echo "Scripts uploaded to ${ENV} environment (LocalStack)"
echo "============================================"

echo "Verifying uploaded files:"
aws s3 ls "$SCRIPTS_PATH" --recursive | grep -E "optimize_delta_table.py|churn_delete_customer_data.py|churn_org_recovery.py|vacuum_delta.py"

echo "Script execution completed successfully!"
