#!/bin/sh

# LocalStack-only variant of idac-dp-config publish_airflow_configs_to_s3.sh.
# env is always "local"; bucket_suffix=local; region=ap-northeast-1; tz=Asia/Tokyo.
#
# Usage: sh publish_airflow_configs_to_s3.sh
# Optional overrides via env vars:
#   VIRTUAL_CLUSTER_ID (default: localstack-placeholder-vc)
#   ARN_ID             (default: 000000000000)

set -eu

ENV="local"
BUCKET_SUFFIX="local"
SCHEDULE_TIMEZONE="${SCHEDULE_TIMEZONE:-Asia/Tokyo}"
AWS_REGION_FOR_CONF="${AWS_REGION_FOR_CONF:-ap-northeast-1}"
VIRTUAL_CLUSTER_ID="${VIRTUAL_CLUSTER_ID:-localstack-placeholder-vc}"
ARN_ID="${ARN_ID:-000000000000}"

DIR="zip_tmp"
SOURCE_DIR="${DIR}/airflow/configs/"
S3_BUCKET_AIRFLOW="idac-airflow-bucket-${BUCKET_SUFFIX}"
TARGET_S3_PATH_AIRFLOW="s3://${S3_BUCKET_AIRFLOW}/Airflow_configs"

error_exit() {
    echo "ERROR: $1" 1>&2
    exit 1
}

aws s3 ls "s3://${S3_BUCKET_AIRFLOW}" > /dev/null 2>&1 \
    || error_exit "S3 bucket does not exist or is not accessible: ${S3_BUCKET_AIRFLOW}"

[ -d "$DIR" ] || error_exit "Temporary directory $DIR does not exist."
[ -d "$SOURCE_DIR" ] || error_exit "Source folder '$SOURCE_DIR' does not exist."

echo "Publishing Airflow_configs (env=${ENV}) from '$SOURCE_DIR'..."

find "$SOURCE_DIR" -type f | while read -r file; do
    filename=$(basename "$file")
    dest_path="${TARGET_S3_PATH_AIRFLOW}/${ENV}_${filename}"

    echo "Publishing: $filename -> $dest_path"
    sed -e "s/{env}/$ENV/g; s/{arn_id}/$ARN_ID/g; s/{bucket_suffix}/$BUCKET_SUFFIX/g; s/{env_specific_virtual_cluster_id}/$VIRTUAL_CLUSTER_ID/g; s|{schedule_timezone}|$SCHEDULE_TIMEZONE|g; s/{aws_region}/$AWS_REGION_FOR_CONF/g" "$file" \
        | aws s3 cp - "$dest_path" || error_exit "Failed to publish $filename to S3."
done

echo "Publish Airflow_configs to S3 completed."
