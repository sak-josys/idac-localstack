#!/bin/sh
# Idempotently provisions the MWAA environment in LocalStack and waits for it to be AVAILABLE.
# Reuses the airflow bucket created by publish-airflow; reads DAGs from "Airflow Orchestration/".
#
# Re-runs are safe: every step does "exists ? reuse : create".

set -eu

# -------- Config ------------------------------------------------------------
AWS_REGION="${AWS_REGION:-ap-northeast-1}"
ENDPOINT="${AWS_ENDPOINT_URL:-http://localstack:4566}"

S3_BUCKET="idac-airflow-bucket-local"
DAG_PATH="/Airflow Orchestration"
ENV_NAME="idac-local-airflow"
ROLE_NAME="idac-local-mwaa-execution"
VPC_NAME="idac-local-mwaa-vpc"
SG_NAME="idac-local-mwaa-sg"
AZ_1="ap-northeast-1a"
AZ_2="ap-northeast-1c"
AIRFLOW_VERSION="${AIRFLOW_VERSION:-2.10.3}"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="$AWS_REGION"

aws_() { aws --endpoint-url="$ENDPOINT" "$@"; }

# -------- 1. Wait for LocalStack S3 to answer -------------------------------
echo "==> Waiting for LocalStack at $ENDPOINT ..."
i=0
until aws_ s3 ls "s3://${S3_BUCKET}" >/dev/null 2>&1; do
    i=$((i + 1))
    if [ "$i" -gt 60 ]; then
        echo "ERROR: ${S3_BUCKET} not reachable on ${ENDPOINT}; is publish-airflow finished?" >&2
        exit 1
    fi
    sleep 1
done
echo "    LocalStack S3 reachable, bucket present."

# -------- 2. VPC ------------------------------------------------------------
VPC_ID=$(aws_ ec2 describe-vpcs --filters "Name=tag:Name,Values=${VPC_NAME}" \
    --query 'Vpcs[0].VpcId' --output text 2>/dev/null || true)
if [ -z "$VPC_ID" ] || [ "$VPC_ID" = "None" ]; then
    VPC_ID=$(aws_ ec2 create-vpc --cidr-block 10.192.0.0/16 \
        --query 'Vpc.VpcId' --output text)
    aws_ ec2 create-tags --resources "$VPC_ID" \
        --tags "Key=Name,Value=${VPC_NAME}" >/dev/null
    echo "==> VPC created: $VPC_ID"
else
    echo "==> VPC reused:  $VPC_ID"
fi

# -------- 3. Subnets (two AZs that LocalStack accepts) ----------------------
ensure_subnet() {
    _name="$1"
    _az="$2"
    _cidr="$3"
    _id=$(aws_ ec2 describe-subnets \
        --filters "Name=tag:Name,Values=${_name}" "Name=vpc-id,Values=${VPC_ID}" \
        --query 'Subnets[0].SubnetId' --output text 2>/dev/null || true)
    if [ -z "$_id" ] || [ "$_id" = "None" ]; then
        _id=$(aws_ ec2 create-subnet --vpc-id "$VPC_ID" \
            --cidr-block "$_cidr" --availability-zone "$_az" \
            --query 'Subnet.SubnetId' --output text)
        aws_ ec2 create-tags --resources "$_id" \
            --tags "Key=Name,Value=${_name}" >/dev/null
        echo "==> Subnet created: $_id ($_az)" >&2
    else
        echo "==> Subnet reused:  $_id ($_az)" >&2
    fi
    echo "$_id"
}
SUBNET_ID_1=$(ensure_subnet "${VPC_NAME}-subnet-1a" "$AZ_1" "10.192.0.0/24" | tail -n1)
SUBNET_ID_2=$(ensure_subnet "${VPC_NAME}-subnet-1c" "$AZ_2" "10.192.1.0/24" | tail -n1)

# -------- 4. Security group -------------------------------------------------
SG_ID=$(aws_ ec2 describe-security-groups \
    --filters "Name=group-name,Values=${SG_NAME}" "Name=vpc-id,Values=${VPC_ID}" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)
if [ -z "$SG_ID" ] || [ "$SG_ID" = "None" ]; then
    SG_ID=$(aws_ ec2 create-security-group --group-name "$SG_NAME" \
        --description "MWAA local" --vpc-id "$VPC_ID" \
        --query 'GroupId' --output text)
    echo "==> SG created: $SG_ID"
else
    echo "==> SG reused:  $SG_ID"
fi

# -------- 5. IAM execution role + inline S3/logs policy ---------------------
TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":["airflow.amazonaws.com","airflow-env.amazonaws.com"]},"Action":"sts:AssumeRole"}]}'
POLICY='{"Version":"2012-10-17","Statement":[
  {"Effect":"Allow","Action":["s3:GetObject","s3:GetBucket*","s3:List*","s3:PutObject"],
   "Resource":["arn:aws:s3:::'"${S3_BUCKET}"'","arn:aws:s3:::'"${S3_BUCKET}"'/*"]},
  {"Effect":"Allow","Action":["logs:*","cloudwatch:*","kms:*","sqs:*"],"Resource":"*"}
]}'

if ! aws_ iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
    aws_ iam create-role --role-name "$ROLE_NAME" \
        --assume-role-policy-document "$TRUST" >/dev/null
    aws_ iam put-role-policy --role-name "$ROLE_NAME" \
        --policy-name mwaa-inline --policy-document "$POLICY"
    echo "==> IAM role created: $ROLE_NAME"
else
    echo "==> IAM role reused:  $ROLE_NAME"
fi
ROLE_ARN=$(aws_ iam get-role --role-name "$ROLE_NAME" \
    --query 'Role.Arn' --output text)

# -------- 6. MWAA environment ----------------------------------------------
create_env() {
    _net=$(printf '{"SubnetIds":["%s","%s"],"SecurityGroupIds":["%s"]}' \
        "$SUBNET_ID_1" "$SUBNET_ID_2" "$SG_ID")
    echo "==> Creating MWAA environment '$ENV_NAME' (3-5 min) ..."
    aws_ mwaa create-environment \
        --name "$ENV_NAME" \
        --source-bucket-arn "arn:aws:s3:::${S3_BUCKET}" \
        --dag-s3-path "$DAG_PATH" \
        --execution-role-arn "$ROLE_ARN" \
        --network-configuration "$_net" \
        --environment-class mw1.small \
        --max-workers 2 --min-workers 1 \
        --airflow-version "$AIRFLOW_VERSION" \
        --requirements-s3-path "Airflow Orchestration/requirements.txt" \
        --webserver-access-mode PUBLIC_ONLY \
        >/dev/null
}

delete_env_and_wait() {
    echo "==> Deleting stale MWAA environment '$ENV_NAME' ..."
    aws_ mwaa delete-environment --name "$ENV_NAME" >/dev/null 2>&1 || true
    j=0
    while aws_ mwaa get-environment --name "$ENV_NAME" >/dev/null 2>&1; do
        j=$((j + 1))
        if [ "$j" -gt 60 ]; then
            echo "ERROR: delete-environment did not complete in 120s" >&2
            exit 1
        fi
        sleep 2
    done
}

# Probe the MWAA webserver via the localstack network alias (the URL LocalStack returns
# uses localhost.localstack.cloud, which resolves to 127.0.0.1 — wrong inside this container).
probe_mwaa_alive() {
    _url=$(aws_ mwaa get-environment --name "$ENV_NAME" \
        --query 'Environment.WebserverUrl' --output text 2>/dev/null || true)
    if [ -z "$_url" ] || [ "$_url" = "None" ]; then
        return 1
    fi
    _probe=$(echo "$_url" | sed 's|localhost\.localstack\.cloud|localstack|')
    curl -sf -o /dev/null --max-time 3 "${_probe}/login" 2>/dev/null
}

EXISTS=$(aws_ mwaa get-environment --name "$ENV_NAME" \
    --query 'Environment.Name' --output text 2>/dev/null || true)

if [ "$EXISTS" = "$ENV_NAME" ]; then
    # Persisted state can lie (Status=AVAILABLE but sibling container gone after a restart).
    # Trust the actual webserver, not the persisted status.
    if probe_mwaa_alive; then
        echo "==> MWAA environment alive: $ENV_NAME"
    else
        echo "==> MWAA environment '$ENV_NAME' exists but proxy is unreachable (sibling container missing). Recreating..."
        delete_env_and_wait
        create_env
    fi
else
    create_env
fi

# -------- 7. Wait for AVAILABLE --------------------------------------------
echo "==> Waiting for $ENV_NAME to become AVAILABLE ..."
i=0
while :; do
    STATUS=$(aws_ mwaa get-environment --name "$ENV_NAME" \
        --query 'Environment.Status' --output text 2>/dev/null || echo "UNKNOWN")
    case "$STATUS" in
        AVAILABLE) break ;;
        CREATE_FAILED | UPDATE_FAILED | DELETE_FAILED | UNAVAILABLE)
            echo "ERROR: MWAA entered status $STATUS" >&2
            aws_ mwaa get-environment --name "$ENV_NAME" >&2 || true
            exit 1
            ;;
    esac
    i=$((i + 1))
    if [ "$i" -gt 600 ]; then
        echo "ERROR: timed out waiting for $ENV_NAME (last status: $STATUS)" >&2
        exit 1
    fi
    sleep 2
done

URL=$(aws_ mwaa get-environment --name "$ENV_NAME" \
    --query 'Environment.WebserverUrl' --output text)

echo ""
echo "============================================================"
echo "MWAA environment AVAILABLE"
echo "  Name : $ENV_NAME"
echo "  URL  : $URL"
echo "  Login: localstack / localstack"
echo "============================================================"
