#!/usr/bin/env bash
# Drive emr-emulator through a full create-vc -> start-job -> poll cycle using
# spark/smoke/hello.py as the job. Verifies the docker exec -> spark-submit
# -> exit-code -> state-machine wiring.

set -euo pipefail

NETWORK="idac-localstack"
ENDPOINT="http://emr-emulator:4567"
REGION="ap-northeast-1"

awscli() {
    docker run --rm \
        --network "$NETWORK" \
        -e AWS_ACCESS_KEY_ID=test \
        -e AWS_SECRET_ACCESS_KEY=test \
        -e AWS_DEFAULT_REGION="$REGION" \
        amazon/aws-cli:latest \
        --endpoint-url "$ENDPOINT" \
        --no-cli-pager \
        "$@"
}

step() { echo; echo "=== $* ==="; }

step "1. list-virtual-clusters (expect empty list)"
awscli emr-containers list-virtual-clusters

step "2. create-virtual-cluster"
# containerProvider is required by the AWS CLI shape but ignored by the emulator.
VC_JSON=$(awscli emr-containers create-virtual-cluster \
    --name "smoke-vc" \
    --container-provider '{"id":"local-eks","type":"EKS","info":{"eksInfo":{"namespace":"default"}}}')
echo "$VC_JSON"
VC_ID=$(echo "$VC_JSON" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
echo "captured VC_ID=$VC_ID"

step "3. start-job-run (entryPoint = /workspace/spark/smoke/hello.py)"
# /workspace is bind-mounted into spark-master so no S3 upload is needed here —
# the s3:// entryPoint path is exercised by scripts/smoke/smoke-spark.sh.
JR_JSON=$(awscli emr-containers start-job-run \
    --virtual-cluster-id "$VC_ID" \
    --name "smoke-job-hello" \
    --execution-role-arn "arn:aws:iam::000000000000:role/emr-containers-jobexecutionrole" \
    --release-label "emr-6.15.0-latest" \
    --job-driver '{"sparkSubmitJobDriver":{"entryPoint":"/workspace/spark/smoke/hello.py","sparkSubmitParameters":"--conf spark.app.name=smoke-hello"}}')
echo "$JR_JSON"
JR_ID=$(echo "$JR_JSON" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
echo "captured JR_ID=$JR_ID"

step "4. poll describe-job-run until terminal (max 120s for JVM cold start)"
DEADLINE=$((SECONDS + 120))
STATE=""
while [ $SECONDS -lt $DEADLINE ]; do
    DESC=$(awscli emr-containers describe-job-run \
        --virtual-cluster-id "$VC_ID" \
        --id "$JR_ID")
    STATE=$(echo "$DESC" | python3 -c 'import json,sys; print(json.load(sys.stdin)["jobRun"]["state"])')
    printf '  state=%s\n' "$STATE"
    case "$STATE" in
        COMPLETED|FAILED|CANCELLED) break ;;
    esac
    sleep 3
done

if [ "$STATE" != "COMPLETED" ]; then
    echo "smoke-emr-emulator: expected COMPLETED, got '$STATE'" >&2
    echo "--- describe-job-run (last) ---" >&2
    awscli emr-containers describe-job-run --virtual-cluster-id "$VC_ID" --id "$JR_ID" >&2
    echo "--- spark-master logs (last 60 lines) ---" >&2
    docker logs --tail 60 idac-localstack-spark-master-1 >&2 || true
    exit 1
fi

step "5. list-job-runs (expect one terminal entry)"
awscli emr-containers list-job-runs --virtual-cluster-id "$VC_ID"

echo
echo "=== smoke-emr-emulator: all checks passed ==="
