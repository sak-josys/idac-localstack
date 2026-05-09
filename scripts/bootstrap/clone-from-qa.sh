#!/usr/bin/env bash
# Mirror s3://idac-data-qa/qa/* into s3://idac-data-local/local/* on LocalStack.
# Self-skips unless BOOTSTRAP=true.

set -euo pipefail

if [ "${BOOTSTRAP:-false}" != "true" ]; then
    echo "==> BOOTSTRAP != true, skipping QA -> local clone (data persisted from previous run)"
    exit 0
fi

if [ -z "${QA_AWS_ACCESS_KEY_ID:-}" ] || [ -z "${QA_AWS_SECRET_ACCESS_KEY:-}" ]; then
    echo "ERROR: BOOTSTRAP=true but QA_AWS_ACCESS_KEY_ID / QA_AWS_SECRET_ACCESS_KEY are missing." >&2
    echo "       Add them to idac-localstack/.env (see env.sample)." >&2
    exit 1
fi

QA_BUCKET="${QA_DATA_BUCKET:-idac-data-qa}"
LOCAL_BUCKET="${LOCAL_DATA_BUCKET:-idac-data-local}"
QA_REGION="${QA_AWS_REGION:-ap-northeast-1}"
LOCAL_ENDPOINT="${AWS_ENDPOINT_URL:-http://localstack:4566}"
TMPDIR="/tmp/qa-clone"

# Order: gold first (smallest, fastest signal), then silver/bronze/common.
QA_PREFIXES_DEFAULT="qa/gold qa/silver qa/bronze qa/common"
QA_PREFIXES="${QA_PREFIXES:-$QA_PREFIXES_DEFAULT}"

# Drop AWS_ENDPOINT_URL* — otherwise AWS CLI v2 routes the QA call to LocalStack.
qa_aws() {
    env -u AWS_ENDPOINT_URL -u AWS_ENDPOINT_URL_S3 \
    AWS_ACCESS_KEY_ID="$QA_AWS_ACCESS_KEY_ID" \
    AWS_SECRET_ACCESS_KEY="$QA_AWS_SECRET_ACCESS_KEY" \
    AWS_SESSION_TOKEN="${QA_AWS_SESSION_TOKEN:-}" \
    AWS_DEFAULT_REGION="$QA_REGION" \
    aws --no-cli-pager "$@"
}

local_aws() {
    AWS_ACCESS_KEY_ID=test \
    AWS_SECRET_ACCESS_KEY=test \
    AWS_DEFAULT_REGION=ap-northeast-1 \
    aws --endpoint-url "$LOCAL_ENDPOINT" --no-cli-pager "$@"
}

echo "==> QA -> Local data clone"
echo "    source   : s3://${QA_BUCKET}/   (region ${QA_REGION})"
echo "    target   : s3://${LOCAL_BUCKET}/ (endpoint ${LOCAL_ENDPOINT})"
echo "    prefixes : ${QA_PREFIXES} (uploaded under local/* in target)"

# Wipe so each enabled run produces a clean snapshot.
if local_aws s3 ls "s3://${LOCAL_BUCKET}" >/dev/null 2>&1; then
    echo "==> Emptying existing s3://${LOCAL_BUCKET}/ ..."
    local_aws s3 rm "s3://${LOCAL_BUCKET}" --recursive --only-show-errors || true
else
    echo "==> Creating s3://${LOCAL_BUCKET}/ ..."
    local_aws s3 mb "s3://${LOCAL_BUCKET}"
fi

# Stage to /tmp then push — `aws s3 sync` can't drive two endpoints in one call.
# Remap qa/* -> local/* on the way out so dp-config's {env}=local table_path
# (s3://idac-data-local/local/<layer>/...) matches the cloned data layout.
rm -rf "$TMPDIR" && mkdir -p "$TMPDIR"

for prefix in $QA_PREFIXES; do
    case "$prefix" in
        qa/*) dst_prefix="local/${prefix#qa/}" ;;
        qa)   dst_prefix="local" ;;
        *)    dst_prefix="$prefix" ;;
    esac

    src="s3://${QA_BUCKET}/${prefix}/"
    dst="s3://${LOCAL_BUCKET}/${dst_prefix}/"
    stage="${TMPDIR}/${prefix}"

    echo
    echo "==> [${prefix}] downloading ${src} -> ${stage}/"
    mkdir -p "$stage"
    qa_aws s3 sync "$src" "$stage/" --only-show-errors

    echo "==> [${prefix}] uploading ${stage}/ -> ${dst}"
    local_aws s3 sync "$stage/" "$dst" --only-show-errors

    rm -rf "$stage"
    echo "==> [${prefix}] done (-> ${dst_prefix}/)"
done

rm -rf "$TMPDIR"

OBJECTS=$(local_aws s3 ls "s3://${LOCAL_BUCKET}/" --recursive --summarize 2>/dev/null | awk '/Total Objects/ {print $3}')
SIZE=$(local_aws s3 ls "s3://${LOCAL_BUCKET}/" --recursive --summarize 2>/dev/null | awk '/Total Size/ {print $3}')
echo "==> Clone complete: objects=${OBJECTS:-?} bytes=${SIZE:-?}"
