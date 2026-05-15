#!/usr/bin/env bash
# Mirror selected EMR artifact prefixes from QA into LocalStack.
# Self-skips unless BOOTSTRAP=true.

set -euo pipefail

if [ "${BOOTSTRAP:-false}" != "true" ]; then
    echo "==> BOOTSTRAP != true, skipping QA -> local EMR artifact clone"
    exit 0
fi

if [ -z "${QA_AWS_ACCESS_KEY_ID:-}" ] || [ -z "${QA_AWS_SECRET_ACCESS_KEY:-}" ]; then
    echo "ERROR: BOOTSTRAP=true but QA_AWS_ACCESS_KEY_ID / QA_AWS_SECRET_ACCESS_KEY are missing." >&2
    echo "       Add them to idac-localstack/.env (see env.sample)." >&2
    exit 1
fi

QA_BUCKET="${QA_EMR_BUCKET:-idac-emr-bucket-qa}"
LOCAL_BUCKET="${LOCAL_EMR_BUCKET:-idac-emr-bucket-local}"
QA_REGION="${QA_AWS_REGION:-ap-northeast-1}"
LOCAL_ENDPOINT="${AWS_ENDPOINT_URL:-http://localstack:4566}"
TMPDIR="/tmp/qa-emr-artifacts-clone"

# These schema/config JSONs are consumed by idac-dp-config/ddl/glueDDL.py while
# registering silver tables. They are tiny, so keeping this targeted is cheap.
QA_EMR_PREFIXES_DEFAULT="qa/silver_transformation/artifacts/configs"
QA_EMR_PREFIXES="${QA_EMR_PREFIXES:-$QA_EMR_PREFIXES_DEFAULT}"

QA_TO_LOCAL_REWRITE="${QA_TO_LOCAL_REWRITE:-true}"

# Drop AWS_ENDPOINT_URL* so the QA read goes to real AWS, not LocalStack.
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

# In-place rewrite of cloned text configs from QA-tagged references to their
# local equivalents. Why this is needed:
#
# Pattern C of the publishing flow (clone-from-qa-s3 -> local-s3) bypasses the
# upstream deploy-time templating that would normally rewrite paths/db-names/env
# tags at the QA -> target-env handoff. Without this pass, the cloned configs
# point at idac-data-qa/qa_silver_db/etc. — which don't exist in LocalStack, so
# silver.jar (or any consumer) fails on the first read. Doing the rewrite here,
# right after download and before upload, makes the rewrite happen exactly once
# per clone and never round-trips through S3 untranslated.
#
# The substitution set is intentionally small + literal (not regex) so it stays
# auditable and the rewrite stays idempotent: re-running on already-translated
# files is a no-op because the LHS strings no longer appear.
#
# Scope is limited to .conf / .json / .yaml / .yml — never touch parquet, jars,
# or anything else that might have these byte sequences for unrelated reasons.
qa_to_local_rewrite() {
    local root="$1"
    local count=0

    # Each `-e` is one substitution. Add new pairs here as we discover them.
    local sed_args=(
        -e 's|idac-data-qa/qa/|idac-data-local/local/|g'
        -e 's|idac-emr-bucket-qa/qa/|idac-emr-bucket-local/local/|g'
        -e 's|qa_browser_extension|local_browser_extension|g'
        -e 's|qa_silver_db|local_silver_db|g'
        -e 's|qa_common_db|local_common_db|g'
        -e 's|qa_bronze_db|local_bronze_db|g'
        -e 's|qa_gold_db|local_gold_db|g'
        # HOCON: env = "qa"  /  env="qa"
        -e 's|env = "qa"|env = "local"|g'
        -e 's|env="qa"|env="local"|g'
        # JSON: "env": "qa"  /  "env":"qa"
        -e 's|"env": "qa"|"env": "local"|g'
        -e 's|"env":"qa"|"env":"local"|g'
        -e 's|s3://|s3a://|g'
    )

    while IFS= read -r -d '' f; do
        local before after
        before="$(md5sum "$f" | awk '{print $1}')"
        sed -i "${sed_args[@]}" "$f"
        after="$(md5sum "$f" | awk '{print $1}')"
        if [ "$before" != "$after" ]; then
            count=$((count + 1))
            echo "    rewrote: ${f#${root}/}"
        fi
    done < <(find "$root" -type f \( -name '*.conf' -o -name '*.json' -o -name '*.yaml' -o -name '*.yml' \) -print0)

    if [ "$count" -gt 0 ]; then
        echo "==> qa->local rewrite: ${count} file(s) updated under ${root}"
    else
        echo "==> qa->local rewrite: no files needed translation under ${root}"
    fi
}

echo "==> QA -> Local EMR artifact clone"
echo "    source   : s3://${QA_BUCKET}/   (region ${QA_REGION})"
echo "    target   : s3://${LOCAL_BUCKET}/ (endpoint ${LOCAL_ENDPOINT})"
echo "    prefixes : ${QA_EMR_PREFIXES} (uploaded under local/* in target)"

if ! local_aws s3 ls "s3://${LOCAL_BUCKET}" >/dev/null 2>&1; then
    echo "==> Creating s3://${LOCAL_BUCKET}/ ..."
    local_aws s3 mb "s3://${LOCAL_BUCKET}"
fi

rm -rf "$TMPDIR" && mkdir -p "$TMPDIR"

for prefix in $QA_EMR_PREFIXES; do
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

    # Translate QA-tagged paths/db-names/env tags to local before upload, so
    # the configs land in LocalStack already pointing at local resources.
    if [ "$QA_TO_LOCAL_REWRITE" = "true" ]; then
        echo "==> [${prefix}] applying qa->local rewrite"
        qa_to_local_rewrite "$stage"
    else
        echo "==> [${prefix}] QA_TO_LOCAL_REWRITE=false — skipping rewrite (configs will point at QA)"
    fi

    echo "==> [${prefix}] refreshing ${dst}"
    local_aws s3 rm "$dst" --recursive --only-show-errors || true
    local_aws s3 sync "$stage/" "$dst" --only-show-errors

    rm -rf "$stage"
    echo "==> [${prefix}] done (-> ${dst_prefix}/)"
done

rm -rf "$TMPDIR"

OBJECTS=$(local_aws s3 ls "s3://${LOCAL_BUCKET}/local/silver_transformation/artifacts/configs/" --recursive --summarize 2>/dev/null | awk '/Total Objects/ {print $3}')
SIZE=$(local_aws s3 ls "s3://${LOCAL_BUCKET}/local/silver_transformation/artifacts/configs/" --recursive --summarize 2>/dev/null | awk '/Total Size/ {print $3}')
echo "==> EMR artifact clone complete: objects=${OBJECTS:-?} bytes=${SIZE:-?}"
