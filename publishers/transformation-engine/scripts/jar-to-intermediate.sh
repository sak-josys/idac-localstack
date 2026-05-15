#!/usr/bin/env bash
# Stage 1 of the local 2-stage publish pipeline:
#   build  ->  intermediate (THIS SCRIPT)  ->  env-specific S3
#
# Wraps a transformation-engine fat JAR into the same zip shape upstream's CI
# emits (so the env-specific stage can be a near-verbatim fork of upstream's
# transformation_code_artifact_to_env_specific_S3.sh) and uploads it to a
# stable intermediate prefix in LocalStack S3.
#
# Why an intermediate stage at all (without CodeArtifact)?
# It mirrors the QA flow's debug surface — devs can `aws s3 ls intermediate/`
# to confirm "did my last publish even reach S3?" — and isolates the build
# output from the env-specific layout the EMR job actually reads from.
#
# Zip layout (matches upstream silver.yaml: `zip -r silver-0.1.zip ./silver/target/scala-2.12/silver.jar`):
#   <module>-0.1.zip
#   └── <module>/target/scala-2.12/<module>.jar
#
# Target path (stable; overwritten on each publish — same semantics as upstream's tmp/):
#   s3://${LOCAL_EMR_BUCKET}/local/intermediate/transformation-engine/<module>/<module>-0.1.zip
#
# Output:
#   stdout -> exactly one line: the s3:// URI of the published zip
#   stderr -> human-readable progress logs

set -euo pipefail

usage() {
    cat >&2 <<EOF
usage: jar-to-intermediate.sh --module <name> --jar <path> [--version <v>]

  --module    silver | gold | migration | data_compaction | data_quality | commons
  --jar       absolute path to the fat JAR (typically the output of build-or-detect-jar.sh)
  --version   informational version tag stamped on the S3 object as metadata
              (default: <git-sha7>-local-<unix-ts>, or local-<unix-ts> if not in a git repo)

env vars (all optional):
  LOCAL_EMR_BUCKET    default: idac-emr-bucket-local
  AWS_ENDPOINT_URL    default: http://localstack:4566
  AWS_DEFAULT_REGION  default: ap-northeast-1

stdout: s3:// URI of the staged zip (one line)
EOF
}

MODULE=""
JAR=""
VERSION=""

while [ $# -gt 0 ]; do
    case "$1" in
        --module)  MODULE="$2"; shift 2 ;;
        --jar)     JAR="$2"; shift 2 ;;
        --version) VERSION="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "ERROR: unknown arg '$1'." >&2; usage; exit 2 ;;
    esac
done

if [ -z "$MODULE" ] || [ -z "$JAR" ]; then
    usage
    exit 2
fi

case "$MODULE" in
    silver|gold|migration|data_compaction|data_quality|commons) ;;
    *) echo "ERROR: unknown module '$MODULE'." >&2; exit 2 ;;
esac

if [ ! -f "$JAR" ]; then
    echo "ERROR: jar not found at ${JAR}" >&2
    exit 1
fi

LOCAL_BUCKET="${LOCAL_EMR_BUCKET:-idac-emr-bucket-local}"
LOCAL_ENDPOINT="${AWS_ENDPOINT_URL:-http://localstack:4566}"
REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"

ASSET_NAME="${MODULE}-0.1.zip"
S3_KEY="local/intermediate/transformation-engine/${MODULE}/${ASSET_NAME}"
S3_URI="s3://${LOCAL_BUCKET}/${S3_KEY}"

log() { echo "==> $*" >&2; }

local_aws() {
    AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}" \
    AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}" \
    AWS_DEFAULT_REGION="$REGION" \
    aws --endpoint-url "$LOCAL_ENDPOINT" --no-cli-pager "$@"
}

if [ -z "$VERSION" ]; then
    SHA7=""
    if command -v git >/dev/null 2>&1; then
        ENGINE_DIR="$(cd "$(dirname "$JAR")/../../.." && pwd)"
        SHA7="$(git -C "$ENGINE_DIR" rev-parse --short=7 HEAD 2>/dev/null || true)"
    fi
    TS="$(date +%s)"
    if [ -n "$SHA7" ]; then
        VERSION="${SHA7}-local-${TS}"
    else
        VERSION="local-${TS}"
    fi
fi

log "module      : ${MODULE}"
log "jar         : ${JAR}"
log "asset name  : ${ASSET_NAME}"
log "version tag : ${VERSION}"
log "target      : ${S3_URI}"

if ! local_aws s3 ls "s3://${LOCAL_BUCKET}" >/dev/null 2>&1; then
    log "creating bucket s3://${LOCAL_BUCKET}/"
    local_aws s3 mb "s3://${LOCAL_BUCKET}" >/dev/null
fi

# Reproduce upstream's zip layout exactly: the JAR sits at
# <module>/target/scala-2.12/<module>.jar inside the zip. Achieved by copying
# the JAR into a mirrored tree under a tmp staging dir and zipping from there,
# so absolute paths never leak into the archive.
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

mkdir -p "${STAGE}/${MODULE}/target/scala-2.12"
cp "$JAR" "${STAGE}/${MODULE}/target/scala-2.12/${MODULE}.jar"

ZIP_PATH="${STAGE}/${ASSET_NAME}"
( cd "$STAGE" && zip -rq "$ASSET_NAME" "./${MODULE}/target/scala-2.12/${MODULE}.jar" )
log "zip ready   : ${ZIP_PATH}  ($(du -h "$ZIP_PATH" | awk '{print $1}'))"

# --metadata stamps version + source jar mtime on the S3 object so a curious dev
# can `aws s3api head-object` to see exactly what's staged without unzipping.
local_aws s3 cp "$ZIP_PATH" "$S3_URI" \
    --metadata "version=${VERSION},source-jar=${JAR},published-at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --only-show-errors

log "published   : ${S3_URI}"
echo "$S3_URI"
