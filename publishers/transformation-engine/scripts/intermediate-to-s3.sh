#!/usr/bin/env bash
# Stage 2 of the local 2-stage publish pipeline:
#   build  ->  intermediate  ->  env-specific S3 (THIS SCRIPT)
#
# Forked from upstream's transformation_code_artifact_to_env_specific_S3.sh
# (silver, gold, migration, data_compaction variants) and reduced to the
# local-only, single-account case. The final S3 layout is preserved 1:1 because
# the spark-submit invocations baked into the DAGs read from these exact paths.
#
# Per-module final layout (must match upstream so DAGs work unchanged):
#   silver           -> s3://${LOCAL_EMR_BUCKET}/local/silver_transformation/artifacts/jars/silver.jar
#                       s3://${LOCAL_EMR_BUCKET}/local/silver_transformation/artifacts/jars/dependencies/delta-*.jar
#   gold             -> s3://${LOCAL_EMR_BUCKET}/local/gold_transformation/artifacts/jars/gold.jar (+ dependencies/)
#   migration        -> s3://${LOCAL_EMR_BUCKET}/local/migration_transformation/artifacts/jars/migration.jar (+ dependencies/)
#   data_compaction  -> s3://${LOCAL_EMR_BUCKET}/local/data_compaction/artifacts/jars/data_compaction.jar (+ dependencies/)
#                       (no `_transformation` suffix — this is intentional, matches upstream)
#
# What this script does NOT handle (intentional scope split):
#   - default.conf templating for migration / data_compaction. Configs come from
#     scripts/bootstrap/clone-emr-artifacts-from-qa.sh, mirroring the upstream
#     EMR config bucket. Mixing config-publish into the JAR pipeline would
#     blur responsibilities and risk drift.
#   - cross-account handoff. Local has one account; the cross-account OIDC
#     hops in upstream's CI are a no-op here.

set -euo pipefail

usage() {
    cat >&2 <<EOF
usage: intermediate-to-s3.sh --module <name>

  --module    silver | gold | migration | data_compaction

env vars (all optional):
  LOCAL_EMR_BUCKET    default: idac-emr-bucket-local
  AWS_ENDPOINT_URL    default: http://localstack:4566
  AWS_DEFAULT_REGION  default: ap-northeast-1
  MAVEN_CACHE_DIR     local cache for delta-*.jar fetches (default: \${TMPDIR:-/tmp}/idac-maven-cache)
  KEEP_STAGING        if "true", do NOT delete the transient CodeArtifact/ S3 prefix after promotion
                      (useful for debugging — default is to clean up like upstream does)
EOF
}

MODULE=""
while [ $# -gt 0 ]; do
    case "$1" in
        --module)  MODULE="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "ERROR: unknown arg '$1'." >&2; usage; exit 2 ;;
    esac
done

if [ -z "$MODULE" ]; then
    usage
    exit 2
fi

# Per-module path schema. Mirrors upstream silver/gold/migration/data_compaction
# scripts EXACTLY — values were derived by reading each script's BASE_PATH +
# S3_UPLOAD_PATH literals. Tweaking these will desync from the DAGs.
case "$MODULE" in
    silver)
        BASE_PATH="silver_transformation/artifacts"
        STAGING_PREFIX="CodeArtifact/silver_transformation/silver"
        ;;
    gold)
        BASE_PATH="gold_transformation/artifacts"
        STAGING_PREFIX="CodeArtifact/gold"
        ;;
    migration)
        BASE_PATH="migration_transformation/artifacts"
        STAGING_PREFIX="CodeArtifact/migration_transformation/migration"
        ;;
    data_compaction)
        BASE_PATH="data_compaction/artifacts"
        STAGING_PREFIX="CodeArtifact/data_compaction"
        ;;
    *)
        echo "ERROR: module '$MODULE' is not publishable to env-specific S3." >&2
        echo "       only silver | gold | migration | data_compaction have target layouts." >&2
        exit 2
        ;;
esac

LOCAL_BUCKET="${LOCAL_EMR_BUCKET:-idac-emr-bucket-local}"
LOCAL_ENDPOINT="${AWS_ENDPOINT_URL:-http://localstack:4566}"
REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"
MAVEN_CACHE="${MAVEN_CACHE_DIR:-${TMPDIR:-/tmp}/idac-maven-cache}"
KEEP_STAGING="${KEEP_STAGING:-false}"

ASSET_NAME="${MODULE}-0.1.zip"
INTERMEDIATE_KEY="local/intermediate/transformation-engine/${MODULE}/${ASSET_NAME}"
INTERMEDIATE_URI="s3://${LOCAL_BUCKET}/${INTERMEDIATE_KEY}"

# Final layout — what spark-submit reads from.
S3_PREFIX="s3://${LOCAL_BUCKET}/local"
JARS_PATH="${S3_PREFIX}/${BASE_PATH}/jars"
DEPENDENCIES_PATH="${JARS_PATH}/dependencies/"
FINAL_JAR_URI="${JARS_PATH}/${MODULE}.jar"

# Transient staging — exists only between sync and copy steps, mirrors upstream's
# `s3://.../CodeArtifact/...` round-trip and gets cleaned up at the end.
STAGING_S3_PREFIX="${S3_PREFIX}/${STAGING_PREFIX}"

log() { echo "==> $*" >&2; }

local_aws() {
    AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}" \
    AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}" \
    AWS_DEFAULT_REGION="$REGION" \
    aws --endpoint-url "$LOCAL_ENDPOINT" --no-cli-pager "$@"
}

# Prefer curl for the maven fetches — it's universal across mac/linux base images.
# The publish-transformation container in step 7 will install curl explicitly.
fetch_url() {
    local url="$1" out="$2"
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$url" -o "$out"
    elif command -v wget >/dev/null 2>&1; then
        wget -q "$url" -O "$out"
    else
        echo "ERROR: neither curl nor wget available — cannot fetch ${url}" >&2
        return 1
    fi
}

log "module          : ${MODULE}"
log "intermediate    : ${INTERMEDIATE_URI}"
log "final jar       : ${FINAL_JAR_URI}"
log "dependencies/   : ${DEPENDENCIES_PATH}"

# Precondition — the intermediate must exist (otherwise the prior stage failed silently).
if ! local_aws s3api head-object \
        --bucket "${LOCAL_BUCKET}" \
        --key "${INTERMEDIATE_KEY}" >/dev/null 2>&1; then
    echo "ERROR: intermediate not found at ${INTERMEDIATE_URI}" >&2
    echo "       run jar-to-intermediate.sh first." >&2
    exit 1
fi

# 1. Download the zip from the intermediate prefix into a fresh tmp staging dir.
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

log "[1/5] fetch intermediate -> ${TMP}/${ASSET_NAME}"
local_aws s3 cp "$INTERMEDIATE_URI" "${TMP}/${ASSET_NAME}" --only-show-errors

# 2. Unzip in place. Layout inside the zip is <module>/target/scala-2.12/<module>.jar
#    so unzip lands the jar at ${TMP}/${MODULE}/target/scala-2.12/${MODULE}.jar.
log "[2/5] unzip -> ${TMP}/${MODULE}/target/scala-2.12/${MODULE}.jar"
( cd "$TMP" && unzip -oq "$ASSET_NAME" )

UNZIPPED_DIR="${TMP}/${MODULE}/target/scala-2.12"
if [ ! -f "${UNZIPPED_DIR}/${MODULE}.jar" ]; then
    echo "ERROR: expected ${UNZIPPED_DIR}/${MODULE}.jar after unzip — got:" >&2
    ls -laR "$TMP" >&2
    exit 1
fi

# 3. Mirror upstream: round-trip the jar through the transient `CodeArtifact/...`
#    staging prefix, then copy to the final jars/ location. The two-hop is
#    cosmetic locally (single account), kept so the visual flow matches QA logs.
log "[3/5] sync to transient staging ${STAGING_S3_PREFIX}/"
local_aws s3 sync "${UNZIPPED_DIR}/" "${STAGING_S3_PREFIX}/" --only-show-errors

# Wipe any stale jars/ contents from a prior publish so we don't accumulate
# orphaned dep files (e.g. an older delta-core_2.12-2.3.0.jar lingering forever).
log "[4/5] reset ${JARS_PATH}/ and promote jar + delta dependencies"
local_aws s3 rm "${JARS_PATH}/" --recursive --only-show-errors >/dev/null 2>&1 || true
local_aws s3 cp "${STAGING_S3_PREFIX}/${MODULE}.jar" "${FINAL_JAR_URI}" --only-show-errors

# Delta JARs from Maven Central — exact versions pinned to upstream's build.sbt
# (delta-core 2.4.0 with delta-storage 2.4.0 — required by Spark 3.4 + Scala 2.12).
mkdir -p "$MAVEN_CACHE"
DELTA_STORAGE_URL="https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar"
DELTA_CORE_URL="https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar"

for url in "$DELTA_STORAGE_URL" "$DELTA_CORE_URL"; do
    fname="$(basename "$url")"
    cache_path="${MAVEN_CACHE}/${fname}"
    if [ -f "$cache_path" ]; then
        log "  cache hit  : ${fname}"
    else
        log "  fetching   : ${fname} (cache miss)"
        fetch_url "$url" "$cache_path"
    fi
    local_aws s3 cp "$cache_path" "${DEPENDENCIES_PATH}${fname}" --only-show-errors
done

# 5. Cleanup the transient staging prefix (matches upstream behaviour). Devs can
#    set KEEP_STAGING=true when poking at the round-trip during debugging.
log "[5/5] cleanup transient staging"
if [ "$KEEP_STAGING" = "true" ]; then
    log "  KEEP_STAGING=true — leaving ${STAGING_S3_PREFIX}/ for inspection"
else
    local_aws s3 rm "${STAGING_S3_PREFIX}/" --recursive --only-show-errors >/dev/null 2>&1 || true
fi

log "publish complete:"
log "  jar  : ${FINAL_JAR_URI}"
log "  deps : ${DEPENDENCIES_PATH}"
echo "$FINAL_JAR_URI"
