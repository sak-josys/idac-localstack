#!/usr/bin/env bash
# One-shot QA -> LocalStack data sync that's narrowed to exactly what
# local_sidsum_dag needs to run end-to-end. Pulls bronze sources for source_data
# silver, all silver/CDC tables the gold SQL reads (whole tables, see below),
# gold seed, the common audit log, and EMR-side configs + dependency jars.
#
# Why this script exists alongside clone-from-qa.sh / clone-emr-artifacts-from-qa.sh:
#   - The broader BOOTSTRAP=true clone defaults to "every layer" which is too
#     coarse and slow for one-DAG iteration.
#   - The EMR-artifact clone's qa->local content rewrite covers DB names but
#     misses qa_vin_topic / qa_master_data_topic, so source_data silver's conf
#     still points at qa_* paths after a clone. This script applies the full
#     rewrite set so the cloned configs land already pointing at local objects.
#
# Why pull whole tables for silver/CDC/gold (no tenant slice):
#   QA's silver auditlog and CDC tables are tiny (<25 MB each, all of CDC <2 MB).
#   They have inconsistent per-tenant coverage — e.g. cdc/employee_identities
#   only has organization_id=3 and =8411 in QA, so any tenant-slice strategy
#   silently drops gold's join input on most tenants. Pulling the whole table
#   is cheap, eliminates the tenant-intersection problem, and lets gold join
#   across whatever tenants happen to overlap. Bronze browser_extension is also
#   pulled whole (<1 MB). Bronze master_data_topic / vin_topic have no tenant
#   partition and are pulled whole (vin ~31 MB, master_data ~316 MB — the only
#   genuinely big prefix; idempotent skip means it's only paid once).
#
# Idempotent: each prefix is skipped if the local target already has objects.
# Set FORCE_REFRESH=true to wipe and re-pull everything EXCEPT the prefixes
# in BIG_PREFIXES (master_data_topic etc.) — those are never wiped by
# FORCE_REFRESH because they take 15+ min to re-pull and rarely change in QA.
# To force-refresh the big ones, set FORCE_REFRESH_BIG=true as well.
#
# Required env (typically loaded from idac-localstack/.env):
#   QA_AWS_ACCESS_KEY_ID
#   QA_AWS_SECRET_ACCESS_KEY
#   QA_AWS_SESSION_TOKEN     (optional, when using STS-issued temp creds)
#
# Optional env:
#   QA_AWS_REGION            default ap-northeast-1
#   AWS_ENDPOINT_URL         default http://localstack:4566 (override with
#                            http://localhost:4566 if you run on the host)
#   QA_DATA_BUCKET           default idac-data-qa
#   QA_EMR_BUCKET            default idac-emr-bucket-qa
#   LOCAL_DATA_BUCKET        default idac-data-local
#   LOCAL_EMR_BUCKET         default idac-emr-bucket-local
#   FORCE_REFRESH            true|false (default false)
#   FORCE_REFRESH_BIG        true|false (default false) — also wipe big prefixes
#   QA_TO_LOCAL_REWRITE      true|false (default true)

set -euo pipefail

QA_AWS_REGION="${QA_AWS_REGION:-ap-northeast-1}"
LOCAL_ENDPOINT="${AWS_ENDPOINT_URL:-http://localstack:4566}"
QA_DATA_BUCKET="${QA_DATA_BUCKET:-idac-data-qa}"
QA_EMR_BUCKET="${QA_EMR_BUCKET:-idac-emr-bucket-qa}"
LOCAL_DATA_BUCKET="${LOCAL_DATA_BUCKET:-idac-data-local}"
LOCAL_EMR_BUCKET="${LOCAL_EMR_BUCKET:-idac-emr-bucket-local}"
FORCE_REFRESH="${FORCE_REFRESH:-false}"
FORCE_REFRESH_BIG="${FORCE_REFRESH_BIG:-false}"
QA_TO_LOCAL_REWRITE="${QA_TO_LOCAL_REWRITE:-true}"
TMPDIR="${TMPDIR:-/tmp/sidsum-sync}"

# Prefixes that are big enough that re-pulling them every FORCE_REFRESH=true
# costs ~15+ min. They rarely change in QA, so we keep them across normal
# refreshes and only re-pull when FORCE_REFRESH_BIG=true is also set, OR
# when the local target is empty (first-run case).
BIG_PREFIXES=(
    "qa/bronze/raw/qa_master_data_topic/"
)

if [ -z "${QA_AWS_ACCESS_KEY_ID:-}" ] || [ -z "${QA_AWS_SECRET_ACCESS_KEY:-}" ]; then
    echo "ERROR: QA_AWS_ACCESS_KEY_ID / QA_AWS_SECRET_ACCESS_KEY required." >&2
    echo "       Add them to idac-localstack/.env (see env.sample)." >&2
    exit 1
fi

log() { printf '==> %s\n' "$*" >&2; }

# Drop AWS_ENDPOINT_URL* in qa_aws so the QA call hits real AWS, not LocalStack.
qa_aws() {
    env -u AWS_ENDPOINT_URL -u AWS_ENDPOINT_URL_S3 \
    AWS_ACCESS_KEY_ID="$QA_AWS_ACCESS_KEY_ID" \
    AWS_SECRET_ACCESS_KEY="$QA_AWS_SECRET_ACCESS_KEY" \
    AWS_SESSION_TOKEN="${QA_AWS_SESSION_TOKEN:-}" \
    AWS_DEFAULT_REGION="$QA_AWS_REGION" \
    aws --no-cli-pager "$@"
}

local_aws() {
    AWS_ACCESS_KEY_ID=test \
    AWS_SECRET_ACCESS_KEY=test \
    AWS_DEFAULT_REGION=ap-northeast-1 \
    aws --endpoint-url "$LOCAL_ENDPOINT" --no-cli-pager "$@"
}

# Path rewriter: qa/<rest> -> local/<rest>; qa_<x> -> local_<x>.
# Same shape as clone-from-qa.sh's qa_to_local_data_path so the directory
# layout in idac-data-local matches what dp-config / configs expect.
qa_to_local_path() {
    local path="$1"
    if [ "$QA_TO_LOCAL_REWRITE" != "true" ]; then
        printf '%s\n' "$path"
        return
    fi
    printf '%s\n' "$path" | sed -E \
        -e 's|^qa/|local/|' \
        -e 's|/qa/|/local/|g' \
        -e 's#(^|/)qa_#\1local_#g'
}

# Content rewriter for .conf/.json/.yaml/.yml files. Superset of the rewrite
# set in clone-emr-artifacts-from-qa.sh — adds qa_vin_topic /
# qa_master_data_topic which sidsum's source_data silver conf needs.
#
# Important: keep s3:// in config files. The transformation engine's S3Reader
# reads nested JSON/config paths via AWS SDK AmazonS3URI, which rejects s3a://.
# Spark table/data reads can still handle s3:// locally because emr-emulator
# injects the s3 -> S3A filesystem aliases at spark-submit time.
qa_to_local_content() {
    local root="$1"
    [ "$QA_TO_LOCAL_REWRITE" = "true" ] || return 0
    # GNU sed in extended-regex mode. Why this short list:
    #   - Bucket+path forms in two passes: the /qa/ variant first (rewrites
    #     both bucket and path segment), then bare-bucket as a fallback for
    #     things like output_loc="s3://idac-data-qa/".
    #   - Table/DB identifiers use a whitelisted alternation. A blanket
    #     qa_ -> local_ would risk over-matching column names etc.
    #   - Single env="qa" rule covers HOCON and JSON (with/without spaces).
    local sed_args=(
        -E
        -e 's|idac-data-qa/qa/|idac-data-local/local/|g'
        -e 's|idac-emr-bucket-qa/qa/|idac-emr-bucket-local/local/|g'
        -e 's|idac-data-qa\b|idac-data-local|g'
        -e 's|idac-emr-bucket-qa\b|idac-emr-bucket-local|g'
        -e 's/\bqa_(silver|gold|common|bronze)_db\b/local_\1_db/g'
        -e 's/\bqa_(browser_extension|vin_topic|vin_zoom_topic|master_data_topic)\b/local_\1/g'
        -e 's/(env[[:space:]]*=[[:space:]]*|"env"[[:space:]]*:[[:space:]]*)"qa"/\1"local"/g'
    )
    local count=0
    while IFS= read -r -d '' f; do
        local before after
        before="$(md5sum "$f" | awk '{print $1}')"
        sed -i "${sed_args[@]}" "$f"
        after="$(md5sum "$f" | awk '{print $1}')"
        if [ "$before" != "$after" ]; then
            count=$((count + 1))
        fi
    done < <(find "$root" -type f \( -name '*.conf' -o -name '*.json' -o -name '*.yaml' -o -name '*.yml' \) -print0)
    [ "$count" -gt 0 ] && log "  rewrote ${count} config file(s)"
}

ensure_bucket() {
    local b="$1"
    if ! local_aws s3 ls "s3://${b}" >/dev/null 2>&1; then
        log "creating bucket s3://${b}"
        local_aws s3 mb "s3://${b}"
    fi
}

is_big_prefix() {
    local needle="$1"
    for big in "${BIG_PREFIXES[@]}"; do
        [ "$big" = "$needle" ] && return 0
    done
    return 1
}

sync_prefix() {
    local src_bucket="$1"
    local dst_bucket="$2"
    local prefix="$3"
    local apply_content_rewrite="$4"

    local dst_prefix
    dst_prefix="$(qa_to_local_path "$prefix")"

    local src="s3://${src_bucket}/${prefix}"
    local dst="s3://${dst_bucket}/${dst_prefix}"
    local stage="${TMPDIR}/${prefix}"

    log "[${prefix}]"
    log "  src: ${src}"
    log "  dst: ${dst}"

    local target_has_data="false"
    [ -n "$(local_aws s3 ls "$dst" 2>/dev/null | head -1)" ] && target_has_data="true"

    # Big prefixes: never wiped except when FORCE_REFRESH_BIG=true. If they
    # already have data, we trust it (parquet is content-addressed by Delta's
    # _delta_log; stale rows at the tail won't break sidsum).
    if is_big_prefix "$prefix"; then
        if [ "$FORCE_REFRESH_BIG" = "true" ]; then
            log "  big prefix: FORCE_REFRESH_BIG=true → wiping and re-pulling"
            local_aws s3 rm "$dst" --recursive --only-show-errors || true
            target_has_data="false"
        elif [ "$target_has_data" = "true" ]; then
            log "  big prefix: target populated; skipping (set FORCE_REFRESH_BIG=true to refresh)"
            return 0
        else
            log "  big prefix: target empty → first-time pull"
        fi
    elif [ "$FORCE_REFRESH" = "true" ]; then
        log "  wiping ${dst} ..."
        local_aws s3 rm "$dst" --recursive --only-show-errors || true
        target_has_data="false"
    elif [ "$target_has_data" = "true" ]; then
        log "  skip: target already populated (FORCE_REFRESH=true to wipe)"
        return 0
    fi

    rm -rf "$stage" && mkdir -p "$stage"

    log "  downloading from QA ..."
    if ! qa_aws s3 sync "$src" "$stage/" --only-show-errors 2>/tmp/sidsum-sync.err; then
        log "  WARNING: download failed (prefix may not exist in QA) — continuing"
        cat /tmp/sidsum-sync.err >&2 || true
        rm -rf "$stage"
        return 0
    fi

    if [ -z "$(ls -A "$stage" 2>/dev/null)" ]; then
        log "  skip: prefix is empty in QA"
        rm -rf "$stage"
        return 0
    fi

    if [ "$apply_content_rewrite" = "true" ]; then
        qa_to_local_content "$stage"
    fi

    log "  uploading to LocalStack ..."
    local_aws s3 sync "$stage/" "$dst" --only-show-errors
    rm -rf "$stage"
    log "  done"
}

# === sidsum-specific manifest ===
#
# Each prefix is documented with WHY sidsum needs it, so the list is auditable
# and easy to extend when a new sidsum variant surfaces a new dependency.

DATA_PREFIXES=(
    # Bronze raw — input for source_data_silver_transformation.
    # browser_extension drives sidsum's discovered-app-user pipeline; vin_topic
    # and master_data_topic are read by source_data silver alongside it. All
    # three are pulled whole (browser_extension is <1 MB total; the other two
    # have no tenant partition).
    "qa/bronze/raw/qa_browser_extension/"
    "qa/bronze/raw/qa_vin_topic/"
    "qa/bronze/raw/qa_master_data_topic/"

    # Silver auditlog tables — produced by silver, but we still cache QA's
    # data so gold has rows to join even if the local silver run produces
    # zero rows for any tenant. Whole table: each is <25 MB.
    "qa/silver/browserextensiondatastreaming/valid/"
    "qa/silver/gsuite_auditlog/valid/"
    "qa/silver/office365_auditlog/valid/"
    "qa/silver/okta_auditlog/valid/"
    "qa/silver/onelogin_auditlog/valid/"

    # CDC silver tables — NOT produced by silver locally; populated by upstream
    # CDC pipelines we don't run here. discovered_app_user_gold_transformation
    # joins all three; without them the gold SQL hits TABLE_OR_VIEW_NOT_FOUND.
    # Whole tables: combined <2 MB.
    "qa/cdc/qa_silver_db/software_lookups/"
    "qa/cdc/qa_silver_db/source_dictionary_mappings/"
    "qa/cdc/qa_silver_db/employee_identities/"

    # Gold seed — Athena gates in sidsum.conf (task_1, task_2) probe gold +
    # common.auditlog before silver/gold tasks run. Whole tables make the
    # first run pass those gates with realistic data; gold writes its own
    # rows on subsequent runs.
    "qa/gold/data_collection/valid/"
    "qa/gold/rollup_logic/valid/"
    "qa/gold/software_apps_cad/valid/"
    "qa/gold/target_da/valid/"
    "qa/gold/target_dau/valid/"

    # Common auditlog — gold's getPrevLayerRunDetails reads silver's COMPLETED
    # rows from this table to compute its data-window. Without it gold falls
    # back to the 1970-01-01 window which still works, but the QA seed gives
    # the local run prod-shaped audit history.
    "qa/common/auditlog/"
)

EMR_PREFIXES=(
    # Configs the silver/gold jobs read on every spark-submit. The content
    # rewriter translates qa_* DB names and bucket paths to local equivalents
    # in-place so jobs land on LocalStack, not real AWS.
    "qa/silver_transformation/artifacts/configs/"
    "qa/gold_transformation/artifacts/configs/"

    # Dependency jars (delta-core, delta-storage, etc.) Spark needs at runtime.
    # These are binary, no rewrite — uploaded as-is.
    "qa/silver_transformation/artifacts/jars/dependencies/"
    "qa/gold_transformation/artifacts/jars/dependencies/"
)

log "=========================================================="
log "  sidsum data sync"
log "  qa region        : ${QA_AWS_REGION}"
log "  local endpoint   : ${LOCAL_ENDPOINT}"
log "  force refresh    : ${FORCE_REFRESH}"
log "  force refresh big: ${FORCE_REFRESH_BIG}"
log "  rewrite paths    : ${QA_TO_LOCAL_REWRITE}"
log "=========================================================="

ensure_bucket "$LOCAL_DATA_BUCKET"
ensure_bucket "$LOCAL_EMR_BUCKET"

log
log "==== Phase 1/2: data prefixes (idac-data-qa -> idac-data-local) ===="
for prefix in "${DATA_PREFIXES[@]}"; do
    sync_prefix "$QA_DATA_BUCKET" "$LOCAL_DATA_BUCKET" "$prefix" "false"
done

log
log "==== Phase 2/2: EMR artifact prefixes (configs + dep jars) ===="
for prefix in "${EMR_PREFIXES[@]}"; do
    sync_prefix "$QA_EMR_BUCKET" "$LOCAL_EMR_BUCKET" "$prefix" "true"
done

log
log "==== Summary ===="
log "  Data prefixes synced/verified: ${#DATA_PREFIXES[@]}"
log "  EMR prefixes synced/verified : ${#EMR_PREFIXES[@]}"
log
log "Done. Next steps:"
log "  1. If Glue catalog is missing entries, re-run setup-glue:"
log "     BOOTSTRAP=true docker compose up -d setup-glue"
log "  2. Trigger local_sidsum_dag from the Airflow UI."
