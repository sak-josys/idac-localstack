#!/usr/bin/env bash
# Apply transient source patches needed to make a fat-JAR build work against
# the local stack (LocalStack S3 instead of real AWS).
#
# Why this exists at all:
#   The upstream transformation-engine instantiates AWS Java SDK v1 S3 clients
#   directly via AmazonS3ClientBuilder.standard() (S3Reader.scala:25). That call
#   path is invisible to the Hadoop S3A layer, so none of our --conf
#   spark.hadoop.fs.s3a.* overrides reach it. With no explicit region or
#   endpoint, the SDK falls all the way down its provider chain to EC2 IMDS
#   (169.254.169.254). From inside spark-master that's unreachable; the request
#   times out and the driver exits with "Unable to find a region via the region
#   provider chain", well before any real work runs.
#
# Why we patch transiently instead of editing upstream:
#   idac-transformation-engine is the shared QA/prod source — we explicitly
#   keep it pristine. Build-time patching gives us the local fix without
#   committing local-only behaviour into a shared repo. The flow is:
#
#     apply-local-build-patches.sh   <-- this script
#     sbt assembly  (inside builder container)
#     revert-local-build-patches.sh  <-- trap-protected, runs even on failure
#
# Idempotence and recovery:
#   If a *.idac-bak sibling exists from a previous interrupted run, we restore
#   from it before re-applying. That makes `apply` safe to retry without ever
#   double-patching the source.

set -euo pipefail

ENGINE_DIR="${1:?usage: apply-local-build-patches.sh <engine_dir>}"

if [ ! -f "${ENGINE_DIR}/build.sbt" ]; then
    echo "ERROR: ${ENGINE_DIR} does not look like the transformation-engine root (no build.sbt)." >&2
    exit 1
fi

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"
PATCHES_DIR="$(CDPATH= cd -- "${SCRIPT_DIR}/../patches" && pwd)"

log() { echo "==> [patches] $*" >&2; }

# Replace exactly one line in $target_file (the one matching $match_line
# verbatim — including leading whitespace) with the literal contents of
# $payload_file. The matched line itself is dropped.
# Validates that exactly one line matched; refuses to mutate the file if not.
#
# Implementation notes:
#   - BusyBox sed (Alpine default) handles multi-line `c\` differently from
#     GNU sed, so we use awk — its semantics are identical across both.
#   - We use awk's `==` (string equality), not `~` (regex), to side-step the
#     fact that awk's `-v` variable assignment processes backslash escapes
#     before the value reaches the regex engine. Literal match is also a
#     stronger contract: a single character drift in upstream blows up
#     loudly instead of silently mis-matching.
replace_line_with_payload() {
    local target_file="$1"
    local match_line="$2"
    local payload_file="$3"

    if [ ! -f "$target_file" ]; then
        echo "ERROR: target file not found: ${target_file}" >&2
        return 1
    fi
    if [ ! -f "$payload_file" ]; then
        echo "ERROR: payload file not found: ${payload_file}" >&2
        return 1
    fi

    # Pre-check the match count so we fail loudly when upstream diverges
    # rather than silently producing a no-op or duplicate patch. -F = fixed
    # string, -x = whole line, -c = count.
    local match_count
    match_count="$(grep -Fxc -- "$match_line" "$target_file" || true)"
    if [ "$match_count" -ne 1 ]; then
        echo "ERROR: expected exactly 1 line matching the patch target in ${target_file}, found ${match_count}." >&2
        echo "       upstream has likely diverged — refusing to patch." >&2
        echo "       expected line: ${match_line}" >&2
        return 1
    fi

    # awk does the actual replace into a sibling .patched, then we atomically
    # mv it over the original (only after backing the original up).
    local patched="${target_file}.idac-patched"
    awk -v match_line="$match_line" -v payload_file="$payload_file" '
        $0 == match_line {
            while ((getline line < payload_file) > 0) print line
            close(payload_file)
            replaced = 1
            next
        }
        { print }
        END {
            if (!replaced) {
                print "awk: target line not matched in " FILENAME > "/dev/stderr"
                exit 1
            }
        }
    ' "$target_file" > "$patched"

    cp -p "$target_file" "${target_file}.idac-bak"
    mv "$patched" "$target_file"
}

# --------- Patch 1: S3Reader.scala -- env-aware S3 client builder -----------
S3READER="${ENGINE_DIR}/commons/src/main/scala/com/josys/idac/commons/utils/S3Reader.scala"
S3READER_PAYLOAD="${PATCHES_DIR}/S3Reader.scala.local-s3client.payload"

# Recover from a prior interrupted apply: if the .idac-bak still exists we treat
# it as the canonical source and restore from it before patching.
if [ -f "${S3READER}.idac-bak" ]; then
    log "recovering from prior interrupted apply: restoring ${S3READER}"
    mv -f "${S3READER}.idac-bak" "$S3READER"
fi

log "patching $(basename "$S3READER") for local-aware S3 client"

# Literal upstream line at S3Reader.scala:25 — leading whitespace included.
# Any drift (e.g. someone reformats the file) will be caught by the grep -Fxc
# precheck inside replace_line_with_payload and surface as a clear error.
S3READER_MATCH='    val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider).build()'

replace_line_with_payload "$S3READER" "$S3READER_MATCH" "$S3READER_PAYLOAD"

# Sanity-check: the patched file should now contain the unique marker we
# embedded in the payload. If it doesn't, something is very wrong — restore.
if ! grep -q '\[LOCAL-PATCH\]' "$S3READER"; then
    echo "ERROR: post-patch verification failed (LOCAL-PATCH marker missing)." >&2
    if [ -f "${S3READER}.idac-bak" ]; then
        mv -f "${S3READER}.idac-bak" "$S3READER"
    fi
    exit 1
fi

log "applied: ${S3READER}"

# --------- Patch 2: SparkSessionWrapper.scala -- env-aware Spark conf ---------
# Why: SparkSession.builder.config(...) hard-codes prod values for the S3A
# endpoint and credentials provider, which run AFTER spark-submit has parsed
# our --conf overrides. That means every post-init Spark S3A call (e.g.
# AuditTableHandler.exists, which is the first thing GoldMain does) is routed
# at real AWS and 403s our test/test creds. The two patches replace those two
# lines with `sys.env`-aware variants that fall back to the upstream prod
# values when AWS_ENDPOINT_URL is unset, so QA/prod behaviour is unchanged.
SSW="${ENGINE_DIR}/commons/src/main/scala/com/josys/idac/commons/utils/SparkSessionWrapper.scala"
SSW_ENDPOINT_PAYLOAD="${PATCHES_DIR}/SparkSessionWrapper.scala.local-endpoint.payload"
SSW_CREDS_PAYLOAD="${PATCHES_DIR}/SparkSessionWrapper.scala.local-creds-provider.payload"

# Recover from a prior interrupted apply on this file too.
if [ -f "${SSW}.idac-bak" ]; then
    log "recovering from prior interrupted apply: restoring ${SSW}"
    mv -f "${SSW}.idac-bak" "$SSW"
fi

log "patching $(basename "$SSW") for env-aware S3A endpoint"
SSW_ENDPOINT_MATCH='      .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")'
replace_line_with_payload "$SSW" "$SSW_ENDPOINT_MATCH" "$SSW_ENDPOINT_PAYLOAD"

# Note: replace_line_with_payload's first action backs the original to
# .idac-bak. The second call below overwrites that backup with the *patched*
# state from this call, which would break revert. So we sidestep that by
# stashing the canonical-original backup ourselves before patch #2 runs, and
# putting it back as the .idac-bak at the end.
ORIGINAL_BAK="${SSW}.idac-bak"
ORIGINAL_BAK_STASH="${SSW}.idac-bak.stash"
cp -p "$ORIGINAL_BAK" "$ORIGINAL_BAK_STASH"

log "patching $(basename "$SSW") for env-aware S3A credentials provider"
SSW_CREDS_MATCH='      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")'
replace_line_with_payload "$SSW" "$SSW_CREDS_MATCH" "$SSW_CREDS_PAYLOAD"

# Restore the canonical pre-patch backup so revert returns the file to its
# true upstream state, not to the half-patched intermediate.
mv -f "$ORIGINAL_BAK_STASH" "$ORIGINAL_BAK"

# Sanity-check: both LOCAL-PATCH markers should now be present in the file.
if [ "$(grep -Fc '[LOCAL-PATCH]' "$SSW")" -lt 2 ]; then
    echo "ERROR: post-patch verification failed for ${SSW} (expected >=2 LOCAL-PATCH markers)." >&2
    if [ -f "${SSW}.idac-bak" ]; then
        mv -f "${SSW}.idac-bak" "$SSW"
    fi
    exit 1
fi

log "applied: ${SSW}"

# --------- Patch 3: GlueClientProvider.scala -- env-aware Glue client ---------
# Why: the upstream non-local branch builds AWSGlueClientBuilder.standard()
# with no explicit endpoint and falls all the way through to glue.<region>
# .amazonaws.com — which 400s with UnrecognizedClientException for the test/
# test creds we use against LocalStack. The upstream "local" branch is gated
# on FILE_SYSTEM=local and hard-codes http://localhost:9999, neither of which
# match our stack. Patching to honour AWS_ENDPOINT_URL keeps QA/prod on the
# default branch (no behaviour change) while routing local runs at LocalStack.
GCP="${ENGINE_DIR}/commons/src/main/scala/com/josys/idac/commons/utils/GlueClientProvider.scala"
GCP_PAYLOAD="${PATCHES_DIR}/GlueClientProvider.scala.local-glue-endpoint.payload"

if [ -f "${GCP}.idac-bak" ]; then
    log "recovering from prior interrupted apply: restoring ${GCP}"
    mv -f "${GCP}.idac-bak" "$GCP"
fi

log "patching $(basename "$GCP") for env-aware Glue endpoint"
GCP_MATCH='        AWSGlueClientBuilder.standard().build()'
replace_line_with_payload "$GCP" "$GCP_MATCH" "$GCP_PAYLOAD"

if ! grep -q '\[LOCAL-PATCH\]' "$GCP"; then
    echo "ERROR: post-patch verification failed for ${GCP} (LOCAL-PATCH marker missing)." >&2
    if [ -f "${GCP}.idac-bak" ]; then
        mv -f "${GCP}.idac-bak" "$GCP"
    fi
    exit 1
fi

log "applied: ${GCP}"
log "all patches applied — sbt assembly can now produce a localstack-friendly JAR"
