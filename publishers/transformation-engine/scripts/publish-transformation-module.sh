#!/usr/bin/env bash
# Top-level publisher for transformation-engine modules.
#
# Chains the three pipeline stages into one command:
#   1. build-or-detect-jar.sh   ->  produces (or reuses) the fat JAR
#   2. jar-to-intermediate.sh   ->  zips + uploads to s3://.../intermediate/
#   3. intermediate-to-s3.sh    ->  promotes to env-specific s3 layout
#
# Why a thin orchestrator rather than inlining? Each stage is independently
# useful — devs can call build-or-detect-jar.sh in isolation when iterating in
# their IDE, intermediate-to-s3.sh when debugging the S3 layout, etc. The
# orchestrator just wires them together for the common 'I want to ship my code'
# workflow.
#
# Publishable modules: silver | gold | migration | data_compaction
# (commons + data_quality are library deps rolled into silver/gold by sbt
# assembly — they don't get separate spark-submit JARs in upstream, so we
# don't publish them either.)
#
# Output:
#   stdout -> one s3:// URI per module published (final jar location)
#   stderr -> human-readable progress logs

set -euo pipefail

PUBLISHABLE_MODULES="silver gold migration data_compaction"

usage() {
    cat >&2 <<EOF
usage: publish-transformation-module.sh <module> [<module>...]

  <module>  silver | gold | migration | data_compaction | all

env vars (all optional — passthrough to chained scripts):
  FORCE_REBUILD                 if "true", rebuild even if a fresh JAR exists
                                (forwarded to build-or-detect-jar.sh)
  CODE_VERSION                  informational version stamped on intermediate S3 metadata
                                (forwarded to jar-to-intermediate.sh --version)
  KEEP_STAGING                  if "true", keep the transient CodeArtifact/ S3 prefix
                                (forwarded to intermediate-to-s3.sh)
  MAVEN_CACHE_DIR               override delta-*.jar cache location
  TRANSFORMATION_ENGINE_DIR     engine repo path (build-or-detect-jar.sh respects it)
  HOST_TRANSFORMATION_ENGINE_DIR  host-side path when running inside a container
  BUILDER_IMAGE                 sbt builder image tag

Examples:
  publish-transformation-module.sh silver
  publish-transformation-module.sh gold silver
  FORCE_REBUILD=true publish-transformation-module.sh silver
  publish-transformation-module.sh all
EOF
}

if [ $# -lt 1 ]; then
    usage
    exit 2
fi

MODULES=""
for target in "$@"; do
    case "$target" in
        silver|gold|migration|data_compaction)
            MODULES="${MODULES} ${target}"
            ;;
        all)
            MODULES="${MODULES} ${PUBLISHABLE_MODULES}"
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        commons|data_quality)
            echo "ERROR: '${target}' is a library dep, not a publishable module." >&2
            echo "       it is rolled into silver/gold by sbt assembly. Publish silver or gold instead." >&2
            exit 2
            ;;
        *)
            echo "ERROR: unknown target '${target}'." >&2
            usage
            exit 2
            ;;
    esac
done

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"
BUILD_DETECT="${SCRIPT_DIR}/build-or-detect-jar.sh"
JAR_TO_INTERMEDIATE="${SCRIPT_DIR}/jar-to-intermediate.sh"
INTERMEDIATE_TO_S3="${SCRIPT_DIR}/intermediate-to-s3.sh"

for s in "$BUILD_DETECT" "$JAR_TO_INTERMEDIATE" "$INTERMEDIATE_TO_S3"; do
    if [ ! -x "$s" ]; then
        echo "ERROR: required script not executable: $s" >&2
        exit 1
    fi
done

log() { echo "==> $*" >&2; }
hr()  { echo "============================================================" >&2; }

# Publish a single module end-to-end. Echos the final jar URI to stdout on success.
publish_one() {
    local mod="$1"

    hr
    log "publishing module: ${mod}"
    hr

    # Stage 1 — build or detect.
    log "[stage 1/3] build-or-detect-jar.sh ${mod}"
    local jar
    jar="$("$BUILD_DETECT" "$mod")"
    if [ -z "$jar" ] || [ ! -f "$jar" ]; then
        echo "ERROR: build-or-detect-jar.sh did not return a valid JAR path for ${mod}." >&2
        return 1
    fi
    log "  jar      : ${jar}"

    # Stage 2 — wrap + upload to intermediate. Forward CODE_VERSION as --version when set.
    log "[stage 2/3] jar-to-intermediate.sh --module ${mod}"
    local intermediate
    if [ -n "${CODE_VERSION:-}" ]; then
        intermediate="$("$JAR_TO_INTERMEDIATE" --module "$mod" --jar "$jar" --version "$CODE_VERSION")"
    else
        intermediate="$("$JAR_TO_INTERMEDIATE" --module "$mod" --jar "$jar")"
    fi
    log "  staged   : ${intermediate}"

    # Stage 3 — promote to env-specific S3 layout.
    log "[stage 3/3] intermediate-to-s3.sh --module ${mod}"
    local final_uri
    final_uri="$("$INTERMEDIATE_TO_S3" --module "$mod")"
    log "  final    : ${final_uri}"

    echo "$final_uri"
}

# Resolved target list. 'all' expands to the publishable set in upstream-build order
# (commons -> data_quality -> migration -> silver -> gold -> data_compaction);
# we only publish the subset that has a spark-submit JAR.
declare -a OUT=()
declare -a FAIL=()

for mod in $MODULES; do
    if ! uri="$(publish_one "$mod")"; then
        FAIL+=("$mod")
        continue
    fi
    OUT+=("${mod}|${uri}")
done

hr
log "summary"
hr
# Bash 3.2 (macOS default) plus `set -u` errors on `${arr[@]}` when the array is
# empty. The `${arr[@]+"${arr[@]}"}` form only expands when the array has been
# assigned, so empty-array case is a no-op instead of an unbound-variable abort.
for entry in ${OUT[@]+"${OUT[@]}"}; do
    log "  OK      ${entry%%|*}  ->  ${entry##*|}"
done
for f in ${FAIL[@]+"${FAIL[@]}"}; do
    log "  FAILED  ${f}"
done

# stdout — one jar URI per successfully published module, in publish order.
for entry in ${OUT[@]+"${OUT[@]}"}; do
    echo "${entry##*|}"
done

if [ "${#FAIL[@]}" -gt 0 ]; then
    exit 1
fi
