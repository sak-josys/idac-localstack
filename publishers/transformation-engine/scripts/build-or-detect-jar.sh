#!/usr/bin/env bash
# Build a transformation-engine module's fat JAR, or reuse an existing one.
#
# Hot path  : if <module>/target/scala-2.12/<module>.jar already exists, print its
#             absolute path and exit. Lets devs reuse their IDE/sbt builds for free.
# Cold path : run the builder image (Dockerfile.builder) with sbt + JDK 11, mounting
#             the engine repo at /work and a named cache volume for ~/.ivy2 / ~/.sbt
#             / coursier so the second build is fast.
#
# Output:
#   stderr -> human-readable progress logs
#   stdout -> exactly one line: the absolute path to the produced JAR
# This split lets the orchestrator capture the path with `JAR=$(... build-or-detect-jar.sh silver)`
# without parsing log noise.

set -euo pipefail

usage() {
    cat >&2 <<EOF
usage: build-or-detect-jar.sh <module>

  <module>  one of: silver | gold | migration | data_compaction | data_quality | commons

env vars (all optional unless noted):
  TRANSFORMATION_ENGINE_DIR        path to the engine repo as visible to *this* script
                                   (default: /transformation-engine inside the container,
                                    or auto-detected as ../../../idac-transformation-engine on host)
  HOST_TRANSFORMATION_ENGINE_DIR   path to the engine repo on the *Docker host* — required
                                   only when this script runs inside a container that
                                   mounts /var/run/docker.sock (so the builder sibling
                                   container can mount the same repo). Defaults to
                                   TRANSFORMATION_ENGINE_DIR.
  BUILDER_IMAGE                    builder image tag (default: idac-localstack/transformation-builder:latest)
  BUILDER_CACHE_VOLUME             named docker volume for sbt/ivy/coursier caches
                                   (default: idac-transformation-builder-cache)
  FORCE_REBUILD                    if "true", ignore an existing JAR and rebuild

stdout: absolute path to the JAR (one line)
EOF
}

if [ $# -ne 1 ]; then
    usage
    exit 2
fi

MODULE="$1"
case "$MODULE" in
    silver|gold|migration|data_compaction|data_quality|commons) ;;
    -h|--help) usage; exit 0 ;;
    *) echo "ERROR: unknown module '$MODULE'." >&2; usage; exit 2 ;;
esac

# Resolve the engine repo path. Prefer the explicit env var; otherwise fall back to
# a host-layout default (idac-transformation-engine sits next to idac-localstack).
# Climb out of idac-localstack/publishers/transformation-engine/scripts/ (4 levels)
# to reach the workspace root that holds idac-transformation-engine as a sibling repo.
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"
DEFAULT_ENGINE_DIR_HOST="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)/idac-transformation-engine"

ENGINE_DIR="${TRANSFORMATION_ENGINE_DIR:-}"
if [ -z "$ENGINE_DIR" ]; then
    if [ -d "/transformation-engine" ]; then
        ENGINE_DIR="/transformation-engine"
    else
        ENGINE_DIR="$DEFAULT_ENGINE_DIR_HOST"
    fi
fi
HOST_ENGINE_DIR="${HOST_TRANSFORMATION_ENGINE_DIR:-$ENGINE_DIR}"

if [ ! -f "${ENGINE_DIR}/build.sbt" ]; then
    echo "ERROR: build.sbt not found at ${ENGINE_DIR}/build.sbt" >&2
    echo "       set TRANSFORMATION_ENGINE_DIR to the repo root, or place the repo at ${DEFAULT_ENGINE_DIR_HOST}." >&2
    exit 1
fi

JAR_REL="${MODULE}/target/scala-2.12/${MODULE}.jar"
JAR_PATH="${ENGINE_DIR}/${JAR_REL}"

BUILDER_IMAGE="${BUILDER_IMAGE:-idac-localstack/transformation-builder:latest}"
BUILDER_CACHE_VOLUME="${BUILDER_CACHE_VOLUME:-idac-transformation-builder-cache}"
FORCE_REBUILD="${FORCE_REBUILD:-false}"

log() { echo "==> $*" >&2; }

# Per build.sbt: every module depends on commons; silver also pulls data_quality + migration;
# gold pulls data_quality. Edits anywhere in the transitive set must invalidate the JAR.
deps_for() {
    case "$1" in
        silver)          echo "silver commons data_quality migration" ;;
        gold)            echo "gold commons data_quality" ;;
        migration)       echo "migration commons" ;;
        data_compaction) echo "data_compaction commons" ;;
        data_quality)    echo "data_quality commons" ;;
        commons)         echo "commons" ;;
    esac
}

# Returns the path of the first source file newer than $1 across the module's
# transitive deps + root build files + this build harness's patch payloads.
# Empty output = JAR is up to date.
# Skips target/ (build outputs would always look "newer"), .git, and node_modules.
#
# Why include the patch harness in the trigger set:
#   We mutate engine sources transiently before sbt assembly (see
#   apply-local-build-patches.sh). A change to a patch payload or to the apply
#   script itself is just as much a build input as an engine source edit, but
#   neither lives under $engine — so a naive find-on-engine misses it and we'd
#   silently keep shipping a JAR built against the previous patch revision.
newer_source_for() {
    local jar="$1"
    local module="$2"
    local engine="$3"
    local -a roots=()
    for d in $(deps_for "$module"); do
        roots+=("${engine}/${d}")
    done
    roots+=("${engine}/build.sbt" "${engine}/project")

    # Patch harness lives at <repo>/idac-localstack/publishers/transformation-engine/{patches,scripts}/.
    # SCRIPT_DIR is the scripts/ dir; patches/ is its sibling.
    local patches_dir="${SCRIPT_DIR}/../patches"
    [ -d "$patches_dir" ] && roots+=("$patches_dir")
    roots+=("${SCRIPT_DIR}/apply-local-build-patches.sh")

    find "${roots[@]}" \
        -type f \
        -not -path '*/target/*' \
        -not -path '*/node_modules/*' \
        -not -path '*/.git/*' \
        -newer "$jar" \
        -print \
        -quit 2>/dev/null
}

log "module     : ${MODULE}"
log "engine dir : ${ENGINE_DIR}"
log "jar target : ${JAR_PATH}"

if [ "$FORCE_REBUILD" = "true" ]; then
    log "FORCE_REBUILD=true — rebuilding regardless of JAR/source state"
elif [ -f "$JAR_PATH" ]; then
    NEWER_SRC="$(newer_source_for "$JAR_PATH" "$MODULE" "$ENGINE_DIR")"
    if [ -z "$NEWER_SRC" ]; then
        log "found existing JAR with no newer sources — skipping build"
        echo "$JAR_PATH"
        exit 0
    fi
    log "JAR exists but newer source detected — rebuilding"
    log "  trigger : ${NEWER_SRC}"
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: docker CLI not found. The cold-build path needs it to run the builder image." >&2
    exit 1
fi

if ! docker image inspect "$BUILDER_IMAGE" >/dev/null 2>&1; then
    echo "ERROR: builder image '${BUILDER_IMAGE}' is not present locally." >&2
    echo "       build it first:  docker build -f publishers/transformation-engine/Dockerfile.builder -t ${BUILDER_IMAGE} publishers/transformation-engine" >&2
    exit 1
fi

# Named volume for sbt/ivy/coursier caches — created on first use, reused thereafter.
# Without this, every cold build re-downloads ~hundreds of MB of dependencies.
if ! docker volume inspect "$BUILDER_CACHE_VOLUME" >/dev/null 2>&1; then
    log "creating cache volume '${BUILDER_CACHE_VOLUME}'"
    docker volume create "$BUILDER_CACHE_VOLUME" >/dev/null
fi

log "running sbt \"project ${MODULE}\" assembly  (this can take several minutes on first build)"
log "host mount : ${HOST_ENGINE_DIR} -> /work"

# Apply transient source patches needed for the JAR to talk to LocalStack
# (e.g. injecting an env-aware S3 client builder into commons/S3Reader.scala).
# Upstream stays untouched: the `trap ... EXIT` below restores every patched
# file from its .idac-bak sibling whether sbt succeeds, fails, or crashes.
# See apply-local-build-patches.sh for the full rationale + patch list.
"${SCRIPT_DIR}/apply-local-build-patches.sh" "$ENGINE_DIR"
trap '"${SCRIPT_DIR}/revert-local-build-patches.sh" "$ENGINE_DIR" || true' EXIT

# `project <module>` scopes assembly to one subproject and pulls its dep graph
# (e.g. silver depends on commons + data_quality + migration, all of which build).
# stderr from sbt is intentionally left attached to ours so the dev sees progress.
docker run --rm \
    -v "${HOST_ENGINE_DIR}:/work" \
    -v "${BUILDER_CACHE_VOLUME}:/root/.ivy2" \
    -v "${BUILDER_CACHE_VOLUME}:/root/.cache/coursier" \
    "$BUILDER_IMAGE" \
    "project ${MODULE}" assembly >&2

if [ ! -f "$JAR_PATH" ]; then
    echo "ERROR: build appeared to succeed but ${JAR_PATH} is missing." >&2
    echo "       inspect ${ENGINE_DIR}/${MODULE}/target/scala-2.12/ to debug." >&2
    exit 1
fi

log "build complete: $(du -h "$JAR_PATH" | awk '{print $1}') -> ${JAR_PATH}"
echo "$JAR_PATH"
