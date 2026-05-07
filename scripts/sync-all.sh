#!/bin/sh
# Single entrypoint for the sync-sources compose service.
# Runs every upstream-source sync we have, sequentially, so we only spin up
# one alpine container instead of one per publisher. Sequential keeps logs
# tidy and the file-copy work is fast enough that parallelism isn't worth it.

set -eu

ROOT="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"

echo "==> sync-all: dp-config"
sh "$ROOT/sync-dp-config.sh"

echo "==> sync-all: workflow-orchestration"
sh "$ROOT/sync-workflow-orchestration.sh"

echo "==> sync-all: spark-scripts"
sh "$ROOT/sync-spark-scripts.sh"

echo "==> sync-all: complete"
