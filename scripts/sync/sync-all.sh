#!/bin/sh
# Single entrypoint for the sync-sources compose service. Runs each upstream
# sync sequentially in one alpine container.

set -eu

ROOT="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"

echo "==> sync-all: dp-config"
sh "$ROOT/sync-dp-config.sh"

echo "==> sync-all: workflow-orchestration"
sh "$ROOT/sync-workflow-orchestration.sh"

echo "==> sync-all: spark-scripts"
sh "$ROOT/sync-spark-scripts.sh"

echo "==> sync-all: complete"
