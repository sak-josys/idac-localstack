#!/bin/sh
# Copy idac-dp-config/airflow/configs -> idac-localstack/publishers/dp-config/airflow/configs (same layout).
# Override source with DP_CONFIG_AIRFLOW_CONFIGS.

set -eu

ROOT="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
SRC="${DP_CONFIG_AIRFLOW_CONFIGS:-$ROOT/../idac-dp-config/airflow/configs}"
DST="$ROOT/publishers/dp-config/airflow/configs"

if [ ! -d "$SRC" ]; then
  echo "sync-dp-config: missing source: $SRC" >&2
  exit 1
fi

rm -rf "$DST"
mkdir -p "$DST"
cp -a "$SRC"/. "$DST"/

echo "sync-dp-config: $SRC -> $DST"
