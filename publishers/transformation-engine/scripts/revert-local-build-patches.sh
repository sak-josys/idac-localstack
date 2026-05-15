#!/usr/bin/env bash
# Revert every transient patch applied by apply-local-build-patches.sh.
#
# Strategy: walk the engine tree for *.idac-bak siblings and `mv` each one back
# over its original (drops the .idac-bak suffix). This is a directory-scoped
# inverse of cp, so it works even if the apply script grows new patches in the
# future without this script needing to learn about them.
#
# Designed to be called from a `trap '... EXIT'` so it runs whether the build
# succeeds, fails, or crashes mid-flight. Idempotent: a clean tree (no .idac-bak
# files) is a no-op, not an error.

set -euo pipefail

ENGINE_DIR="${1:?usage: revert-local-build-patches.sh <engine_dir>}"

if [ ! -d "$ENGINE_DIR" ]; then
    # Don't fail in trap context if the dir is gone — just bail quietly.
    exit 0
fi

log() { echo "==> [patches] $*" >&2; }

# -print0 / read -d '' so paths with spaces are handled correctly. We only
# look under tracked source directories (commons, silver, gold, ...) to avoid
# touching anything in target/ or hidden dirs.
restored=0
while IFS= read -r -d '' bak; do
    original="${bak%.idac-bak}"
    if [ -f "$original" ] || [ ! -e "$original" ]; then
        mv -f "$bak" "$original"
        log "restored: ${original#${ENGINE_DIR}/}"
        restored=$((restored + 1))
    fi
done < <(
    find "$ENGINE_DIR" \
        -type f \
        -name '*.idac-bak' \
        -not -path '*/target/*' \
        -not -path '*/.git/*' \
        -not -path '*/node_modules/*' \
        -print0
)

if [ "$restored" -eq 0 ]; then
    log "no .idac-bak files found — tree is already clean"
else
    log "reverted ${restored} file(s)"
fi
