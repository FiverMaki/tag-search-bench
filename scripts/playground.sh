#!/usr/bin/env bash
# Start a local TiDB cluster with tiup playground (run inside WSL / Linux / macOS).
#
#   bash scripts/playground.sh            # pinned LTS
#   TIDB_VERSION=v9.0.0 bash scripts/playground.sh
#
# The version is pinned: without one, playground v1.17.1 resolves the
# monitoring components to "nightly" and fails to bootstrap.
#
# --tag keeps data under ~/.tiup/data/tagbench, so generated data survives restarts.
# --host 0.0.0.0 lets the Go program on the Windows side connect via 127.0.0.1:4000.
set -euo pipefail

export PATH="$HOME/.tiup/bin:$PATH"

if ! command -v tiup >/dev/null 2>&1; then
  echo "tiup not found, installing..."
  curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
fi

exec tiup playground "${TIDB_VERSION:-v8.5.8}" \
  --tag tagbench \
  --host 0.0.0.0 \
  --pd 1 \
  --kv 3 \
  --kv.config "$(dirname "$0")/tikv.toml" \
  --db 1 \
  --tiflash 0
