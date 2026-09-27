#!/usr/bin/env bash
# Detiene el clúster local levantado con run-local.sh
set -uo pipefail
cd "$(dirname "$0")/.."
RUN_DIR=${DFSHA_RUN_DIR:-$(pwd)/.run}
for pidfile in "$RUN_DIR"/*.pid; do
  [[ -f "$pidfile" ]] || continue
  pid=$(cat "$pidfile")
  if kill "$pid" 2>/dev/null; then echo "detenido $(basename "$pidfile" .pid) (pid $pid)"; fi
  rm -f "$pidfile"
done
