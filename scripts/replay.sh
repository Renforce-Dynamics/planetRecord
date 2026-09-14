#!/usr/bin/env bash
set -euo pipefail
TASK_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TASK_ENV="${PLANET_VENV:-$TASK_ROOT/.venv}"
exec "$TASK_ENV/bin/python" -m planetr replay "$@"
