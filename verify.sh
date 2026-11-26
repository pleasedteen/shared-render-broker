#!/usr/bin/env bash
# One-command end-to-end verification.  It creates a temporary SQLite DB and
# artifact directory, starts all local processes, and exits non-zero on failure.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

exec python3 verify.py
