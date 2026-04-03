#!/usr/bin/env bash
# test_console.sh - E2E test suite wrapper for PTY/TTY console output.
#
# Runs the implementation in scripts/e2e_console.sh.
# Authoritative invocation:
#   am e2e run --project . console
# Compatibility fallback:
#   AM_E2E_FORCE_LEGACY=1 ./scripts/e2e_test.sh console

set -euo pipefail

WRAPPER_SUITE="console"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/test_helpers.sh"
wrapper_exec "${SCRIPT_DIR}/../../scripts/e2e_console.sh"
