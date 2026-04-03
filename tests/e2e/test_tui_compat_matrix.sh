#!/usr/bin/env bash
# test_tui_compat_matrix.sh - E2E test suite wrapper for TUI compatibility matrix (tmux/resize/unicode).
#
# Runs the implementation in scripts/e2e_tui_compat_matrix.sh.
# Authoritative invocation:
#   am e2e run --project . tui_compat_matrix
# Compatibility fallback:
#   AM_E2E_FORCE_LEGACY=1 ./scripts/e2e_test.sh tui_compat_matrix

set -euo pipefail

WRAPPER_SUITE="tui_compat_matrix"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/test_helpers.sh"
wrapper_exec "${SCRIPT_DIR}/../../scripts/e2e_tui_compat_matrix.sh"
