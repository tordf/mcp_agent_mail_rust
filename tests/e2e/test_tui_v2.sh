#!/usr/bin/env bash
# test_tui_v2.sh - E2E test suite wrapper for TUI V2 features (br-2bbt.11.2)
#
# Runs the implementation in scripts/e2e_tui_v2.sh.
# Authoritative invocation:
#   am e2e run --project . tui_v2
# Compatibility fallback:
#   AM_E2E_FORCE_LEGACY=1 ./scripts/e2e_test.sh tui_v2

set -euo pipefail

WRAPPER_SUITE="tui_v2"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/test_helpers.sh"
wrapper_exec "${SCRIPT_DIR}/../../scripts/e2e_tui_v2.sh"
