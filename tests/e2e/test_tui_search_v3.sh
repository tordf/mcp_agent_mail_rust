#!/usr/bin/env bash
# test_tui_search_v3.sh - E2E test suite wrapper for TUI Search V3 cockpit.
#
# Runs the implementation in scripts/e2e_tui_search_v3.sh.
# Authoritative invocation:
#   am e2e run --project . tui_search_v3
# Compatibility fallback:
#   AM_E2E_FORCE_LEGACY=1 ./scripts/e2e_test.sh tui_search_v3

set -euo pipefail

WRAPPER_SUITE="tui_search_v3"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/test_helpers.sh"
wrapper_exec "${SCRIPT_DIR}/../../scripts/e2e_tui_search_v3.sh"
