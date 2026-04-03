#!/usr/bin/env bash
# test_tui_interaction.sh - E2E test suite wrapper for TUI interaction flows.
#
# Runs the implementation in scripts/e2e_tui_interaction.sh.
# Authoritative invocation:
#   am e2e run --project . tui_interaction
# Compatibility fallback:
#   AM_E2E_FORCE_LEGACY=1 ./scripts/e2e_test.sh tui_interaction

set -euo pipefail

WRAPPER_SUITE="tui_interaction"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/test_helpers.sh"
wrapper_exec "${SCRIPT_DIR}/../../scripts/e2e_tui_interaction.sh"
