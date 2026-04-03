#!/usr/bin/env bash
# test_dual_mode.sh - E2E wrapper for dual-mode suite (br-21gj.5.6)

set -euo pipefail

WRAPPER_SUITE="dual_mode"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/test_helpers.sh"
wrapper_exec "${SCRIPT_DIR}/../../scripts/e2e_dual_mode.sh" "$@"
