#!/usr/bin/env bash
# test_mode_matrix.sh - E2E wrapper for mode matrix suite (br-21gj.5.2)

set -euo pipefail

WRAPPER_SUITE="mode_matrix"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/test_helpers.sh"
wrapper_exec "${SCRIPT_DIR}/../../scripts/e2e_mode_matrix.sh" "$@"
