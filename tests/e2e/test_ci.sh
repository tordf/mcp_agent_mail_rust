#!/usr/bin/env bash
# test_ci.sh - E2E wrapper for `am ci` native gate runner test suite (br-271i)
#
# Delegates to scripts/e2e_ci.sh following the standard E2E pattern.

set -euo pipefail

WRAPPER_SUITE="ci"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/test_helpers.sh"
wrapper_exec "${SCRIPT_DIR}/../../scripts/e2e_ci.sh" "$@"
