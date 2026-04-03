#!/usr/bin/env bash
# test_share_verify_live.sh - E2E wrapper for verify-live matrix suite.

set -euo pipefail

WRAPPER_SUITE="share_verify_live"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/test_helpers.sh"
wrapper_exec "${SCRIPT_DIR}/../../scripts/e2e_share_verify_live.sh" "$@"
