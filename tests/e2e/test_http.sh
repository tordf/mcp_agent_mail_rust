#!/usr/bin/env bash
# test_http.sh - E2E test suite wrapper for unified HTTP server parity.
#
# Runs the implementation in scripts/e2e_http.sh.
# Authoritative invocation:
#   am e2e run --project . http
# Compatibility fallback:
#   AM_E2E_FORCE_LEGACY=1 ./scripts/e2e_test.sh http

set -euo pipefail

WRAPPER_SUITE="http"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/test_helpers.sh"
wrapper_exec "${SCRIPT_DIR}/../../scripts/e2e_http.sh"
