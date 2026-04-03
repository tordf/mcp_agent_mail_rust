#!/usr/bin/env bash
# test_mcp_api_parity.sh - E2E test suite wrapper for MCP/API mode switching parity.
#
# Runs the implementation in scripts/e2e_mcp_api_parity.sh.
# Authoritative invocation:
#   am e2e run --project . mcp_api_parity
# Compatibility fallback:
#   AM_E2E_FORCE_LEGACY=1 ./scripts/e2e_test.sh mcp_api_parity

set -euo pipefail

WRAPPER_SUITE="mcp_api_parity"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/test_helpers.sh"
wrapper_exec "${SCRIPT_DIR}/../../scripts/e2e_mcp_api_parity.sh"
