#!/usr/bin/env bash
# test_share_wizard.sh - E2E wrapper for native share wizard suite (br-18tuh)

set -euo pipefail

WRAPPER_SUITE="share_wizard"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/test_helpers.sh"
wrapper_exec "${SCRIPT_DIR}/../../scripts/e2e_share_wizard.sh"
