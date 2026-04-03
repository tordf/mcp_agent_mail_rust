#!/usr/bin/env bash
# test_archive.sh - E2E test suite wrapper for archive side-effects.
#
# Runs the implementation in scripts/e2e_archive.sh so the suite can also
# be invoked manually while keeping the e2e runner discovery contract.

set -euo pipefail

WRAPPER_SUITE="archive"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/test_helpers.sh"
wrapper_exec "${SCRIPT_DIR}/../../scripts/e2e_archive.sh"

