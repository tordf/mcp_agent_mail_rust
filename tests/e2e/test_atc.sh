#!/usr/bin/env bash
# test_atc.sh - E2E test for Air Traffic Controller subsystem
#
# Verifies the ATC module compiles, its unit tests pass, and the
# `am robot atc` CLI surface works correctly.
#
# Tests:
#   1. ATC unit tests pass (82 tests)
#   2. `am robot atc --format json` returns valid JSON
#   3. `am robot atc --help` shows all flags
#   4. ATC types are accessible from the server crate

E2E_SUITE="atc"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../scripts/e2e_lib.sh
source "${SCRIPT_DIR}/../../scripts/e2e_lib.sh"

e2e_init_artifacts
e2e_banner "Air Traffic Controller E2E Test Suite"

# ── Test 1: ATC unit tests ─────────────────────────────────────────

e2e_section "ATC unit tests"
if cargo test -p mcp-agent-mail-server --lib 'atc::' -- --test-threads=4 2>&1 | tee "${E2E_ARTIFACT_DIR}/atc_unit_tests.log" | tail -5 | grep -q 'test result: ok'; then
    PASS_COUNT=$(grep 'test result: ok' "${E2E_ARTIFACT_DIR}/atc_unit_tests.log" | grep -oP '\d+ passed' | head -1)
    e2e_pass "ATC unit tests: ${PASS_COUNT}"
else
    e2e_fail "ATC unit tests failed — see ${E2E_ARTIFACT_DIR}/atc_unit_tests.log"
fi

# ── Test 2: Robot atc subcommand ───────────────────────────────────

e2e_section "Robot ATC CLI"

# Build the am binary
e2e_ensure_binary "am" >/dev/null
export PATH="${CARGO_TARGET_DIR}/debug:${PATH}"

# Test --help flag
if AM_INTERFACE_MODE=cli am robot atc --help 2>&1 | grep -q 'decisions'; then
    e2e_pass "am robot atc --help shows --decisions flag"
else
    e2e_fail "am robot atc --help missing --decisions flag"
fi

if AM_INTERFACE_MODE=cli am robot atc --help 2>&1 | grep -q 'liveness'; then
    e2e_pass "am robot atc --help shows --liveness flag"
else
    e2e_fail "am robot atc --help missing --liveness flag"
fi

if AM_INTERFACE_MODE=cli am robot atc --help 2>&1 | grep -q 'conflicts'; then
    e2e_pass "am robot atc --help shows --conflicts flag"
else
    e2e_fail "am robot atc --help missing --conflicts flag"
fi

if AM_INTERFACE_MODE=cli am robot atc --help 2>&1 | grep -q 'summary'; then
    e2e_pass "am robot atc --help shows --summary flag"
else
    e2e_fail "am robot atc --help missing --summary flag"
fi

# Test JSON output
OUTPUT=$(AM_INTERFACE_MODE=cli am robot atc --format json 2>/dev/null || true)
if echo "$OUTPUT" | python3 -c 'import json,sys; json.load(sys.stdin)' 2>/dev/null; then
    e2e_pass "am robot atc --format json returns valid JSON"
else
    e2e_fail "am robot atc --format json: invalid JSON output"
fi

if echo "$OUTPUT" | grep -q '"enabled"'; then
    e2e_pass "JSON output contains 'enabled' field"
else
    e2e_fail "JSON output missing 'enabled' field"
fi

# ── Test 3: ATC types compile correctly ────────────────────────────

e2e_section "ATC type system"
if cargo check -p mcp-agent-mail-server 2>&1 | tail -3 | grep -q 'Finished'; then
    e2e_pass "ATC module compiles without errors"
else
    e2e_fail "ATC module has compilation errors"
fi

# ── Summary ────────────────────────────────────────────────────────

e2e_summary
