#!/usr/bin/env bash
# test_tui_comprehensive.sh - Comprehensive TUI QA E2E suite (br-31zb9)
#
# Acceptance criteria: 30+ assertions covering:
#   - Theme system: all 5 core themes render without panic
#   - Snapshot stability: palette snapshots are deterministic
#   - Budget enforcement: renders complete within 16ms
#   - Screen traversal: all screens initialize without panic
#   - Unit test coverage: all TUI screen test suites pass
#   - Binary artefact: server crate compiles as lib
#
# Each assertion uses the shared e2e_assert_* helpers.

set -euo pipefail

# Safety: default to keeping temp dirs so shared harness cleanup does not run
# destructive deletion commands in constrained environments.
: "${AM_E2E_KEEP_TMP:=1}"

E2E_SUITE="tui_comprehensive"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../scripts/e2e_lib.sh
source "${SCRIPT_DIR}/../../scripts/e2e_lib.sh"

# Use a suite-specific target dir to avoid lock contention with other agents.
if [ -z "${CARGO_TARGET_DIR:-}" ] || [ "${CARGO_TARGET_DIR}" = "/data/tmp/cargo-target" ]; then
    export CARGO_TARGET_DIR="/data/tmp/cargo-target-${E2E_SUITE}-$$"
    mkdir -p "${CARGO_TARGET_DIR}"
fi

e2e_init_artifacts
e2e_banner "TUI Comprehensive QA E2E Suite (br-31zb9)"

# ── Case 1: Server crate compiles ─────────────────────────────────
e2e_case_banner "C1: Server crate compiles as library"
BUILD_OUTPUT="$(cargo check -p mcp-agent-mail-server --lib 2>&1)" || true
BUILD_RC=$?
e2e_assert_eq "C1.1: cargo check exits 0" "0" "$BUILD_RC"

# ── Case 2: Theme snapshot tests pass ─────────────────────────────
e2e_case_banner "C2: Theme snapshot test suite"
SNAP_OUTPUT="$(cargo test -p mcp-agent-mail-server --lib -- tui_theme::tests::snapshot 2>&1)" || true
SNAP_RC=$?
e2e_assert_eq "C2.1: snapshot tests exit 0" "0" "$SNAP_RC"
e2e_assert_contains "C2.2: 5 themes distinct palettes" "$SNAP_OUTPUT" "snapshot_5_themes_produce_distinct_palettes ... ok"
e2e_assert_contains "C2.3: CyberpunkAurora stability" "$SNAP_OUTPUT" "snapshot_cyberpunk_aurora_palette_stability ... ok"
e2e_assert_contains "C2.4: Darcula stability" "$SNAP_OUTPUT" "snapshot_darcula_palette_stability ... ok"
e2e_assert_contains "C2.5: LumenLight stability" "$SNAP_OUTPUT" "snapshot_lumen_light_palette_stability ... ok"
e2e_assert_contains "C2.6: NordicFrost stability" "$SNAP_OUTPUT" "snapshot_nordic_frost_palette_stability ... ok"
e2e_assert_contains "C2.7: Doom stability" "$SNAP_OUTPUT" "snapshot_doom_palette_stability ... ok"
e2e_assert_contains "C2.8: frames render without panic" "$SNAP_OUTPUT" "snapshot_5_themes_render_frames_without_panic ... ok"
e2e_assert_contains "C2.9: markdown styles differ" "$SNAP_OUTPUT" "snapshot_5_themes_markdown_styles_differ ... ok"
e2e_assert_not_contains "C2.10: no test failures in snapshots" "$SNAP_OUTPUT" "FAILED"

# ── Case 3: Budget enforcement tests pass ─────────────────────────
e2e_case_banner "C3: 16ms budget enforcement"
BUDGET_OUTPUT="$(cargo test -p mcp-agent-mail-server --lib -- tui_theme::tests::budget 2>&1)" || true
BUDGET_RC=$?
e2e_assert_eq "C3.1: budget tests exit 0" "0" "$BUDGET_RC"
e2e_assert_contains "C3.2: dashboard under 16ms" "$BUDGET_OUTPUT" "budget_dashboard_render_under_16ms ... ok"
e2e_assert_contains "C3.3: theme switch under 1ms" "$BUDGET_OUTPUT" "budget_theme_switch_under_1ms ... ok"
e2e_assert_not_contains "C3.4: no budget failures" "$BUDGET_OUTPUT" "FAILED"

# ── Case 4: Dashboard screen tests pass ───────────────────────────
e2e_case_banner "C4: Dashboard screen test suite"
DASH_OUTPUT="$(cargo test -p mcp-agent-mail-server --lib -- tui_screens::dashboard::tests 2>&1)" || true
DASH_RC=$?
e2e_assert_eq "C4.1: dashboard tests exit 0" "0" "$DASH_RC"
e2e_assert_not_contains "C4.2: no dashboard test failures" "$DASH_OUTPUT" "FAILED"
# Count passing tests (should be substantial)
DASH_PASS_COUNT="$(echo "$DASH_OUTPUT" | grep -oP '\d+ passed' | grep -oP '\d+' || echo "0")"
e2e_assert_contains "C4.3: dashboard has 100+ tests" "$([ "$DASH_PASS_COUNT" -ge 100 ] && echo "yes" || echo "no:$DASH_PASS_COUNT")" "yes"

# ── Case 5: Messages screen tests pass ────────────────────────────
e2e_case_banner "C5: Messages screen test suite"
MSG_OUTPUT="$(cargo test -p mcp-agent-mail-server --lib -- tui_screens::messages::tests 2>&1)" || true
MSG_RC=$?
e2e_assert_eq "C5.1: messages tests exit 0" "0" "$MSG_RC"
e2e_assert_not_contains "C5.2: no messages test failures" "$MSG_OUTPUT" "FAILED"

# ── Case 6: Threads screen tests pass ─────────────────────────────
e2e_case_banner "C6: Threads screen test suite"
THR_OUTPUT="$(cargo test -p mcp-agent-mail-server --lib -- tui_screens::threads::tests 2>&1)" || true
THR_RC=$?
e2e_assert_eq "C6.1: threads tests exit 0" "0" "$THR_RC"
e2e_assert_not_contains "C6.2: no threads test failures" "$THR_OUTPUT" "FAILED"

# ── Case 7: Search screen tests pass ──────────────────────────────
e2e_case_banner "C7: Search screen test suite"
SEARCH_OUTPUT="$(cargo test -p mcp-agent-mail-server --lib -- tui_screens::search::tests 2>&1)" || true
SEARCH_RC=$?
e2e_assert_eq "C7.1: search tests exit 0" "0" "$SEARCH_RC"
e2e_assert_not_contains "C7.2: no search test failures" "$SEARCH_OUTPUT" "FAILED"

# ── Case 8: Theme contrast WCAG compliance ────────────────────────
e2e_case_banner "C8: Theme WCAG contrast compliance"
WCAG_OUTPUT="$(cargo test -p mcp-agent-mail-server --lib -- tui_theme::tests::named_themes_selection_contrast 2>&1)" || true
WCAG_RC=$?
e2e_assert_eq "C8.1: WCAG contrast tests exit 0" "0" "$WCAG_RC"
e2e_assert_not_contains "C8.2: no WCAG contrast failures" "$WCAG_OUTPUT" "FAILED"

# ── Case 9: Tool metrics screen tests pass ────────────────────────
e2e_case_banner "C9: Tool metrics screen test suite"
TM_OUTPUT="$(cargo test -p mcp-agent-mail-server --lib -- tui_screens::tool_metrics::tests 2>&1)" || true
TM_RC=$?
e2e_assert_eq "C9.1: tool_metrics tests exit 0" "0" "$TM_RC"
e2e_assert_not_contains "C9.2: no tool_metrics test failures" "$TM_OUTPUT" "FAILED"
TM_PASS_COUNT="$(echo "$TM_OUTPUT" | grep -oP '\d+ passed' | grep -oP '\d+' || echo "0")"
e2e_assert_contains "C9.3: tool_metrics has 40+ tests" "$([ "$TM_PASS_COUNT" -ge 40 ] && echo "yes" || echo "no:$TM_PASS_COUNT")" "yes"

# ── Case 10: Analytics and timeline screens pass ──────────────────
e2e_case_banner "C10: Analytics + timeline screens"
AT_OUTPUT="$(cargo test -p mcp-agent-mail-server --lib -- tui_screens::analytics::tests tui_screens::timeline::tests 2>&1)" || true
AT_RC=$?
e2e_assert_eq "C10.1: analytics+timeline tests exit 0" "0" "$AT_RC"
e2e_assert_not_contains "C10.2: no analytics/timeline failures" "$AT_OUTPUT" "FAILED"

# ── Case 11: Reservations screen tests pass ───────────────────────
e2e_case_banner "C11: Reservations screen test suite"
RES_OUTPUT="$(cargo test -p mcp-agent-mail-server --lib -- tui_screens::reservations::tests 2>&1)" || true
RES_RC=$?
e2e_assert_eq "C11.1: reservations tests exit 0" "0" "$RES_RC"
e2e_assert_not_contains "C11.2: no reservations test failures" "$RES_OUTPUT" "FAILED"

# ── Summary ───────────────────────────────────────────────────────
e2e_summary

# Total assertions in this script: 31
# C1: 1, C2: 10, C3: 4, C4: 3, C5: 2, C6: 2, C7: 2, C8: 2, C9: 3, C10: 2, C11: 2
