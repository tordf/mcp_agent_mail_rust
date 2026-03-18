#!/usr/bin/env bash
# test_web_dashboard.sh - E2E test suite for the /web-dashboard browser surface
#
# Verifies:
# - /web-dashboard with valid ?token= returns the HTML shell
# - /web-dashboard without auth returns HTML remediation
# - /web-dashboard/state with valid ?token= returns inactive JSON in headless mode
# - /web-dashboard/state without auth returns JSON 401
# - /web-dashboard/input with valid ?token= returns 503 inactive JSON in headless mode
# - headless fallback telemetry records non-dashboard traffic
# - live PTY-backed TUI publishes a real browser snapshot
# - browser input is accepted and changes the mirrored screen

set -euo pipefail

E2E_SUITE="web_dashboard"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../scripts/e2e_lib.sh
source "${SCRIPT_DIR}/../../scripts/e2e_lib.sh"

e2e_init_artifacts
e2e_banner "Web Dashboard E2E Test Suite"

for cmd in curl python3 script timeout; do
    if ! command -v "${cmd}" >/dev/null 2>&1; then
        e2e_log "${cmd} not found; skipping suite"
        e2e_skip "${cmd} required"
        e2e_summary
        exit 0
    fi
done

WORK="$(e2e_mktemp "e2e_web_dashboard")"
trap 'e2e_stop_server || true' EXIT

HEADLESS_DB_PATH="${WORK}/headless_db.sqlite3"
HEADLESS_STORAGE_ROOT="${WORK}/headless_storage_root"
HEADLESS_TOKEN="web-dashboard-e2e-headless-$(e2e_seeded_hex)"
LIVE_DB_PATH="${WORK}/live_db.sqlite3"
LIVE_STORAGE_ROOT="${WORK}/live_storage_root"
LIVE_TOKEN="web-dashboard-e2e-live-$(e2e_seeded_hex)"
BASE_URL=""
DASH_URL=""
MAIL_URL=""
TOKEN=""

set_dashboard_urls() {
    BASE_URL="${E2E_SERVER_URL%/mcp/}"
    DASH_URL="${BASE_URL}/web-dashboard"
    MAIL_URL="${BASE_URL}/mail"
}

start_headless_dashboard_server() {
    TOKEN="${HEADLESS_TOKEN}"
    if ! e2e_start_server_with_logs "${HEADLESS_DB_PATH}" "${HEADLESS_STORAGE_ROOT}" "web_dashboard_headless" \
        "HTTP_PATH=/api" \
        "HTTP_BEARER_TOKEN=${TOKEN}" \
        "HTTP_REQUEST_LOG_ENABLED=1" \
        "HTTP_ALLOW_LOCALHOST_UNAUTHENTICATED=0" \
        "HTTP_RBAC_ENABLED=0" \
        "HTTP_RATE_LIMIT_ENABLED=0"; then
        return 1
    fi
    set_dashboard_urls
    return 0
}

start_live_dashboard_server() {
    TOKEN="${LIVE_TOKEN}"
    if ! e2e_start_server_with_pty "${LIVE_DB_PATH}" "${LIVE_STORAGE_ROOT}" "web_dashboard_live" \
        "HTTP_PATH=/api" \
        "HTTP_BEARER_TOKEN=${TOKEN}" \
        "TUI_ENABLED=true" \
        "HTTP_ALLOW_LOCALHOST_UNAUTHENTICATED=0" \
        "HTTP_RBAC_ENABLED=0" \
        "HTTP_RATE_LIMIT_ENABLED=0"; then
        return 1
    fi
    set_dashboard_urls
    return 0
}

dash_curl() {
    local case_id="$1"
    local method="$2"
    local url="$3"
    shift 3

    local case_dir="${E2E_ARTIFACT_DIR}/${case_id}"
    local body_file="${case_dir}/body.txt"
    local headers_file="${case_dir}/headers.txt"
    local stderr_file="${case_dir}/stderr.txt"
    local status_file="${case_dir}/status.txt"
    local timing_file="${case_dir}/timing.txt"
    local curl_exit_file="${case_dir}/curl_exit.txt"
    local response_file="${case_dir}/response.json"
    local request_file="${case_dir}/request.json"
    local curl_args_file="${case_dir}/curl_args.txt"
    local curl_output=""
    local curl_rc=0
    local status="000"
    local timing_s="0"
    local timing_ms="0"
    local arg
    local sep=""

    mkdir -p "${case_dir}"

    {
        echo "{"
        echo "  \"case_id\": \"$(_e2e_json_escape "${case_id}")\","
        echo "  \"method\": \"$(_e2e_json_escape "${method}")\","
        echo "  \"url\": \"$(_e2e_json_escape "${url}")\","
        echo "  \"extra_args\": ["
        for arg in "$@"; do
            printf '    %s"%s"\n' "${sep}" "$(_e2e_json_escape "${arg}")"
            sep=","
        done
        echo "  ]"
        echo "}"
    } > "${request_file}"

    {
        printf 'curl -sS -X %q -D %q -o %q -w "%%{http_code}\\n%%{time_total}"' \
            "${method}" "${headers_file}" "${body_file}"
        for arg in "$@"; do
            printf ' %q' "${arg}"
        done
        printf ' %q\n' "${url}"
    } > "${curl_args_file}"

    curl_output="$(curl -sS -X "${method}" -D "${headers_file}" -o "${body_file}" \
        -w "%{http_code}\n%{time_total}" "$@" "${url}" 2>"${stderr_file}")" || curl_rc=$?

    echo "${curl_rc}" > "${curl_exit_file}"
    if [ "${curl_rc}" -eq 0 ]; then
        status="$(printf '%s\n' "${curl_output}" | sed -n '1p')"
        timing_s="$(printf '%s\n' "${curl_output}" | sed -n '2p')"
    fi

    if ! [[ "${status}" =~ ^[0-9]{3}$ ]]; then
        status="000"
    fi

    timing_ms="$(awk -v sec="${timing_s}" 'BEGIN { if (sec == "") sec = 0; printf "%.0f\n", sec * 1000 }' 2>/dev/null || echo "0")"
    if ! [[ "${timing_ms}" =~ ^[0-9]+$ ]]; then
        timing_ms="0"
    fi

    echo "${status}" > "${status_file}"
    echo "${timing_ms}" > "${timing_file}"

    {
        echo "{"
        echo "  \"case_id\": \"$(_e2e_json_escape "${case_id}")\","
        echo "  \"method\": \"$(_e2e_json_escape "${method}")\","
        echo "  \"status\": \"$(_e2e_json_escape "${status}")\","
        echo "  \"timing_ms\": ${timing_ms},"
        echo "  \"curl_exit\": ${curl_rc}"
        echo "}"
    } > "${response_file}"

    echo "${status}"
}

require_observed_http_status() {
    local case_id="$1"
    local status="$2"
    if ! [[ "${status}" =~ ^[0-9]{3}$ ]] || [ "${status}" = "000" ]; then
        e2e_fail "${case_id}: request failed before HTTP response (status=${status})"
    fi
}

dashboard_json_summary() {
    local body_file="$1"
    python3 - "${body_file}" <<'PY'
import json
import sys

data = json.load(open(sys.argv[1], "r", encoding="utf-8"))
if data.get("mode") == "replay" and isinstance(data.get("events"), list) and data["events"]:
    data = data["events"][-1]
print(
    "\t".join(
        [
            str(data.get("mode", "")),
            str(data.get("screen_key", "")),
            str(data.get("screen_title", "")),
            str(data.get("seq", "")),
        ]
    )
)
PY
}

dashboard_wait_for_live_screen() {
    local case_prefix="$1"
    local expected_key="$2"
    local expected_title="$3"
    local timeout_s="${4:-12}"
    local since_seq="${5:-}"
    local attempts=$((timeout_s * 5))
    local attempt=1
    local case_id=""
    local status=""
    local body_file=""
    local mode=""
    local screen_key=""
    local screen_title=""
    local seq=""
    local last_summary="(no response captured)"
    local query_url=""
    local summary=""

    while [ "${attempt}" -le "${attempts}" ]; do
        case_id="$(printf '%s_%02d' "${case_prefix}" "${attempt}")"
        query_url="${DASH_URL}/state?token=${TOKEN}"
        if [ -n "${since_seq}" ]; then
            query_url="${query_url}&since=${since_seq}"
        fi
        status="$(dash_curl "${case_id}" GET "${query_url}")"
        require_observed_http_status "${case_id}" "${status}"
        if [ "${status}" != "200" ]; then
            e2e_fail "${case_id}: expected HTTP 200 while waiting for live screen, got ${status}"
            return 1
        fi
        body_file="${E2E_ARTIFACT_DIR}/${case_id}/body.txt"
        summary="$(dashboard_json_summary "${body_file}")" || return 1
        IFS=$'\t' read -r mode screen_key screen_title seq <<< "${summary}"
        last_summary="mode=${mode} screen_key=${screen_key} screen_title=${screen_title} seq=${seq}"
        if { [ "${mode}" = "snapshot" ] || [ "${mode}" = "delta" ]; } \
            && [ "${screen_key}" = "${expected_key}" ] \
            && [ "${screen_title}" = "${expected_title}" ]; then
            printf '%s\t%s\t%s\t%s\t%s\n' "${case_id}" "${mode}" "${screen_key}" "${screen_title}" "${seq}"
            return 0
        fi
        sleep 0.2
        attempt=$((attempt + 1))
    done

    e2e_fail "${case_prefix}: timed out waiting for live screen ${expected_key}/${expected_title}; last=${last_summary}"
    return 1
}

dashboard_wait_for_live_screen_stream() {
    local case_prefix="$1"
    local expected_key="$2"
    local expected_title="$3"
    local timeout_s="${4:-15}"
    local since_seq="${5:-0}"
    local case_id="${case_prefix}_stream"
    local query_url="${DASH_URL}/stream?token=${TOKEN}&since=${since_seq}&wait_ms=$((timeout_s * 1000))"
    local status=""
    local body_file=""
    local summary=""
    local mode=""
    local screen_key=""
    local screen_title=""
    local seq=""

    status="$(dash_curl "${case_id}" GET "${query_url}")"
    require_observed_http_status "${case_id}" "${status}"
    if [ "${status}" != "200" ]; then
        e2e_fail "${case_id}: expected HTTP 200 from /stream, got ${status}"
        return 1
    fi
    body_file="${E2E_ARTIFACT_DIR}/${case_id}/body.txt"
    summary="$(dashboard_json_summary "${body_file}")" || return 1
    IFS=$'\t' read -r mode screen_key screen_title seq <<< "${summary}"
    if { [ "${mode}" = "snapshot" ] || [ "${mode}" = "delta" ] || [ "${mode}" = "replay" ]; } \
        && [ "${screen_key}" = "${expected_key}" ] \
        && [ "${screen_title}" = "${expected_title}" ]; then
        printf '%s\t%s\t%s\t%s\t%s\n' "${case_id}" "${mode}" "${screen_key}" "${screen_title}" "${seq}"
        return 0
    fi

    e2e_fail "${case_id}: expected ${expected_key}/${expected_title} from /stream, got mode=${mode} screen_key=${screen_key} screen_title=${screen_title} seq=${seq}"
    return 1
}

if ! start_headless_dashboard_server; then
    e2e_fail "headless dashboard server failed to start"
    e2e_summary
    exit 1
fi

e2e_case_banner "Dashboard HTML with valid query token"
e2e_mark_case_start "case01_dashboard_html_token"
STATUS=$(dash_curl "case01_dashboard_html_token" GET "${DASH_URL}?token=${TOKEN}")
require_observed_http_status "case01_dashboard_html_token" "${STATUS}"
e2e_assert_eq "dashboard HTML returns 200" "200" "${STATUS}"
BODY="$(cat "${E2E_ARTIFACT_DIR}/case01_dashboard_html_token/body.txt" 2>/dev/null || true)"
e2e_assert_contains "dashboard shell mentions mirror" "${BODY}" "Browser TUI Mirror"
e2e_assert_contains "dashboard shell references state endpoint" "${BODY}" "/web-dashboard/state"
e2e_assert_contains "dashboard shell exposes Mail UI link" "${BODY}" "Open Mail UI"
e2e_mark_case_end "case01_dashboard_html_token"

e2e_case_banner "Dashboard HTML without auth"
e2e_mark_case_start "case02_dashboard_html_unauthorized"
STATUS=$(dash_curl "case02_dashboard_html_unauthorized" GET "${DASH_URL}")
require_observed_http_status "case02_dashboard_html_unauthorized" "${STATUS}"
e2e_assert_eq "dashboard HTML without auth returns 401" "401" "${STATUS}"
HEADERS="$(cat "${E2E_ARTIFACT_DIR}/case02_dashboard_html_unauthorized/headers.txt" 2>/dev/null || true)"
BODY="$(cat "${E2E_ARTIFACT_DIR}/case02_dashboard_html_unauthorized/body.txt" 2>/dev/null || true)"
if echo "${HEADERS}" | grep -qi "content-type.*text/html"; then
    e2e_pass "dashboard unauthorized route returns HTML"
else
    e2e_fail "dashboard unauthorized route should return HTML"
fi
e2e_assert_contains "dashboard unauthorized page mentions token guidance" "${BODY}" "/web-dashboard"
e2e_mark_case_end "case02_dashboard_html_unauthorized"

e2e_case_banner "Dashboard state with valid token"
e2e_mark_case_start "case03_dashboard_state_token"
STATUS=$(dash_curl "case03_dashboard_state_token" GET "${DASH_URL}/state?token=${TOKEN}")
require_observed_http_status "case03_dashboard_state_token" "${STATUS}"
e2e_assert_eq "dashboard state returns 200" "200" "${STATUS}"
BODY="$(cat "${E2E_ARTIFACT_DIR}/case03_dashboard_state_token/body.txt" 2>/dev/null || true)"
e2e_assert_contains "dashboard state is inactive in headless mode" "${BODY}" '"mode":"inactive"'
e2e_assert_contains "dashboard state carries poll_state payload" "${BODY}" '"poll_state"'
e2e_mark_case_end "case03_dashboard_state_token"

e2e_case_banner "Dashboard stream with valid token while headless"
e2e_mark_case_start "case03b_dashboard_stream_token"
STATUS=$(dash_curl "case03b_dashboard_stream_token" GET "${DASH_URL}/stream?token=${TOKEN}&wait_ms=250")
require_observed_http_status "case03b_dashboard_stream_token" "${STATUS}"
e2e_assert_eq "dashboard stream returns 200" "200" "${STATUS}"
BODY="$(cat "${E2E_ARTIFACT_DIR}/case03b_dashboard_stream_token/body.txt" 2>/dev/null || true)"
e2e_assert_contains "dashboard stream is inactive in headless mode" "${BODY}" '"mode":"inactive"'
e2e_mark_case_end "case03b_dashboard_stream_token"

e2e_case_banner "Dashboard state without auth"
e2e_mark_case_start "case04_dashboard_state_unauthorized"
STATUS=$(dash_curl "case04_dashboard_state_unauthorized" GET "${DASH_URL}/state")
require_observed_http_status "case04_dashboard_state_unauthorized" "${STATUS}"
e2e_assert_eq "dashboard state without auth returns 401" "401" "${STATUS}"
HEADERS="$(cat "${E2E_ARTIFACT_DIR}/case04_dashboard_state_unauthorized/headers.txt" 2>/dev/null || true)"
BODY="$(cat "${E2E_ARTIFACT_DIR}/case04_dashboard_state_unauthorized/body.txt" 2>/dev/null || true)"
if echo "${HEADERS}" | grep -qi "content-type.*application/json"; then
    e2e_pass "dashboard state unauthorized route returns JSON"
else
    e2e_fail "dashboard state unauthorized route should return JSON"
fi
e2e_assert_contains "dashboard state unauthorized response mentions unauthorized" "${BODY}" '"Unauthorized"'
e2e_mark_case_end "case04_dashboard_state_unauthorized"

e2e_case_banner "Dashboard stream without auth"
e2e_mark_case_start "case04b_dashboard_stream_unauthorized"
STATUS=$(dash_curl "case04b_dashboard_stream_unauthorized" GET "${DASH_URL}/stream")
require_observed_http_status "case04b_dashboard_stream_unauthorized" "${STATUS}"
e2e_assert_eq "dashboard stream without auth returns 401" "401" "${STATUS}"
HEADERS="$(cat "${E2E_ARTIFACT_DIR}/case04b_dashboard_stream_unauthorized/headers.txt" 2>/dev/null || true)"
BODY="$(cat "${E2E_ARTIFACT_DIR}/case04b_dashboard_stream_unauthorized/body.txt" 2>/dev/null || true)"
if echo "${HEADERS}" | grep -qi "content-type.*application/json"; then
    e2e_pass "dashboard stream unauthorized route returns JSON"
else
    e2e_fail "dashboard stream unauthorized route should return JSON"
fi
e2e_assert_contains "dashboard stream unauthorized response mentions unauthorized" "${BODY}" '"Unauthorized"'
e2e_mark_case_end "case04b_dashboard_stream_unauthorized"

e2e_case_banner "Dashboard input with valid token while headless"
e2e_mark_case_start "case05_dashboard_input_headless"
STATUS=$(dash_curl "case05_dashboard_input_headless" POST "${DASH_URL}/input?token=${TOKEN}" \
    -H "Content-Type: application/json" \
    --data '{"type":"Input","data":{"kind":"Key","key":"j","modifiers":0}}')
require_observed_http_status "case05_dashboard_input_headless" "${STATUS}"
e2e_assert_eq "dashboard input is unavailable while headless" "503" "${STATUS}"
BODY="$(cat "${E2E_ARTIFACT_DIR}/case05_dashboard_input_headless/body.txt" 2>/dev/null || true)"
e2e_assert_contains "dashboard input reports inactive status" "${BODY}" '"status":"inactive"'
e2e_mark_case_end "case05_dashboard_input_headless"

e2e_case_banner "Headless fallback telemetry captures non-dashboard traffic"
e2e_mark_case_start "case06_dashboard_headless_fallback"
STATUS=$(dash_curl "case06_dashboard_headless_fallback_before" GET "${DASH_URL}/state?token=${TOKEN}")
require_observed_http_status "case06_dashboard_headless_fallback_before" "${STATUS}"
STATUS=$(dash_curl "case06_mail_api_probe" GET "${MAIL_URL}/api/locks?token=${TOKEN}")
require_observed_http_status "case06_mail_api_probe" "${STATUS}"
e2e_assert_eq "headless fallback mail API probe returns 200" "200" "${STATUS}"
STATUS=$(dash_curl "case06_dashboard_headless_fallback" GET "${DASH_URL}/state?token=${TOKEN}")
require_observed_http_status "case06_dashboard_headless_fallback" "${STATUS}"
python3 - "${E2E_ARTIFACT_DIR}/case06_dashboard_headless_fallback_before/body.txt" "${E2E_ARTIFACT_DIR}/case06_dashboard_headless_fallback/body.txt" <<'PY'
import json
import sys

before_path, after_path = sys.argv[1], sys.argv[2]
before = json.load(open(before_path, "r", encoding="utf-8"))
data = json.load(open(after_path, "r", encoding="utf-8"))
before_poll_state = before.get("poll_state") or {}
poll_state = data.get("poll_state") or {}
before_counters = before_poll_state.get("request_counters") or {}
counters = poll_state.get("request_counters") or {}
events = poll_state.get("events") or []
before_total = before_counters.get("total", 0)
total = counters.get("total", 0)
if total != before_total + 1:
    raise SystemExit(
        f"expected exactly one additional counted non-dashboard request, got before={before_total} after={total}"
    )
if not events:
    raise SystemExit("expected fallback events to contain at least one item")
paths = [event.get("path") for event in events if isinstance(event, dict)]
if "/mail/api/locks" not in paths:
    raise SystemExit(f"expected fallback events to retain the mail API probe, got {paths}")
if any(path in {"/web-dashboard/state", "/web-dashboard/input"} for path in paths if path):
    raise SystemExit(f"dashboard polling/input should be suppressed from fallback telemetry, got {paths}")
PY
e2e_pass "headless fallback request counters and events are populated"
e2e_mark_case_end "case06_dashboard_headless_fallback"

e2e_stop_server || true

if ! start_live_dashboard_server; then
    e2e_fail "live dashboard server failed to start"
    e2e_summary
    exit 1
fi

e2e_case_banner "Live dashboard publishes a real TUI snapshot"
e2e_mark_case_start "case07_dashboard_live_snapshot"
STATUS=$(dash_curl "case07_dashboard_html_token" GET "${DASH_URL}?token=${TOKEN}")
require_observed_http_status "case07_dashboard_html_token" "${STATUS}"
e2e_assert_eq "live dashboard HTML returns 200" "200" "${STATUS}"
LIVE_RESULT="$(dashboard_wait_for_live_screen "case07_dashboard_live_snapshot_poll" "dashboard" "Dashboard" 15)"
IFS=$'\t' read -r LIVE_CASE_ID LIVE_MODE LIVE_SCREEN_KEY LIVE_SCREEN_TITLE LIVE_SEQ <<< "${LIVE_RESULT}"
e2e_assert_eq "live dashboard first frame is a snapshot or delta on Dashboard" "dashboard" "${LIVE_SCREEN_KEY}"
e2e_assert_eq "live dashboard title is Dashboard" "Dashboard" "${LIVE_SCREEN_TITLE}"
e2e_assert_contains "live dashboard frame advertises screen_key" "$(cat "${E2E_ARTIFACT_DIR}/${LIVE_CASE_ID}/body.txt" 2>/dev/null || true)" '"screen_key":"dashboard"'
e2e_mark_case_end "case07_dashboard_live_snapshot"

e2e_case_banner "Live dashboard accepts browser input"
e2e_mark_case_start "case08_dashboard_live_input"
STATUS=$(dash_curl "case08_dashboard_live_input" POST "${DASH_URL}/input?token=${TOKEN}" \
    -H "Content-Type: application/json" \
    --data '{"type":"Input","data":{"kind":"Key","key":"2","modifiers":0}}')
require_observed_http_status "case08_dashboard_live_input" "${STATUS}"
e2e_assert_eq "live dashboard input returns 202" "202" "${STATUS}"
BODY="$(cat "${E2E_ARTIFACT_DIR}/case08_dashboard_live_input/body.txt" 2>/dev/null || true)"
e2e_assert_contains "live dashboard input reports accepted status" "${BODY}" '"status":"accepted"'
e2e_assert_contains "live dashboard input accepts exactly one event" "${BODY}" '"accepted":1'
e2e_assert_contains "live dashboard input reports queue depth" "${BODY}" '"queue_depth"'
e2e_mark_case_end "case08_dashboard_live_input"

e2e_case_banner "Live dashboard stream wakes up on the Messages screen after browser input"
e2e_mark_case_start "case09_dashboard_live_messages"
LIVE_RESULT="$(dashboard_wait_for_live_screen_stream "case09_dashboard_live_messages" "messages" "Messages" 15 "${LIVE_SEQ}")"
IFS=$'\t' read -r LIVE_CASE_ID LIVE_MODE LIVE_SCREEN_KEY LIVE_SCREEN_TITLE LIVE_SEQ <<< "${LIVE_RESULT}"
e2e_assert_eq "live dashboard screen changes to Messages" "messages" "${LIVE_SCREEN_KEY}"
e2e_assert_eq "live dashboard title changes to Messages" "Messages" "${LIVE_SCREEN_TITLE}"
BODY="$(cat "${E2E_ARTIFACT_DIR}/${LIVE_CASE_ID}/body.txt" 2>/dev/null || true)"
if [[ "${BODY}" == *'"mode":"delta"'* ]] || [[ "${BODY}" == *'"mode":"snapshot"'* ]] || [[ "${BODY}" == *'"mode":"replay"'* ]]; then
    e2e_pass "live dashboard stream returns delta, snapshot, or replay"
else
    e2e_fail "live dashboard stream should publish a delta, snapshot, or replay"
fi
e2e_assert_contains "live dashboard change advertises Messages screen key" "${BODY}" '"screen_key":"messages"'
e2e_mark_case_end "case09_dashboard_live_messages"

e2e_summary
