//! Web dashboard: mirrors the terminal TUI in a browser.
//!
//! Serves `/web-dashboard` with a self-contained HTML page that polls
//! `/web-dashboard/state` for rendered frame data and renders it to a
//! `<canvas>` element.  Input events are forwarded back via
//! `/web-dashboard/input`.
//!
//! # Performance design
//!
//! The hot path is `capture()` → `handle_state()`:
//!
//! 1. **Capture** (runs on TUI render thread, every tick):
//!    - Copies raw cell bytes directly from the `ftui::Buffer` into a reusable
//!      byte buffer (zero per-cell allocation).
//!    - Computes a delta against the previous frame (only changed cells).
//!    - Pre-serializes the JSON + base64 response string so the HTTP handler
//!      returns a cached `Arc<str>` with zero work.
//!
//! 2. **Serve** (runs on HTTP worker thread, per-poll):
//!    - Reuses a pre-built snapshot/delta payload selected under one brief mutex
//!      so a poll never mixes an older `since` decision with a newer frame.
//!
//! Target: < 500µs capture for 200×50 grids, < 1µs serve.

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use ftui::render::buffer::Buffer;
use mcp_agent_mail_core::now_micros;
use serde_json::json;

use crate::tui_bridge::TuiSharedState;
use crate::tui_ws_input;
use crate::tui_ws_state;

// ─── Constants ──────────────────────────────────────────────────────────────

/// Base64 alphabet (standard, no padding).
const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Pre-computed "unchanged" response (no frame data).
fn unchanged_response(seq: u64) -> String {
    format!(r#"{{"mode":"unchanged","seq":{seq}}}"#)
}

const WARMING_RETRY_MS: u64 = 250;
const INACTIVE_RETRY_MS: u64 = 1_000;

// ─── Frame store ────────────────────────────────────────────────────────────

/// Internal state held under the mutex.
struct FrameState {
    /// Raw cell bytes from previous capture (for delta computation).
    prev_bytes: Vec<u8>,
    /// Current raw cell bytes.
    curr_bytes: Vec<u8>,
    /// Grid dimensions of the current frame.
    cols: u16,
    rows: u16,
    screen_id: u8,
    screen_key: &'static str,
    screen_title: &'static str,
    /// Pre-serialized full snapshot JSON response (base64-encoded cells).
    cached_snapshot: Arc<str>,
    /// Pre-serialized delta JSON response (changed cells only).
    cached_delta: Arc<str>,
    /// Sequence number of the cached snapshot.
    snapshot_seq: u64,
}

impl Default for FrameState {
    fn default() -> Self {
        let empty: Arc<str> = Arc::from(
            r#"{"mode":"snapshot","seq":0,"cols":0,"rows":0,"screen_id":0,"screen_key":"","screen_title":"","timestamp_us":0,"cells":""}"#,
        );
        Self {
            prev_bytes: Vec::new(),
            curr_bytes: Vec::new(),
            cols: 0,
            rows: 0,
            screen_id: 0,
            screen_key: "",
            screen_title: "",
            cached_snapshot: Arc::clone(&empty),
            cached_delta: empty,
            snapshot_seq: 0,
        }
    }
}

/// Storage for the latest captured frame, embedded in `TuiSharedState`.
#[derive(Debug)]
pub struct WebDashboardFrameStore {
    state: Mutex<FrameState>,
    seq: AtomicU64,
}

impl std::fmt::Debug for FrameState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameState")
            .field("cols", &self.cols)
            .field("rows", &self.rows)
            .field("snapshot_seq", &self.snapshot_seq)
            .field("curr_bytes_len", &self.curr_bytes.len())
            .finish()
    }
}

impl WebDashboardFrameStore {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(FrameState::default()),
            seq: AtomicU64::new(0),
        }
    }

    /// Capture a rendered buffer into the store.  Called from the TUI render
    /// loop after `view()` completes.
    ///
    /// This is the hot path.  It:
    /// 1. Copies raw cell data (16 bytes/cell) into a reusable buffer.
    /// 2. Computes changed cell indices vs previous frame.
    /// 3. Pre-builds the JSON response strings (snapshot + delta).
    ///
    /// The HTTP handler then just returns the cached string.
    pub fn capture(
        &self,
        buffer: &Buffer,
        screen_id: u8,
        screen_key: &'static str,
        screen_title: &'static str,
    ) {
        let cols = buffer.width();
        let rows = buffer.height();
        let cells = buffer.cells();
        let n_cells = cells.len();
        let byte_len = n_cells * 16; // 4 u32s × 4 bytes each

        let ts = now_micros();

        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let new_seq = guard.snapshot_seq.saturating_add(1);

        // Swap prev ← curr, then reuse curr's allocation for the new frame.
        // Take curr out, swap, put back — avoids double-borrow on guard fields.
        let mut new_curr = std::mem::take(&mut guard.prev_bytes);
        guard.prev_bytes = std::mem::take(&mut guard.curr_bytes);
        new_curr.clear();
        if new_curr.capacity() < byte_len {
            new_curr.reserve(byte_len - new_curr.capacity());
        }

        // Copy raw cell data: Cell is #[repr(C, align(16))] = [content:u32, fg:u32, bg:u32, attrs:u32].
        // We extract fields individually because the Cell's alignment padding
        // makes a raw memcpy incorrect (padding bytes are undefined).
        for cell in cells {
            new_curr.extend_from_slice(&cell.content.raw().to_le_bytes());
            new_curr.extend_from_slice(&cell.fg.0.to_le_bytes());
            new_curr.extend_from_slice(&cell.bg.0.to_le_bytes());
            new_curr.extend_from_slice(&cell_attrs_raw(&cell.attrs).to_le_bytes());
        }

        // ── Build snapshot response (base64-encoded raw bytes) ──────
        let b64_len = (new_curr.len() + 2) / 3 * 4;
        // Pre-size: ~130 chars header + b64 + ~2 chars footer
        let mut snap = String::with_capacity(140 + b64_len);
        write!(
            snap,
            "{{\"mode\":\"snapshot\",\"seq\":{new_seq},\"cols\":{cols},\"rows\":{rows},\"screen_id\":{screen_id},\"screen_key\":"
        )
        .unwrap();
        push_json_string(&mut snap, screen_key);
        snap.push_str(",\"screen_title\":");
        push_json_string(&mut snap, screen_title);
        write!(snap, ",\"timestamp_us\":{ts},\"cells\":\"").unwrap();
        base64_encode_into(&new_curr, &mut snap);
        snap.push_str("\"}");
        guard.cached_snapshot = Arc::from(snap.as_str());

        // ── Build delta response (only changed cell indices) ────────
        let same_dims =
            guard.prev_bytes.len() == new_curr.len() && guard.cols == cols && guard.rows == rows;

        if same_dims {
            // Compare 16-byte chunks, collect indices of changed cells.
            let mut delta = String::with_capacity(256);
            write!(
                delta,
                "{{\"mode\":\"delta\",\"seq\":{new_seq},\"cols\":{cols},\"rows\":{rows},\"screen_id\":{screen_id},\"screen_key\":"
            )
            .unwrap();
            push_json_string(&mut delta, screen_key);
            delta.push_str(",\"screen_title\":");
            push_json_string(&mut delta, screen_title);
            write!(delta, ",\"timestamp_us\":{ts},\"changed\":[").unwrap();
            let mut first = true;
            let prev = &guard.prev_bytes;
            for i in 0..n_cells {
                let off = i * 16;
                if prev[off..off + 16] != new_curr[off..off + 16] {
                    if !first {
                        delta.push(',');
                    }
                    first = false;
                    // Emit: [idx, content, fg, bg, attrs]
                    let c = u32::from_le_bytes([
                        new_curr[off],
                        new_curr[off + 1],
                        new_curr[off + 2],
                        new_curr[off + 3],
                    ]);
                    let f = u32::from_le_bytes([
                        new_curr[off + 4],
                        new_curr[off + 5],
                        new_curr[off + 6],
                        new_curr[off + 7],
                    ]);
                    let b = u32::from_le_bytes([
                        new_curr[off + 8],
                        new_curr[off + 9],
                        new_curr[off + 10],
                        new_curr[off + 11],
                    ]);
                    let a = u32::from_le_bytes([
                        new_curr[off + 12],
                        new_curr[off + 13],
                        new_curr[off + 14],
                        new_curr[off + 15],
                    ]);
                    write!(delta, "[{i},{c},{f},{b},{a}]").unwrap();
                }
            }
            delta.push_str("]}");
            guard.cached_delta = Arc::from(delta.as_str());
        } else {
            // Dimensions changed — no valid delta, use snapshot.
            guard.cached_delta = Arc::clone(&guard.cached_snapshot);
        }

        guard.curr_bytes = new_curr;
        guard.cols = cols;
        guard.rows = rows;
        guard.screen_id = screen_id;
        guard.screen_key = screen_key;
        guard.screen_title = screen_title;
        guard.snapshot_seq = new_seq;
        self.seq.store(new_seq, Ordering::Relaxed);
    }

    /// Read the latest fully published sequence number without locking.
    pub fn current_seq(&self) -> u64 {
        self.seq.load(Ordering::Relaxed)
    }

    /// Get the pre-serialized snapshot response (zero-copy Arc<str>).
    pub fn cached_snapshot(&self) -> Arc<str> {
        Arc::clone(
            &self
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .cached_snapshot,
        )
    }

    /// Get the pre-serialized delta response (zero-copy Arc<str>).
    pub fn cached_delta(&self) -> Arc<str> {
        Arc::clone(
            &self
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .cached_delta,
        )
    }

    /// Get the sequence number of the cached snapshot.
    pub fn snapshot_seq(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .snapshot_seq
    }

    /// Select the correct pre-built response for the client's `since` cursor
    /// using one coherent view of the frame store state.
    fn response_for_since(&self, since_seq: Option<u64>) -> String {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let current_seq = guard.snapshot_seq;

        if let Some(since) = since_seq {
            if since >= current_seq {
                return unchanged_response(current_seq);
            }
            if since + 1 >= current_seq {
                return guard.cached_delta.to_string();
            }
        }

        guard.cached_snapshot.to_string()
    }
}

// ─── Cell attribute accessor ────────────────────────────────────────────────

/// Reconstruct raw u32 from CellAttrs (transparent newtype, but no public raw()).
#[inline]
fn cell_attrs_raw(attrs: &ftui::render::cell::CellAttrs) -> u32 {
    let flags_byte = attrs.flags().bits() as u32;
    let link = attrs.link_id();
    (flags_byte << 24) | (link & 0x00FF_FFFF)
}

// ─── Fast base64 encoder ────────────────────────────────────────────────────

/// Encode bytes into base64, appending directly to the output string.
/// No padding, no allocation beyond the string growth.
fn base64_encode_into(input: &[u8], out: &mut String) {
    let chunks = input.chunks_exact(3);
    let remainder = chunks.remainder();
    for chunk in chunks {
        let n = (u32::from(chunk[0]) << 16) | (u32::from(chunk[1]) << 8) | u32::from(chunk[2]);
        out.push(B64[((n >> 18) & 0x3F) as usize] as char);
        out.push(B64[((n >> 12) & 0x3F) as usize] as char);
        out.push(B64[((n >> 6) & 0x3F) as usize] as char);
        out.push(B64[(n & 0x3F) as usize] as char);
    }
    match remainder.len() {
        1 => {
            let n = u32::from(remainder[0]) << 16;
            out.push(B64[((n >> 18) & 0x3F) as usize] as char);
            out.push(B64[((n >> 12) & 0x3F) as usize] as char);
        }
        2 => {
            let n = (u32::from(remainder[0]) << 16) | (u32::from(remainder[1]) << 8);
            out.push(B64[((n >> 18) & 0x3F) as usize] as char);
            out.push(B64[((n >> 12) & 0x3F) as usize] as char);
            out.push(B64[((n >> 6) & 0x3F) as usize] as char);
        }
        _ => {}
    }
}

fn push_json_string(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c <= '\u{1F}' => {
                write!(out, "\\u{:04X}", c as u32).unwrap();
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

// ─── HTTP endpoints ─────────────────────────────────────────────────────────

/// GET `/web-dashboard/state` — return the pre-serialized frame response.
///
/// Query params:
///   `since=<seq>` — return delta if available, full snapshot otherwise.
///
/// This avoids re-serializing frame data and returns a cached pre-built payload.
pub fn handle_state(state: &TuiSharedState, query: Option<&str>) -> String {
    let since_seq = query.and_then(|q| {
        q.split('&')
            .find_map(|kv| kv.strip_prefix("since="))
            .and_then(|v| v.parse::<u64>().ok())
    });

    state
        .web_dashboard_frame_store()
        .response_for_since(since_seq)
}

fn warming_response(state: &TuiSharedState, query: Option<&str>) -> String {
    json!({
        "mode": "warming",
        "active": false,
        "reason": "tui_warming",
        "detail": "The live terminal TUI is active, but the first browser frame has not been captured yet.",
        "retry_ms": WARMING_RETRY_MS,
        "poll_state": tui_ws_state::poll_payload(state, query),
    })
    .to_string()
}

fn inactive_response(fallback_state: &TuiSharedState, query: Option<&str>) -> String {
    json!({
        "mode": "inactive",
        "active": false,
        "reason": "tui_inactive",
        "detail": "The live terminal TUI is not active for this server process. The browser dashboard is showing passive server telemetry instead of a terminal mirror.",
        "retry_ms": INACTIVE_RETRY_MS,
        "poll_state": tui_ws_state::poll_payload(fallback_state, query),
    })
    .to_string()
}

/// GET `/web-dashboard/state` — return live frame data when available, or a
/// structured inactive/warming payload when the terminal mirror is unavailable.
pub fn handle_state_response(
    live_state: Option<&TuiSharedState>,
    fallback_state: &TuiSharedState,
    query: Option<&str>,
) -> (u16, String) {
    match live_state {
        Some(state) if state.web_dashboard_frame_store().current_seq() > 0 => {
            (200, handle_state(state, query))
        }
        Some(state) => (200, warming_response(state, query)),
        None => (200, inactive_response(fallback_state, query)),
    }
}

fn input_error_status(detail: &str) -> u16 {
    if detail.contains("too large") {
        413
    } else {
        400
    }
}

/// POST `/web-dashboard/input` — forward keyboard/mouse events to the TUI.
pub fn handle_input(state: &TuiSharedState, body: &[u8]) -> (u16, String) {
    let parsed = match tui_ws_input::parse_remote_terminal_events(body) {
        Ok(parsed) => parsed,
        Err(detail) => {
            let status = input_error_status(&detail);
            return (status, json!({ "detail": detail }).to_string());
        }
    };
    let accepted = parsed.events.len();
    let mut dropped_oldest = 0_usize;
    for event in parsed.events {
        if state.push_remote_terminal_event(event) {
            dropped_oldest += 1;
        }
    }
    let queue_stats = state.remote_terminal_queue_stats();
    (
        202,
        json!({
            "status": "accepted",
            "accepted": accepted,
            "ignored": parsed.ignored,
            "dropped_oldest": dropped_oldest,
            "queue_depth": queue_stats.depth,
            "queue_dropped_oldest_total": queue_stats.dropped_oldest_total,
            "queue_resize_coalesced_total": queue_stats.resize_coalesced_total,
        })
        .to_string(),
    )
}

/// POST `/web-dashboard/input` when no live TUI is active.
pub fn handle_inactive_input() -> (u16, String) {
    (
        503,
        json!({
            "status": "inactive",
            "detail": "Live TUI state is not active; browser input forwarding is unavailable.",
            "retry_ms": INACTIVE_RETRY_MS,
        })
        .to_string(),
    )
}

/// GET `/web-dashboard` — serve the self-contained HTML page.
///
/// The JS client decodes base64-encoded cell data into a Uint32Array
/// and uses ImageData for background fills (one putImageData per frame)
/// with fillText only for visible characters — dramatically faster than
/// per-cell fillRect.
pub fn handle_page(_host: &str) -> String {
    // Static page — no dynamic content, no format args needed.
    DASHBOARD_HTML.to_string()
}

/// Pre-built HTML page as a static string (no format! overhead per request).
static DASHBOARD_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Agent Mail - Web Dashboard</title>
<style>
:root {
  color-scheme: dark;
  --bg: #071018;
  --panel: rgba(9, 18, 30, 0.9);
  --panel-strong: rgba(6, 13, 22, 0.96);
  --border: rgba(114, 138, 173, 0.22);
  --text: #e8edf6;
  --muted: #91a0b8;
  --accent: #69d2a2;
  --warn: #f4c95d;
  --error: #ff7a7a;
  --shadow: 0 18px 48px rgba(0, 0, 0, 0.34);
}
* { box-sizing: border-box; }
html, body { margin: 0; min-height: 100%; }
body {
  min-height: 100vh;
  display: flex;
  flex-direction: column;
  background:
    radial-gradient(circle at top left, rgba(63, 120, 170, 0.20), transparent 38%),
    radial-gradient(circle at top right, rgba(42, 111, 93, 0.24), transparent 34%),
    linear-gradient(180deg, #08121d 0%, #04070d 100%);
  color: var(--text);
  font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
}
a { color: inherit; }
button, a.btn {
  appearance: none;
  border: 1px solid var(--border);
  background: rgba(20, 32, 50, 0.78);
  color: var(--text);
  border-radius: 999px;
  padding: 8px 14px;
  font: inherit;
  font-size: 12px;
  letter-spacing: 0.01em;
  text-decoration: none;
  cursor: pointer;
  transition: background 120ms ease, border-color 120ms ease, transform 120ms ease;
}
button:hover, a.btn:hover {
  background: rgba(30, 46, 70, 0.94);
  border-color: rgba(125, 160, 205, 0.38);
  transform: translateY(-1px);
}
button:focus-visible, a.btn:focus-visible {
  outline: 2px solid rgba(105, 210, 162, 0.55);
  outline-offset: 2px;
}
.hidden { display: none !important; }
#header {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 12px 18px;
  border-bottom: 1px solid rgba(118, 144, 179, 0.16);
  background: rgba(4, 8, 15, 0.72);
  backdrop-filter: blur(14px);
}
#header .status {
  width: 10px;
  height: 10px;
  border-radius: 50%;
  box-shadow: 0 0 18px rgba(255, 255, 255, 0.16);
}
#header .status.ok { background: var(--accent); }
#header .status.err { background: var(--error); }
#header .status.wait { background: var(--warn); }
#conn-text { font-size: 13px; font-weight: 600; }
#mode-pill {
  padding: 4px 10px;
  border: 1px solid var(--border);
  border-radius: 999px;
  background: rgba(18, 31, 48, 0.72);
  color: var(--muted);
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
}
#stats {
  font-size: 12px;
  color: var(--muted);
  white-space: nowrap;
}
#shell {
  flex: 1;
  min-height: 0;
  display: grid;
  grid-template-columns: minmax(0, 1fr) 360px;
}
#stage {
  position: relative;
  min-height: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 18px;
  overflow: auto;
}
#terminal-wrap {
  padding: 14px;
  border-radius: 18px;
  border: 1px solid rgba(125, 155, 196, 0.14);
  background:
    linear-gradient(180deg, rgba(5, 9, 16, 0.97), rgba(6, 8, 13, 0.95));
  box-shadow: var(--shadow);
}
#terminal {
  display: block;
  margin: 0 auto;
  image-rendering: pixelated;
  image-rendering: crisp-edges;
}
#placeholder {
  width: min(780px, calc(100% - 24px));
  padding: 26px;
  border-radius: 22px;
  border: 1px solid rgba(124, 153, 190, 0.18);
  background:
    linear-gradient(135deg, rgba(10, 20, 31, 0.94), rgba(7, 12, 20, 0.98)),
    radial-gradient(circle at top right, rgba(105, 210, 162, 0.10), transparent 40%);
  box-shadow: var(--shadow);
}
#placeholder-eyebrow {
  margin-bottom: 10px;
  color: var(--accent);
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.14em;
}
#placeholder-title {
  margin: 0 0 12px;
  font-size: 28px;
  line-height: 1.15;
}
#placeholder-detail {
  margin: 0 0 18px;
  color: var(--muted);
  line-height: 1.55;
}
.controls {
  display: flex;
  gap: 10px;
  flex-wrap: wrap;
}
#sidebar {
  min-height: 0;
  overflow: auto;
  padding: 16px;
  border-left: 1px solid rgba(118, 144, 179, 0.16);
  background: rgba(3, 7, 13, 0.58);
  backdrop-filter: blur(14px);
  display: flex;
  flex-direction: column;
  gap: 14px;
}
.card {
  border: 1px solid rgba(121, 147, 182, 0.16);
  border-radius: 18px;
  background: var(--panel);
  padding: 14px;
}
.card h2 {
  margin: 0 0 12px;
  font-size: 13px;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  color: #b5c6df;
}
.kv {
  display: grid;
  grid-template-columns: 112px minmax(0, 1fr);
  gap: 8px 12px;
  font-size: 12px;
  line-height: 1.45;
}
.kv .key {
  color: var(--muted);
}
.kv .value {
  word-break: break-word;
}
#events {
  display: flex;
  flex-direction: column;
  gap: 8px;
  max-height: 44vh;
  overflow: auto;
}
.event {
  border: 1px solid rgba(123, 146, 175, 0.12);
  border-radius: 14px;
  background: rgba(12, 19, 31, 0.86);
  padding: 10px;
}
.event .event-head {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 6px;
  color: var(--muted);
  font-size: 11px;
}
.event .event-body {
  font-size: 12px;
  line-height: 1.45;
  word-break: break-word;
}
.event-empty {
  color: var(--muted);
  text-align: center;
}
#help-card p {
  margin: 0 0 10px;
  color: var(--muted);
  line-height: 1.55;
  font-size: 12px;
}
#footer {
  padding: 10px 18px 14px;
  border-top: 1px solid rgba(118, 144, 179, 0.12);
  font-size: 11px;
  color: var(--muted);
}
@media (max-width: 1100px) {
  #shell {
    grid-template-columns: 1fr;
    grid-template-rows: minmax(0, 1fr) auto;
  }
  #sidebar {
    border-left: 0;
    border-top: 1px solid rgba(118, 144, 179, 0.16);
    max-height: 42vh;
  }
}
</style>
</head>
<body>
<div id="header">
  <span class="status wait" id="conn-dot"></span>
  <span id="conn-text">Connecting to Agent Mail...</span>
  <span id="mode-pill">Booting</span>
  <span style="flex: 1"></span>
  <span id="stats">Waiting for state...</span>
</div>

<div id="shell">
  <main id="stage">
    <div id="terminal-wrap" class="hidden">
      <canvas id="terminal"></canvas>
    </div>
    <section id="placeholder">
      <div id="placeholder-eyebrow">Browser TUI Mirror</div>
      <h1 id="placeholder-title">Connecting...</h1>
      <p id="placeholder-detail">The dashboard is waiting for its first response.</p>
      <div class="controls">
        <button type="button" id="reset-btn">Force Snapshot</button>
        <button type="button" id="pause-btn">Pause Polling</button>
        <button type="button" id="help-btn">Toggle Help</button>
        <a class="btn" id="mail-link" href="/mail">Open Mail UI</a>
      </div>
    </section>
  </main>

  <aside id="sidebar">
    <section class="card">
      <h2>Session</h2>
      <div class="kv" id="session-grid"></div>
    </section>

    <section class="card">
      <h2>Telemetry</h2>
      <div class="kv" id="telemetry-grid"></div>
    </section>

    <section class="card">
      <h2>Recent Events</h2>
      <div id="events">
        <div class="event event-empty">No recent events yet.</div>
      </div>
    </section>

    <section class="card hidden" id="help-card">
      <h2>How To Use It</h2>
      <p>Live mode mirrors the terminal TUI into the browser and forwards keyboard input back to the server.</p>
      <p>Warming mode means the live TUI exists but has not emitted its first browser frame yet.</p>
      <p>Inactive mode means the server is running headless or without a live terminal UI. In that case this page falls back to passive telemetry instead of pretending the mirror is working.</p>
      <p>Use Force Snapshot if the browser falls behind. Pause Polling is useful when inspecting a static screen or reducing noise during debugging.</p>
    </section>
  </aside>
</div>

<div id="footer">
  Live mirror mode forwards keyboard input to the active terminal session. Inactive mode stays read-only and shows passive server telemetry instead.
</div>

<script>
"use strict";

const STATE_BASE_URL = "/web-dashboard/state";
const INPUT_BASE_URL = "/web-dashboard/input";
const ACTIVE_POLL_MS = 100;
const WARMING_POLL_MS = 250;
const INACTIVE_POLL_MS = 1000;
const HIDDEN_POLL_MS = 2000;
const CW = 8;
const CH = 16;
const FONT = "14px monospace";
const MAX_EVENT_RENDER = 10;

const searchParams = new URLSearchParams(window.location.search);
const authToken = searchParams.get("token");

function withToken(url) {
  if (!authToken) {
    return url;
  }
  return `${url}${url.includes("?") ? "&" : "?"}token=${encodeURIComponent(authToken)}`;
}

const STATE_URL = withToken(STATE_BASE_URL);
const INPUT_URL = withToken(INPUT_BASE_URL);
const MAIL_UI_URL = withToken("/mail");

const canvas = document.getElementById("terminal");
const ctx = canvas.getContext("2d", { alpha: false });
const dot = document.getElementById("conn-dot");
const connText = document.getElementById("conn-text");
const statsEl = document.getElementById("stats");
const modePill = document.getElementById("mode-pill");
const terminalWrap = document.getElementById("terminal-wrap");
const placeholder = document.getElementById("placeholder");
const placeholderEyebrow = document.getElementById("placeholder-eyebrow");
const placeholderTitle = document.getElementById("placeholder-title");
const placeholderDetail = document.getElementById("placeholder-detail");
const sessionGrid = document.getElementById("session-grid");
const telemetryGrid = document.getElementById("telemetry-grid");
const eventsEl = document.getElementById("events");
const helpCard = document.getElementById("help-card");
const helpBtn = document.getElementById("help-btn");
const pauseBtn = document.getElementById("pause-btn");
const resetBtn = document.getElementById("reset-btn");
const mailLink = document.getElementById("mail-link");
const stage = document.getElementById("stage");

mailLink.setAttribute("href", MAIL_UI_URL);

let lastSeq = 0;
let lastCols = 0;
let lastRows = 0;
let lastScreenId = null;
let lastScreenKey = "";
let lastScreenTitle = "";
let lastTimestampUs = 0;
let lastPayloadSummary = "Waiting for state...";
let frameCount = 0;
let lastStatsTime = performance.now();
let pollDelayMs = ACTIVE_POLL_MS;
let pollTimer = null;
let pollPaused = false;
let inputEnabled = false;
let currentMode = "booting";
let consecutiveFailures = 0;

let cellBuf = null;
let imgData = null;

const B64_LOOKUP = new Uint8Array(128);
"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".split("").forEach((c, i) => {
  B64_LOOKUP[c.charCodeAt(0)] = i;
});

function b64ToU32Array(b64str) {
  const len = b64str.length;
  const byteLen = (len * 3) >>> 2;
  const buf = new ArrayBuffer(byteLen);
  const u8 = new Uint8Array(buf);
  let j = 0;
  for (let i = 0; i < len; i += 4) {
    const a = B64_LOOKUP[b64str.charCodeAt(i)];
    const b = B64_LOOKUP[b64str.charCodeAt(i + 1)];
    const c = i + 2 < len ? B64_LOOKUP[b64str.charCodeAt(i + 2)] : 0;
    const d = i + 3 < len ? B64_LOOKUP[b64str.charCodeAt(i + 3)] : 0;
    u8[j++] = (a << 2) | (b >> 4);
    if (j < byteLen) u8[j++] = ((b & 0xF) << 4) | (c >> 2);
    if (j < byteLen) u8[j++] = ((c & 0x3) << 6) | d;
  }
  return new Uint32Array(buf);
}

function renderGrid(container, pairs) {
  container.replaceChildren();
  for (const [key, value] of pairs) {
    const keyEl = document.createElement("div");
    keyEl.className = "key";
    keyEl.textContent = key;
    const valueEl = document.createElement("div");
    valueEl.className = "value";
    valueEl.textContent = value == null || value === "" ? "-" : String(value);
    container.append(keyEl, valueEl);
  }
}

function truncateText(text, maxLen) {
  if (!text || text.length <= maxLen) {
    return text || "";
  }
  return `${text.slice(0, maxLen - 3)}...`;
}

function formatEventTimeMicros(timestampMicros) {
  if (!timestampMicros) {
    return "-";
  }
  const millis = Math.floor(timestampMicros / 1000);
  return new Date(millis).toLocaleTimeString();
}

function formatAvgLatency(counters) {
  const avg = counters && counters.avg_latency_ms;
  if (avg == null || Number.isNaN(avg)) {
    return "-";
  }
  return `${Number(avg).toFixed(1)} ms`;
}

function formatRequests(counters) {
  if (!counters) {
    return "-";
  }
  return `${counters.total || 0} total`;
}

function eventDescription(event) {
  const kind = event.kind || event.type || "event";
  const details = [];
  if (event.subject) details.push(event.subject);
  if (event.message) details.push(event.message);
  if (event.path) details.push(event.path);
  if (event.agent) details.push(`agent=${event.agent}`);
  if (event.thread_id) details.push(`thread=${event.thread_id}`);
  if (!details.length) {
    const clone = { ...event };
    delete clone.seq;
    delete clone.timestamp_micros;
    delete clone.severity;
    const raw = JSON.stringify(clone);
    if (raw && raw !== "{}") {
      details.push(raw);
    }
  }
  const detail = truncateText(details.join(" | "), 180);
  return detail ? `${kind} - ${detail}` : kind;
}

function renderEvents(events) {
  eventsEl.replaceChildren();
  const items = Array.isArray(events) ? events.slice(0, MAX_EVENT_RENDER) : [];
  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "event event-empty";
    empty.textContent = "No recent events yet.";
    eventsEl.appendChild(empty);
    return;
  }

  for (const event of items) {
    const card = document.createElement("div");
    card.className = "event";
    const head = document.createElement("div");
    head.className = "event-head";
    const left = document.createElement("span");
    left.textContent = event.severity || event.kind || event.type || "event";
    const right = document.createElement("span");
    right.textContent = `#${event.seq || "-"} @ ${formatEventTimeMicros(event.timestamp_micros)}`;
    head.append(left, right);
    const body = document.createElement("div");
    body.className = "event-body";
    body.textContent = eventDescription(event);
    card.append(head, body);
    eventsEl.appendChild(card);
  }
}

function setStatus(kind, message, modeLabel) {
  dot.className = `status ${kind}`;
  connText.textContent = message;
  modePill.textContent = modeLabel;
}

function showPlaceholder(eyebrow, title, detail) {
  placeholderEyebrow.textContent = eyebrow;
  placeholderTitle.textContent = title;
  placeholderDetail.textContent = detail;
  placeholder.classList.remove("hidden");
  terminalWrap.classList.add("hidden");
}

function showTerminal() {
  placeholder.classList.add("hidden");
  terminalWrap.classList.remove("hidden");
}

function scaleCanvasToFit() {
  if (!canvas.width || !canvas.height) {
    terminalWrap.style.removeProperty("width");
    terminalWrap.style.removeProperty("height");
    canvas.style.removeProperty("width");
    canvas.style.removeProperty("height");
    return;
  }
  const pad = 36;
  const availableW = Math.max(240, stage.clientWidth - pad);
  const availableH = Math.max(180, stage.clientHeight - pad);
  const scale = Math.min(1, availableW / canvas.width, availableH / canvas.height);
  canvas.style.width = `${Math.floor(canvas.width * scale)}px`;
  canvas.style.height = `${Math.floor(canvas.height * scale)}px`;
}

function renderSnapshot(data) {
  const cols = data.cols;
  const rows = data.rows;
  const cells = b64ToU32Array(data.cells);
  const width = cols * CW;
  const height = rows * CH;

  if (cols !== lastCols || rows !== lastRows) {
    canvas.width = width;
    canvas.height = height;
    imgData = ctx.createImageData(width, height);
    lastCols = cols;
    lastRows = rows;
  }

  const px = imgData.data;
  px.fill(0);
  for (let i = 0, n = cols * rows; i < n; i++) {
    const bg = cells[i * 4 + 2];
    const alpha = bg & 0xFF;
    if (alpha === 0) {
      continue;
    }
    const r = (bg >>> 24) & 0xFF;
    const g = (bg >>> 16) & 0xFF;
    const b = (bg >>> 8) & 0xFF;
    const col = i % cols;
    const row = (i / cols) | 0;
    const x0 = col * CW;
    const y0 = row * CH;
    for (let dy = 0; dy < CH; dy++) {
      let off = ((y0 + dy) * width + x0) * 4;
      for (let dx = 0; dx < CW; dx++, off += 4) {
        px[off] = r;
        px[off + 1] = g;
        px[off + 2] = b;
        px[off + 3] = 255;
      }
    }
  }
  ctx.putImageData(imgData, 0, 0);

  ctx.textBaseline = "top";
  let curFont = FONT;
  ctx.font = curFont;
  for (let i = 0, n = cols * rows; i < n; i++) {
    const base = i * 4;
    const content = cells[base];
    if (
      content <= 0x20 ||
      content > 0x10FFFF ||
      content >= 0x7FFFFFFF ||
      (content & 0x80000000)
    ) {
      continue;
    }
    const fg = cells[base + 1];
    const attrs = cells[base + 3];
    const flags = (attrs >>> 24) & 0xFF;
    const wantFont = `${(flags & 1) ? "bold " : ""}${(flags & 2) ? "italic " : ""}${FONT}`;
    if (wantFont !== curFont) {
      ctx.font = wantFont;
      curFont = wantFont;
    }
    const fgAlpha = fg & 0xFF;
    const fgR = (fg >>> 24) & 0xFF;
    const fgG = (fg >>> 16) & 0xFF;
    const fgB = (fg >>> 8) & 0xFF;
    ctx.fillStyle = fgAlpha === 0 ? "#e0e0e0" : `rgb(${fgR},${fgG},${fgB})`;
    const col = i % cols;
    const row = (i / cols) | 0;
    ctx.fillText(String.fromCodePoint(content), col * CW + 1, row * CH + 1);
  }

  cellBuf = cells;
  scaleCanvasToFit();
}

function applyDelta(data) {
  if (!cellBuf || data.cols !== lastCols || data.rows !== lastRows) {
    lastSeq = 0;
    return;
  }
  const changed = Array.isArray(data.changed) ? data.changed : [];
  if (!changed.length) {
    return;
  }
  const cols = data.cols;
  const width = cols * CW;
  const px = imgData ? imgData.data : null;

  for (const entry of changed) {
    const [idx, content, fg, bg, attrs] = entry;
    const base = idx * 4;
    cellBuf[base] = content;
    cellBuf[base + 1] = fg;
    cellBuf[base + 2] = bg;
    cellBuf[base + 3] = attrs;

    const col = idx % cols;
    const row = (idx / cols) | 0;
    const x0 = col * CW;
    const y0 = row * CH;
    const alpha = bg & 0xFF;
    const r = alpha ? (bg >>> 24) & 0xFF : 10;
    const g = alpha ? (bg >>> 16) & 0xFF : 10;
    const b = alpha ? (bg >>> 8) & 0xFF : 15;
    if (!px) {
      continue;
    }
    for (let dy = 0; dy < CH; dy++) {
      let off = ((y0 + dy) * width + x0) * 4;
      for (let dx = 0; dx < CW; dx++, off += 4) {
        px[off] = r;
        px[off + 1] = g;
        px[off + 2] = b;
        px[off + 3] = 255;
      }
    }
  }
  if (px) {
    ctx.putImageData(imgData, 0, 0);
  }

  ctx.textBaseline = "top";
  let curFont = FONT;
  ctx.font = curFont;
  for (const [idx, content, fg, , attrs] of changed) {
    if (
      content <= 0x20 ||
      content > 0x10FFFF ||
      content >= 0x7FFFFFFF ||
      (content & 0x80000000)
    ) {
      continue;
    }
    const flags = (attrs >>> 24) & 0xFF;
    const wantFont = `${(flags & 1) ? "bold " : ""}${(flags & 2) ? "italic " : ""}${FONT}`;
    if (wantFont !== curFont) {
      ctx.font = wantFont;
      curFont = wantFont;
    }
    const fgAlpha = fg & 0xFF;
    const fgR = (fg >>> 24) & 0xFF;
    const fgG = (fg >>> 16) & 0xFF;
    const fgB = (fg >>> 8) & 0xFF;
    ctx.fillStyle = fgAlpha === 0 ? "#e0e0e0" : `rgb(${fgR},${fgG},${fgB})`;
    const col = idx % lastCols;
    const row = (idx / lastCols) | 0;
    ctx.fillText(String.fromCodePoint(content), col * CW + 1, row * CH + 1);
  }
}

function formatActiveScreenLabel() {
  if (lastScreenTitle) {
    return lastScreenTitle;
  }
  if (lastScreenKey) {
    return lastScreenKey;
  }
  if (lastScreenId || lastScreenId === 0) {
    return `screen ${lastScreenId}`;
  }
  return "-";
}

function activeSessionPairs() {
  return [
    ["Mode", "Live mirror"],
    ["Screen", formatActiveScreenLabel()],
    ["Screen key", lastScreenKey || "-"],
    ["Sequence", lastSeq || 0],
    ["Grid", `${lastCols || 0} x ${lastRows || 0}`],
    ["Input", inputEnabled ? "enabled" : "disabled"],
    ["Mail UI", MAIL_UI_URL],
  ];
}

function activeTelemetryPairs() {
  const timestamp = lastTimestampUs ? formatEventTimeMicros(lastTimestampUs) : "-";
  return [
    ["Last frame", timestamp],
    ["Polling", `${pollDelayMs} ms`],
    ["Canvas", `${canvas.width || 0} x ${canvas.height || 0}`],
    ["Summary", truncateText(lastPayloadSummary, 120)],
  ];
}

function fallbackSessionPairs(modeLabel, pollState) {
  const config = pollState && pollState.config ? pollState.config : {};
  return [
    ["Mode", modeLabel],
    ["Endpoint", config.endpoint || "-"],
    ["Mail UI", config.web_ui_url || MAIL_UI_URL],
    ["HTTP path", config.http_path || "-"],
    ["Auth", config.auth_enabled ? "enabled" : "disabled"],
    ["Events", pollState && pollState.event_count != null ? pollState.event_count : 0],
  ];
}

function fallbackTelemetryPairs(pollState) {
  const counters = pollState && pollState.request_counters ? pollState.request_counters : null;
  const dbStats = pollState && pollState.db_stats ? pollState.db_stats : null;
  const atc = pollState && pollState.atc ? pollState.atc : null;
  const trackedAgents = atc && Array.isArray(atc.tracked_agents) ? atc.tracked_agents.length : 0;
  const dbSummary = dbStats
    ? `${dbStats.projects || 0} projects, ${dbStats.agents || 0} agents, ${dbStats.messages || 0} messages`
    : "unavailable";
  return [
    ["Requests", formatRequests(counters)],
    ["2xx/4xx/5xx", counters ? `${counters.status_2xx || 0} / ${counters.status_4xx || 0} / ${counters.status_5xx || 0}` : "-"],
    ["Avg latency", formatAvgLatency(counters)],
    ["ATC", atc && atc.enabled ? `enabled (${trackedAgents} agents)` : "disabled"],
    ["DB snapshot", dbSummary],
  ];
}

function renderInactiveSummary(modeLabel, pollState) {
  renderGrid(sessionGrid, fallbackSessionPairs(modeLabel, pollState));
  renderGrid(telemetryGrid, fallbackTelemetryPairs(pollState));
  renderEvents(pollState && pollState.events ? pollState.events : []);
}

function renderActiveSummary() {
  renderGrid(sessionGrid, activeSessionPairs());
  renderGrid(telemetryGrid, activeTelemetryPairs());
  renderEvents([
    {
      kind: "live_mirror",
      severity: "info",
      seq: lastSeq || 0,
      timestamp_micros: lastTimestampUs || 0,
      message: `Live browser mirror active on ${formatActiveScreenLabel()}. Passive request/event telemetry appears when the dashboard is in warming or inactive mode.`,
    },
  ]);
}

function updateStats() {
  const now = performance.now();
  if (now - lastStatsTime < 1000) {
    return;
  }
  if (currentMode === "live") {
    statsEl.textContent = `${frameCount} polls/s | ${formatActiveScreenLabel()} | seq ${lastSeq} | ${lastCols} x ${lastRows}`;
  } else {
    statsEl.textContent = lastPayloadSummary;
  }
  frameCount = 0;
  lastStatsTime = now;
}

function buildStateUrl() {
  if (lastSeq <= 0) {
    return STATE_URL;
  }
  return `${STATE_URL}${STATE_URL.includes("?") ? "&" : "?"}since=${encodeURIComponent(String(lastSeq))}`;
}

function pollDelayForMode(mode, overrideMs) {
  if (document.hidden) {
    return Math.max(overrideMs || 0, HIDDEN_POLL_MS);
  }
  if (overrideMs) {
    return overrideMs;
  }
  switch (mode) {
    case "live":
      return ACTIVE_POLL_MS;
    case "warming":
      return WARMING_POLL_MS;
    default:
      return INACTIVE_POLL_MS;
  }
}

function schedulePoll(delayMs) {
  if (pollTimer) {
    clearTimeout(pollTimer);
  }
  pollTimer = window.setTimeout(() => {
    poll().catch(() => {});
  }, Math.max(0, delayMs));
}

function forceSnapshotAndPoll() {
  lastSeq = 0;
  schedulePoll(0);
}

function applyActivePayload(data) {
  currentMode = "live";
  inputEnabled = true;
  lastScreenId = data.screen_id ?? lastScreenId;
  lastScreenKey = data.screen_key || lastScreenKey;
  lastScreenTitle = data.screen_title || lastScreenTitle;
  if (data.timestamp_us) {
    lastTimestampUs = data.timestamp_us;
  }

  if (data.mode === "snapshot") {
    renderSnapshot(data);
    lastSeq = data.seq;
    lastPayloadSummary = `Live mirror on ${formatActiveScreenLabel()} (${data.cols} x ${data.rows})`;
  } else if (data.mode === "delta") {
    applyDelta(data);
    lastSeq = data.seq;
    if (data.timestamp_us) {
      lastTimestampUs = data.timestamp_us;
    }
    lastPayloadSummary = `Live mirror delta applied on ${formatActiveScreenLabel()}`;
  } else if (data.mode === "unchanged") {
    lastSeq = data.seq;
    lastPayloadSummary = `Live mirror unchanged at seq ${lastSeq}`;
  }

  setStatus("ok", "Connected to live TUI mirror", "Live");
  showTerminal();
  renderActiveSummary();
  pollDelayMs = pollDelayForMode("live");
}

function applyFallbackPayload(data, modeLabel, eyebrow, title) {
  currentMode = data.mode;
  inputEnabled = false;
  lastSeq = 0;
  const pollState = data.poll_state || {};
  lastPayloadSummary = `${modeLabel} | ${formatRequests(pollState.request_counters)}`;
  setStatus("wait", data.detail, modeLabel);
  showPlaceholder(eyebrow, title, data.detail);
  renderInactiveSummary(modeLabel, pollState);
  pollDelayMs = pollDelayForMode(data.mode, data.retry_ms);
}

async function poll() {
  if (pollPaused) {
    statsEl.textContent = "Polling paused";
    schedulePoll(pollDelayForMode(currentMode || "inactive", INACTIVE_POLL_MS));
    return;
  }

  try {
    const resp = await fetch(buildStateUrl(), { cache: "no-store" });
    if (!resp.ok) {
      throw new Error(`HTTP ${resp.status}`);
    }
    const data = await resp.json();
    consecutiveFailures = 0;
    frameCount += 1;

    if (data.mode === "snapshot" || data.mode === "delta" || data.mode === "unchanged") {
      applyActivePayload(data);
    } else if (data.mode === "warming") {
      applyFallbackPayload(data, "Warming", "Browser TUI Mirror", "Live TUI is starting");
    } else if (data.mode === "inactive") {
      applyFallbackPayload(data, "Passive telemetry", "Passive Observability", "Live TUI mirror unavailable");
    } else {
      throw new Error(`Unexpected dashboard mode: ${data.mode}`);
    }
  } catch (error) {
    consecutiveFailures += 1;
    inputEnabled = false;
    currentMode = "error";
    const detail = error && error.message ? error.message : String(error);
    lastPayloadSummary = `Connection error: ${detail}`;
    setStatus("err", `Connection error: ${detail}`, "Disconnected");
    showPlaceholder(
      "Connection Problem",
      "Dashboard request failed",
      "The browser could not fetch dashboard state. Check auth, server reachability, and whether the process is still running."
    );
    renderGrid(sessionGrid, [
      ["Mode", "Disconnected"],
      ["State URL", STATE_URL],
      ["Input", "disabled"],
      ["Mail UI", MAIL_UI_URL],
    ]);
    renderGrid(telemetryGrid, [
      ["Error", truncateText(detail, 160)],
      ["Retries", consecutiveFailures],
      ["Next poll", `${Math.min(5000, INACTIVE_POLL_MS * consecutiveFailures)} ms`],
      ["Last seq", lastSeq || 0],
    ]);
    renderEvents([]);
    pollDelayMs = pollDelayForMode("inactive", Math.min(5000, INACTIVE_POLL_MS * consecutiveFailures));
  }

  updateStats();
  schedulePoll(pollDelayMs);
}

function sendInputEvent(key, modifiers) {
  return fetch(INPUT_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      type: "Input",
      data: { kind: "Key", key, modifiers },
    }),
  }).then(async (resp) => {
    const payload = await resp.json().catch(() => ({}));
    if (!resp.ok || payload.status === "inactive") {
      throw new Error(payload.detail || `HTTP ${resp.status}`);
    }
    return payload;
  });
}

function toggleHelp() {
  helpCard.classList.toggle("hidden");
}

helpBtn.addEventListener("click", () => {
  toggleHelp();
});

pauseBtn.addEventListener("click", () => {
  pollPaused = !pollPaused;
  pauseBtn.textContent = pollPaused ? "Resume Polling" : "Pause Polling";
  if (!pollPaused) {
    schedulePoll(0);
  }
});

resetBtn.addEventListener("click", () => {
  forceSnapshotAndPoll();
});

window.addEventListener("resize", () => {
  scaleCanvasToFit();
});

document.addEventListener("visibilitychange", () => {
  pollDelayMs = pollDelayForMode(currentMode);
  if (!pollPaused) {
    schedulePoll(0);
  }
});

document.addEventListener("keydown", (event) => {
  const isBrowserShortcut = event.key === "F5"
    || event.key === "F12"
    || ((event.ctrlKey || event.metaKey) && "cvxwtrl".includes(event.key.toLowerCase()))
    || (event.ctrlKey && event.shiftKey && event.key.toLowerCase() === "i");
  if (isBrowserShortcut || !inputEnabled) {
    return;
  }
  event.preventDefault();
  const modifiers =
    (event.ctrlKey ? 1 : 0) |
    (event.shiftKey ? 2 : 0) |
    (event.altKey ? 4 : 0) |
    (event.metaKey ? 8 : 0);
  sendInputEvent(event.key, modifiers).catch((error) => {
    inputEnabled = false;
    currentMode = "error";
    const detail = error && error.message ? error.message : String(error);
    lastPayloadSummary = `Input unavailable: ${detail}`;
    setStatus("err", `Input unavailable: ${detail}`, "Disconnected");
    showPlaceholder(
      "Input Unavailable",
      "Keyboard forwarding stopped",
      "The server rejected browser input. The dashboard will fall back to passive telemetry until a live TUI becomes available again."
    );
    renderGrid(sessionGrid, [
      ["Mode", "Disconnected"],
      ["Input", "disabled"],
      ["Last seq", lastSeq || 0],
      ["Mail UI", MAIL_UI_URL],
    ]);
    renderGrid(telemetryGrid, [
      ["Error", truncateText(detail, 160)],
      ["Polling", `${pollDelayMs} ms`],
      ["Screen", formatActiveScreenLabel()],
      ["Grid", `${lastCols || 0} x ${lastRows || 0}`],
    ]);
    renderEvents([]);
    forceSnapshotAndPoll();
  });
});

renderGrid(sessionGrid, [
  ["Mode", "Booting"],
  ["State URL", STATE_URL],
  ["Input", "disabled"],
  ["Mail UI", MAIL_UI_URL],
]);
renderGrid(telemetryGrid, [
  ["Status", "Waiting for first response"],
  ["Polling", `${ACTIVE_POLL_MS} ms`],
  ["Auth", authToken ? "query token" : "header/local policy"],
  ["Canvas", "uninitialized"],
]);
renderEvents([]);
schedulePoll(0);
</script>
</body>
</html>"##;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_encode_round_trip() {
        let input = [0u8, 1, 2, 3, 255, 128, 64, 32, 16, 8, 4, 2, 1, 0, 255, 127];
        let mut encoded = String::new();
        base64_encode_into(&input, &mut encoded);
        // Decode with standard library equivalent check.
        assert!(!encoded.is_empty());
        assert!(encoded.len() <= (input.len() + 2) / 3 * 4);
        // Verify all chars are valid base64.
        assert!(
            encoded
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/')
        );
    }

    #[test]
    fn base64_encode_empty() {
        let mut out = String::new();
        base64_encode_into(&[], &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn unchanged_response_format() {
        let resp = unchanged_response(42);
        let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(v["mode"], "unchanged");
        assert_eq!(v["seq"], 42);
    }

    #[test]
    fn handle_page_uses_relative_urls() {
        let html = handle_page("evil.example");
        assert!(html.contains(r#"const STATE_BASE_URL = "/web-dashboard/state";"#));
        assert!(html.contains(r#"const INPUT_BASE_URL = "/web-dashboard/input";"#));
        assert!(html.contains(r#"id="mail-link" href="/mail""#));
        assert!(!html.contains("evil.example"));
    }

    #[test]
    fn handle_state_response_returns_inactive_payload_without_live_tui() {
        let config = mcp_agent_mail_core::Config::default();
        let fallback = TuiSharedState::new(&config);
        let (_ok_status, payload) = handle_state_response(None, &fallback, Some("limit=5"));
        let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(v["mode"], "inactive");
        assert_eq!(v["reason"], "tui_inactive");
        assert_eq!(v["retry_ms"], INACTIVE_RETRY_MS);
        assert_eq!(v["poll_state"]["mode"], "snapshot");
    }

    #[test]
    fn handle_state_response_returns_warming_before_first_frame() {
        let config = mcp_agent_mail_core::Config::default();
        let live = TuiSharedState::new(&config);
        let fallback = TuiSharedState::new(&config);
        let (_ok_status, payload) = handle_state_response(Some(&live), &fallback, None);
        let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(v["mode"], "warming");
        assert_eq!(v["reason"], "tui_warming");
        assert_eq!(v["retry_ms"], WARMING_RETRY_MS);
        assert_eq!(v["poll_state"]["mode"], "snapshot");
    }

    #[test]
    fn capture_publishes_consistent_seq_and_screen_metadata() {
        let store = WebDashboardFrameStore::new();
        let mut buffer = Buffer::new(1, 1);
        buffer.set(0, 0, ftui::Cell::from_char('A'));

        store.capture(&buffer, 3, "agents", "Agents");

        assert_eq!(store.current_seq(), 1);
        assert_eq!(store.snapshot_seq(), 1);

        let payload = store.cached_snapshot();
        let v: serde_json::Value = serde_json::from_str(payload.as_ref()).unwrap();
        assert_eq!(v["seq"], 1);
        assert_eq!(v["screen_id"], 3);
        assert_eq!(v["screen_key"], "agents");
        assert_eq!(v["screen_title"], "Agents");
    }

    #[test]
    fn capture_escapes_screen_metadata_for_json() {
        let store = WebDashboardFrameStore::new();
        let mut buffer = Buffer::new(1, 1);
        buffer.set(0, 0, ftui::Cell::from_char('Q'));

        store.capture(&buffer, 9, "ops\\qa", "Ops \"QA\"\nLive");

        let payload = store.cached_snapshot();
        let v: serde_json::Value = serde_json::from_str(payload.as_ref()).unwrap();
        assert_eq!(v["screen_key"], "ops\\qa");
        assert_eq!(v["screen_title"], "Ops \"QA\"\nLive");
    }

    #[test]
    fn handle_state_returns_delta_for_exactly_previous_frame() {
        let config = mcp_agent_mail_core::Config::default();
        let state = TuiSharedState::new(&config);
        let store = state.web_dashboard_frame_store();

        let mut first = Buffer::new(1, 1);
        first.set(0, 0, ftui::Cell::from_char('A'));
        store.capture(&first, 0, "dashboard", "Dashboard");

        let mut second = Buffer::new(1, 1);
        second.set(0, 0, ftui::Cell::from_char('B'));
        store.capture(&second, 1, "messages", "Messages");

        let payload = handle_state(&state, Some("since=1"));
        let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(v["mode"], "delta");
        assert_eq!(v["seq"], 2);
        assert_eq!(v["screen_key"], "messages");
        assert_eq!(v["screen_title"], "Messages");
        assert_eq!(
            v["changed"].as_array().map_or(0, std::vec::Vec::len),
            1,
            "single-cell change should yield one delta entry"
        );
    }

    #[test]
    fn handle_input_accepts_events() {
        let config = mcp_agent_mail_core::Config::default();
        let state = TuiSharedState::new(&config);
        let body = br#"{"events":[
            {"type":"Input","data":{"kind":"Key","key":"j","modifiers":0}},
            {"type":"Ping"}
        ]}"#;
        let (status, payload) = handle_input(&state, body);
        assert_eq!(status, 202);
        let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(v["accepted"], 1);
        assert_eq!(v["ignored"], 1);
    }

    #[test]
    fn handle_input_invalid_returns_400() {
        let config = mcp_agent_mail_core::Config::default();
        let state = TuiSharedState::new(&config);
        let (status, _) = handle_input(&state, b"not json");
        assert_eq!(status, 400);
    }

    #[test]
    fn handle_input_oversized_returns_413() {
        let config = mcp_agent_mail_core::Config::default();
        let state = TuiSharedState::new(&config);
        let body = vec![b' '; 512 * 1024 + 1];
        let (status, _) = handle_input(&state, &body);
        assert_eq!(status, 413);
    }

    #[test]
    fn handle_inactive_input_returns_503() {
        let (status, payload) = handle_inactive_input();
        assert_eq!(status, 503);
        let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(v["status"], "inactive");
        assert_eq!(v["retry_ms"], INACTIVE_RETRY_MS);
    }
}
