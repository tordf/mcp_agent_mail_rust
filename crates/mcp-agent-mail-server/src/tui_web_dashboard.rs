//! Web dashboard: mirrors the terminal TUI in a browser.
//!
//! Serves `/web-dashboard` with a self-contained HTML page that polls
//! `/web-dashboard/state` for rendered frame data and renders it to a
//! `<canvas>` element.  Input events are forwarded back via
//! `/web-dashboard/input`.
//!
//! # Architecture
//!
//! The terminal TUI renders each tick into an `ftui::Buffer`.  After each
//! render, the buffer's cells are serialized into a compact JSON payload
//! stored in [`WebDashboardFrame`] inside [`TuiSharedState`].  The HTTP
//! endpoints read this shared state to serve snapshots/deltas to browsers.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use ftui::render::buffer::Buffer;
use mcp_agent_mail_core::now_micros;
use serde_json::{Value, json};

use crate::tui_bridge::TuiSharedState;
use crate::tui_ws_input;

// ─── Frame capture ──────────────────────────────────────────────────────────

/// Packed representation of one rendered frame, ready for JSON serialization.
#[derive(Debug, Clone)]
pub struct WebDashboardFrame {
    /// Sequence number, incremented on each capture.
    pub seq: u64,
    /// Terminal grid width.
    pub cols: u16,
    /// Terminal grid height.
    pub rows: u16,
    /// Microsecond timestamp of capture.
    pub timestamp_us: i64,
    /// Screen identifier (tab index).
    pub screen_id: u8,
    /// Per-cell data: for each cell, 4 values [content_u32, fg_u32, bg_u32, attrs_u32].
    /// Length = cols * rows * 4.
    pub cells: Vec<u32>,
}

impl Default for WebDashboardFrame {
    fn default() -> Self {
        Self {
            seq: 0,
            cols: 0,
            rows: 0,
            timestamp_us: 0,
            screen_id: 0,
            cells: Vec::new(),
        }
    }
}

/// Storage for the latest captured frame, embedded in `TuiSharedState`.
#[derive(Debug)]
pub struct WebDashboardFrameStore {
    frame: Mutex<WebDashboardFrame>,
    seq: AtomicU64,
}

impl WebDashboardFrameStore {
    pub fn new() -> Self {
        Self {
            frame: Mutex::new(WebDashboardFrame::default()),
            seq: AtomicU64::new(0),
        }
    }

    /// Capture a rendered buffer into the store.  Called from the TUI render
    /// loop after `view()` completes.
    pub fn capture(&self, buffer: &Buffer, screen_id: u8) {
        let cols = buffer.width();
        let rows = buffer.height();
        let cells_slice = buffer.cells();
        let n = cells_slice.len();

        let mut packed = Vec::with_capacity(n * 4);
        for cell in cells_slice {
            packed.push(cell.content.raw());
            packed.push(cell.fg.0);
            packed.push(cell.bg.0);
            packed.push(cell.attrs.raw());
        }

        let new_seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let mut guard = self.frame.lock().unwrap_or_else(|e| e.into_inner());
        *guard = WebDashboardFrame {
            seq: new_seq,
            cols,
            rows,
            timestamp_us: now_micros(),
            screen_id,
            cells: packed,
        };
    }

    /// Read the current sequence number without locking.
    pub fn current_seq(&self) -> u64 {
        self.seq.load(Ordering::Relaxed)
    }

    /// Clone the latest frame (snapshot).
    pub fn snapshot(&self) -> WebDashboardFrame {
        self.frame.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

// ─── Cell attribute accessor ────────────────────────────────────────────────

/// CellAttrs is a transparent newtype around u32; expose raw() for packing.
trait CellAttrsRaw {
    fn raw(self) -> u32;
}

impl CellAttrsRaw for ftui::render::cell::CellAttrs {
    fn raw(self) -> u32 {
        // CellAttrs is #[repr(transparent)] over u32.  Access the inner
        // value via transmute-free bit extraction.  The public API exposes
        // flags() and link_id() but not the raw u32, so we reconstruct it.
        let flags_byte = self.flags().bits() as u32;
        let link = self.link_id();
        (flags_byte << 24) | (link & 0x00FF_FFFF)
    }
}

// ─── HTTP endpoints ─────────────────────────────────────────────────────────

/// GET `/web-dashboard/state` — return the latest frame as JSON.
///
/// Query params:
///   `since=<seq>` — if the client already has this seq, return 304-equivalent
///                    empty delta to save bandwidth.
pub fn handle_state(state: &TuiSharedState, query: Option<&str>) -> String {
    let since_seq = query.and_then(|q| {
        q.split('&')
            .find_map(|kv| kv.strip_prefix("since="))
            .and_then(|v| v.parse::<u64>().ok())
    });

    let store = state.web_dashboard_frame_store();
    let current_seq = store.current_seq();

    // If client is up-to-date, return a minimal "no change" response.
    if let Some(since) = since_seq {
        if since >= current_seq {
            return json!({
                "mode": "unchanged",
                "seq": current_seq,
            })
            .to_string();
        }
    }

    let frame = store.snapshot();

    // Encode cells as a flat JSON array of u32 values.
    // For 80×24 = 1920 cells × 4 u32s = 7680 numbers.
    let cells_json: Vec<Value> = frame.cells.iter().map(|&v| Value::from(v)).collect();

    json!({
        "mode": "snapshot",
        "seq": frame.seq,
        "cols": frame.cols,
        "rows": frame.rows,
        "screen_id": frame.screen_id,
        "timestamp_us": frame.timestamp_us,
        "cells": cells_json,
    })
    .to_string()
}

fn input_error_status(detail: &str) -> u16 {
    if detail.contains("too large") {
        413
    } else {
        400
    }
}

/// POST `/web-dashboard/input` — forward keyboard/mouse events to the TUI.
///
/// Reuses the same JSON format as `/mail/ws-input`.
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

/// GET `/web-dashboard` — serve the self-contained HTML page.
pub fn handle_page(_host: &str) -> String {
    format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Agent Mail — Web Dashboard</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ background: #0a0a0f; color: #e0e0e0; font-family: monospace; overflow: hidden; }}
#header {{ padding: 4px 12px; background: #16161e; border-bottom: 1px solid #2a2a3a;
           display: flex; align-items: center; gap: 12px; font-size: 13px; }}
#header .status {{ width: 8px; height: 8px; border-radius: 50%; }}
#header .status.ok {{ background: #50fa7b; }}
#header .status.err {{ background: #ff5555; }}
#header .status.wait {{ background: #f1fa8c; }}
#terminal {{ display: block; margin: 0 auto; image-rendering: pixelated; }}
#info {{ position: fixed; bottom: 4px; right: 8px; font-size: 11px; color: #666; }}
</style>
</head>
<body>
<div id="header">
  <span class="status wait" id="conn-dot"></span>
  <span id="conn-text">Connecting…</span>
  <span style="flex:1"></span>
  <span id="stats"></span>
</div>
<canvas id="terminal"></canvas>
<div id="info">Agent Mail Web Dashboard — polling /web-dashboard/state</div>

<script>
"use strict";
const STATE_URL = "/web-dashboard/state";
const INPUT_URL = "/web-dashboard/input";
const POLL_INTERVAL_MS = 100;   // 10 Hz
const CELL_W = 8;              // pixels per cell width
const CELL_H = 16;             // pixels per cell height
const FONT = "14px monospace";

const canvas = document.getElementById("terminal");
const ctx = canvas.getContext("2d");
const dot = document.getElementById("conn-dot");
const connText = document.getElementById("conn-text");
const statsEl = document.getElementById("stats");

let lastSeq = 0;
let frameCount = 0;
let lastFrameTime = performance.now();
let lastCols = 0;
let lastRows = 0;

// ── Color decoding ──────────────────────────────────────────────
function unpackRgba(u32) {{
  return [
    (u32 >>> 24) & 0xFF,        // r
    (u32 >>> 16) & 0xFF,        // g
    (u32 >>> 8)  & 0xFF,        // b
    u32 & 0xFF,                 // a
  ];
}}

function rgbaCss(u32) {{
  const [r, g, b, a] = unpackRgba(u32);
  if (a === 0) return null;     // transparent
  return `rgb(${{r}},${{g}},${{b}})`;
}}

// ── Render frame to canvas ──────────────────────────────────────
function renderFrame(data) {{
  const cols = data.cols;
  const rows = data.rows;
  const cells = data.cells;     // flat array: [content, fg, bg, attrs, content, fg, bg, attrs, ...]

  // Only resize canvas when dimensions change (avoids clearing context state).
  if (cols !== lastCols || rows !== lastRows) {{
    canvas.width = cols * CELL_W;
    canvas.height = rows * CELL_H;
    lastCols = cols;
    lastRows = rows;
  }}
  ctx.font = FONT;
  ctx.textBaseline = "top";

  // Clear
  ctx.fillStyle = "#0a0a0f";
  ctx.fillRect(0, 0, canvas.width, canvas.height);

  for (let i = 0; i < cols * rows; i++) {{
    const base = i * 4;
    const content = cells[base];
    const fg = cells[base + 1];
    const bg = cells[base + 2];
    const attrs = cells[base + 3];

    const col = i % cols;
    const row = Math.floor(i / cols);
    const x = col * CELL_W;
    const y = row * CELL_H;

    // Draw background
    const bgCss = rgbaCss(bg);
    if (bgCss) {{
      ctx.fillStyle = bgCss;
      ctx.fillRect(x, y, CELL_W, CELL_H);
    }}

    // Draw character
    if (content > 0x20 && content < 0x7FFFFFFF && !(content & 0x80000000)) {{
      const ch = String.fromCodePoint(content);
      const fgCss = rgbaCss(fg) || "#e0e0e0";

      // Bold check (bit 24 of attrs = flags byte, bit 0 of flags = bold)
      const flags = (attrs >>> 24) & 0xFF;
      const bold = flags & 1;
      const italic = flags & 2;
      ctx.font = (bold ? "bold " : "") + (italic ? "italic " : "") + FONT;

      ctx.fillStyle = fgCss;
      ctx.fillText(ch, x + 1, y + 1);
    }}
  }}

  // FPS counter
  frameCount++;
  const now = performance.now();
  if (now - lastFrameTime > 1000) {{
    statsEl.textContent = `${{frameCount}} fps | seq ${{data.seq}} | ${{cols}}×${{rows}}`;
    frameCount = 0;
    lastFrameTime = now;
  }}
}}

// ── Polling loop ────────────────────────────────────────────────
async function poll() {{
  try {{
    const url = lastSeq > 0
      ? `${{STATE_URL}}?since=${{lastSeq}}`
      : STATE_URL;
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`HTTP ${{resp.status}}`);
    const data = await resp.json();

    dot.className = "status ok";
    connText.textContent = "Connected";

    if (data.mode === "snapshot") {{
      renderFrame(data);
      lastSeq = data.seq;
    }} else if (data.mode === "unchanged") {{
      lastSeq = data.seq;
    }}
  }} catch (e) {{
    dot.className = "status err";
    connText.textContent = "Disconnected: " + e.message;
  }}
}}

// Use setTimeout chaining to prevent overlapping polls on slow networks.
function schedulePoll() {{
  setTimeout(async () => {{
    await poll();
    schedulePoll();
  }}, POLL_INTERVAL_MS);
}}
schedulePoll();

// ── Keyboard forwarding ─────────────────────────────────────────
document.addEventListener("keydown", (e) => {{
  // Allow browser-critical shortcuts through (reload, close, dev tools, copy/paste).
  const browserKey = e.key === "F5" || e.key === "F12"
    || ((e.ctrlKey || e.metaKey) && ["c","v","x","w","t","r","l","shift","i"].includes(e.key.toLowerCase()))
    || (e.ctrlKey && e.shiftKey && e.key.toLowerCase() === "i");
  if (browserKey) return;

  e.preventDefault();
  const modifiers =
    (e.ctrlKey  ? 1 : 0) |
    (e.shiftKey ? 2 : 0) |
    (e.altKey   ? 4 : 0) |
    (e.metaKey  ? 8 : 0);
  const payload = JSON.stringify({{
    type: "Input",
    data: {{ kind: "Key", key: e.key, modifiers }},
  }});
  fetch(INPUT_URL, {{
    method: "POST",
    headers: {{ "Content-Type": "application/json" }},
    body: payload,
  }}).catch(() => {{}});
}});
</script>
</body>
</html>"##
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handle_page_uses_same_origin_relative_endpoints() {
        let html = handle_page(r#"bad.example"><script>alert(1)</script>"#);
        assert!(html.contains(r#"const STATE_URL = "/web-dashboard/state";"#));
        assert!(html.contains(r#"const INPUT_URL = "/web-dashboard/input";"#));
        assert!(html.contains("polling /web-dashboard/state"));
        assert!(!html.contains("http://bad.example"));
        assert!(!html.contains("alert(1)"));
    }

    #[test]
    fn handle_input_accepts_events_and_reports_queue_stats() {
        let config = mcp_agent_mail_core::Config::default();
        let state = TuiSharedState::new(&config);
        let body = br#"{
            "events": [
                {"type":"Input","data":{"kind":"Key","key":"k","modifiers":1}},
                {"type":"Resize","data":{"cols":120,"rows":40}},
                {"type":"Ping"}
            ]
        }"#;

        let (status, payload) = handle_input(&state, body);
        assert_eq!(status, 202);

        let json: Value = serde_json::from_str(&payload).expect("accepted json");
        assert_eq!(json["status"], "accepted");
        assert_eq!(json["accepted"], 2);
        assert_eq!(json["ignored"], 1);
        assert_eq!(json["dropped_oldest"], 0);
        assert_eq!(json["queue_depth"], 2);
        assert_eq!(json["queue_dropped_oldest_total"], 0);
        assert_eq!(json["queue_resize_coalesced_total"], 0);
    }

    #[test]
    fn handle_input_invalid_payload_returns_400_detail() {
        let config = mcp_agent_mail_core::Config::default();
        let state = TuiSharedState::new(&config);

        let (status, payload) = handle_input(&state, br#"{"type":"Input""#);
        assert_eq!(status, 400);

        let json: Value = serde_json::from_str(&payload).expect("error json");
        assert!(
            json["detail"]
                .as_str()
                .is_some_and(|detail| detail.contains("Invalid /mail/ws-input payload"))
        );
    }

    #[test]
    fn handle_input_oversized_payload_returns_413_detail() {
        let config = mcp_agent_mail_core::Config::default();
        let state = TuiSharedState::new(&config);
        let body = vec![b' '; 512 * 1024 + 1];

        let (status, payload) = handle_input(&state, &body);
        assert_eq!(status, 413);

        let json: Value = serde_json::from_str(&payload).expect("error json");
        assert!(
            json["detail"]
                .as_str()
                .is_some_and(|detail| detail.contains("too large"))
        );
    }
}
