//! Re-exports from `mcp_agent_mail_core::timestamps` for backward compatibility.

pub use mcp_agent_mail_core::timestamps::{
    ClockSkewMetrics, clock_skew_metrics, clock_skew_reset, iso_to_micros, micros_to_iso,
    micros_to_naive, naive_to_micros, now_micros, now_micros_raw,
};
