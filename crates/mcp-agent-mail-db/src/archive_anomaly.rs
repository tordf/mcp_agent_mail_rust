//! Archive anomaly taxonomy and safe remediation classes (br-97gc6.5.2.4.1).
//!
//! This module is the single source of truth for classifying archive-level
//! anomalies and determining which remediation actions are safe for each class.
//! It is consumed by the CLI (`doctor archive-scan`, `doctor archive-normalize`),
//! the server (system health TUI), and the mailbox supervisor (automated
//! normalization workflows).
//!
//! # Design Principles
//!
//! 1. **Non-destructive by default.** No remediation action deletes data.
//!    The most aggressive automatic action is quarantine (rename aside).
//! 2. **Typed over stringly-typed.** Every anomaly class has a variant in
//!    [`ArchiveAnomalyKind`] so downstream code can pattern-match rather than
//!    substring-match on human-readable messages.
//! 3. **Severity is intrinsic** to the anomaly kind (not assigned ad-hoc by
//!    the scanner). This ensures consistent triage across all surfaces.
//! 4. **Remediation is classified, not executed** here. This module says
//!    *what class* of fix is appropriate; the scanner and normalizer modules
//!    decide *when* to apply it.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

// ============================================================================
// Anomaly severity
// ============================================================================

/// How severe an archive anomaly is, from least to most urgent.
///
/// Severity determines default sort order, whether the finding appears in
/// concise summaries, and whether it blocks promotion from `Stale` → `Healthy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnomalySeverity {
    /// Informational oddity that does not affect correctness.
    Info,
    /// Something is suboptimal but data integrity is not at risk.
    Warning,
    /// Data integrity may be compromised; remediation recommended.
    Error,
    /// Archive is unsafe for reconstruction; operator attention required.
    Critical,
}

impl AnomalySeverity {
    /// Numeric level for sorting (higher = worse).
    #[must_use]
    pub const fn level(self) -> u8 {
        match self {
            Self::Info => 0,
            Self::Warning => 1,
            Self::Error => 2,
            Self::Critical => 3,
        }
    }

    /// Human-readable label for CLI output.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
            Self::Critical => "critical",
        }
    }
}

impl std::fmt::Display for AnomalySeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ============================================================================
// Remediation classes
// ============================================================================

/// What class of remediation is safe for a given anomaly.
///
/// These classes form a trust hierarchy: `ReportOnly` needs zero authority,
/// `SafeAuto` can be run unattended, `NeedsConfirmation` requires an operator
/// prompt, and `ManualOnly` must be handled entirely by a human.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemediationClass {
    /// No safe automatic action exists; report the finding and move on.
    /// The operator can investigate at their discretion.
    ReportOnly,

    /// A non-destructive fix can be applied without confirmation.
    /// Examples: writing a missing `project.json` from known-good data,
    /// annotating a canonical file with a correction marker.
    SafeAuto,

    /// A non-destructive fix exists but its side effects warrant an
    /// explicit operator confirmation before execution.
    /// Examples: quarantining duplicate canonical files (renames them aside),
    /// rewriting project metadata when the canonical slug is ambiguous.
    NeedsConfirmation,

    /// No automated remediation is safe. The operator must manually
    /// inspect and resolve the anomaly.
    /// Examples: orphaned agents with no parent project, archive files
    /// with corrupted binary content, identity conflicts that cannot be
    /// disambiguated programmatically.
    ManualOnly,
}

impl RemediationClass {
    /// Human-readable label for CLI/JSON output.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::SafeAuto => "safe_auto",
            Self::NeedsConfirmation => "needs_confirmation",
            Self::ManualOnly => "manual_only",
        }
    }

    /// Whether this class permits any automatic action (with or without
    /// confirmation).
    #[must_use]
    pub const fn has_automated_action(self) -> bool {
        matches!(self, Self::SafeAuto | Self::NeedsConfirmation)
    }

    /// Whether this class can proceed without operator interaction.
    #[must_use]
    pub const fn is_unattended(self) -> bool {
        matches!(self, Self::SafeAuto)
    }
}

impl std::fmt::Display for RemediationClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ============================================================================
// Anomaly kinds
// ============================================================================

/// Exhaustive classification of archive anomaly types.
///
/// Each variant carries the minimum structured data needed to render a useful
/// finding. The [`ArchiveAnomalyKind::severity`] and
/// [`ArchiveAnomalyKind::remediation_class`] methods return the intrinsic
/// classification for the anomaly type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ArchiveAnomalyKind {
    // -- Duplicate canonical IDs -----------------------------------------
    /// Two or more archive `.md` files resolve to the same positive message id.
    DuplicateCanonicalId {
        /// The duplicated message id.
        message_id: i64,
        /// Path to the file that will be kept (first encountered).
        keep_path: PathBuf,
        /// Paths to the duplicate files.
        duplicate_paths: Vec<PathBuf>,
    },

    // -- Malformed message frontmatter -----------------------------------
    /// A `.md` file under `messages/YYYY/MM/` has no JSON frontmatter block.
    MissingFrontmatter {
        /// Path to the affected file.
        path: PathBuf,
    },

    /// A `.md` file has a JSON frontmatter block that fails to parse.
    UnparseableFrontmatter {
        /// Path to the affected file.
        path: PathBuf,
        /// The parse error message.
        parse_error: String,
    },

    /// Frontmatter parses as valid JSON but is missing the required `id` field
    /// or the id is not a positive integer.
    InvalidMessageId {
        /// Path to the affected file.
        path: PathBuf,
        /// Description of what is wrong with the id (missing, zero, negative).
        detail: String,
    },

    /// Frontmatter is valid JSON with a positive id, but required fields
    /// (`from`, `to`, `subject`, `created_at`) are missing or malformed.
    IncompleteFrontmatter {
        /// Path to the affected file.
        path: PathBuf,
        /// List of missing or invalid required field names.
        missing_fields: Vec<String>,
    },

    // -- Project metadata ------------------------------------------------
    /// A project directory has no `project.json` file.
    MissingProjectMetadata {
        /// Path to the project directory.
        project_dir: PathBuf,
        /// Fallback slug derived from the directory name.
        fallback_slug: String,
    },

    /// `project.json` exists but contains invalid JSON or is missing
    /// required fields (`slug`, `human_key`).
    InvalidProjectMetadata {
        /// Path to the `project.json` file.
        path: PathBuf,
        /// The slug (possibly from fallback).
        slug: String,
        /// A canonical human_key if one can be inferred from the DB or
        /// directory structure.
        canonical_human_key: Option<String>,
        /// Description of the problem.
        detail: String,
    },

    // -- Suspicious / ephemeral projects ---------------------------------
    /// A project appears to be ephemeral (tmp, dev, test prefix/root) and
    /// should not be in the production archive.
    SuspiciousEphemeralProject {
        /// Path to the project directory.
        project_dir: PathBuf,
        /// The project slug.
        slug: String,
        /// The human_key, if available.
        human_key: Option<String>,
        /// Why this project is considered suspicious.
        reason: String,
    },

    // -- Orphaned / inconsistent agents ----------------------------------
    /// An agent profile directory exists under a project that is not itself
    /// represented in the archive or DB.
    OrphanedAgentProfile {
        /// Path to the agent's `profile.json`.
        profile_path: PathBuf,
        /// The agent name from the directory.
        agent_name: String,
        /// The parent project directory that is missing or unrecognized.
        parent_project_dir: PathBuf,
    },

    /// An agent profile's `profile.json` is missing or unparseable.
    MalformedAgentProfile {
        /// Path to the expected `profile.json`.
        profile_path: PathBuf,
        /// The agent name from the directory.
        agent_name: String,
        /// Description of the problem (missing file, parse error, etc.).
        detail: String,
    },

    // -- Archive structure anomalies -------------------------------------
    /// A year or month directory under `messages/` has an unexpected name
    /// (not 4-digit year or 2-digit month).
    InvalidDateDirectory {
        /// Path to the malformed directory.
        path: PathBuf,
        /// Whether this is a year-level or month-level directory.
        level: DateDirectoryLevel,
        /// The actual directory name.
        name: String,
    },

    /// A file exists under `messages/YYYY/MM/` that is not a `.md` file.
    UnexpectedFileInMessageDir {
        /// Path to the unexpected file.
        path: PathBuf,
    },

    /// A symlink was found where a real directory or file was expected.
    /// Symlinks in the archive are never canonical and may indicate
    /// filesystem-level tampering or misconfigured storage.
    UnexpectedSymlink {
        /// Path to the symlink.
        path: PathBuf,
        /// What the symlink points to (if resolvable).
        target: Option<PathBuf>,
    },

    // -- Identity mismatches ---------------------------------------------
    /// The archive contains a project identity that does not match any
    /// project in the database. This may indicate archive drift or a
    /// project that was deleted from the DB but not the archive.
    ArchiveDbProjectMismatch {
        /// The archive-side identity.
        archive_slug: String,
        /// The archive-side human_key, if available.
        archive_human_key: Option<String>,
        /// Description of the mismatch.
        detail: String,
    },

    /// The message count in the archive differs significantly from the DB.
    ArchiveDbCountDrift {
        /// Number of unique message ids in the archive.
        archive_count: usize,
        /// Number of messages in the DB.
        db_count: usize,
        /// The absolute difference.
        drift: usize,
    },
}

/// Whether an [`InvalidDateDirectory`](ArchiveAnomalyKind::InvalidDateDirectory)
/// is at the year or month level of the `messages/YYYY/MM/` hierarchy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DateDirectoryLevel {
    Year,
    Month,
}

impl std::fmt::Display for DateDirectoryLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Year => f.write_str("year"),
            Self::Month => f.write_str("month"),
        }
    }
}

impl ArchiveAnomalyKind {
    /// Intrinsic severity for this anomaly type.
    #[must_use]
    pub const fn severity(&self) -> AnomalySeverity {
        match self {
            // Duplicates are an error — reconstruction will skip them but data
            // may have diverged between the copies.
            Self::DuplicateCanonicalId { .. } => AnomalySeverity::Error,

            // Malformed frontmatter is an error — the message cannot be
            // recovered without manual inspection.
            Self::MissingFrontmatter { .. }
            | Self::UnparseableFrontmatter { .. }
            | Self::InvalidMessageId { .. } => AnomalySeverity::Error,

            // Incomplete frontmatter is a warning — the message id is valid
            // so reconstruction can partially recover it.
            Self::IncompleteFrontmatter { .. } => AnomalySeverity::Warning,

            // Missing project metadata is a warning — the directory name
            // provides a usable fallback slug.
            Self::MissingProjectMetadata { .. } => AnomalySeverity::Warning,

            // Invalid project metadata is a warning or error depending on
            // whether a canonical value can be inferred.
            Self::InvalidProjectMetadata {
                canonical_human_key,
                ..
            } => {
                if canonical_human_key.is_some() {
                    AnomalySeverity::Warning
                } else {
                    AnomalySeverity::Error
                }
            }

            // Suspicious ephemeral projects are informational — they are
            // valid archives, just not production-grade.
            Self::SuspiciousEphemeralProject { .. } => AnomalySeverity::Info,

            // Orphaned agents are an error — they cannot be associated with
            // a project during reconstruction.
            Self::OrphanedAgentProfile { .. } => AnomalySeverity::Error,

            // Malformed agent profiles are a warning — the agent directory
            // name still identifies the agent.
            Self::MalformedAgentProfile { .. } => AnomalySeverity::Warning,

            // Structural anomalies are informational — they don't affect
            // reconstruction of properly-formed data.
            Self::InvalidDateDirectory { .. }
            | Self::UnexpectedFileInMessageDir { .. }
            | Self::UnexpectedSymlink { .. } => AnomalySeverity::Info,

            // Identity mismatches are warnings — they indicate drift but
            // both archive and DB remain internally consistent.
            Self::ArchiveDbProjectMismatch { .. } => AnomalySeverity::Warning,

            // Count drift is severity-dependent on magnitude, but the kind
            // itself is classified as warning. Callers can upgrade based on
            // the `drift` magnitude.
            Self::ArchiveDbCountDrift { .. } => AnomalySeverity::Warning,
        }
    }

    /// The safe remediation class for this anomaly type.
    #[must_use]
    pub const fn remediation_class(&self) -> RemediationClass {
        match self {
            // Duplicate canonical files: quarantine (rename aside) the extras.
            // Needs confirmation because it changes the archive directory layout.
            Self::DuplicateCanonicalId { .. } => RemediationClass::NeedsConfirmation,

            // Missing/unparseable frontmatter: we cannot synthesize valid
            // message content, so report only.
            Self::MissingFrontmatter { .. } | Self::UnparseableFrontmatter { .. } => {
                RemediationClass::ReportOnly
            }

            // Invalid message id: the frontmatter may be repairable if we
            // can derive the id from the filename, but that is too risky
            // without human review.
            Self::InvalidMessageId { .. } => RemediationClass::ManualOnly,

            // Incomplete frontmatter: the message can be partially recovered
            // but missing fields need human decision.
            Self::IncompleteFrontmatter { .. } => RemediationClass::ReportOnly,

            // Missing project metadata: we can safely write a `project.json`
            // from the directory name (fallback slug).
            Self::MissingProjectMetadata { .. } => RemediationClass::SafeAuto,

            // Invalid project metadata with known canonical value: safe auto
            // rewrite. Without canonical value: needs manual resolution.
            Self::InvalidProjectMetadata {
                canonical_human_key,
                ..
            } => {
                if canonical_human_key.is_some() {
                    RemediationClass::SafeAuto
                } else {
                    RemediationClass::ManualOnly
                }
            }

            // Suspicious ephemeral projects: report only. The operator
            // decides whether to remove or reclassify them.
            Self::SuspiciousEphemeralProject { .. } => RemediationClass::ReportOnly,

            // Orphaned agents: manual only. We cannot safely create or
            // associate a parent project without operator guidance.
            Self::OrphanedAgentProfile { .. } => RemediationClass::ManualOnly,

            // Malformed agent profiles: report only. The agent directory
            // still identifies the agent for reconstruction.
            Self::MalformedAgentProfile { .. } => RemediationClass::ReportOnly,

            // Structural oddities: report only.
            Self::InvalidDateDirectory { .. }
            | Self::UnexpectedFileInMessageDir { .. }
            | Self::UnexpectedSymlink { .. } => RemediationClass::ReportOnly,

            // Identity mismatches: report only. Resolving drift requires
            // understanding which side is authoritative.
            Self::ArchiveDbProjectMismatch { .. } => RemediationClass::ReportOnly,

            // Count drift: report only. The actual resolution depends on
            // which messages are missing from which side.
            Self::ArchiveDbCountDrift { .. } => RemediationClass::ReportOnly,
        }
    }

    /// Short machine-readable tag for this anomaly kind (without payload).
    #[must_use]
    pub const fn tag(&self) -> &'static str {
        match self {
            Self::DuplicateCanonicalId { .. } => "duplicate_canonical_id",
            Self::MissingFrontmatter { .. } => "missing_frontmatter",
            Self::UnparseableFrontmatter { .. } => "unparseable_frontmatter",
            Self::InvalidMessageId { .. } => "invalid_message_id",
            Self::IncompleteFrontmatter { .. } => "incomplete_frontmatter",
            Self::MissingProjectMetadata { .. } => "missing_project_metadata",
            Self::InvalidProjectMetadata { .. } => "invalid_project_metadata",
            Self::SuspiciousEphemeralProject { .. } => "suspicious_ephemeral_project",
            Self::OrphanedAgentProfile { .. } => "orphaned_agent_profile",
            Self::MalformedAgentProfile { .. } => "malformed_agent_profile",
            Self::InvalidDateDirectory { .. } => "invalid_date_directory",
            Self::UnexpectedFileInMessageDir { .. } => "unexpected_file_in_message_dir",
            Self::UnexpectedSymlink { .. } => "unexpected_symlink",
            Self::ArchiveDbProjectMismatch { .. } => "archive_db_project_mismatch",
            Self::ArchiveDbCountDrift { .. } => "archive_db_count_drift",
        }
    }

    /// One-line human-readable summary of this anomaly.
    #[must_use]
    pub fn summary(&self) -> String {
        match self {
            Self::DuplicateCanonicalId {
                message_id,
                duplicate_paths,
                ..
            } => format!(
                "message id {message_id} has {} duplicate archive file(s)",
                duplicate_paths.len()
            ),
            Self::MissingFrontmatter { path } => {
                format!("no JSON frontmatter in {}", path.display())
            }
            Self::UnparseableFrontmatter { path, parse_error } => {
                format!("bad JSON frontmatter in {}: {parse_error}", path.display())
            }
            Self::InvalidMessageId { path, detail } => {
                format!("invalid message id in {}: {detail}", path.display())
            }
            Self::IncompleteFrontmatter {
                path,
                missing_fields,
            } => format!(
                "incomplete frontmatter in {} (missing: {})",
                path.display(),
                missing_fields.join(", ")
            ),
            Self::MissingProjectMetadata {
                project_dir,
                fallback_slug,
            } => format!(
                "missing project.json in {} (fallback slug: {fallback_slug})",
                project_dir.display()
            ),
            Self::InvalidProjectMetadata {
                path, slug, detail, ..
            } => format!(
                "invalid project.json at {} for {slug}: {detail}",
                path.display()
            ),
            Self::SuspiciousEphemeralProject { slug, reason, .. } => {
                format!("suspicious project '{slug}': {reason}")
            }
            Self::OrphanedAgentProfile {
                agent_name,
                parent_project_dir,
                ..
            } => format!(
                "orphaned agent '{agent_name}' under unrecognized project {}",
                parent_project_dir.display()
            ),
            Self::MalformedAgentProfile {
                agent_name, detail, ..
            } => format!("malformed profile for agent '{agent_name}': {detail}"),
            Self::InvalidDateDirectory {
                path, level, name, ..
            } => format!(
                "invalid {level} directory name '{name}' at {}",
                path.display()
            ),
            Self::UnexpectedFileInMessageDir { path } => {
                format!("unexpected non-.md file at {}", path.display())
            }
            Self::UnexpectedSymlink { path, target } => {
                let suffix = target
                    .as_ref()
                    .map_or(String::new(), |t| format!(" → {}", t.display()));
                format!("unexpected symlink at {}{suffix}", path.display())
            }
            Self::ArchiveDbProjectMismatch {
                archive_slug,
                detail,
                ..
            } => format!("archive/DB project mismatch for '{archive_slug}': {detail}"),
            Self::ArchiveDbCountDrift {
                archive_count,
                db_count,
                drift,
            } => format!(
                "message count drift: archive={archive_count}, db={db_count} (delta={drift})"
            ),
        }
    }
}

impl std::fmt::Display for ArchiveAnomalyKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.severity(), self.summary())
    }
}

// ============================================================================
// Anomaly finding (kind + context)
// ============================================================================

/// A single archive anomaly finding with full context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveAnomaly {
    /// The classified anomaly.
    pub kind: ArchiveAnomalyKind,
    /// Timestamp when the anomaly was detected (microseconds since epoch).
    pub detected_at: i64,
}

impl ArchiveAnomaly {
    /// Create a new anomaly finding with the current timestamp.
    #[must_use]
    pub fn now(kind: ArchiveAnomalyKind) -> Self {
        Self {
            kind,
            detected_at: mcp_agent_mail_core::timestamps::now_micros(),
        }
    }

    /// Severity (delegated to the kind).
    #[must_use]
    pub const fn severity(&self) -> AnomalySeverity {
        self.kind.severity()
    }

    /// Remediation class (delegated to the kind).
    #[must_use]
    pub const fn remediation_class(&self) -> RemediationClass {
        self.kind.remediation_class()
    }
}

impl std::fmt::Display for ArchiveAnomaly {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)
    }
}

// ============================================================================
// Anomaly report (collection of findings)
// ============================================================================

/// Aggregated archive anomaly report.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ArchiveAnomalyReport {
    /// All detected anomalies, in detection order.
    pub anomalies: Vec<ArchiveAnomaly>,
}

impl ArchiveAnomalyReport {
    /// Create an empty report.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an anomaly to the report.
    pub fn push(&mut self, anomaly: ArchiveAnomaly) {
        self.anomalies.push(anomaly);
    }

    /// Add an anomaly kind (auto-timestamped).
    pub fn record(&mut self, kind: ArchiveAnomalyKind) {
        self.push(ArchiveAnomaly::now(kind));
    }

    /// Total number of anomalies.
    #[must_use]
    pub fn len(&self) -> usize {
        self.anomalies.len()
    }

    /// Whether the report is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.anomalies.is_empty()
    }

    /// Count of anomalies at or above a given severity.
    #[must_use]
    pub fn count_at_severity(&self, min_severity: AnomalySeverity) -> usize {
        self.anomalies
            .iter()
            .filter(|a| a.severity().level() >= min_severity.level())
            .count()
    }

    /// Count of anomalies that have any automated remediation available.
    #[must_use]
    pub fn actionable_count(&self) -> usize {
        self.anomalies
            .iter()
            .filter(|a| a.remediation_class().has_automated_action())
            .count()
    }

    /// Count of anomalies that can be fixed without operator confirmation.
    #[must_use]
    pub fn safe_auto_count(&self) -> usize {
        self.anomalies
            .iter()
            .filter(|a| a.remediation_class().is_unattended())
            .count()
    }

    /// Anomalies filtered to a specific remediation class.
    #[must_use]
    pub fn by_remediation_class(&self, class: RemediationClass) -> Vec<&ArchiveAnomaly> {
        self.anomalies
            .iter()
            .filter(|a| a.remediation_class() == class)
            .collect()
    }

    /// Anomalies filtered by tag.
    #[must_use]
    pub fn by_tag(&self, tag: &str) -> Vec<&ArchiveAnomaly> {
        self.anomalies
            .iter()
            .filter(|a| a.kind.tag() == tag)
            .collect()
    }

    /// Highest severity in the report, or `None` if empty.
    #[must_use]
    pub fn max_severity(&self) -> Option<AnomalySeverity> {
        self.anomalies.iter().map(|a| a.severity()).max()
    }

    /// Sort anomalies by severity (highest first), then by tag for stability.
    pub fn sort_by_severity(&mut self) {
        self.anomalies.sort_by(|a, b| {
            b.severity()
                .level()
                .cmp(&a.severity().level())
                .then_with(|| a.kind.tag().cmp(b.kind.tag()))
        });
    }
}

// ============================================================================
// Convenience: all known anomaly tags
// ============================================================================

/// All known anomaly tags, useful for documentation and schema validation.
pub const ALL_ANOMALY_TAGS: &[&str] = &[
    "archive_db_count_drift",
    "archive_db_project_mismatch",
    "duplicate_canonical_id",
    "incomplete_frontmatter",
    "invalid_date_directory",
    "invalid_message_id",
    "invalid_project_metadata",
    "malformed_agent_profile",
    "missing_frontmatter",
    "missing_project_metadata",
    "orphaned_agent_profile",
    "suspicious_ephemeral_project",
    "unexpected_file_in_message_dir",
    "unexpected_symlink",
];

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn severity_ordering() {
        assert!(AnomalySeverity::Info < AnomalySeverity::Warning);
        assert!(AnomalySeverity::Warning < AnomalySeverity::Error);
        assert!(AnomalySeverity::Error < AnomalySeverity::Critical);
    }

    #[test]
    fn remediation_ordering() {
        assert!(RemediationClass::ReportOnly < RemediationClass::SafeAuto);
        assert!(RemediationClass::SafeAuto < RemediationClass::NeedsConfirmation);
        assert!(RemediationClass::NeedsConfirmation < RemediationClass::ManualOnly);
    }

    #[test]
    fn duplicate_canonical_id_classification() {
        let kind = ArchiveAnomalyKind::DuplicateCanonicalId {
            message_id: 42,
            keep_path: PathBuf::from("/archive/messages/2026/01/msg_42.md"),
            duplicate_paths: vec![PathBuf::from("/archive/messages/2026/02/msg_42.md")],
        };
        assert_eq!(kind.severity(), AnomalySeverity::Error);
        assert_eq!(
            kind.remediation_class(),
            RemediationClass::NeedsConfirmation
        );
        assert_eq!(kind.tag(), "duplicate_canonical_id");
        assert!(kind.summary().contains("42"));
    }

    #[test]
    fn missing_frontmatter_classification() {
        let kind = ArchiveAnomalyKind::MissingFrontmatter {
            path: PathBuf::from("/archive/messages/2026/01/orphan.md"),
        };
        assert_eq!(kind.severity(), AnomalySeverity::Error);
        assert_eq!(kind.remediation_class(), RemediationClass::ReportOnly);
        assert_eq!(kind.tag(), "missing_frontmatter");
    }

    #[test]
    fn unparseable_frontmatter_classification() {
        let kind = ArchiveAnomalyKind::UnparseableFrontmatter {
            path: PathBuf::from("/archive/messages/2026/01/bad.md"),
            parse_error: "expected comma".to_string(),
        };
        assert_eq!(kind.severity(), AnomalySeverity::Error);
        assert_eq!(kind.remediation_class(), RemediationClass::ReportOnly);
        assert_eq!(kind.tag(), "unparseable_frontmatter");
    }

    #[test]
    fn invalid_message_id_classification() {
        let kind = ArchiveAnomalyKind::InvalidMessageId {
            path: PathBuf::from("/archive/messages/2026/01/neg.md"),
            detail: "id is negative".to_string(),
        };
        assert_eq!(kind.severity(), AnomalySeverity::Error);
        assert_eq!(kind.remediation_class(), RemediationClass::ManualOnly);
    }

    #[test]
    fn incomplete_frontmatter_classification() {
        let kind = ArchiveAnomalyKind::IncompleteFrontmatter {
            path: PathBuf::from("/archive/messages/2026/01/partial.md"),
            missing_fields: vec!["from".to_string(), "subject".to_string()],
        };
        assert_eq!(kind.severity(), AnomalySeverity::Warning);
        assert_eq!(kind.remediation_class(), RemediationClass::ReportOnly);
    }

    #[test]
    fn missing_project_metadata_classification() {
        let kind = ArchiveAnomalyKind::MissingProjectMetadata {
            project_dir: PathBuf::from("/archive/projects/my-project"),
            fallback_slug: "my-project".to_string(),
        };
        assert_eq!(kind.severity(), AnomalySeverity::Warning);
        assert_eq!(kind.remediation_class(), RemediationClass::SafeAuto);
    }

    #[test]
    fn invalid_project_metadata_with_canonical_key() {
        let kind = ArchiveAnomalyKind::InvalidProjectMetadata {
            path: PathBuf::from("/archive/projects/foo/project.json"),
            slug: "foo".to_string(),
            canonical_human_key: Some("Foo Project".to_string()),
            detail: "malformed JSON".to_string(),
        };
        assert_eq!(kind.severity(), AnomalySeverity::Warning);
        assert_eq!(kind.remediation_class(), RemediationClass::SafeAuto);
    }

    #[test]
    fn invalid_project_metadata_without_canonical_key() {
        let kind = ArchiveAnomalyKind::InvalidProjectMetadata {
            path: PathBuf::from("/archive/projects/foo/project.json"),
            slug: "foo".to_string(),
            canonical_human_key: None,
            detail: "malformed JSON".to_string(),
        };
        assert_eq!(kind.severity(), AnomalySeverity::Error);
        assert_eq!(kind.remediation_class(), RemediationClass::ManualOnly);
    }

    #[test]
    fn suspicious_ephemeral_project_classification() {
        let kind = ArchiveAnomalyKind::SuspiciousEphemeralProject {
            project_dir: PathBuf::from("/archive/projects/tmp-test"),
            slug: "tmp-test".to_string(),
            human_key: None,
            reason: "project slug 'tmp-test' looks ephemeral".to_string(),
        };
        assert_eq!(kind.severity(), AnomalySeverity::Info);
        assert_eq!(kind.remediation_class(), RemediationClass::ReportOnly);
    }

    #[test]
    fn orphaned_agent_classification() {
        let kind = ArchiveAnomalyKind::OrphanedAgentProfile {
            profile_path: PathBuf::from("/archive/projects/ghost/agents/BraveEagle/profile.json"),
            agent_name: "BraveEagle".to_string(),
            parent_project_dir: PathBuf::from("/archive/projects/ghost"),
        };
        assert_eq!(kind.severity(), AnomalySeverity::Error);
        assert_eq!(kind.remediation_class(), RemediationClass::ManualOnly);
    }

    #[test]
    fn malformed_agent_profile_classification() {
        let kind = ArchiveAnomalyKind::MalformedAgentProfile {
            profile_path: PathBuf::from("/archive/projects/foo/agents/Bar/profile.json"),
            agent_name: "Bar".to_string(),
            detail: "file is empty".to_string(),
        };
        assert_eq!(kind.severity(), AnomalySeverity::Warning);
        assert_eq!(kind.remediation_class(), RemediationClass::ReportOnly);
    }

    #[test]
    fn structural_anomalies_are_info() {
        let kinds = [
            ArchiveAnomalyKind::InvalidDateDirectory {
                path: PathBuf::from("/archive/projects/foo/messages/abcd"),
                level: DateDirectoryLevel::Year,
                name: "abcd".to_string(),
            },
            ArchiveAnomalyKind::UnexpectedFileInMessageDir {
                path: PathBuf::from("/archive/projects/foo/messages/2026/01/notes.txt"),
            },
            ArchiveAnomalyKind::UnexpectedSymlink {
                path: PathBuf::from("/archive/projects/foo/messages/link"),
                target: Some(PathBuf::from("/tmp/somewhere")),
            },
        ];
        for kind in &kinds {
            assert_eq!(
                kind.severity(),
                AnomalySeverity::Info,
                "kind: {}",
                kind.tag()
            );
            assert_eq!(
                kind.remediation_class(),
                RemediationClass::ReportOnly,
                "kind: {}",
                kind.tag()
            );
        }
    }

    #[test]
    fn archive_db_mismatch_classification() {
        let kind = ArchiveAnomalyKind::ArchiveDbProjectMismatch {
            archive_slug: "orphan-proj".to_string(),
            archive_human_key: Some("/data/orphan".to_string()),
            detail: "no matching DB project".to_string(),
        };
        assert_eq!(kind.severity(), AnomalySeverity::Warning);
        assert_eq!(kind.remediation_class(), RemediationClass::ReportOnly);
    }

    #[test]
    fn archive_db_count_drift_classification() {
        let kind = ArchiveAnomalyKind::ArchiveDbCountDrift {
            archive_count: 150,
            db_count: 140,
            drift: 10,
        };
        assert_eq!(kind.severity(), AnomalySeverity::Warning);
        assert_eq!(kind.remediation_class(), RemediationClass::ReportOnly);
    }

    #[test]
    fn report_aggregation() {
        let mut report = ArchiveAnomalyReport::new();
        assert!(report.is_empty());
        assert_eq!(report.max_severity(), None);

        report.record(ArchiveAnomalyKind::MissingProjectMetadata {
            project_dir: PathBuf::from("/a"),
            fallback_slug: "a".to_string(),
        });
        report.record(ArchiveAnomalyKind::DuplicateCanonicalId {
            message_id: 1,
            keep_path: PathBuf::from("/k"),
            duplicate_paths: vec![PathBuf::from("/d")],
        });
        report.record(ArchiveAnomalyKind::SuspiciousEphemeralProject {
            project_dir: PathBuf::from("/tmp"),
            slug: "tmp-x".to_string(),
            human_key: None,
            reason: "ephemeral".to_string(),
        });

        assert_eq!(report.len(), 3);
        assert_eq!(report.max_severity(), Some(AnomalySeverity::Error));
        assert_eq!(report.count_at_severity(AnomalySeverity::Error), 1);
        assert_eq!(report.count_at_severity(AnomalySeverity::Warning), 2);
        assert_eq!(report.actionable_count(), 2); // safe_auto + needs_confirmation
        assert_eq!(report.safe_auto_count(), 1); // missing_project_metadata
        assert_eq!(report.by_tag("duplicate_canonical_id").len(), 1);
    }

    #[test]
    fn report_sort_by_severity() {
        let mut report = ArchiveAnomalyReport::new();
        report.record(ArchiveAnomalyKind::SuspiciousEphemeralProject {
            project_dir: PathBuf::from("/tmp"),
            slug: "tmp-x".to_string(),
            human_key: None,
            reason: "ephemeral".to_string(),
        });
        report.record(ArchiveAnomalyKind::DuplicateCanonicalId {
            message_id: 1,
            keep_path: PathBuf::from("/k"),
            duplicate_paths: vec![PathBuf::from("/d")],
        });
        report.record(ArchiveAnomalyKind::MissingProjectMetadata {
            project_dir: PathBuf::from("/a"),
            fallback_slug: "a".to_string(),
        });

        report.sort_by_severity();

        assert_eq!(report.anomalies[0].severity(), AnomalySeverity::Error);
        assert_eq!(report.anomalies[1].severity(), AnomalySeverity::Warning);
        assert_eq!(report.anomalies[2].severity(), AnomalySeverity::Info);
    }

    #[test]
    fn all_anomaly_tags_sorted_and_complete() {
        // Verify the constant is sorted.
        let mut sorted = ALL_ANOMALY_TAGS.to_vec();
        sorted.sort();
        assert_eq!(ALL_ANOMALY_TAGS, sorted.as_slice());

        // Verify every tag is represented.
        assert_eq!(ALL_ANOMALY_TAGS.len(), 14);
    }

    #[test]
    fn display_formats_include_severity() {
        let kind = ArchiveAnomalyKind::MissingFrontmatter {
            path: PathBuf::from("/test.md"),
        };
        let display = format!("{kind}");
        assert!(display.starts_with("[error]"));
        assert!(display.contains("/test.md"));
    }

    #[test]
    fn serde_roundtrip() {
        let kind = ArchiveAnomalyKind::DuplicateCanonicalId {
            message_id: 99,
            keep_path: PathBuf::from("/keep.md"),
            duplicate_paths: vec![PathBuf::from("/dup1.md"), PathBuf::from("/dup2.md")],
        };
        let anomaly = ArchiveAnomaly::now(kind);
        let json = serde_json::to_string(&anomaly).expect("serialize");
        let roundtripped: ArchiveAnomaly = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(roundtripped.kind, anomaly.kind);
    }

    #[test]
    fn by_remediation_class_filter() {
        let mut report = ArchiveAnomalyReport::new();
        report.record(ArchiveAnomalyKind::MissingProjectMetadata {
            project_dir: PathBuf::from("/a"),
            fallback_slug: "a".to_string(),
        });
        report.record(ArchiveAnomalyKind::OrphanedAgentProfile {
            profile_path: PathBuf::from("/b/profile.json"),
            agent_name: "X".to_string(),
            parent_project_dir: PathBuf::from("/b"),
        });

        assert_eq!(
            report
                .by_remediation_class(RemediationClass::SafeAuto)
                .len(),
            1
        );
        assert_eq!(
            report
                .by_remediation_class(RemediationClass::ManualOnly)
                .len(),
            1
        );
        assert_eq!(
            report
                .by_remediation_class(RemediationClass::ReportOnly)
                .len(),
            0
        );
    }
}
