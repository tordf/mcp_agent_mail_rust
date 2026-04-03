# SPEC: Doctor Forensic Bundle Schema

**Status:** Draft
**Date:** 2026-04-02
**Bead:** `br-97gc6.5.2.3.1`
**Purpose:** Define the stable on-disk contract for forensic bundles captured
before mailbox recovery mutates the active SQLite family.

This spec defines the bundle contract for mailbox forensics captured before
`am doctor repair` or `am doctor reconstruct` mutate recovery state.

The goal is not to export a user-safe artifact by default. The goal is to
preserve enough local evidence that later recovery paths can make comparable,
auditable decisions from a stable layout and manifest.

## Goals

- Every doctor-triggered forensic capture emits the same versioned manifest.
- Bundle names and directory layout are deterministic enough for tooling and
  operator handoffs.
- Raw SQLite-family evidence is copied before mutation; derived facts are
  referenced or summarized separately.
- Lifecycle policy is explicit: retention, deletion authority, and redaction
  rules are encoded in the manifest instead of being tribal knowledge.

## Non-Goals

- Defining who may read or export forensic bundles off-box.
- Defining encrypted-export packaging or attachment workflows.
- Copying the full Git archive, attachments, or unrelated databases into the
  default v1 bundle.
- Inventing automatic pruning before the storage-budget bead lands.

## Bundle Layout

Bundle root directory:

`<storage_root>/doctor/forensics/<db-family>/<command>-<timestamp>/`

Examples:

- `~/.local/share/mcp-agent-mail/git_mailbox_repo/doctor/forensics/storage.sqlite3/repair-20260402_231753_906/`
- `./doctor/forensics/storage.sqlite3/reconstruct-20260402_231812_114/`

Required files in v1:

- `manifest.json` — authoritative bundle manifest
- `summary.json` — terse operator-facing capture summary
- `sqlite/<db-family>` — copied database file

Optional files in v1:

- `sqlite/<db-family>-wal`
- `sqlite/<db-family>-shm`

Reserved paths for future beads:

- `references/` — process inventory, lock-holder reports, archive drift scans
- `receipts/` — recovery receipts, candidate validation verdicts

Layout invariants:

- All paths stored in `manifest.json` are bundle-root-relative and use `/` as
  the separator, even on non-Unix hosts.
- Absolute local paths are allowed only under `source`.
- `sqlite/` is the only directory that may contain copied raw mailbox bytes in
  v1.
- Reserved directories may be absent on disk until a later bead populates them.

## Copy vs Reference Policy

Copied before mutation:

- live SQLite database file
- live `-wal` sidecar when present
- live `-shm` sidecar when present

Referenced or summarized in v1 instead of copied:

- archive scan counters
- integrity-check detail
- future process/lock-holder inventory
- future archive drift and candidate validation reports
- Git archive trees, attachment payloads, and unrelated SQLite families

Rule:

- Raw mailbox bytes are copied only for the SQLite family being repaired or
  reconstructed.
- Derived facts should live in JSON reports and be referenced from
  `manifest.json` rather than duplicating large or unstable inputs.
- If a future recovery path needs extra raw evidence, it must either fit under a
  reserved directory or trigger a schema version change.

## Naming Rules

- `db-family` is the database filename, for example `storage.sqlite3`.
- `command` is the doctor entrypoint that triggered capture, currently
  `repair` or `reconstruct`.
- `timestamp` uses UTC `YYYYMMDD_HHMMSS_mmm`.
- `bundle_name` is exactly `<command>-<timestamp>`.
- If a computed bundle directory already exists, the capture logic must retry
  with a fresh timestamp rather than appending ad-hoc suffixes such as
  `-retry`, `-copy`, or `-v2`.

This means repeated captures for the same database family naturally group under
one parent directory without losing chronological ordering.

## Manifest Schema (v1)

Top-level required keys:

```json
{
  "schema": { "name": "mcp-agent-mail-doctor-forensics", "major": 1, "minor": 0 },
  "bundle_kind": "mailbox-doctor-forensics",
  "bundle_name": "repair-20260402_231753_906",
  "command": "repair",
  "timestamp": "20260402_231753_906",
  "generated_at": "2026-04-02T23:17:53Z",
  "source": {
    "database_url": "sqlite:///... or redacted URL",
    "db_path": "/abs/path/storage.sqlite3",
    "db_family": "storage.sqlite3",
    "storage_root": "/abs/path/storage-root"
  },
  "layout": {
    "sqlite_dir": "sqlite",
    "summary_path": "summary.json",
    "manifest_path": "manifest.json",
    "copied_before_mutation": ["sqlite/storage.sqlite3"],
    "referenced_evidence": ["archive_scan", "integrity_detail"],
    "reserved_paths": ["references/", "receipts/"]
  },
  "retention": {
    "policy": "manual_review",
    "review_after_days": 14,
    "delete_after_days": null,
    "automatic_deletion": false,
    "deletion_requires_explicit_operator_action": true
  },
  "redaction": {
    "database_url": "credentials_redacted",
    "sqlite_family": "raw_local_only",
    "manifest_and_summary": "shareable_after_human_review",
    "raw_sqlite_export": "requires_explicit_redaction_or_encrypted_export"
  },
  "artifacts": {
    "summary": { "path": "summary.json", "schema": "doctor-forensics-summary.v1" },
    "sqlite": {
      "db": { "path": "sqlite/storage.sqlite3", "status": "captured", "required": true },
      "wal": { "path": "sqlite/storage.sqlite3-wal", "status": "captured", "required": false },
      "shm": { "path": "sqlite/storage.sqlite3-shm", "status": "missing", "required": false }
    }
  },
  "files": [
    {
      "path": "sqlite/storage.sqlite3",
      "sha256": "…",
      "bytes": 12345,
      "kind": "sqlite",
      "role": "db",
      "schema": null,
      "contains_raw_mailbox_data": true
    }
  ]
}
```

### Compatibility rules

- `schema.major` changes for breaking layout or semantic changes.
- `schema.minor` changes for additive fields only.
- Validators must accept newer minor versions within the same major.
- Future beads may append new artifact sections under `artifacts` or `files`
  without changing existing keys.

### Manifest normalization rules

- `layout.copied_before_mutation`, `layout.referenced_evidence`, and
  `layout.reserved_paths` are lexicographically sorted.
- Every copied artifact path must appear in all three places that describe it:
  `layout.copied_before_mutation`, the corresponding `artifacts` entry, and the
  `files` array.
- `files` entries are sorted lexicographically by `path`.
- Optional artifacts that were looked for but not present stay listed in
  `artifacts` with `status: "missing"` so captures remain comparable.
- Status vocabulary is fixed in v1 to `captured`, `missing`, `referenced`, or
  `skipped`.

## Summary Schema

`summary.json` is a compact, operator-readable report with:

- schema identity
- command, bundle name, timestamp, created time
- redacted database URL
- absolute local source paths
- integrity detail
- archive scan summary
- per-artifact capture outcomes

The summary is allowed to preserve absolute local filesystem paths because the
default bundle is a local forensic artifact, not a scrubbed share package.

`summary.json` is still local-by-default. "Operator-readable" does not mean
"safe to publish externally" without a later redaction/export step.

## Retention Policy

Policy for v1:

- Review after 14 days.
- No automatic deletion.
- Deletion requires explicit operator action.
- The manifest remains the authoritative record of why a retained bundle still
  exists; later cleanup tooling must consult it rather than infer policy from
  directory names alone.

Rationale:

- The repo has a hard safety rule against deleting artifacts without explicit
  permission.
- Storage-budget automation is planned separately under
  `br-97gc6.5.2.6.5.3`, so this bead establishes a safe holding policy instead
  of inventing premature cleanup behavior.

## Redaction Discipline

The bundle has two sensitivity classes:

1. `manifest.json` and `summary.json`
   - redact credentials embedded in database URLs
   - may include absolute local paths
   - must not inline bearer tokens, passwords, or secret headers
   - preserve enough URL structure to identify the backend and target path after
     credential removal
2. `sqlite/**`
   - copied byte-for-byte
   - treated as local-only raw evidence
   - must not be attached to tickets or shared externally without explicit
     redaction or an encrypted-export path

Field-level rules for v1:

- `source.database_url` preserves scheme and location context but redacts
  credentials and sensitive query values.
- `source.db_path` and `source.storage_root` remain absolute local paths because
  the default bundle is a same-host diagnostic artifact.
- `files[].sha256`, `files[].bytes`, and capture status values are not redacted.
- Environment snapshots, auth headers, and process arguments are out of scope
  for this bead and must not be smuggled into `manifest.json` as ad-hoc fields.

This split is deliberate:

- operators need authentic bytes for postmortem recovery work
- downstream sharing/export beads need machine-readable labels describing why
  raw SQLite-family artifacts are more sensitive than the JSON metadata

## Expected Evolution

Follow-on beads should extend this contract by filling the reserved surfaces:

- `br-97gc6.5.2.3.2` adds process and lock-holder evidence
- `br-97gc6.5.2.3.3` adds archive drift and candidate-validation references
- `br-97gc6.5.2.1.15` formalizes compatibility and recovery-receipt versioning
- `br-97gc6.5.2.3.7` constrains sharing and encrypted export flows

This bead intentionally stops short of prescribing filesystem permissions or
off-box sharing mechanics. Those are separate policy surfaces and should not be
inferred from the retention or redaction rules above.
