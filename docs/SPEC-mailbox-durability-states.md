# SPEC: Mailbox Durability State Machine

> **Bead**: br-97gc6.5.2.1.1  
> **Status**: Draft  
> **Author**: Session 2026-04-02  
> **Purpose**: Define the authoritative mailbox durability states, transitions, and ownership invariants so CLI/server/doctor implementations use consistent semantics.

---

## 1. Overview

The mailbox durability state machine governs how the system responds to varying health conditions of the storage layer (SQLite database, Git archive, and sidecars). This specification defines:

1. The **seven canonical states** a mailbox can be in
2. **Evidence** that causes transitions between states
3. **Allowed operations** in each state
4. **Ownership rules** for who can make transitions

---

## 2. Canonical States

| State | Description | Reads? | Writes? | Recovery Active? |
|-------|-------------|--------|---------|------------------|
| `Healthy` | All probes pass, archive/DB in sync, no anomalies | Yes, live DB preferred | Yes | No |
| `Stale` | Archive authority is intact, but live freshness/parity is lagging | Yes, archive snapshot preferred | Owner-routed only | No |
| `Suspect` | Evidence is conflicting or incomplete; the live mutation path is no longer trusted | Yes, archive snapshot preferred | Owner-routed only | No |
| `Broken` | No trustworthy live mailbox path is available | No | No | No |
| `Recovering` | Exclusive supervisor-owned recovery operation is in progress | Yes, archive snapshot required | Supervisor only | Yes |
| `DegradedReadOnly` | Verified snapshot reads are safe, but normal writes are stopped | Yes, archive snapshot required | Supervisor only | No |
| `Escalate` | Requires human intervention; automated recovery is unsafe or ambiguous | No | No | No |

### 2.1 State Definitions

#### `Healthy`
- All startup probes pass:
  - DB file exists and is non-zero size
  - SQLite `quick_check` passes
  - Pool initialization succeeds
  - Foreign key integrity check passes
  - Schema version matches expected
  - Archive directory is readable/writable
  - No orphan WAL/SHM sidecars without active holders
- Archive/DB message counts match (within tolerance)
- No circuit breakers open
- No recovery artifacts pending review

#### `Stale`
- DB accessible and readable
- **Evidence of staleness**:
  - Archive has messages not in DB (count mismatch > threshold)
  - `schema_version` is 0 or lower than expected
  - Last successful sync timestamp is too old
  - WAL file is large (>10MB) suggesting uncommitted work
- **Allowed**: Read operations with archive snapshots preferred
- **Writes**: Only through the single mailbox owner/broker, never peer-direct mutation

#### `Suspect`
- **Evidence of anomaly** (not yet definitively broken):
  - `PRAGMA integrity_check` reports "never used" pages (benign bloat)
  - WAL exists without SHM when using frankensqlite (expected)
  - Foreign key violations detected but DB still readable
  - Circuit breaker in half-open state
  - Archive contains duplicate message IDs
- **Allowed**: Cautious reads with verified archive snapshots preferred
- **Writes**: Only through the single mailbox owner/broker while the verdict engine investigates

#### `Broken`
- **Evidence of corruption**:
  - DB file is zero bytes
  - SQLite `quick_check` fails with actual corruption (not just unused pages)
  - Pool initialization fails consistently
  - `database disk image is malformed` on page read
  - Cannot open file (permission denied, I/O error)
- **Blocked**: All operations
- **Required**: Transition to `Recovering` or `Escalate`

#### `Recovering`
- **Exclusive state** during active recovery:
  - `am doctor reconstruct` running
  - `am doctor repair` running
  - Archive-to-DB resync in progress
- **Owner**: Single process holds recovery lock
- **Invariant**: No other writers during recovery; reads must come from a verified snapshot/candidate path
- **On completion**: Transition to `Healthy`, `DegradedReadOnly`, or `Escalate`

#### `DegradedReadOnly`
- Recovery completed but with residual issues:
  - Archive/DB counts still differ (partial recovery)
  - Some messages couldn't be imported
  - Schema downgraded for compatibility
- **Allowed**: Read operations from a verified archive snapshot or validated candidate
- **Writes**: Only supervisor-owned receipts/checkpoints; user-facing writes remain stopped
- **Exit**: Manual approval or successful incremental repair

#### `Escalate`
- **Human intervention required**:
  - Multiple recovery attempts failed
  - Data loss detected that cannot be auto-recovered
  - Conflicting recovery artifacts from prior crashes
  - Archive source-of-truth is also corrupted/unreadable
- **Blocked**: All automated operations
- **Required**: Human runs explicit repair command with acknowledgment

---

## 3. State Transitions

```
                              ┌─────────────────────────────────────────────┐
                              │                                             │
                              ▼                                             │
    ┌─────────┐         ┌─────────┐         ┌─────────────────┐             │
    │ Healthy │◀───────▶│  Stale  │────────▶│ DegradedReadOnly│─────────────┤
    └────┬────┘         └────┬────┘         └────────┬────────┘             │
         │                   │                       │                      │
         │                   │                       │                      │
         ▼                   ▼                       ▼                      │
    ┌─────────┐         ┌─────────┐         ┌────────────┐                  │
    │ Suspect │────────▶│ Broken  │────────▶│ Recovering │──────────────────┘
    └────┬────┘         └────┬────┘         └────────────┘
         │                   │                    │
         │                   │                    │
         └───────────────────┴────────────────────┴─────▶ ┌──────────┐
                                                          │ Escalate │
                                                          └──────────┘
```

### 3.1 Transition Rules

| From | To | Trigger | Owner |
|------|----|---------|-------|
| `Healthy` | `Stale` | Archive count > DB count beyond threshold | Startup probe |
| `Healthy` | `Suspect` | Benign integrity warning (unused pages) | Startup probe |
| `Stale` | `Healthy` | Sync completes, counts match | Sync process |
| `Stale` | `Suspect` | Additional anomaly detected during sync | Sync process |
| `Suspect` | `Healthy` | Investigation clears anomaly | Doctor check |
| `Suspect` | `Broken` | Definitive corruption confirmed | Doctor check |
| `Broken` | `Recovering` | Recovery process acquires lock | Doctor command |
| `Broken` | `Escalate` | Auto-recovery blocked (see rules) | Doctor command |
| `Recovering` | `Healthy` | Full recovery succeeds | Recovery process |
| `Recovering` | `DegradedReadOnly` | Partial recovery completes | Recovery process |
| `Recovering` | `Escalate` | Recovery fails or detects data loss | Recovery process |
| `DegradedReadOnly` | `Healthy` | Incremental repair succeeds | Repair process |
| `DegradedReadOnly` | `Escalate` | Repair fails, human approval needed | Repair process |
| `Escalate` | `Recovering` | Human runs explicit recovery with `--force` | Human command |

### 3.2 Forbidden Transitions

- **No direct `Broken` → `Healthy`**: must pass through verified recovery or read-only degradation first
- **No direct `Healthy` → `Recovering`**: recovery starts only after a degraded or broken verdict
- **No direct `Escalate` → `Suspect`**: operator exits are explicit and must land in `Recovering`, `DegradedReadOnly`, or `Healthy`
- **No direct `DegradedReadOnly` → `Stale`**: once writes are stopped, only verified promotion or a worse failure may move the system forward

---

## 4. Evidence Mapping

### 4.1 Evidence → State

| Evidence | Resulting State |
|----------|-----------------|
| All probes pass | `Healthy` |
| Archive count > DB count (>5% or >100 messages) | `Stale` |
| Schema version mismatch | `Stale` |
| `integrity_check` reports unused pages only | `Suspect` |
| WAL without SHM (frankensqlite expected) | `Suspect` (documented benign) |
| Foreign key violations (readable DB) | `Suspect` |
| Zero-byte DB file | `Broken` |
| `quick_check` fails with corruption message | `Broken` |
| Pool init fails with I/O error | `Broken` |
| Recovery lock held by current process | `Recovering` |
| Recovery completed with residual count mismatch | `DegradedReadOnly` |
| Multiple prior recovery attempts failed | `Escalate` |
| Archive also unreadable | `Escalate` |

### 4.2 Composite Evidence

When multiple evidence items are present, the centralized verdict engine in
`mcp-agent-mail-db` must compute the result using the canonical transition and
contract model from `mcp-agent-mail-core::mailbox_durability`. Entry points may
not invent a local severity ordering.

Special rules:
- `Recovering` takes precedence when an exclusive recovery owner has fenced writes.
- `Escalate` takes precedence whenever archive authority or operator safety rules are violated.
- `DegradedReadOnly` is valid only when a verified snapshot/candidate read path exists.

---

## 5. Operation Semantics by State

### 5.1 Read Operations

| State | `SELECT` queries | Resource reads | Archive reads |
|-------|------------------|----------------|---------------|
| `Healthy` | Allowed | Allowed | Allowed |
| `Stale` | Allowed (snapshot preferred) | Allowed (snapshot preferred) | Allowed |
| `Suspect` | Allowed (snapshot preferred) | Allowed (snapshot preferred) | Allowed |
| `Broken` | Blocked | Blocked | Allowed only if a verified snapshot can be produced |
| `Recovering` | Allowed only via verified snapshot/candidate | Allowed only via verified snapshot/candidate | Allowed |
| `DegradedReadOnly` | Allowed via verified snapshot/candidate | Allowed via verified snapshot/candidate | Allowed |
| `Escalate` | Blocked | Blocked | Blocked |

### 5.2 Write Operations

| State | `INSERT/UPDATE/DELETE` | Tool mutations | Archive commits |
|-------|------------------------|----------------|-----------------|
| `Healthy` | Allowed | Allowed | Allowed |
| `Stale` | Owner-routed only | Owner-routed only | Owner-routed only |
| `Suspect` | Owner-routed only | Owner-routed only | Owner-routed only |
| `Broken` | Blocked | Blocked | Blocked |
| `Recovering` | Supervisor-only | Supervisor-only | Supervisor-only |
| `DegradedReadOnly` | Supervisor-only | Supervisor-only | Supervisor-only |
| `Escalate` | Blocked | Blocked | Blocked |

### 5.3 Mutating Operations During `DegradedReadOnly`

Per bead br-97gc6.5.2.1.8, user-facing mutating operations during
`DegradedReadOnly` remain blocked by default. The only allowed writes in this
state are supervisor-owned recovery receipts, checkpoints, and other
control-plane metadata required to complete or audit recovery safely.

---

## 6. Ownership Rules

### 6.1 State Determination

The **DB layer** (`mcp-agent-mail-db`) is the authoritative owner for computing
the mailbox health verdict. The canonical state machine and invariant contract
live in `crates/mcp-agent-mail-core/src/mailbox_durability.rs`; other layers
query those types but do not independently assess state.

```rust
// Canonical entry point (per bead br-97gc6.5.2.1.2)
pub fn compute_mailbox_verdict(
    db_path: &Path,
    archive_root: &Path,
    options: VerdictOptions,
) -> MailboxVerdict {
    // ... layered probes
}
```

### 6.2 Transition Authority

| State family | Authority |
|--------------|-----------|
| `Healthy`, `Stale`, `Suspect`, `Broken`, `DegradedReadOnly` | Central verdict engine |
| `Recovering` | Single-flight mailbox supervisor |
| `Escalate` exit | Operator/human |

### 6.3 Recovery Lock Protocol

1. **Acquire**: Recovery process calls `acquire_recovery_lock(db_path)`
2. **Verify**: No other readers/writers (check via `/proc/*/fd` on Linux)
3. **Hold**: Lock held for duration of recovery
4. **Release**: On completion or crash (lock file includes PID for stale detection)

---

## 7. Implementation Requirements

### 7.1 Type Definitions (implemented in `mcp-agent-mail-core`)

```rust
pub enum MailboxDurabilityState { /* ... */ }
pub enum MailboxReadPolicy { /* ... */ }
pub enum MailboxWritePolicy { /* ... */ }
pub enum MailboxRecoveryRequirement { /* ... */ }
pub enum MailboxTransitionAuthority { /* ... */ }
pub struct MailboxDurabilityContract { /* ... */ }
pub struct MailboxDurabilityTransition { /* ... */ }
pub struct MailboxDurabilityInvariant { /* ... */ }
```

Authoritative exports:
- `MAILBOX_DURABILITY_CONTRACTS`
- `MAILBOX_DURABILITY_TRANSITIONS`
- `MAILBOX_DURABILITY_INVARIANTS`
- `validate_mailbox_durability_transition(...)`

### 7.2 Verdict Computation (centralized in DB layer)

```rust
pub struct VerdictOptions {
    /// Skip archive count check (for offline/fast-path).
    pub skip_archive_count: bool,
    /// Stale threshold: archive messages minus DB messages.
    pub stale_threshold: (f64, usize), // (percentage, absolute)
}

impl Default for VerdictOptions {
    fn default() -> Self {
        Self {
            skip_archive_count: false,
            stale_threshold: (0.05, 100), // 5% or 100 messages
        }
    }
}
```

### 7.3 Non-Negotiable Invariants

The following invariants are canonical and must remain machine-checkable:

1. Mailbox durability classification is centralized; entrypoints do not invent local semantics.
2. Any non-`Healthy` state forbids peer-direct live-SQLite mutation.
3. `DegradedReadOnly` and `Recovering` reads require a verified snapshot/candidate path.
4. Only one exclusive recovery owner may hold `Recovering` at a time.
5. Quarantined recovery artifacts block silent fresh-start reinitialization.
6. Returning to `Healthy` from `Broken`, `DegradedReadOnly`, or `Recovering` requires verified promotion evidence.
7. `Escalate` does not auto-clear; an operator must authorize the exit.

---

## 8. CLI/Server Behavior by State

### 8.1 `am` CLI

| State | `am mail inbox` | `am send` | `am doctor check` | `am doctor reconstruct` |
|-------|-----------------|-----------|-------------------|-------------------------|
| `Healthy` | Normal | Normal | Reports healthy | Not needed |
| `Stale` | Normal (stale warning) | Owner-routed only | Reports stale | Suggests sync/replay |
| `Suspect` | Fallback path | Owner-routed only | Reports suspect | Suggests investigation |
| `Broken` | Error | Error | Reports broken | Available |
| `Recovering` | Verified snapshot only | Error | Reports recovering | Blocked (already running) |
| `DegradedReadOnly` | Normal (degraded warning) | Error | Reports degraded | Suggests incremental repair |
| `Escalate` | Error | Error | Reports escalate | Requires `--force` |

### 8.2 MCP Server

| State | Tool dispatch | Resource reads | Health endpoint |
|-------|---------------|----------------|-----------------|
| `Healthy` | Normal | Normal | `{"status": "healthy"}` |
| `Stale` | Owner-routed only | Normal (stale flag) | `{"status": "stale"}` |
| `Suspect` | Owner-routed only | Normal (suspect flag) | `{"status": "suspect"}` |
| `Broken` | Error | Error | `{"status": "broken"}` |
| `Recovering` | Error | Snapshot-only | `{"status": "recovering"}` |
| `DegradedReadOnly` | Read-only | Normal | `{"status": "degraded_read_only"}` |
| `Escalate` | Error | Error | `{"status": "escalate"}` |

---

## 9. Testing Requirements

Per bead br-97gc6.5.2.6.1.1, the following test coverage is required:

1. **Verdict matrix tests**: Every combination of probe results → expected state
2. **Transition tests**: Every valid transition fires correctly
3. **Forbidden transition tests**: Invalid transitions are rejected
4. **Operation semantics tests**: Reads/writes behave correctly per state
5. **Recovery lock tests**: Lock acquisition, release, and stale detection

---

## 10. Relation to Existing Code

### 10.1 What This Replaces

- Ad-hoc `"ok"`, `"warn"`, `"fail"` status strings in `handle_doctor_check_with`
- Implicit health assumptions scattered across CLI/server/tools
- Per-subsystem circuit breaker states (retained but subordinate)

### 10.2 What This Integrates With

- `DbHealthStatus` in `retry.rs` — circuit breakers remain for subsystem health
- `DbLockStatus` in `startup_checks.rs` — lock status is one input to verdict
- `ProbeResult` pattern in doctor check — probes feed into verdict computation

---

## 11. Open Questions

1. **Threshold tuning**: What archive/DB count mismatch triggers `Stale`?
   - Current proposal: >5% or >100 messages
   
2. **Recovery lock persistence**: File lock vs. advisory database flag?
   - Proposal: File lock with PID for stale detection

3. **Escalate criteria**: When is automated recovery "unsafe"?
   - Proposal: After 3 failed attempts or when archive is also corrupted

---

## Changelog

- **2026-04-02**: Initial draft per bead br-97gc6.5.2.1.1
