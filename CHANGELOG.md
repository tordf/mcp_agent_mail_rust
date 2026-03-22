# Changelog

All notable changes to [MCP Agent Mail (Rust)](https://github.com/Dicklesworthstone/mcp_agent_mail_rust) are documented in this file.

Versions marked with **[Release]** have published [GitHub Releases](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/releases) with downloadable binaries. Versions marked with **[Tag only]** exist as git tags but have no GitHub Release or binary artifacts.

---

## [v0.2.12](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/releases/tag/v0.2.12) — 2026-03-21 **[Release — Latest]**

2 commits since v0.2.11 | [Compare](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/compare/v0.2.11...v0.2.12)

Dependency version bump for crates.io publish cascade. Packages the FrankenSQLite WAL compatibility fixes from v0.2.10 and v0.2.11 into a clean release with aligned workspace dependency versions.

### Changes

- **crates.io publish cascade**: Updated workspace dependency versions so all crates in the workspace can be published to crates.io in the correct order ([b679466](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/b679466468648e09e3700c752c28f953f8242064))
- Updated Cargo.lock dependency versions ([b6819d8](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/b6819d8))

---

## [v0.2.11](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/releases/tag/v0.2.11) — 2026-03-21 **[Release]**

1 commit since v0.2.10 | [Compare](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/compare/v0.2.10...v0.2.11)

Fixes the root cause of "database is busy (snapshot conflict on pages)" errors when installing on machines with existing Python mcp_agent_mail databases.

### Fix: Python Database Migration WAL Checkpoint

The migration checkpoint function was using FrankenSQLite (`FrankenConnection`) to open Python-created databases. FrankenSQLite cannot read C SQLite's WAL format because they use different page formats. When the Python database had uncheckpointed WAL pages, the migration copied the main file without those pages, leaving B-tree references to nonexistent pages.

- `checkpoint_sqlite_for_copy()` now uses C SQLite (`SqliteConnection`) to properly flush the Python WAL before copying ([12d5ed5](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/12d5ed5351596cac6a789c35a3320a21ee7558c3))
- `inspect_db_signature()` also uses C SQLite for robustness when examining Python source databases
- Installer `copy_sqlite_snapshot()` now fails hard if WAL checkpoint fails instead of silently producing a truncated copy
- Added `FramedCodec::with_frame_hooks` to asupersync gRPC codec

**Recovery**: `curl -fsSL ".../install.sh?$(date +%s)" | bash -s -- --version v0.2.11 --force`

---

## [v0.2.10](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/releases/tag/v0.2.10) — 2026-03-21 **[Release]**

3 commits since v0.2.9 | [Compare](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/compare/v0.2.9...v0.2.10)

Fixes FrankenSQLite `BusySnapshot` crash-recovery bug that prevented `am` from starting after an unclean shutdown.

### Fix: FrankenSQLite BusySnapshot on Crash Recovery

During pager refresh, FrankenSQLite trusted the database header's `page_count` field without cross-checking the actual file size. A crash between growing the file and updating the header left `page_count` stale. On reopen, the MVCC snapshot boundary was set too low, rejecting the legitimately-committed page as a BusySnapshot conflict.

- Pager refresh now uses `max(header.page_count, file_size / page_size)` to include all physically-present pages ([3011762](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/3011762))
- Clippy compliance, dead code removal, and test modernization across all crates
- Also fixes `am doctor repair` hanging with the same error

---

## [v0.2.9](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/releases/tag/v0.2.9) — 2026-03-21 **[Release]**

4 commits since v0.2.8 | [Compare](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/compare/v0.2.8...v0.2.9)

Bundles the v0.2.8 HTTP server deadlock fix with additional clippy/lint fixes and sibling dependency repairs.

### Changes

- Glob case sensitivity and ATC pattern counting logic fixes ([b1836d0](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/b1836d0))
- Clippy lint fixes for ATC labeling and VoI control ([118081b](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/118081b))
- Clippy and lint fixes across core, guard, and search-core crates ([ae3d572](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/ae3d57211ae18594784e17e654931f64ecc01a77))

---

## [v0.2.8](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/releases/tag/v0.2.8) — 2026-03-21 **[Release]**

152 commits since v0.2.7 | [Compare](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/compare/v0.2.7...v0.2.8)

Largest release since v0.2.0. Introduces the ATC (Agent Traffic Control) learning stack, fixes a critical HTTP server deadlock, overhauls the web dashboard, and lands hundreds of correctness and performance fixes.

### Critical Fix: HTTP Server Hang Under Concurrent Load

Fixed a compound deadlock that caused the HTTP server to become permanently unresponsive when multiple MCP clients connected simultaneously (e.g., Codex + Claude Code).

**Root cause** — three interacting issues:
1. `dispatch()` was synchronous, blocking async worker threads on every JSON-RPC request
2. ATC operator runtime auto-selected io_uring, causing kernel D-state hangs
3. `push_event()` used `std::thread::sleep()` in async context

**Fixes**:
- `dispatch()` offloads sync router/DB work to `spawn_blocking` ([c406943](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/c406943))
- ATC operator runtime explicitly uses epoll reactor
- HTTP handler uses `push_event_async()` instead of blocking `push_event()`

### ATC (Agent Traffic Control) Learning Stack

A complete causal inference and adaptive coordination engine, built across 14+ modules:

- **Core decision engine**: martingale-based anomaly detection, calibration guard, conflict graph, liveness feedback ([bf23258](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/bf23258))
- **Learning modules**: labeling, risk budgets, regime detection, adaptation policies, experience tuples ([7271588](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/7271588))
- **Effect semantics**: preconditions, cooldown, escalation, semantic messages ([7f29595](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/7f29595))
- **Policy promotion**: doubly-robust evaluation, confidence sequences ([edb871b](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/edb871b))
- **VoI control**: value-of-information, identifiability debt, safe experiment design ([52dbff7](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/52dbff7))
- **User surfaces**: state taxonomy, noise control, safe defaults, golden workflows ([46da9f0](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/46da9f0))
- **TUI ATC dashboard**: agent/decision/detail panels ([8d32023](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/8d32023))
- Numerous numerical stability fixes: overflow, unsafe subtraction, shrinkage bias, DR variance, e-process predictability ([cdbc31d](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/cdbc31d), [2b3fde2](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/2b3fde2), [43e94e6](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/43e94e6))

### Web Dashboard Overhaul

- Screen metadata, delta streaming, and full HTML/JS client ([6654f2d](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/6654f2d))
- `/stream` endpoint with long-poll, delta journal, and viewer tracking ([158b323](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/158b323))

### Performance

- Replace O(n^2) `Vec::contains` dedup with `HashSet` in recipient handling ([943d398](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/943d398))
- `Vec` to `VecDeque` for bounded collections across DB, server, and search-core
- Eliminate unnecessary string allocations in case-insensitive comparisons ([0b14d24](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/0b14d24))
- Byte-level ASCII lowercasing for sort comparisons
- Raise Tantivy writer arena from 3MB to 15MB minimum ([4de5d7b](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/4de5d7b))

### Security

- Untrack MCP config files containing bearer tokens ([89f5e9b](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/89f5e9b))
- SVG XSS prevention ([d83cdfd](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/d83cdfd))
- 1MB file-size limit for reservation JSON in archive scanner ([1eb10dd](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/1eb10dd))
- 50MB safety limit on message file reads ([ae88f77](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/ae88f77))

### Other Notable Changes

- `saturating_sub` for all timestamp arithmetic across core, ATC, and CLI ([df98813](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/df98813))
- Preserve error context in 11 `map_err(|_|)` lock-poisoning handlers ([0e68b09](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/0e68b09))
- Replace `unreachable!()` with error return in coalesce joiner on leader panic ([711339a](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/711339a))
- Unicode-width for correct table column alignment with CJK and emoji ([a057d74](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/a057d74))
- Fix dotenv parser emitting literal backslash before escaped char ([94d9e5b](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/94d9e5b))
- Identity module expansion and reconstruct overhaul ([09f114b](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/09f114b))
- Schema expansions and search service query capabilities ([1ccd3fb](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/1ccd3fb))
- TUI compose view expansion ([ed4a8ab](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/ed4a8ab))
- Exposed `list_agents` MCP tool ([b848567](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/b848567))

---

## [v0.2.7](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/releases/tag/v0.2.7) — 2026-03-16 **[Release]**

53 commits since v0.2.6 | [Compare](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/compare/v0.2.6...v0.2.7)

Major expansion: introduces the ATC (Agent Traffic Control) module, XDG Base Directory support, comprehensive security hardening, and S3-FIFO cache improvements.

### ATC (Agent Traffic Control) Module

The foundational ATC infrastructure landed in this release:

- Decision core, liveness tracking, conflict detection, martingale testing, calibration, synthesis engine ([bf23258](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/bf23258))
- CalibrationGuard safe-mode policy engine ([0952c27](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/0952c27))
- Load router with learning-augmented capacity model ([22b5625](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/22b5625))
- Predictive coordination intelligence ([7221f97](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/7221f97))
- VCG mechanism design, queueing theory, PID controller ([b870d8f](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/b870d8f))
- `am robot atc` subcommand for ATC status queries ([aeacb1a](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/aeacb1a))
- E2E test script and load router tests ([5f4404d](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/5f4404d))
- 147 total ATC tests, including 29 new edge case tests ([f028279](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/f028279))

### Security Hardening

- Crypto passphrase leak prevention, SQL identifier escaping, Unicode path folding ([badeec3](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/badeec3))
- Harden PID hint file against symlink TOCTOU attacks ([efb4f58](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/efb4f58), [dc64384](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/dc64384))
- systemd TOCTOU fix, unit file parsing, PID hint timestamps ([965364c](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/965364c))
- SQL identifier validation to prevent injection via table aliases ([9ed3ec8](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/9ed3ec8))

### Other Highlights

- XDG Base Directory spec support with backward compatibility ([722d91f](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/722d91f))
- Composite tmux pane IDs to prevent collisions in multi-session setups ([b19147e](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/b19147e))
- Auto-stop conflicting systemd service before launching interactive TUI ([3313205](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/3313205))
- SQL plan search for Agent/Project doc kinds, cursor pagination, query facets ([f1a202d](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/f1a202d))
- S3-FIFO cache sequence tracking to prevent ghost entry amnesia ([f9154d4](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/f9154d4))
- Project boundary enforcement in `get_messages_details_by_ids` ([0b18c8a](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/0b18c8a298610affc3942e9734f1bea294861e91))
- Increased cache capacities and `CompiledPattern::cached()` ([e90e95d](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/e90e95d))

---

## [v0.2.6](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/releases/tag/v0.2.6) — 2026-03-14 **[Release]**

3 commits since v0.2.5 | [Compare](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/compare/v0.2.5...v0.2.6)

Performance-focused patch release targeting TUI responsiveness.

### Changes

- Throttle full DB snapshots when `PRAGMA data_version` is unavailable, reducing unnecessary I/O ([2f2e92c](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/2f2e92c))
- Extend poller sleep interval when `PRAGMA data_version` unavailable ([2a3c2ca](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/2a3c2cad04ace770930fdf480caf257be14c158a))
- Harden static file serving against symlink traversal; deduplicate dashboard footer widgets on dense surfaces ([f4f9a39](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/f4f9a39))

---

## [v0.2.5](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/releases/tag/v0.2.5) — 2026-03-14 **[Release]**

3 commits since v0.2.4 | [Compare](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/compare/v0.2.4...v0.2.5)

Patch release fixing project-qualified agent identity and TUI theme correctness.

### Changes

- Project-qualified agent identity, theme cache correctness, and dispatch hardening ([b752fff](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/b752fff))
- Reformat agents screen for rustfmt compliance; update tests for project-qualified identity ([9a98f4b](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/9a98f4b))

---

## v0.2.4 — 2026-03-13 **[Tag only]**

59 commits since v0.2.3 | [Compare](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/compare/v0.2.3...v0.2.4)

Major hardening release focused on symlink security, SQLite disaster recovery, installer robustness, and cross-project message isolation.

### Symlink Security Audit

Comprehensive symlink-safe filesystem traversal across the entire codebase:

- SQLite backup/recovery hardened against symlink traversal ([5e7cddc](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/5e7cddc))
- Guard plugin rewritten to read archive directly, hardened against symlinks ([c99cc0d](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/c99cc0d))
- Symlink-safe static file serving via `O_NOFOLLOW` ([9935a20](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/9935a20))
- Bundle export and deployment hardened against symlink traversal ([6072f6e](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/6072f6e))
- Consolidated `PRAGMA` checks and explicit `storage_root` threading ([7a7e7e0](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/7a7e7e0))

### SQLite Disaster Recovery

- Salvage-based disaster recovery with archive reconstruction and merge ([dcd2a47](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/dcd2a47))
- Reconstruct file reservations from archive storage ([331e920](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/331e920))
- Eliminate per-connection `journal_mode WAL` contention; harden write-retry logic ([fbb4baf](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/fbb4baf))
- MVCC retry extraction, BusySnapshot recognized as MVCC conflict ([5a5f715](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/5a5f715), [1b1e029](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/1b1e029))

### Installer Hardening

- Legacy launcher takeover shims, i64 DB adoption, env parsing hardening ([dfbefe7](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/dfbefe7))
- Detect aliases in sourced files (ACFS) and kill all Python processes ([80137e9](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/80137e9))
- Repair same-version installs when `am` is still shadowed by Python ([9215e86](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/9215e86))
- Harden PATH management for login shells and non-interactive zsh ([a60a46c](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/a60a46c))

### Other Highlights

- Cross-project message isolation, multi-addr health check, batch tracking ([ec7a7c4](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/ec7a7c4))
- Server-first dispatch for `send`, `reply`, and `inbox` commands ([652c245](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/652c245))
- Database lock probe and startup pipeline hardening ([27e46f0](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/27e46f0))
- Release bundle validation, graceful TUI signal termination ([00909be](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/00909be))
- Coalescer depth counter underflow fix with saturating CAS decrement ([eb413ac](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/eb413ac))
- IPv4/IPv6 wildcard normalization for client connections ([019f1b6](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/019f1b6))

---

## [v0.2.3](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/releases/tag/v0.2.3) — 2026-03-11 **[Release]**

93 commits since v0.2.2 | [Compare](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/compare/v0.2.2...v0.2.3)

Large feature release with DbConnGuard RAII wrapper, doctor subcommand enhancements, TOML config support, and extensive query/storage improvements.

### Database Layer

- `DbConnGuard` RAII wrapper for explicit SQLite connection cleanup ([14867d3](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/14867d3))
- All short-lived pool/search connections wrapped in `DbConnGuard` ([228891d](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/228891d))
- Release reservations returning IDs, search cache authorization keying ([a0b1742](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/a0b1742))
- Timestamps module with clock skew detection ([c51dc23](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/c51dc23))
- Batch thread participant lookup and unified inbox pagination fix ([5bae811](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/5bae811))
- Denormalize `recipients_json` on message insert ([45052f1](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/45052f1))

### CLI and Doctor Enhancements

- Foreign key integrity checks and orphaned recipient cleanup ([d69bbf7](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/d69bbf7))
- `sqlite3 quick_check` rescue and new integration tests ([4502029](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/4502029))
- SQLite health probes, doctor orphan detection, MCP config URL repair ([890e40d](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/890e40d))
- Auto-stop conflicting systemd service before TUI launch

### Configuration and Tooling

- TOML config support, HTTP URL mode detection, pool-scoped caching, provider prefix stripping ([dd71439](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/dd71439))
- Tool-aware MCP config rewriting, SQLite lock retry, snapshot hardening ([08876b7](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/08876b7))
- Codex integration switched from stale JSON/HTTP to TOML/stdio config ([ca6e0dc](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/ca6e0dc))

### Messaging and Agent Resolution

- Agent name normalization to PascalCase across all entry points ([0d3136e](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/0d3136e))
- LLM integration hardening: Anthropic auth, JSON extraction, char boundary safety ([758604c](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/758604c))
- BCC redaction in inbox copies, proper BCC archival ([f46de2f](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/f46de2f))
- Strict validation for limits, repo paths, and ordered-prefix parsing ([595af1d](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/595af1d))

### Other Highlights

- Async supervisor architecture, SQL query caching, MVCC async backoff ([038e53c](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/038e53c))
- Robust HTTP supervisor lifecycle with timeout-escalated shutdown ([43f6a11](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/43f6a11))
- Race condition fix in `now_micros()` monotonic clock ([4a71727](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/4a71727))
- Removed optional kafka feature from asupersync dependency (enables Windows builds) ([a813517](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/a81351741a39b876156b45103f07ca55ec3cb5b7))

---

## v0.2.2 — 2026-03-07 **[Tag only]**

84 commits since v0.2.1 | [Compare](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/compare/v0.2.1...v0.2.2)

Massive stabilization release. Case-insensitive agent resolution everywhere, durability probes, TUI V3 screens, and deep query/storage hardening.

### Case-Insensitive Agent Resolution

Unified case-insensitive agent name matching across the entire stack — DB, CLI, server, tools, and resources:

- ([baa350f](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/baa350f), [516a089](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/516a089), [f5ab55e](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/f5ab55e))

### TUI Improvements

- Shared tick event batching, interior mutability, layout artifact prevention ([adad36c](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/adad36c))
- JSON tree detail view, search filter presets, contrast guard cadence ([898510f](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/898510f))
- JSON tree clipboard copy support and contextual copy actions ([67eeec0](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/67eeec0))
- Dashboard hotspot remediation with thread-local caches ([75e511b](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/75e511b))
- Dirty-state gated data ingestion on all TUI screens ([b9bff58](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/b9bff58))

### Database and Storage

- Durability probes, pool improvements, hardened agent/message operations ([fa9b3e9](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/fa9b3e9))
- Enhanced search v3, integrity metrics, query pagination, JSONL reconstruction ([eb7b21b](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/eb7b21b))
- Schema migrations through canonical SQLite to prevent index corruption ([c630e7f](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/c630e7f))
- SQL injection fix, WAL compatibility, agent dedup, metric safety ([3eab38d](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/3eab38d))
- Post-migration integrity guard ([cbc574c](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/cbc574c))

### Installer and CLI

- SHA256 checksum verification in `install.ps1` and E2E test hardening ([8006931](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/8006931))
- `--no-tui` flag, `--rollback` migration, expanded doctor checks, and startup refactor ([8449aee](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/8449aee))
- Service management CLI, pane identity tools ([7c374ff](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/7c374ff))
- Kafka transport enablement ([cfcaa05](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/cfcaa05))

### Other Highlights

- Health signature headers, PID-aware port clearing ([9a08dad](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/9a08dad))
- Attachment processing, thread ID validation, guard environment tests ([3496194](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/3496194))
- TUI spin watchdog, sqlite auto-recovery ([eff669d](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/eff669d))
- Deferred background worker startup and ambient renderer optimization ([95c4ba9](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/95c4ba9))
- Lazy screen materialization, semantic db-stats diffing ([f0a09af](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/f0a09af))

---

## [v0.2.1](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/releases/tag/v0.2.1) — 2026-03-03 **[Release]**

27 commits since v0.2.0 | [Compare](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/compare/v0.2.0...v0.2.1)

Focused on `am doctor fix`, TUI V2 testing, and installer/CLI robustness.

### am doctor fix

- Automatic remediation for 6 fixable checks via `am doctor fix` subcommand ([e9a7dbe](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/e9a7dbe0e5bfa08be518419a6080af9d8f5deea3))
- Bug fixes, robustness hardening, and performance improvements across core/db/server/tools ([acd475f](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/acd475f))

### Installer

- `--dry-run` preview mode and piped install confirmation ([7e2f875](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/7e2f875))

### TUI

- Batch `mark_unread` + 21 batch selection tests ([53a5051](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/53a5051))
- 31 V2 TUI tests across 4 modules ([30c9d43](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/30c9d43))
- Theme snapshot tests with 16ms budget enforcement ([81adf8f](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/81adf8f))
- Eliminate double housekeeping tick, persist contrast-guard cache, fix search hot-loop ([18489a5](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/18489a5))
- Reservation expiry-driven refresh ([7777e6d](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/7777e6d))

### Performance

- Static `LazyLock` regexes, `getrandom` for agent names, coalescer `worker_count` ([c821a4f](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/c821a4f))
- Persistent caches for cleanup prober, embedding queue drain, retry scheduling ([5eba4d5](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/5eba4d5))

### Testing

- Truth oracle, incident capture, and migration test infrastructure ([9981998](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/9981998))
- Incident regression gates, robust alias displacement, E2E test hardening ([29e48dd](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/29e48dd))

---

## [v0.2.0](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/releases/tag/v0.2.0) — 2026-03-02 **[Release]**

325 commits since v0.1.0 | [Compare](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/compare/v0.1.0...v0.2.0)

Massive release with improvements across every subsystem. Introduces Search V3, v13 database migrations, installer uninstall mode, and deep security/performance hardening.

### Search V3 Architecture

Complete search rewrite from SQL-based to two-tier Tantivy + lexical bridge architecture:

- Decomposed monolithic search into focused modules ([43ec691](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/43ec691))
- Incremental Tantivy backfill with watermark-based skip ([bf7a6c2](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/bf7a6c2))
- Scope-aware cache discriminator to prevent cross-scope query collisions ([d376b82](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/d376b82))
- CLI and robot search routed through Search V3 service ([c758017](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/c758017))
- All TUI screens migrated from SQL planner to unified search service ([c94f5cd](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/c94f5cd))
- Removed SQL LIKE fallback entirely ([9429825](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/9429825))

### Database Improvements

- v13 poller indexes, `busy_timeout` pragma, lock-retry migration engine ([8322891](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/8322891))
- Three-way JOIN replaced with two-phase sampling in consistency probe ([df6e0c7](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/df6e0c7))
- Extreme load resilience: SQLite hardening, async commit coalescer, write-behind queue ([da5e317](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/da5e317))

### Performance

- Deferred backfill, integrity cache, persistent poller connections ([24b5636](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/24b5636))
- Startup latency optimization with deferred work ([27cd3fe](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/27cd3fe))
- Suppress noisy fsqlite tracing, minimize worker pool allocations ([44ecfc3](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/44ecfc3))

### Security

- TOCTOU race fix in env file creation ([bba526a](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/bba526a))
- Enforce 0600 permissions on env files containing bearer tokens ([2acd47d](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/2acd47d))
- Path traversal prevention in agent detection module ([a827c2e](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/a827c2e))
- Structured config classification and correctness fixes ([899e0cd](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/899e0cd2164ef1c3992d9d1b37798bdb69bf25f1))

### Installer

- Uninstall mode, MCP config management, Windows installer ([77b4215](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/77b4215))
- Setup self-heal fingerprint cache and preflight optimization ([3d9c9f0](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/3d9c9f0))

### TUI

- Resize coalescing and queue stats for remote terminal WebSocket ([bfbbadb](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/bfbbadb))
- TUI poller improvements with dirty-state gating ([7777e6d](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/7777e6d))
- Pre-TUI startup banner, smarter reservation validation, port migration to 8899 ([ef15f00](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/ef15f00))

### Server

- Ack TTL, cleanup, disk monitor, retention, integrity guard ([48976b8](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/48976b8))
- Retention engine, health endpoints, tool metrics, mail UI module ([2eb5a8f](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/2eb5a8f))
- RBAC/JWT enforcement, tool instrumentation, mail UI pagination ([86dd07d](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/86dd07d))

### CLI and Tools

- ~15 CLI commands implemented, replacing `NotImplemented` stubs ([935b183](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/935b183))
- CLI overhaul with rich output and expanded conformance test runner ([9953f94](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/9953f94))
- Guard rewrite with rename and ignorecase support ([c4c742a](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/c4c742a))
- LLM stub mode, identity resource, tool metrics reset ([a748623](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/a748623))

### Other Highlights

- TOON output format, share pipeline, CLI expansion ([285036b](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/285036b))
- Addressed GitHub issues #8-#18 across multiple subsystems ([d3ec890](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/d3ec890))
- Embedding jobs, rollout system, search updater improvements ([df10995](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/df10995))
- S3-FIFO frequency count preservation on main queue promotion ([3d393dc](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/3d393dc))
- Glob-to-regex rewrite with `[]`, `{}` syntax support ([894ebb1](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/894ebb1))

---

## [v0.1.0](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/releases/tag/v0.1.0) — 2026-02-24 **[Release — Initial]**

802 commits | [Compare](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/compare/213eac7750fa368ca2b39fa72e455034158023ff...v0.1.0)

Initial public release of the Rust port of [mcp_agent_mail](https://github.com/Dicklesworthstone/mcp_agent_mail). Full feature parity with the Python reference implementation plus substantial performance improvements.

Development began on 2026-02-05 with the [initial commit](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/213eac7750fa368ca2b39fa72e455034158023ff).

### Capabilities at Launch

- **34 MCP tools** across 9 clusters: messaging, reservations, search, macros, build slots, identity, resources, contacts, and products
- **23+ MCP resources** with conformance-tested JSON output
- **15-screen TUI** operations console with real-time dashboard, search, timeline, and analytics
- **Dual-mode interface**: MCP server (stdio/HTTP transport) + operator CLI (`am`)
- **Git-backed archive** for human-auditable message history
- **SQLite** with WAL, connection pooling, write-behind cache, FTS5 full-text search
- **Pre-commit guard** for file reservation enforcement with conflict detection
- **Share/export pipeline** with snapshot, scope, scrub, finalize, bundle, and optional encryption
- **Conformance test suite** against Python reference fixtures
- **TOON output format** with deterministic stub encoders
- **E2E test harness** with guard test suite
- **Benchmarks** with baseline budgets and golden outputs
- **Multi-platform binaries**: Linux x86_64, macOS arm64, Windows x86_64
- **curl-bash installer** with auto-detection and Codex CLI auto-configuration

### Key Implementation Details

- Full storage layer: git-backed archive, attachment pipeline with WebP conversion, advisory file locks, commit queue batching ([c05bb3b](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/c05bb3b), [eb5bb09](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/eb5bb09), [ec3bd47](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/ec3bd47))
- Share/export pipeline with snapshot, scrub, bundle, and crypto ([be68db2](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/be68db2))
- Query tracking and instrumentation module ([6526d80](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/6526d80))
- Guard conflict detection with reservation reading and rename handling ([09aa77e](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/09aa77e))
- Tool filtering profiles and config-aware builder ([040298e](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/040298e))
- Multi-platform release distribution and curl-bash installer ([1c569d7](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/1c569d7b1a3f51e48c0f0d4fe97a8846a118c7a3))
- Console split layout, command palette, PTY E2E tests ([dbf52f1](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/dbf52f1), [d601d55](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/commit/d601d55))
