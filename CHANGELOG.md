# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.9.0] - 2026-08-14

First release of the XChain Platform release train. Every component in the train
now shares one platform version, so "XChain 0.9.0" names an exact, reproducible
set of software rather than a rough era.

### Changed
- Adopted the platform version stream. This component moves from `1.7.3` to
  `0.9.0`. **The number is lower but the release is newer**: the platform stream
  starts at 0.9.0 for the testnet series, and 1.0.0 is reserved for mainnet.

<!-- ------------------------------------------------------------------------
     Versions BELOW this line are this component's own legacy stream, from
     before the release train. They are kept for history and are NOT comparable
     to the platform versions above: a higher legacy number is an older release.
     ------------------------------------------------------------------------ -->

## [1.7.3] - 2026-08-13

### Fixed
- CORS_ORIGIN is now a comma-separated allowlist instead of being echoed back to every origin.
- Server status no longer certifies a stalled SQL replica as caught up.
- Stake-weighted quorum now rejects a validator entry with a missing or invalid weight instead of silently lowering the quorum threshold.
- Checkpoint verification now requires a valid weight and full commitment roots on every validator entry.
- Indexer public keys now ride every incremental snapshot so replicas no longer freeze at bootstrap height.
- State-commitment and rollback handling picked up several correctness fixes around catch-up, bootstrap resolution, and parity guards.

### Added
- Optional per-table content checksums on /status catch row-count-equal content substitution.
- A new bootstrap-stampede performance scenario exercises concurrent snapshot downloads against the server's shedding logic.
- State-commitment activation heights are now armed for BTC, LTC, and DOGE.
- State-hash gating now covers the token supply and poll-finalization classes.
- The table-lifecycle registry now generates stream topology and rollback table sets directly, so they can no longer drift from the source indexer.
- Hub client requests now carry an API key when one is configured.

### Fixed
- Chain tickers are now normalized consistently across every activation lookup, fixing a mismatch that had halted production replicas and disabled state-commitment roots fleet-wide.
- Each forward-sync batch now reads from a single consistent database snapshot instead of a live, shifting one.
- The hub client's database host fallback now resolves over IPv4 instead of a broken IPv6 default.
- Governance voting tables now replicate per block and roll back correctly on reorg.

### Added
- Poll finalization now replicates to followers, with a matching rollback path.
- New unit coverage pins the exact SQL write modes used for full-dump table inserts.

## [1.7.2] - 2026-07-16

### Fixed
- Burst-built blocks now ship a null state hash through the existing skip gate, avoiding a false halt.
- Escrow action-index wire semantics are now documented consistently everywhere they're referenced.
- Merkle reorg handling now classifies unstreamed tables correctly and heals already-imported copies, backed by a new exhaustiveness guard.
- Dispenser data now streams as one consistent response instead of being reconciled across multiple requests.
- Operator-local table handling now derives from the shared table-lifecycle registry instead of a hand-maintained list.
- Decoder rollback table order now derives from the replicated-tables registry instead of copied literals.
- The orphan-sweep parity guard now derives its checks from the lifecycle registry, including the icons sweep.
- A vacuous decoder-exclusion test was fixed and pinned to the correct table count.

## [1.7.1] - 2026-06-20

### Security
- A reorg deeper than the configured rollback limit now halts the replica instead of silently dropping canonical blocks.
- Contract-stake slash rollback now breaks same-block ties the same way the source does, keeping replicas byte-identical.
- Block hash queries now use a full deterministic sort order, preventing divergent hashes from engine row ordering.
- The admin halt-clear endpoint now fails closed when no API key is configured, and startup logs a warning in open mode.
- Startup now logs an explicit security warning describing the service's data-integrity posture.
- Table names from a remote sync source are now validated before use in SQL, closing a read-injection path.
- A vulnerable dev-only dependency was pinned to a patched version.
- CORS now defaults to disabled instead of allowing any origin.

### Fixed
- A new updated-rows channel replicates in-place mutations to existing rows, closing a permanent divergence class.
- The server poller now resumes its broadcast cursor from the last confirmed block instead of the live tip, so downtime no longer causes skipped blocks.
- Token and address controller events now roll back on reorg and stream per block.
- A reorg now correctly resets an anchor record that had been left in an inconsistent state by an orphaned chunk.
- Five tables that were missing from rollback and per-block streaming were added to both.
- A failed snapshot bootstrap now retries with backoff and fails loudly instead of silently starting from an empty database.
- Schema catch-up now rejects a malformed column definition, closing a DDL-injection path.
- Reorg rollback now purges stale price rows instead of leaving them until the next sync.
- The client now detects a fork at the tip by comparing block hashes and catches up automatically instead of discarding the mismatched block.
- The row-count completeness check now includes the sync-metadata table.
- A server-side reorg now prunes stale transparency-log data, with each invalidated epoch recorded for audit.
- Hub client failures now record which endpoint was tried and why, for better diagnostics.
- Live per-block payloads now carry all interning tables instead of just two, preventing dangling references.
- Test fixtures now clear the mempool table between end-to-end runs.
- The decoder events table is now fully re-dumped on every incremental snapshot, so followers no longer miss new rows.
- A table that should never grow via insert-only streaming was removed from replication, fixing a permanent row-count mismatch.
- Three tables that never existed were removed from rollback and replication, and the real attestation table was added in their place.
- A misnamed attestation-aggregate table reference was corrected so orphan cleanup on reorg actually runs.
- The hub client now sticks with the last responding endpoint instead of retrying the first one every cycle.
- A decoder bootstrap now cross-checks per-table row counts instead of trusting the snapshot blindly.
- The service now gives up waiting on the hub after a configurable timeout instead of retrying forever.
- The dependency lockfile was regenerated to match the declared dependency versions.
- A status gap check no longer races a live event with a redundant catch-up when the source is exactly one block ahead.
- Schema replication now propagates newly added columns and self-heals two ownership columns even when the source is unreachable.
- Balance rebuild steps now rethrow unexpected database errors instead of silently swallowing them.
- Reorg rollback now correctly drops orphaned attestation statistics rows.
- Contract balances now recompute correctly on reorg and whenever relevant tables change.
- Reorg rollback now purges orphaned price rows.
- Two invalid per-block table references that had been silently failing every block were removed.
- A snapshot builder race that could mix data from different block heights was fixed by reading everything in one transaction.
- Startup resume now requests the correct next block, fixing a restart loop.
- Applying an incremental snapshot now recomputes balances when relevant rows are present.
- Several missing tables were added to reorg rollback so orphaned rows no longer survive a reorg.
- Newly added protocol tables from a schema expansion were wired into live block payloads and snapshots.

### Added
- Validator status now reports roster members that have never checked in, and lapsed entries age out gracefully instead of being deleted.
- The health endpoint now reports how stale the service's view of the hub configuration is.
- A configuration template documents every environment variable with safe defaults.
- Database queries now time out instead of being able to hang a pooled connection indefinitely.
- A new health endpoint reports per-database circuit state and returns an error status when any database is unavailable.
- Subscriber status now distinguishes "no data yet" from a genuine caught-up state.
- Subscriber status now reports lag confidence and a structured validator summary.
- The client now compares its own row counts against the source and logs any shortfall.
- A new rollback-coverage test guard fails CI if any replicated table is left unhandled by rollback logic.
- Decoder table lists are now exposed for the rollback-coverage guard to verify.
- Per-subscriber tracking and heartbeats were added, along with a subscriber breakdown on the status endpoint.

### Changed
- Dependency versions are now pinned exactly instead of using flexible ranges, for reproducible installs.
- The snapshot schema version is now tracked per database type so indexer and decoder can advance independently.
- Rate-limiting configuration was updated for compatibility with the current rate-limit library.
- Hub configuration polling is now incremental, reducing per-cycle data transfer.
- A dependency range was aligned with the rest of the platform.
- A backpressure threshold is now configurable by operators instead of hardcoded.
- The Docker image build now fails if the lockfile is out of sync instead of silently drifting.
- Startup failures now log full detail and exit, making crashes visible to process supervisors.
- A rollback query was aligned with the source indexer's equivalent logic.

## [1.7.0] - 2026-04-08

### Added
- Subscribers can now request a lighter "infrastructure-only" sync mode per chain.
- An infrastructure-only table set was defined for the lighter sync mode.
- Broadcast payloads now filter down to infrastructure tables for infra-only subscribers.
- The client now supports configuring sync mode per chain via environment variables.
- Price actions are now replicated like other on-chain actions.

## [1.6.1] - 2026-04-06

### Changed
- Moved the coverage badge to its own line in the README for cleaner formatting.

## [1.6.0] - 2026-04-06

### Added
- Added a Merkle tree utility supporting proof generation and verification.
- Added automatic Merkle root commitment over configurable epoch windows.
- Added a table for storing committed epoch roots.
- Added an endpoint for fetching a Merkle inclusion proof for a block.
- Added an endpoint for fetching the latest committed Merkle root.
- Added rate limiting to the transparency endpoints.
- Added configuration options for epoch size and transparency rate limiting.

## [1.5.2] - 2026-04-05

### Changed
- Moved the mutation-testing configuration files into their own test directory.
- Updated the mutation-testing npm scripts to match the new config paths.

## [1.5.1] - 2026-04-05

### Changed
- Updated the README to match the platform's standard structure and badges.

## [1.5.0] - 2026-04-05

### Added
- Added mutation testing infrastructure with full and quick-run configurations.
- Added npm scripts for running full, quick, and incremental mutation tests.
- Ignored mutation-testing report output in version control.
- Added the mutation-testing dev dependencies.

## [1.4.0] - 2026-04-05

### Added
- Added a performance and load-testing suite covering throughput, snapshots, bootstrap, scaling, sustained sync, catch-up, and rollback.
- Added performance test infrastructure for metrics collection and reporting.
- Added npm scripts for full and quick performance runs.

## [1.3.0] - 2026-04-05

### Fixed
- The chain-continuity verifier no longer crashes on a null or missing payload.
- The hub client no longer crashes on a malformed hub response.

### Added
- Added a property-based fuzz testing suite across six areas of the sync pipeline.
- Added fuzz test infrastructure and generators.
- Added the fuzz-testing dev dependency.
- Added npm scripts for running fuzz tests by tier.

## [1.2.0] - 2026-04-05

### Fixed
- Config parsing now accepts zero as a valid value instead of falling back to a default.
- Config parsing now clamps negative values instead of accepting them.
- Hash verification is now case-insensitive when reading its enable/disable setting.
- Hub client port parsing now handles zero and negative values correctly.
- Snapshot bootstrap no longer recurses infinitely when every source fails.
- Database results no longer return raw BigInt objects.

### Added
- Added a boundary test suite covering config parsing and core protocol limits.

## [1.1.0] - 2026-04-04

### Added
- Added a comprehensive unit test suite covering every module.
- Added a dev dependency for mocking the database driver in tests.

### Changed
- Replaced copied SQL schema files with dynamic schema replication from the live database.
- Renamed a database verification method for clarity.
- The snapshot builder now discovers tables dynamically instead of using a hardcoded list.
- The client now fetches table schemas from the server during bootstrap.

### Added
- Added a REST endpoint returning table schemas for remote clients.

## [1.0.0] - 2026-04-03

### Added
- Initial release.
- Dual-mode architecture: server mode serves data, client mode replicates it.
- Automatic discovery of installed databases via the hub.
- Single-instance support for multiple chains and networks.
- A REST API for status, snapshots, and transparency data.
- A WebSocket API for real-time block and reorg streaming.
- Full snapshot export with compression for bootstrapping new nodes.
- Incremental snapshot export for catching up after downtime.
- Real-time block broadcasting with per-chain subscriptions.
- Hash-chain verification using the indexer's existing per-block hashes.
- Cross-source hash comparison for clients syncing from multiple servers.
- An append-only transparency log for auditability.
- Full-snapshot client bootstrap with hash verification.
- Client catch-up via incremental snapshots and gap detection.
- Client rollback mirroring the indexer's reorg handling.
- Rate limiting on snapshot endpoints.
- WebSocket connection limiting.
- WebSocket backpressure handling for slow subscribers.
- Circuit-breaker database connections with automatic recovery.
- Periodic re-polling of the hub to detect newly installed chains.
- A per-chain database connection pool with independent circuit breakers.
