# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.6.1] - 2026-04-06

### Changed
- Move coverage badge to its own line in README.md for cleaner formatting

## [1.6.0] - 2026-04-06

### Added
- Merkle tree utility (`MerkleTree.js`) — binary SHA-256 tree with proof generation and verification
- Merkle epoch system — automatic Merkle root commitment over configurable epoch windows (default 100 blocks)
- `merkle_epochs` table for storing committed epoch roots
- `GET /transparency/:chain/:network/proof/:block_index` — Merkle inclusion proof endpoint
- `GET /transparency/:chain/:network/root/latest` — latest committed Merkle root endpoint
- Rate limiting on all transparency endpoints (configurable, default 10 req/min per IP)
- `MERKLE_EPOCH_SIZE` and `TRANSPARENCY_RATE_LIMIT` config options

## [1.5.2] - 2026-04-05

### Changed
- Moved Stryker mutation configs (`stryker.config.json`, `stryker.quick.config.json`) from project root into `test/mutation/`
- Updated `test:mutate`, `test:mutate:quick`, and `test:mutate:check` npm scripts to reference new config paths

## [1.5.1] - 2026-04-05

### Changed
- `README.md` — updated to match platform README structure: version badge to 1.5.0, added tests (725 passing) and coverage badges, added Test Suite breakdown table, expanded Scripts from 3 to 18 entries, added Input validation and test count to Features, updated Development dependencies with all current packages (chai, fast-check, proxyquire, Stryker)

## [1.5.0] - 2026-04-05

### Added
- Mutation testing infrastructure using StrykerJS v8.7.1
  - `stryker.config.json` — Full mutation config: 15 source files, 2218 mutants, perTest coverage analysis, HTML/JSON/clear-text reporters
  - `stryker.quick.config.json` — Priority 1 modules only (ClientApplier, HashVerifier, validation, ServerPoller, ClientRollback, TransparencyLog): 6 files, 795 mutants
  - Thresholds: break at 80%, low at 85%, high at 95% mutation score
  - Incremental mode for CI: only re-tests mutants in changed files
  - StringLiteral mutations excluded (stubbed console produces equivalent mutants)
  - Static mutants ignored (module-level compiled regexes in validation.js)
- npm scripts: `test:mutate` (full run), `test:mutate:quick` (Priority 1 only), `test:mutate:check` (incremental)
- `.gitignore` for mutation report outputs and Stryker temp files
- `@stryker-mutator/core` and `@stryker-mutator/mocha-runner` dev dependencies

## [1.4.0] - 2026-04-05

### Added
- Performance and load testing suite: 7 scenario files across `test/perf/scenarios/`
  - `01-payload-throughput.test.js` — Block payload build rate at varying action densities (1, 10, 50, 200 actions/block)
  - `02-snapshot-performance.test.js` — Full and incremental snapshot export timing with compression metrics
  - `03-bootstrap-apply.test.js` — Client bootstrap throughput measuring rows/second across dataset sizes
  - `04-subscriber-scaling.test.js` — WebSocket broadcast overhead scaling from 1 to 50 concurrent subscribers
  - `05-sustained-sync.test.js` — Long-running server+client sync with degradation detection and memory leak checks
  - `06-incremental-catchup.test.js` — Catch-up performance across gap sizes (10 to 500 blocks) with linearity checks
  - `07-rollback-performance.test.js` — Reorg rollback timing by depth (1 to 100 blocks) with scaling assertions
- Performance test infrastructure: `test/perf/setup/`
  - `metrics-collector.js` — Block-level and operation-level timing with percentile distributions, memory snapshots, and event loop delay monitoring
  - `report-generator.js` — Console summary, JSON, and Markdown report output
  - `data-generator.js` — Bulk data seeder wrapping e2e fixtures with batch control
  - `perf-setup.js` — Shared environment boot/teardown reusing e2e helpers
- npm scripts: `test:perf` (unlimited timeout), `test:perf:quick` (reduced block count)

## [1.3.0] - 2026-04-05

### Fixed
- HashVerifier: `verifyChainContinuity()` now guards against null/undefined payload instead of crashing with `TypeError: Cannot read properties of null`
- HubClient: `getIndexerConfigs()` now guards against null/non-object values in the hub response tree instead of crashing when a network value is null

### Added
- Fuzz testing suite: 55 property-based tests across 6 suites using `fast-check`
  - `tier1-client-applier.fuzz.js` — crash safety for applyBlock, snapshots, _insertRows; transaction leak detection; INSERT IGNORE correctness; batch sizing; null coercion
  - `tier1-hash-verifier.fuzz.js` — crash safety for compareBlockHashes and verifyChainContinuity; return shape invariants; match/mismatch bidirectional correctness; sequential block validation
  - `tier2-server-poller.fuzz.js` — _buildBlockPayload with fuzzed DB returns; per-table error isolation; payload shape verification
  - `tier2-client-rollback.fuzz.js` — rollback with fuzzed block_index; transaction safety; per-table error tolerance; balance rebuild ordering; sync_meta cleanup
  - `tier2-hub-client.fuzz.js` — getIndexerConfigs with adversarial responses; _parsePort with boundary values; network error resilience
  - `tier3-config.fuzz.js` — getConfig with fuzzed env vars; type/range invariants; VERIFY_HASHES bidirectional spec; hardcoded constant immutability
- Fuzz test infrastructure: `test/fuzz/setup/harness.js`, generators (`values.js`, `rows.js`, `payloads.js`)
- `fast-check` dev dependency
- npm scripts: `test:fuzz`, `test:fuzz:tier1`, `test:fuzz:tier2`, `test:fuzz:tier3`, `test:fuzz:quick`

## [1.2.0] - 2026-04-05

### Fixed
- Config parsing: `parseInt(x) || default` pattern replaced with `parseIntSafe()` to correctly handle zero as a valid value (e.g. `SNAPSHOT_RATE_FULL=0` no longer silently becomes `1`)
- Config parsing: negative values now clamped — `WS_MAX_PER_IP=-1` clamps to 1 (previously accepted, causing all WebSocket connections to be rejected)
- Config parsing: `VERIFY_HASHES` is now case-insensitive — `"FALSE"`, `"False"`, `"false"` all disable verification (previously only exact lowercase `"false"` worked)
- HubClient: `db_port` parsing uses same safe pattern — zero preserved, negatives default to 3306
- ClientSync: `_bootstrapFromSnapshot()` no longer recurses infinitely when all sync sources fail — stops after trying each source once
- Database: added `bigIntAsNumber: true` to MariaDB connection pool to prevent BigInt object returns

### Added
- Boundary test suite: 159 tests across 12 files in `test/unit/boundaries/`
  - `config-parsing.test.js` — zero values, negative clamping, NaN fallbacks, VERIFY_HASHES case insensitivity
  - `poll-limit.test.js` — 99/100/101 blocks per poll boundary
  - `batch-insert.test.js` — 99/100/101 rows per INSERT batch boundary
  - `transparency-page.test.js` — page/limit clamping (0, -1, 1000, 1001, NaN, floats)
  - `hash-continuity.test.js` — exact +1 block requirement, null bootstrap, zero-based chains
  - `reorg-detection.test.js` — same-height non-detection, null currentBlock, first poll
  - `websocket-limits.test.js` — per-IP limit at 1/3/4, backpressure at 49/50/51
  - `rollback-scope.test.js` — rollback at block 0/1/highest/non-existent, null action index
  - `circuit-breaker.test.js` — 9/10 failure threshold, half-open recovery, 30-attempt max, exponential backoff arithmetic
  - `block-index.test.js` — block_index=0 handling, JS safe integer limits, BigInt precision loss
  - `source-array.test.js` — empty/trailing/leading commas, whitespace, recursion limit
  - `hub-port-parsing.test.js` — HubClient._parsePort zero/null/negative/NaN boundaries

## [1.1.0] - 2026-04-04

### Added
- Comprehensive unit test suite: 203 tests across 12 test files covering all modules
- Test files: utility, config, HashVerifier, HubClient, TransparencyLog, BlockBroadcaster, ClientApplier, ClientRollback, ServerPoller, SnapshotBuilder, ClientSync, SyncService
- `proxyquire` dev dependency for mocking ESM-only mariadb module in SyncService tests

### Changed
- Replaced 77 copied SQL files with dynamic schema replication from the live indexer database via `SHOW CREATE TABLE` — eliminates drift risk when indexer tables are added or modified
- `db.js`: added `replicateSchema(sourceDb)` method; renamed `verifyTables()` to `verifySyncTables()` (only handles sync-service-owned tables like `sync_meta`)
- `SnapshotBuilder.js`: discovers tables dynamically from `information_schema` instead of using a hardcoded table list
- `ClientSync.js`: added `_fetchAndApplySchema()` to fetch table DDLs from server's new `GET /schema/:chain/:network` endpoint during client bootstrap

### Added
- `GET /schema/:chain/:network` REST endpoint — returns all table DDLs for schema replication by remote clients

## [1.0.0] - 2026-04-03

### Added
- Initial release of xchain-indexer-sync
- Dual-mode architecture: server mode (serves data) and client mode (replicates data)
- Hub auto-discovery: calls xchain-hub `getallconfigs` to find all installed indexer databases
- Single-instance multi-chain support: one process serves all chains/networks on the node
- REST API: `GET /status`, `GET /snapshot/:chain/:network`, `GET /snapshot/:chain/:network/since/:blockHeight`, `GET /transparency/:chain/:network/roots`
- WebSocket API: `WS /subscribe/:chain/:network` for real-time block and reorg streaming
- Full snapshot export with gzip streaming compression for bootstrap
- Incremental snapshot export for catch-up after downtime
- Real-time block broadcasting via WebSocket with per-chain/network subscriptions
- Hash chain verification using indexer's existing per-block SHA256 hashes (ledger, actions, contracts)
- Cross-source hash comparison for clients syncing from multiple servers
- Transparency log: append-only per-block hash record for auditability
- Client bootstrap: full snapshot download with hash verification against secondary source
- Client catch-up: incremental snapshot + WebSocket subscription for gap detection and self-healing
- Client rollback: mirrors indexer's Rollback.js table lists for reorg handling
- Rate limiting on snapshot endpoints (configurable per-IP limits)
- WebSocket connection limiting (configurable per-IP)
- WebSocket backpressure: drops subscribers that fall behind (>50 buffered messages)
- Circuit-breaker database connections with automatic recovery
- Periodic hub re-polling (5 min) to detect newly installed chains
- MariaDB connection pool per chain/network with independent circuit breakers
