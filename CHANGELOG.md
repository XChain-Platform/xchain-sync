# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
