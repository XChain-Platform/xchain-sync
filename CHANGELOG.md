# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `test/unit/rollback-coverage.test.js` — a rollback-coverage guard. It reads the set of tables `ServerPoller` replicates (per dbType) and asserts every one is handled by `ClientRollback` on reorg (rolled back, recomputed, special-cased, or explicitly exempt with a reason), for both the `indexer` and `decoder` DB types. `ClientRollback`'s table lists are a hand-maintained mirror of `xchain-indexer/src/rollback.js` and have drifted before; this fails at CI time when a replicated table is left unhandled, instead of surfacing as silent replica divergence after a reorg.
- `ClientRollback.js` — the decoder rollback table lists are now exposed as `decoderBlockTables` / `decoderTxScopedTables` constructor properties (a single source of truth `_rollbackDecoder` consumes), so the coverage guard can verify the decoder path. Behaviour is unchanged.

### Security
- `CORS_ORIGIN` now defaults to `false` (CORS disabled) instead of `'*'` (any origin) when the env var is unset. The wildcard default served cross-origin requests from any domain in a default deployment, diverging from the other services (hub and encoder default to `false`). Operators who need cross-origin access must now set `CORS_ORIGIN` explicitly; the README `.env` examples document the variable. Breaking for any deployment that relied on the implicit wildcard.

### Fixed
- `db.js` / `ClientSync.js` / `SyncService.js` — schema replication now propagates columns added to a table after a replica was first built, instead of only ever creating whole missing tables. Previously both schema paths (`replicateSchema`'s direct source-DB copy and `ClientSync._fetchAndApplySchema`'s server `/schema` fetch) skipped any table that already existed locally, so a column introduced upstream never reached a pre-existing replica — the first snapshot or per-block INSERT carrying that column failed with `Unknown column ... in 'field list'` and rolled back, permanently stalling sync for that table until the column was added by hand. `db.addMissingColumns(table, sourceDdl)` now closes the gap generally for every already-existing table, deriving each missing column's definition from the authoritative `CREATE TABLE` DDL (validated, semicolon-guarded) and applying it with a best-effort `ALTER TABLE ... ADD COLUMN` before any snapshot transaction opens. `db.ensureReplicatedColumns()` is a DDL-free backstop for the `give_ownership`/`get_ownership` columns on `orders` and `swaps` (added for token-ownership trading; both `TINYINT(1) NOT NULL DEFAULT 0`, so the ADD COLUMN backfills existing rows safely) — it runs at the tail of `replicateSchema` and unconditionally in `SyncService`'s client-mode discovery, so an indexer replica self-heals these columns on next startup even when the source DB is unreachable and no DDL is available. Scoped to indexer replicas; tables absent locally are left for fresh creation.
- `ClientApplier.js` / `ClientRollback.js` — the `balances` and `contract_balances` rebuild steps no longer swallow every error. They previously caught and discarded all exceptions to tolerate the expected missing-table case on a decoder replica (`ER_NO_SUCH_TABLE`, errno 1146), but that also hid real DB failures mid-rebuild (lock-wait timeout, connection drop, arithmetic cast overflow), letting the block/rollback transaction commit with stale or empty derived balances and no log entry — a silent divergence from the source. The catch now rethrows anything that isn't errno 1146, so genuine failures propagate to the containing transaction's handler, which rolls back, logs, and rethrows as it already does for every other table.
- `ClientRollback.js` — `_rollbackIndexer` now drops the orphaned-range rows of `attestation_validator_stats` on reorg (those with `last_updated_block >= block_index`), so the replica no longer serves overcounted per-validator `fulfilled`/`missed` aggregates after a chain reorg. Unlike on the source, the replica does NOT recompute these rows: `attestation_validator_stats` is never block-streamed (it only rides along in full snapshots), and the thin replica DB lacks the capability/governance machinery the source uses to reconstruct `missed_count` from the responsible-set snapshot — reproducing that math here would re-introduce the indexer-mirror drift the rollback-coverage guard exists to catch. Correct counts are restored from the (now reorg-safe) source on the next full-snapshot ride-along, the same recovery model already used for `markets`. `rollback-coverage.test.js` reclassifies the table from exempt to special-case.
- `ClientRollback.js` / `ClientApplier.js` — the replica now recomputes the `contract_balances` custody aggregate from the surviving `valid` `deposits`/`withdrawals` rows, mirroring how `balances` is already derived from `credits`/`debits`. `contract_balances` has no `action_index` column, so the source poller's action-scoped JOIN errors for it and it is never streamed per-block — meaning it was previously only ever populated from a full snapshot, leaving stale contract custody balances after a reorg (and stale between snapshots during normal forward sync). `ClientRollback` rebuilds it inside the rollback transaction (after `balances`); `ClientApplier` rebuilds it whenever a block payload or incremental snapshot touches `deposits`/`withdrawals`. The companion derived table `markets` is deliberately left to snapshot recovery: reproducing the source's OHLCV recompute (`getMarketInfo` — last-trade / 24hr price-high-low-change-volume / bid / ask over `orders`/`order_matches`/`dispenses`) in the thin replica DB would re-introduce the very indexer-mirror drift the rollback-coverage guard exists to catch. `rollback-coverage.test.js` reclassifies `contract_balances` from exempt to recomputed and documents the `markets` decision in its exemption note.
- `ClientRollback.js` — added `prices` to the action-scoped `dataTables` rollback list (kept in sync with `xchain-indexer/src/rollback.js`). The table carries an `action_index` and is streamed live via both `ServerPoller.actionScopedTables` and `infraTables`, but was absent from the rollback list, so a reorg left orphaned PRICE rows on the replica — serving prices that were never finalized on-chain through replica-backed read paths until the next snapshot.
- `ServerPoller.js` — removed `delegates` from `actionScopedTables`. No such table exists (the real table is `delegations`, also listed), so the per-block payload query for it errored and was silently swallowed every block.
- `SnapshotBuilder.js` — `streamFullSnapshot` and `streamIncrementalSnapshot` now read the block-height anchor, the hash headers (`X-Ledger-Hash` / `X-Actions-Hash` / `X-Contract-Hash`, or `X-Block-Hash` for decoder), and every paginated table inside a single `REPEATABLE READ` transaction opened with a consistent snapshot before the anchor read. Previously these reads ran with no transaction boundary: the headers were captured first, then the multi-table loop streamed pages one at a time, so a source commit landing mid-loop produced a payload whose rows mixed two block heights while the headers advertised only the first. On a busy chain that caused hash verification to fail on every consumer bootstrap (full) and catch-up (incremental). Added `db.beginReadSnapshot()` to the db layer and unit coverage pinning the transactional boundary. InnoDB MVCC means the long read view does not block the source's writers.
- `ClientSync.js` — the startup resume path in `start()` now requests `lastAppliedBlock + 1` from incremental catch-up instead of `lastAppliedBlock`. The snapshot server uses inclusive `>=` bounds, so passing the last already-applied block re-delivered that block's rows; the non-ignore `INSERT` then threw on the `UNIQUE action_index` constraint, rolling back the whole catch-up and silently freezing the replica at its pre-restart height on every restart of a non-empty replica. The two gap-fill call sites already passed `+ 1` correctly; this aligns the startup path with them. Added unit and integration regression coverage exercising `start()` against a pre-populated replica.
- `ClientApplier.js` — `applyIncrementalSnapshot` now recomputes the `balances` table when the snapshot carries `credits` or `debits` rows, mirroring the existing guard in `applyBlock`. Previously an incremental catch-up (startup-resume, gap-fill, or post-reconnect continuity recovery) inserted new credit/debit rows but left the derived `balances` aggregate stale until the next live block happened to touch credits/debits. On low-traffic or paused networks that staleness window was unbounded, and any consumer reading replica balances during it (explorer balance endpoints, staking eligibility, dispenser fills, order matching) saw incorrect values.
- `ClientRollback.js` — added `slash_events` to the `blockTables` rollback list (kept in sync with `xchain-indexer/src/rollback.js`) so a follower replica purges `slash_events` rows from rolled-back blocks during a reorg. Previously stale rows survived on the replica, surfacing phantom slash events through the replica-backed read paths until the chain advanced past the orphaned tip.
- `ClientRollback.js` — added `gated_files` to the action-scoped `dataTables` rollback list. The table carries an `action_index` column and is already streamed live via `ServerPoller.actionScopedTables`, but it was absent from the rollback list, so a reorg left orphaned gated-file metadata rows on the replica. They survived until the next full/incremental snapshot, diverging the replica from canonical state for token-gated content read paths.
- `ClientRollback.js` — added `contract_unstakes` and `contract_delegations` to the action-scoped `dataTables` rollback list (kept in sync with `xchain-indexer/src/rollback.js`), completing the contract-staking set alongside the already-listed `contract_stakes`. Both tables are keyed on `action_index` and already streamed live via `ServerPoller.actionScopedTables`, but were absent from the rollback list, so a reorg left orphaned contract-staking rows on the replica — most acutely stale `contract_unstakes` rows whose `cooldown_end_block` was computed against the abandoned tip, producing phantom or delayed fund releases through replica-backed read paths until the next snapshot.
- `ServerPoller.js` — registered the protocol tables added in the 2026-05 schema expansion so live block payloads carry them to followers. Action-scoped: `attestation_requests`, `attestation_responses`, `contract_stakes`, `contract_unstakes`, `contract_delegations`, `gated_files`. Block-scoped: `attestation_validator_signatures`, `slash_events` (these carry `block_index` but no `action_index`). A follower bootstrapped before these tables existed previously missed every row written to them during live sync, leaving attestation results, contract-targeted staking, and token-gated file metadata invisible on the replica until the next full snapshot.
- `SnapshotBuilder.js` — added `attestation_validator_signatures` and `slash_events` to the indexer block-scoped set so incremental snapshots filter them by `block_index` rather than skipping them (they have no `action_index` to scope on). Documented inline why `icons`, `attestation_validator_stats`, and `price_snapshots` are intentionally full-snapshot-only: they carry neither a `block_index` nor an `action_index` cursor. `price_snapshots` in particular is mirrored onto every node directly from the cross-chain hub (`xchain-indexer/src/hub_db_sync.js`), so it converges via that channel rather than the block stream.
- `ServerPoller.js` — registered the protocol tables added in the 2026-05 schema expansion so live block payloads carry them to followers. Action-scoped: `attestation_requests`, `attestation_responses`, `contract_stakes`, `contract_unstakes`, `contract_delegations`, `gated_files`. Block-scoped: `attestation_validator_signatures`, `slash_events` (these carry `block_index` but no `action_index`). A follower bootstrapped before these tables existed previously missed every row written to them during live sync, leaving attestation results, contract-targeted staking, and token-gated file metadata invisible on the replica until the next full snapshot.
- `SnapshotBuilder.js` — added `attestation_validator_signatures` and `slash_events` to the indexer block-scoped set so incremental snapshots filter them by `block_index` rather than skipping them (they have no `action_index` to scope on). Documented inline why `icons`, `attestation_validator_stats`, and `price_snapshots` are intentionally full-snapshot-only: they carry neither a `block_index` nor an `action_index` cursor. `price_snapshots` in particular is mirrored onto every node directly from the cross-chain hub (`xchain-indexer/src/hub_db_sync.js`), so it converges via that channel rather than the block stream.

### Changed
- Per-chain poller and client-sync background loops now log the full error object (with stack) and call `process.exit(1)` when their `start()` promise rejects, instead of logging only `error.message` and continuing. A crashed worker previously left the process running and the `/status` endpoint returning a stale `block_height` with a live timestamp, making the failure invisible to health checks; exiting lets the container restart policy surface it.
- `ClientRollback.js` — the `_rollbackIndexer` `contract_emissions` delete now uses the FK-traversing subquery form (`execution_index IN (SELECT action_index FROM contract_executions WHERE action_index >= ?)`) instead of the direct `execution_index >= ?` comparison, matching `xchain-indexer/src/rollback.js`. Behaviour is identical today (`execution_index` is a non-nullable FK to `contract_executions.action_index`), but making the FK dependency explicit in the SQL keeps both rollback paths aligned so they evolve together if the schema or FK semantics ever change, rather than diverging silently.

## [1.7.0] - 2026-04-08

### Added
- Selective sync mode per chain — subscribers can request `full` (default, all tables) or `infra-only` (only cross-chain infrastructure tables) via `?sync_mode=infra-only` query parameter on `/subscribe/:chain/:network`
- `infraTables` set on `ServerPoller` — `stakes`, `delegations`, `validator_rewards`, `prices`, `reward_claims`, plus relevant index tables
- `BlockBroadcaster.broadcast()` now accepts `infraTables` and filters payloads — infra-only subscribers receive only infrastructure table changes
- `ClientSync._connectWebSocket()` reads `SYNC_MODE_<CHAIN>` env vars (e.g. `SYNC_MODE_DOGE=infra-only`) and appends the query parameter when connecting upstream
- `prices` added to `actionScopedTables` so on-chain PRICE actions are replicated like other actions

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
