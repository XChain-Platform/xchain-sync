# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- State-commitment roots get the catch-up-burst exemption, bootstrap resolver converges on signed-latest, ignoreTables derives from the registry, rollback logs real faults with context, parity guards escalate under required-siblings.

### Added
- Bootstrap-stampede perf scenario driving 5-25 concurrent snapshot downloads at one server, asserting the  semaphore sheds the excess with a retryable 503 while poll cycles and live block broadcast stay inside budget.
- Armed the BTC-anchored activation copies at BTC 961000 and `STATE_COMMITMENT_ACTIVATION` per chain (same heights as the state-hash gates); ClientApplier/ServerPoller thread the coin through the state-commitment gate.
- `stateHash.js` twin gains the flag-day-gated `token_supply` class (F-1 closure) and both state-hash gate maps are ARMED per chain; `BlockHasher`/`ClientSync` thread the coin gate parameter so the follower recompute matches the source across each activation height.
- `src/tableLifecycle.js` registry twin (byte-identical to xchain-indexer) now generates the indexer stream topology and both ClientRollback table list sets, so table-set drift vs the source indexer is structurally impossible.
- `stateHash.js` twin gains the flag-day-gated `poll_finalize` class (inert until armed; follower halts on a dropped/divergent poll finalization once armed).
- `HubClient` sends `x-api-key` on hub calls when `HUB_API_KEY` is set (`getallconfigs` is now in the hub's keyed sensitive-read tier).

### Fixed
- The coin threaded into every per-chain activation lookup is now the canonical TICKER (`LTC`), not the sync layer's full chain name (`litecoin`), via a new exported `coinTicker()` normalizer. The activation maps are keyed `'<TICKER>:<network>'` exactly as the source indexer writes them, so the full name silently missed the key: the follower's state-hash classes disagreed with the source and halted every production replica at its armed height (BTC 958500 / LTC 3143000 / DOGE 6291000 / BTC-testnet 145000), and `isStateCommitmentActive` resolved to "off" fleet-wide, so `ServerPoller` published NULL roots and the follower never computed roots or ran the light-client state-commitment check at all. Fixed in `ClientSync` (state-hash gate), `ClientApplier` (commitment gate, root computation, snapshot seeding, and the `state_tree_roots.chain` value) and `ServerPoller` (gate + `getStateRootsRow` lookup); regtest was never affected because it resolves through the bare network key.
- `ServerPoller` pins each forward batch to one REPEATABLE READ snapshot threaded through every payload read, so per-block payloads (above all the `updated_rows` in-place-mutation channel) carry state-at-block instead of the source's live tip-state.
- `src/HubClient.js`: the hub-module DB host fallback now defaults to `127.0.0.1` instead of `localhost`, so a module that omits both `db_host` and `host` connects over IPv4 to MariaDB instead of failing against an unresolvable IPv6 (`::1`) default.
- VOTE governance tables (`polls`, `votes`, `poll_results`, `vote_delegations`) now replicate per block and roll back on reorg (previously entirely absent from sync, so replicas never carried VOTE state and orphaned rows would have survived reorgs).

### Added
- `POLL_FINALIZE_TABLES` updated_rows class: the in-place poll finalization flip on a surviving `polls` row now reaches followers, with the matching re-open reset in `ClientRollback` (mirrors the source indexer).
- `_insertRows` unit coverage pinning the full-dump write modes: `merkle_epochs` -> `INSERT IGNORE` and `markets`/`attest_validator_stats` -> `INSERT ... ON DUPLICATE KEY UPDATE` covering every column, so a future edit can't silently drop either mode.

## [1.7.2] - 2026-07-16

### Fixed
- Burst-built blocks ship state_hash null via the existing NULL-skip gate so the batch-tip updated_rows vs apply-time state_hash mismatch cannot false-halt ().
- escrow_action_index wire-carry semantics documented consistently at all 5 comment sites ().
- merkle_reorgs classified via new SOURCE_UNSTREAMED_TABLES set in SnapshotBuilder; ClientApplier heals already-imported foreign copies; registry-exhaustiveness test guard ().
- streamDispensers serves the full remaining set in one statement-consistent response, eliminating torn cross-request reconciles ().
- OPERATOR_LOCAL_TABLES derived from the tableLifecycle registry; F-5 rewritten to two-direction set-equality ().
- ClientRollback decoder table order derived from replicatedTables.getTopology('decoder') instead of hand-copied literals ().
- Orphan-sweep parity guard derives from lifecycleTwin.ORPHAN_SWEEPS replica flags with table-keyed regexes and key-set equality, adding the icons replica sweep ().
- Vacuous decoder-exclusion test moved into the decoder describe block with a by-value 8-table pin ().


## [1.7.1] - 2026-06-20

### Security
- `src/ClientSync.js`: a reorg deeper than `MAX_ROLLBACK_DEPTH` now halts durably via `_haltOnDivergence('max-rollback-depth-exceeded')` instead of returning bare and silently dropping all canonical re-streamed blocks onto the orphaned fork.
- `src/ClientRollback.js`: contract-stake slash reorg-restore now breaks same-block ties on `(execution_index, slash_position)` instead of `id`, keeping the replica byte-identical to the source indexer.
- `src/BlockHasher.js`: ledger hash queries (`credits`, `debits`, `escrows`) in `computeBlockHashes` now carry full secondary sort `ORDER BY action_index ASC, address_id ASC, tick_id ASC, amount ASC` to prevent engine-arbitrary row order from causing divergent `ledger_hash` values.
- `src/api.js`, `src/middleware.js`, `.env.example`: `POST /halt/clear/:dbType/:chain/:network` now fails closed (returns 401) when no `SYNC_API_KEY` is configured, and startup logs a prominent warning in open mode.
- `src/ClientSync.js`: log an explicit `SECURITY:` startup warning describing the single-source and decoder data-integrity posture (cross-source hash rejection requires `2+` sources and `VERIFY_HASHES`).
- `src/db.js`, `src/ClientSync.js`: table names from a remote sync source's `table_counts` map are now validated via `validation.validateIdentifier` before SQL interpolation, closing a read-injection path on `_verifyTableCounts` and at the `db.js` SQL boundary.
- Pinned `ajv` to `^8.18.0` via `overrides` in `package.json` to resolve a dev-only ReDoS advisory (GHSA-2g4f-4pwh-qvx6) so `npm audit` reports zero vulnerabilities.
- `CORS_ORIGIN` now defaults to `false` (CORS disabled) instead of `'*'` when unset, matching hub and encoder defaults; operators who need cross-origin access must set it explicitly.

### Fixed
- `src/updatedRows.js` (new), `src/ServerPoller.js`, `src/SnapshotBuilder.js`, `src/ClientApplier.js`, `src/ClientRollback.js`, `src/BlockBroadcaster.js`, `src/api.js`, `src/schema-version.js`: introduce an `updated_rows` channel so followers replicate in-place mutations to surviving rows (`deactivation_block`, SLASH amounts, `request_status`, `escrow_action_index`), closing a permanent forward-divergence class; indexer `SCHEMA_VERSION` advances 2 to 3.
- `src/ServerPoller.js`, `src/TransparencyLog.js`: the server poller now seeds its broadcast cursor from `TransparencyLog.getHighWaterMark()` on restart instead of the live source-DB tip, so blocks advanced during downtime are not skipped; `backfillGaps()` self-heals already-damaged nodes.
- `src/ClientRollback.js`, `src/replicatedTables.js`: `token_controllers` and `address_controllers` are now rolled back on reorg and streamed per-block, so orphaned controller events no longer let the replica enforce access policy on state the source chain never finalized.
- `src/ClientRollback.js`: a reorg now resets a parent v1 `anchor_actions` row stamped `invalid_archive` by an orphaned final chunk back to `unverified`, mirroring the same fix in `xchain-indexer`.
- `src/ClientRollback.js`, `src/replicatedTables.js`, `test/unit/rollback-coverage.test.js`: five tables (`cross_chain_settlements`, `cross_chain_call_executions`, `cross_chain_call_callbacks`, `xcalls`, `stake_key_revocations`) were missing from the rollback list and per-block streamed set; added to both; the rollback-coverage guard now also reads `xchain-indexer/src/rollback.js` directly.
- `src/ClientSync.js`, `src/config.js`: a failed full-snapshot bootstrap now retries with backoff (`BOOTSTRAP_MAX_RETRIES`) and throws on final exhaustion instead of returning normally and silently entering live-follow on an empty database.
- `src/validation.js`: `extractColumnDefinition` now rejects a comma at parenthesis-depth 0, closing a DDL-injection path on the schema catch-up `ALTER TABLE ... ADD COLUMN` route.
- `src/ClientRollback.js`: `_rollbackIndexer` now purges `price_snapshots` rows by `reference_block` on reorg, closing the staleness window where orphaned finalized-price rows were served until the next hub-driven delete.
- `src/ClientSync.js`: the decoder live-sync path now detects a fork at the committed tip by comparing the incoming `block_hash` against the stored head hash and triggering incremental catch-up on mismatch, instead of silently discarding the re-delivered block.
- `src/replicatedTables.js`: `sync_meta` is now included in the `/status` row-count completeness check via a new `special` topology category, decoupling "counted for completeness" from "extracted per block."
- `src/ServerPoller.js`, `src/TransparencyLog.js`, `src/sql/merkle_reorgs.sql` (new): `TransparencyLog.pruneFrom()` is now called on a server-side reorg to remove stale `sync_meta` and `merkle_epochs` rows, with each invalidated epoch recorded in a new `merkle_reorgs` audit table.
- `src/HubClient.js`: `_call()` now records per-endpoint failure detail on `connector.lastFailures` so callers receiving `null` can report which endpoints were tried and why each failed.
- `src/ServerPoller.js`: the live per-block indexer payload now carries all ten `index_*` interning tables (not just `index_transactions` and `index_addresses`) to prevent dangling FK references when a value is first interned mid-stream.
- `test/e2e/helpers/decoderFixtures.js`: `truncateAll()` now also clears `mempool_transactions` so stale rows cannot survive between e2e runs.
- `src/SnapshotBuilder.js`, `src/ServerPoller.js`: the decoder `events` table is now re-dumped in full on every incremental snapshot (moved from `decoderSkip` to `decoderFullDump`) so followers no longer silently miss new rows between full bootstraps.
- `src/replicatedTables.js`: removed `dispensers` from the decoder per-block replicated topology; insert-only streaming caused the follower count to grow monotonically above the source, producing a permanent false row-count divergence.
- `src/replicatedTables.js`, `src/ClientRollback.js`, `src/SnapshotBuilder.js`: removed three phantom attestation tables (`attestation_requests`, `attestation_responses`, `attestation_validator_signatures`) and added the actual `attests` table to the action-scoped streamed set and reorg rollback list.
- `src/ClientRollback.js`, `src/SnapshotBuilder.js`, `src/replicatedTables.js`: corrected the per-validator attestation aggregate table name from `attestation_validator_stats` to `attest_validator_stats` so the orphaned-row purge on reorg actually executes instead of throwing and being swallowed.
- `src/HubClient.js`: the hub JSON-RPC client now sticks to the last responding endpoint (wrapping on failure) instead of always starting from the first, eliminating repeated full-timeout penalties when the first endpoint is degraded.
- `src/ClientSync.js`: a decoder full-snapshot bootstrap now cross-checks the source's per-table row counts via `_verifyDecoderCompleteness` (independent of `VERIFY_HASHES`) so a truncated snapshot is no longer silently accepted.
- `src/SyncService.js` / `src/config.js`: `_waitForHub()` now gives up after `MAX_HUB_WAIT_MS` (default 300000 ms) instead of retrying forever, logging an error and exiting non-zero so a supervisor can restart a stuck service.
- Regenerated `package-lock.json` so it matches `package.json` (lockfile was still pinned to `axios@^1.6.7` while `package.json` had moved to `^1.16.0`).
- `ClientSync.js`: the `/status` gap check in `_handleEvent` now uses `>` instead of `>=` so a source exactly one block ahead (normal steady state) no longer races the live WebSocket apply with a redundant incremental catch-up.
- `db.js` / `ClientSync.js` / `SyncService.js`: schema replication now propagates columns added to an existing table via `db.addMissingColumns()`; `db.ensureReplicatedColumns()` self-heals `give_ownership`/`get_ownership` on `orders`/`swaps` even when the source DB is unreachable.
- `ClientApplier.js` / `ClientRollback.js`: the `balances` and `contract_balances` rebuild steps now rethrow any error that is not errno 1146, preventing silent divergence when a real DB failure was being swallowed.
- `ClientRollback.js`: `_rollbackIndexer` now drops orphaned `attestation_validator_stats` rows (by `last_updated_block`) on reorg; correct counts are restored from the source on the next full-snapshot ride-along.
- `ClientRollback.js` / `ClientApplier.js`: the replica now recomputes `contract_balances` from surviving `deposits`/`withdrawals` rows on reorg and whenever a block payload or incremental snapshot touches those tables.
- `ClientRollback.js`: added `prices` to the action-scoped `dataTables` rollback list so orphaned PRICE rows are purged on reorg instead of surviving until the next snapshot.
- `ServerPoller.js`: removed `delegates` from `actionScopedTables` (no such table; the real table is `delegations`) to stop the per-block query from throwing and being swallowed every block.
- `ServerPoller.js`: removed `markets` from `actionScopedTables` (no `action_index` column; OHLCV aggregate converges via snapshot only) to stop a per-block silent error and align the code with actual behavior.
- `SnapshotBuilder.js`: `streamFullSnapshot` and `streamIncrementalSnapshot` now read the block-height anchor, hash headers, and all paginated table data inside a single `REPEATABLE READ` transaction, preventing mixed-block-height payloads on busy chains.
- `ClientSync.js`: startup resume now requests `lastAppliedBlock + 1` from incremental catch-up (was `lastAppliedBlock`), preventing a `UNIQUE action_index` constraint violation that froze the replica at its pre-restart height on every restart.
- `ClientApplier.js`: `applyIncrementalSnapshot` now recomputes `balances` when the snapshot carries `credits` or `debits` rows, so derived balances are not left stale on low-traffic networks between incremental catch-ups.
- `ClientRollback.js`: added `slash_events` to the `blockTables` rollback list so orphaned slash event rows are purged on reorg.
- `ClientRollback.js`: added `gated_files` to the action-scoped `dataTables` rollback list so orphaned gated-file metadata is purged on reorg.
- `ClientRollback.js`: added `contract_unstakes` and `contract_delegations` to the action-scoped `dataTables` rollback list, completing the contract-staking set alongside `contract_stakes`.
- `ServerPoller.js`: registered attestation, contract-staking, and gated-file tables added in the 2026-05 schema expansion (`attestation_requests`, `attestation_responses`, `contract_stakes`, `contract_unstakes`, `contract_delegations`, `gated_files` action-scoped; `attestation_validator_signatures`, `slash_events` block-scoped) so live block payloads carry them to followers.
- `SnapshotBuilder.js`: added `attestation_validator_signatures` and `slash_events` to the indexer block-scoped set so incremental snapshots filter them by `block_index` rather than skipping them.
- `ServerPoller.js`: registered the protocol tables added in the 2026-05 schema expansion so live block payloads carry them to followers (second entry from the original changelog).
- `SnapshotBuilder.js`: added `attestation_validator_signatures` and `slash_events` to the indexer block-scoped incremental snapshot set (second entry from the original changelog).

### Added
- `src/config.js`, `src/BlockBroadcaster.js`, `src/api.js`, `.env.example`: `/validator-status` now surfaces roster members that have never reported (as `absent`) via an optional `EXPECTED_VALIDATORS` env var, and lapsed entries transition to `stale` instead of being hard-deleted.
- `src/HubClient.js`, `src/SyncService.js`, `src/api.js`: `GET /health` now reports `hub_config_age_seconds` so an operator can tell when the sync service's hub view has gone stale.
- `.env.example`: configuration template enumerating every environment variable the service reads, with safe defaults and inline comments; also added `.env` to `.gitignore`.
- `src/db.js`: the MariaDB connection pool now sets `queryTimeout` (`DB_QUERY_TIMEOUT`, default 30000) so a slow or lock-blocked statement cannot hang a pooled connection indefinitely.
- `src/api.js`: new `GET /health` endpoint that lists each replicated database's circuit state and returns HTTP 503 when any is open.
- `src/BlockBroadcaster.js`: each subscriber entry from `getSubscribers()` now carries a `heartbeatReceived` boolean so an operator can distinguish "no data yet" from a genuine `lag: 0` caught-up state.
- `src/BlockBroadcaster.js`, `src/api.js`: subscriber entries gain a `lagStatus` field (`known`/`unknown`) and `/validator-status` returns a structured `{ validators, total, unknown_count }` object instead of a bare validators map.
- Replica-completeness verification: `ClientSync._verifyAgainstSource` compares the source's `table_counts` against the follower's own and logs `TABLE_COUNT_MISMATCH` for any shortfall; additive and never overrides a passing hash result.
- `test/unit/rollback-coverage.test.js`: a rollback-coverage guard that reads the replicated table set and fails at CI time when any replicated table is left unhandled by `ClientRollback`.
- `ClientRollback.js`: `decoderBlockTables` / `decoderTxScopedTables` exposed as constructor properties so the rollback-coverage guard can verify the decoder path.
- Per-subscriber applied-block tracking: `BlockBroadcaster` records `_syncLastSentBlock` / `_syncAppliedBlock` (fed by a new inbound `{ type: 'heartbeat' }` message); `ClientSync` sends debounced heartbeats; `/status` gains a `subscribers` array with `{ ip, lastSentBlock, appliedBlock, lag }`.

### Changed
- `package.json`: pinned `mariadb` 3.5.2 to exact versions (dropped `^` caret ranges) so every install resolves a byte-identical dependency tree, matching the versions already frozen in `package-lock.json`. No source changes.
- `src/schema-version.js`, `src/ClientApplier.js`, `src/SnapshotBuilder.js`: `SCHEMA_VERSION` is now a per-`dbType` object (`{ indexer: 2, decoder: 2 }`) so indexer and decoder snapshot schemas can advance independently without forcing a lockstep restart of all validators.
- `src/api.js`: migrated all four `rateLimit()` calls from the deprecated `max` option to the canonical `limit` replacement (behavior-preserving rename for `express-rate-limit` v8 forward-compatibility).
- `src/HubClient.js`: `getallconfigs()` now polls the hub incrementally by echoing `watermark` as `since_updated_at` and merging delta results, reducing per-cycle transfer to near-zero once chains are known.
- `package.json`: aligned `mariadb` to the `^3.5.2` range used across the platform (previously `~3.4.5`). No source changes.
- `WS_BACKPRESSURE_LIMIT` is now env-configurable via `process.env.WS_BACKPRESSURE_LIMIT` (floored at 1) instead of being hardcoded to 50, letting operators tune disconnect tolerance for validators with heterogeneous replica DB speeds.
- The Docker image is now built with `npm ci` instead of `npm install`, failing the build if the lockfile is missing or out of sync with `package.json`.
- Per-chain poller and client-sync loops now log the full error with stack and call `process.exit(1)` on `start()` rejection, making crashes visible to container restart policies instead of reporting a stale `block_height` with a live timestamp.
- `ClientRollback.js`: the `_rollbackIndexer` `contract_emissions` delete now uses the FK-traversing subquery form to match `xchain-indexer/src/rollback.js` and keep both rollback paths aligned.

## [1.7.0] - 2026-04-08

### Added
- Selective sync mode per chain: subscribers can request `full` (default) or `infra-only` via `?sync_mode=infra-only` on `/subscribe/:chain/:network`.
- `infraTables` set on `ServerPoller`: `stakes`, `delegations`, `validator_rewards`, `prices`, `reward_claims`, plus relevant index tables.
- `BlockBroadcaster.broadcast()` now accepts `infraTables` and filters payloads so infra-only subscribers receive only infrastructure table changes.
- `ClientSync._connectWebSocket()` reads `SYNC_MODE_<CHAIN>` env vars and appends the `?sync_mode` query parameter when connecting upstream.
- `prices` added to `actionScopedTables` so on-chain PRICE actions are replicated like other actions.

## [1.6.1] - 2026-04-06

### Changed
- Move coverage badge to its own line in README.md for cleaner formatting.

## [1.6.0] - 2026-04-06

### Added
- Merkle tree utility (`MerkleTree.js`): binary SHA-256 tree with proof generation and verification.
- Merkle epoch system: automatic Merkle root commitment over configurable epoch windows (default 100 blocks).
- `merkle_epochs` table for storing committed epoch roots.
- `GET /transparency/:chain/:network/proof/:block_index`: Merkle inclusion proof endpoint.
- `GET /transparency/:chain/:network/root/latest`: latest committed Merkle root endpoint.
- Rate limiting on all transparency endpoints (configurable, default 10 req/min per IP).
- `MERKLE_EPOCH_SIZE` and `TRANSPARENCY_RATE_LIMIT` config options.

## [1.5.2] - 2026-04-05

### Changed
- Moved Stryker mutation configs (`stryker.config.json`, `stryker.quick.config.json`) from project root into `test/mutation/`.
- Updated `test:mutate`, `test:mutate:quick`, and `test:mutate:check` npm scripts to reference new config paths.

## [1.5.1] - 2026-04-05

### Changed
- `README.md`: updated to match platform README structure with version/tests/coverage badges, expanded Scripts table, and updated dev dependencies.

## [1.5.0] - 2026-04-05

### Added
- Mutation testing infrastructure using StrykerJS v8.7.1: `stryker.config.json` (15 source files, 2218 mutants) and `stryker.quick.config.json` (6 priority-1 files, 795 mutants); thresholds at 80/85/95%; incremental CI mode.
- npm scripts: `test:mutate` (full run), `test:mutate:quick` (Priority 1 only), `test:mutate:check` (incremental).
- `.gitignore` entries for mutation report outputs and Stryker temp files.
- `@stryker-mutator/core` and `@stryker-mutator/mocha-runner` dev dependencies.

## [1.4.0] - 2026-04-05

### Added
- Performance and load testing suite: 7 scenario files in `test/perf/scenarios/` covering payload throughput, snapshot performance, bootstrap apply, subscriber scaling, sustained sync, incremental catch-up, and rollback performance.
- Performance test infrastructure in `test/perf/setup/`: `metrics-collector.js`, `report-generator.js`, `data-generator.js`, `perf-setup.js`.
- npm scripts: `test:perf` (unlimited timeout), `test:perf:quick` (reduced block count).

## [1.3.0] - 2026-04-05

### Fixed
- `HashVerifier`: `verifyChainContinuity()` now guards against null/undefined payload instead of crashing with `TypeError`.
- `HubClient`: `getIndexerConfigs()` now guards against null/non-object values in the hub response tree instead of crashing.

### Added
- Fuzz testing suite: 55 property-based tests across 6 suites (`tier1-client-applier`, `tier1-hash-verifier`, `tier2-server-poller`, `tier2-client-rollback`, `tier2-hub-client`, `tier3-config`) using `fast-check`.
- Fuzz test infrastructure: `test/fuzz/setup/harness.js`, generators (`values.js`, `rows.js`, `payloads.js`).
- `fast-check` dev dependency.
- npm scripts: `test:fuzz`, `test:fuzz:tier1`, `test:fuzz:tier2`, `test:fuzz:tier3`, `test:fuzz:quick`.

## [1.2.0] - 2026-04-05

### Fixed
- Config parsing: replaced `parseInt(x) || default` with `parseIntSafe()` so zero is accepted as a valid value (e.g. `SNAPSHOT_RATE_FULL=0` no longer silently becomes `1`).
- Config parsing: negative values are now clamped (e.g. `WS_MAX_PER_IP=-1` clamps to 1 instead of being accepted).
- Config parsing: `VERIFY_HASHES` is now case-insensitive so `"FALSE"` / `"False"` / `"false"` all disable verification.
- `HubClient`: `db_port` parsing uses `parseIntSafe()` so zero is preserved and negatives default to 3306.
- `ClientSync`: `_bootstrapFromSnapshot()` no longer recurses infinitely when all sync sources fail; stops after trying each source once.
- Database: added `bigIntAsNumber: true` to the MariaDB connection pool to prevent BigInt object returns.

### Added
- Boundary test suite: 159 tests across 12 files in `test/unit/boundaries/` covering config parsing, poll/batch limits, transparency paging, hash continuity, reorg detection, WebSocket limits, rollback scope, circuit breaker, block index, source array, and hub port parsing.

## [1.1.0] - 2026-04-04

### Added
- Comprehensive unit test suite: 203 tests across 12 test files covering all modules.
- Test files: utility, config, HashVerifier, HubClient, TransparencyLog, BlockBroadcaster, ClientApplier, ClientRollback, ServerPoller, SnapshotBuilder, ClientSync, SyncService.
- `proxyquire` dev dependency for mocking ESM-only `mariadb` in SyncService tests.

### Changed
- Replaced 77 copied SQL files with dynamic schema replication from the live indexer database via `SHOW CREATE TABLE`, eliminating drift risk when indexer tables change.
- `db.js`: added `replicateSchema(sourceDb)` method; renamed `verifyTables()` to `verifySyncTables()`.
- `SnapshotBuilder.js`: discovers tables dynamically from `information_schema` instead of a hardcoded list.
- `ClientSync.js`: added `_fetchAndApplySchema()` to fetch table DDLs from the server's new `GET /schema/:chain/:network` endpoint during bootstrap.

### Added
- `GET /schema/:chain/:network` REST endpoint returning all table DDLs for schema replication by remote clients.

## [1.0.0] - 2026-04-03

### Added
- Initial release of xchain-indexer-sync.
- Dual-mode architecture: server mode (serves data) and client mode (replicates data).
- Hub auto-discovery: calls `xchain-hub` `getallconfigs` to find all installed indexer databases.
- Single-instance multi-chain support: one process serves all chains/networks on the node.
- REST API: `GET /status`, `GET /snapshot/:chain/:network`, `GET /snapshot/:chain/:network/since/:blockHeight`, `GET /transparency/:chain/:network/roots`.
- WebSocket API: `WS /subscribe/:chain/:network` for real-time block and reorg streaming.
- Full snapshot export with gzip streaming compression for bootstrap.
- Incremental snapshot export for catch-up after downtime.
- Real-time block broadcasting via WebSocket with per-chain/network subscriptions.
- Hash chain verification using indexer's existing per-block SHA256 hashes (ledger, actions, contracts).
- Cross-source hash comparison for clients syncing from multiple servers.
- Transparency log: append-only per-block hash record for auditability.
- Client bootstrap: full snapshot download with hash verification against secondary source.
- Client catch-up: incremental snapshot and WebSocket subscription for gap detection and self-healing.
- Client rollback: mirrors indexer's `Rollback.js` table lists for reorg handling.
- Rate limiting on snapshot endpoints (configurable per-IP limits).
- WebSocket connection limiting (configurable per-IP).
- WebSocket backpressure: drops subscribers that fall behind (>50 buffered messages).
- Circuit-breaker database connections with automatic recovery.
- Periodic hub re-polling (5 min) to detect newly installed chains.
- MariaDB connection pool per chain/network with independent circuit breakers.
