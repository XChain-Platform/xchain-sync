<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->
<!-- Copyright © 2025-2026 Dankest, LLC -->

# XChain Sync

<p align="center">
  <img src="https://img.shields.io/badge/version-1.7.1-blue" alt="Version">
  <img src="https://img.shields.io/badge/tests-725%20passing-brightgreen" alt="Tests">
  <img src="https://img.shields.io/badge/node-%3E%3D22-green" alt="Node">
  <img src="https://img.shields.io/badge/license-AGPL--3.0--or--later-blue" alt="License">
</p>

<p align="center">
  <img src="https://img.shields.io/badge/coverage-unit%20%7C%20integration%20%7C%20e2e%20%7C%20security%20%7C%20fuzz%20%7C%20chaos%20%7C%20mutation%20%7C%20performance%20%7C%20smoke-brightgreen" alt="Coverage">
</p>

Database replication service for the XChain Platform. Syncs indexer and decoder databases to validators and other consumers via REST snapshots and real-time WebSocket streaming, enabling lightweight validators that don't need to run full decoder+indexer stacks.

> **Breaking API change (2026-05):** REST and WebSocket paths now carry a `/:dbType/` segment (`indexer` or `decoder`). For example: `/status/indexer/BTC/mainnet`, `WS /subscribe/decoder/BTC/mainnet`. The bare `/status` endpoint now returns a nested `{coin: {network: {dbType: {...}}}}` structure. Clients must update their URLs. Transparency endpoints return HTTP 400 for `dbType=decoder` (decoder has no transparency log by design).

## Features

- **Dual mode**: server mode serves data from authoritative indexer and decoder databases; client mode replicates both into local MariaDB instances
- **Multi-chain single instance**: discovers all installed chains/networks via the hub and serves them from one process on one port
- **Hub auto-discovery**: calls xchain-hub `getallconfigs` at startup; re-polls every 5 minutes to detect newly installed chains
- **Dual DB type support**: syncs both the indexer DB (full ledger) and decoder DB (8 of 9 tables; `TransparencyLog` and `mempool_transactions` excluded by design) behind a `/:dbType/` path segment
- **Schema auto-replication**: client fetches DDL from the server before data, creating tables in the replica with DDL whitelisted to `CREATE TABLE` only
- **Full snapshot export**: compressed, streamed JSON database dumps for bootstrapping new validators
- **Incremental snapshots**: delta exports since any block height for catch-up after downtime
- **Real-time WebSocket streaming**: per-chain/network/dbType subscriptions for new blocks and reorg events
- **Hash chain verification**: per-block chained SHA-256 hashes (ledger, actions, contracts for indexer; block_hash for decoder) validated on apply
- **Cross-source comparison**: clients can sync from 2+ independent servers and hold blocks until all sources confirm identical hashes
- **Transparency log**: append-only per-block hash record with SHA-256 Merkle epoch roots and inclusion proofs (indexer only)
- **SPV state-commitment recompute**: apply-time rebuild of per-block light-client roots (`state_tree_roots`/`state_tree_nodes`) that halts on divergence from the source (indexer only, opt-out via `VERIFY_STATE_COMMITMENT=false`)
- **Checkpoint-quorum anchor**: optional federation anchor that fetches the server's quorum-signed checkpoint, verifies it against a pinned validator set, and asserts the committed state root matches the replica's own recomputed value (indexer only, opt-in via `VERIFY_CHECKPOINT_QUORUM=true`)
- **Rate limiting**: configurable per-IP limits on snapshot downloads and WebSocket connections
- **Reorg propagation**: detects chain reorganizations from the source database and broadcasts rollback events to all subscribers, then rolls back the replica
- **Automatic catch-up**: clients detect block gaps on reconnect and self-heal via incremental REST snapshots
- **Circuit-breaker DB connections**: automatic failure detection and recovery with configurable thresholds
- **Input validation**: SQL identifier sanitization, DDL whitelisting, WebSocket event schema validation
- **725 tests**: unit, integration, e2e, security, fuzz, chaos, mutation, performance, smoke

## Documentation

Full xchain-sync documentation is available in the [xchain-documentation](https://github.com/XChain-Platform/xchain-documentation/tree/master/components/sync) repository:

| Document | Description |
|---|---|
| [README](https://github.com/XChain-Platform/xchain-documentation/blob/master/components/sync/README.md) | Overview, installation, quick start, scripts, dependencies |
| [Architecture](https://github.com/XChain-Platform/xchain-documentation/blob/master/components/sync/ARCHITECTURE.md) | Data pipeline position, dual-mode design, internal components, sync algorithms |
| [Configuration](https://github.com/XChain-Platform/xchain-documentation/blob/master/components/sync/CONFIGURATION.md) | Environment variables, hub discovery, database naming |
| [Operations](https://github.com/XChain-Platform/xchain-documentation/blob/master/components/sync/OPERATIONS.md) | Running, Docker, REST/WebSocket API reference, resilience, troubleshooting |

## Quick Start

```bash
git clone https://github.com/XChain-Platform/xchain-sync.git
cd xchain-sync
npm install
```

Create a `.env` file:

```env
SYNC_MODE=server
SYNC_API_PORT=3006

# Allowed CORS origin (defaults to disabled when unset)
# CORS_ORIGIN=https://your-dashboard.example.com

HUB_API_HOST=localhost
HUB_PORT=10000

# Max time (ms) to wait for the hub at startup before exiting non-zero.
# Defaults to 300000 (5 min). Raise it for environments that intentionally
# bring the hub up after sync.
# MAX_HUB_WAIT_MS=300000
```

In server mode, database credentials are discovered automatically from the hub. No database environment variables are needed. The service calls `getallconfigs` on the hub and connects to every installed indexer database.

For client mode:

```env
SYNC_MODE=client
SYNC_API_PORT=3006

SYNC_SOURCES=http://sync1.example.com:3006,http://sync2.example.com:3006
VERIFY_HASHES=true

# Allowed CORS origin (defaults to disabled when unset)
# CORS_ORIGIN=https://your-dashboard.example.com

REPLICA_DB_HOST=127.0.0.1
REPLICA_DB_PORT=3306
REPLICA_DB_USER=xchain_sync
REPLICA_DB_PASS=your_password

HUB_API_HOST=localhost
HUB_PORT=10000

# Max time (ms) to wait for the hub at startup before exiting non-zero.
# Defaults to 300000 (5 min). Raise it for environments that intentionally
# bring the hub up after sync.
# MAX_HUB_WAIT_MS=300000
```

Start the service:

```bash
npm run api
```

## Scripts

| Command | Description |
|---|---|
| `npm run api` | Start the sync service |
| `npm test` | Run unit tests |
| `npm run test:smoke` | Smoke tests (server + client startup, config loading) |
| `npm run test:integration` | Integration tests (requires MariaDB + running indexer) |
| `npm run test:e2e` | End-to-end tests (full server/client lifecycle) |
| `npm run test:security` | Security tests (validation, injection, auth) |
| `npm run test:fuzz` | Fuzz tests (property-based via fast-check) |
| `npm run test:fuzz:tier1` | Tier 1 fuzz (client applier, hash verifier) |
| `npm run test:fuzz:tier2` | Tier 2 fuzz (rollback, server poller, hub client) |
| `npm run test:fuzz:tier3` | Tier 3 fuzz (config parsing) |
| `npm run test:fuzz:quick` | Quick fuzz (100 iterations) |
| `npm run test:chaos` | Chaos engineering tests (DB failures, network partitions) |
| `npm run test:perf` | Performance tests (throughput, snapshots, scaling) |
| `npm run test:perf:quick` | Quick perf (50 blocks) |
| `npm run test:mutate` | Mutation tests (Stryker) |
| `npm run test:mutate:quick` | Quick mutation tests |
| `npm run test:mutate:check` | Incremental mutation tests |

## Test Suite

| Type | Tests | Description |
|---|---|---|
| Unit - Core | ~205 | `SyncService.test.js`, `ServerPoller.test.js`, `BlockBroadcaster.test.js`, `ClientSync.test.js`, `ClientApplier.test.js`, `ClientRollback.test.js`, `HashVerifier.test.js`, `HubClient.test.js`, `TransparencyLog.test.js`, `SnapshotBuilder.test.js`, `config.test.js`, `utility.test.js` |
| Unit - Boundary | ~159 | Block index limits, poll limits, config parsing, reorg detection, WebSocket limits, hash continuity, transparency pagination, batch insert, rollback scope, circuit breaker, source arrays |
| Security | ~114 | Input validation, SQL injection, DDL whitelisting, API auth, WebSocket auth, client sync safety, applier safety |
| Smoke | ~21 | Server + client startup, config loading, liveness |
| Fuzz | ~57 | Property-based testing via fast-check: client applier, hash verifier, rollback, server poller, hub client, config |
| Chaos | ~38 | Source DB resilience, replica DB resilience, sync resilience, network partitions |
| Integration | ~57 | Server polling, REST API, WebSocket streaming, client bootstrap, live sync, rollback, transparency log, lifecycle |
| E2E | ~40 | Full lifecycle, delta sync, multi-chain, reorg propagation, API endpoints |
| Performance | 7 suites | Payload throughput, snapshot performance, bootstrap apply, subscriber scaling, sustained sync, incremental catchup, rollback performance |
| **Total** | **~725+** | |

---

**Copyright &copy; 2025-2026 Dankest, LLC**

**Based on XChain Platform by Dankest, LLC &ndash; https://dankest.llc**

Licensed under the **GNU Affero General Public License v3.0** (AGPL-3.0-or-later)
with a commercial license available for proprietary use.

You may use, modify, and distribute this material under the terms of the License.
See [LICENSE](./LICENSE.md) and [NOTICE](./NOTICE.md) for full terms.
See the [licensing overview](https://docs.xchain.io/legal/LICENSING.html).
