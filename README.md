<!-- SPDX-License-Identifier: LicenseRef-Dankest-Community -->
<!-- Copyright © 2025 Dankest, LLC -->

# XChain Sync

<p align="center">
  <img src="https://img.shields.io/badge/version-1.6.1-blue" alt="Version">
  <img src="https://img.shields.io/badge/tests-725%20passing-brightgreen" alt="Tests">
  <img src="https://img.shields.io/badge/node-%3E%3D18-green" alt="Node">
  <img src="https://img.shields.io/badge/license-Dankest%20Community-orange" alt="License">
</p>

<p align="center">
  <img src="https://img.shields.io/badge/coverage-unit%20%7C%20integration%20%7C%20e2e%20%7C%20fuzz%20%7C%20chaos%20%7C%20mutation%20%7C%20boundary%20%7C%20smoke-brightgreen" alt="Coverage">
</p>

Database replication service for the XChain Platform. Syncs indexer and decoder databases to validators and other consumers via REST snapshots and real-time WebSocket streaming, enabling lightweight validators that don't need to run full decoder+indexer stacks.

> **Breaking API change (2026-05):** REST and WebSocket paths now carry a `/:dbType/` segment (`indexer` or `decoder`) — for example `/status/indexer/BTC/mainnet`, `WS /subscribe/decoder/BTC/mainnet`. The bare `/status` endpoint now returns a nested `{coin: {network: {dbType: {...}}}}` structure. Clients must update their URLs. Transparency endpoints return HTTP 400 for `dbType=decoder` (decoder has no transparency log by design).

## Features

- **Dual mode** — server mode serves data from authoritative indexer databases; client mode replicates data into local MariaDB instances
- **Multi-chain single instance** — discovers all installed chains/networks via the hub and serves them from one process on one port
- **Hub auto-discovery** — calls xchain-hub `getallconfigs` at startup; re-polls every 5 minutes to detect newly installed chains
- **Full snapshot export** — compressed, streamed JSON database dumps for bootstrapping new validators
- **Incremental snapshots** — delta exports since any block height for catch-up after downtime
- **Real-time WebSocket streaming** — per-chain/network subscriptions for new blocks and reorg events
- **Hash chain verification** — leverages the indexer's existing per-block chained SHA256 hashes (ledger, actions, contracts) for data integrity
- **Cross-source comparison** — clients can sync from 2+ independent servers and compare block hashes to detect tampered data
- **Transparency log** — append-only per-block hash record for public auditability
- **Rate limiting** — configurable per-IP limits on snapshot downloads and WebSocket connections
- **Reorg propagation** — detects chain reorganizations from the indexer database and broadcasts rollback events to all subscribers
- **Automatic catch-up** — clients detect block gaps on reconnect and self-heal via incremental REST snapshots
- **Circuit-breaker DB connections** — automatic failure detection and recovery with configurable thresholds
- **Input validation** — SQL identifier sanitization, DDL whitelisting, WebSocket event schema validation
- **725 tests** — unit, integration, e2e, fuzz, chaos, mutation, boundary, security, performance, smoke

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

HUB_API_HOST=localhost
HUB_PORT=10000
```

In server mode, database credentials are discovered automatically from the hub — no database environment variables are needed. The service calls `getallconfigs` on the hub and connects to every installed indexer database.

For client mode:

```env
SYNC_MODE=client
SYNC_API_PORT=3006

SYNC_SOURCES=http://sync1.example.com:3006,http://sync2.example.com:3006
VERIFY_HASHES=true

REPLICA_DB_HOST=localhost
REPLICA_DB_PORT=3306
REPLICA_DB_USER=xchain_sync
REPLICA_DB_PASS=your_password

HUB_API_HOST=localhost
HUB_PORT=10000
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
| Unit — Core | ~205 | `SyncService.test.js`, `ServerPoller.test.js`, `BlockBroadcaster.test.js`, `ClientSync.test.js`, `ClientApplier.test.js`, `ClientRollback.test.js`, `HashVerifier.test.js`, `HubClient.test.js`, `TransparencyLog.test.js`, `SnapshotBuilder.test.js`, `config.test.js`, `utility.test.js` |
| Unit — Boundary | ~159 | Block index limits, poll limits, config parsing, reorg detection, WebSocket limits, hash continuity, transparency pagination, batch insert, rollback scope, circuit breaker, source arrays |
| Security | ~114 | Input validation, SQL injection, DDL whitelisting, API auth, WebSocket auth, client sync safety, applier safety |
| Smoke | ~21 | Server + client startup, config loading, liveness |
| Fuzz | ~57 | Property-based testing via fast-check: client applier, hash verifier, rollback, server poller, hub client, config |
| Chaos | ~38 | Source DB resilience, replica DB resilience, sync resilience, network partitions |
| Integration | ~57 | Server polling, REST API, WebSocket streaming, client bootstrap, live sync, rollback, transparency log, lifecycle |
| E2E | ~40 | Full lifecycle, delta sync, multi-chain, reorg propagation, API endpoints |
| Performance | 7 suites | Payload throughput, snapshot performance, bootstrap apply, subscriber scaling, sustained sync, incremental catchup, rollback performance |
| **Total** | **~725+** | |

---

**Copyright &copy; 2025 Dankest, LLC**

**Based on XChain Platform by Dankest, LLC &ndash; https://dankest.llc**

Licensed under the **Dankest Community License**
(based on the Apache License 2.0 with additional non-commercial and network-disclosure terms).

You may not use, modify, or distribute this material except in compliance with the License.
See [LICENSE](./LICENSE.md) and [NOTICE](./NOTICE.md) for full terms.
A full copy of the License is also available at: [https://dankest.llc/license](https://dankest.llc/license)
