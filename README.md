<!-- SPDX-License-Identifier: LicenseRef-Dankest-Community -->
<!-- Copyright © 2025 Dankest, LLC -->

# XChain Platform Indexer Sync

<p align="center">
  <img src="https://img.shields.io/badge/version-1.0.0-blue" alt="Version">
  <img src="https://img.shields.io/badge/node-%3E%3D18-green" alt="Node">
  <img src="https://img.shields.io/badge/license-Dankest%20Community-orange" alt="License">
</p>

Database replication service for the XChain Platform. Syncs indexer databases to validators and other consumers via REST snapshots and real-time WebSocket streaming, enabling lightweight validators that don't need to run full decoder+indexer stacks.

## Features

- **Dual mode** — server mode serves data from authoritative indexer databases; client mode replicates data into local MariaDB instances
- **Multi-chain single instance** — discovers all installed chains/networks via the hub and serves them from one process on one port
- **Hub auto-discovery** — calls xchain-hub `getallconfigs` at startup to find all indexer database connections; re-polls every 5 minutes to detect newly installed chains
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

## Documentation

Full indexer-sync documentation is available in the [xchain-documentation](https://github.com/XChain-platform/xchain-documentation/tree/master/components/indexer-sync) repository:

| Document | Description |
|---|---|
| [README](https://github.com/XChain-platform/xchain-documentation/blob/master/components/indexer-sync/README.md) | Overview, installation, quick start, scripts, dependencies |
| [Architecture](https://github.com/XChain-platform/xchain-documentation/blob/master/components/indexer-sync/ARCHITECTURE.md) | Data pipeline position, dual-mode design, internal components, sync algorithms |
| [Configuration](https://github.com/XChain-platform/xchain-documentation/blob/master/components/indexer-sync/CONFIGURATION.md) | Environment variables, hub discovery, database naming |
| [Operations](https://github.com/XChain-platform/xchain-documentation/blob/master/components/indexer-sync/OPERATIONS.md) | Running, Docker, REST/WebSocket API reference, resilience, troubleshooting |

## Quick Start

```bash
git clone https://github.com/XChain-platform/xchain-indexer-sync.git
cd xchain-indexer-sync
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

On startup, the service:
1. Validates required environment variables
2. Calls the hub's `getallconfigs` to discover installed chains and indexer database connections
3. Opens a MariaDB connection pool per chain/network
4. In server mode: starts polling each indexer database for new blocks and serves REST + WebSocket APIs
5. In client mode: bootstraps from remote snapshot, then subscribes to real-time updates via WebSocket

## Scripts

| Command | Description |
|---|---|
| `npm run api` | Start the sync service |
| `npm test` | Run unit tests |
| `npm run test:integration` | Integration tests (requires MariaDB + running indexer) |

## Dependencies

### Runtime

| Package | Purpose |
|---|---|
| `axios` | HTTP client for hub JSON-RPC calls |
| `express` | HTTP server for REST API endpoints |
| `express-rate-limit` | Per-IP rate limiting on snapshot endpoints |
| `helmet` | Security headers |
| `cors` | CORS middleware |
| `mariadb` | MariaDB connection pools (one per chain/network) |
| `ws` | WebSocket server for real-time block streaming |
| `dotenv` | Environment variable loading |

### Development

| Package | Purpose |
|---|---|
| `mocha` | Test framework |
| `sinon` | Test mocking and stubbing |

---

**Copyright &copy; 2025 Dankest, LLC**

**Based on XChain Platform by Dankest, LLC &ndash; https://dankest.llc**

Licensed under the **Dankest Community License**
(based on the Apache License 2.0 with additional non-commercial and network-disclosure terms).

You may not use, modify, or distribute this material except in compliance with the License.
See [LICENSE](./LICENSE.md) and [NOTICE](./NOTICE.md) for full terms.
A full copy of the License is also available at: [https://dankest.llc/license](https://dankest.llc/license)
