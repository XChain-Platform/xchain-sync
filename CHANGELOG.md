# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
