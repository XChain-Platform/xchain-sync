# Maintainers

This file lists the people responsible for `xchain-sync`, what each of them owns, and how to escalate issues that need a human's attention beyond what `CONTRIBUTING.md` and `SECURITY.md` cover.

The XChain Platform is in pre-launch development and ships under a single primary maintainer today. As contributors take on durable responsibility for areas of the codebase, they will be added here. This is a conventional MAINTAINERS file (an open-source norm used by distros and downstream packagers), not an aspirational org chart.

---

## Primary maintainer

| Role | Name | GitHub | Areas |
|---|---|---|---|
| Lead | J-Dog | [@J-Dog](https://github.com/J-Dog) | Everything: database replication to validators, snapshot builder, client sync, WebSocket streaming, releases |

Contact:

- General and non-sensitive: open an issue at <https://github.com/XChain-platform/xchain-sync/issues>.
- Code of Conduct: `conduct@dankest.llc` (per `CODE_OF_CONDUCT.md`).
- Security disclosures: GitHub Private Vulnerability Reporting, or `security@dankest.llc` (per `SECURITY.md`).

---

## Areas of responsibility

Until additional maintainers join, the lead owns every area below. The table is here so a future contributor (or downstream packager) can see what each area entails when scoping a contribution.

| Area | What it covers |
|---|---|
| Snapshot builder | `SnapshotBuilder.js`: full and incremental compressed JSON snapshot exports for bootstrapping validators |
| Client sync and applier | `ClientSync.js`, `ClientApplier.js`: receiving, applying, and validating incoming snapshots on the client side |
| Client rollback | `ClientRollback.js`: applying reorg rollback events received from the server |
| Server poller | `ServerPoller.js`: polling the indexer database for new blocks and reorg events to broadcast |
| WebSocket streaming | `BlockBroadcaster.js`: per-chain/network subscriptions for new blocks and reorg propagation |
| Hash verification | `HashVerifier.js`, `BlockHasher.js`, `stateHash.js`, `MerkleTree.js`: leveraging the indexer's per-block hash chain for data integrity and cross-source comparison |
| Transparency log | `TransparencyLog.js`: append-only per-block hash record for public auditability |
| Replicated-table scope | `replicatedTables.js`: which tables are included in snapshots and streams |
| Hub client and config | `HubClient.js`, `config.js`: hub auto-discovery, chain/network enumeration, environment configuration |
| API | `api.js`, `middleware.js`, `validation.js`, `sqlUtil.js`: the REST and WebSocket API surface, rate limiting, input validation, DDL whitelisting |
| Database layer | `db.js`, `sql/`: circuit-breaker DB connections, parameterized writes, schema versioning |
| Tests | The layered suites under `test/` (unit, integration, e2e, fuzz, chaos, mutation, boundary, security, performance, smoke) |
| Documentation | `README`, `SECURITY`, `CODE_OF_CONDUCT`, `CONTRIBUTING`, `MAINTAINERS`, `CHANGELOG` |

---

## Adding a maintainer

A contributor becomes a maintainer when they have:

1. Sustained contribution in a specific area for at least one release cycle (typically 2 to 3 weeks of active work).
2. Reviewed and merged at least three PRs from outside contributors.
3. Demonstrated awareness of the project's conventions: the replication protocol integrity model (hash-chain verification, cross-source comparison), raw parameterized SQL with no ORM, the `Keep a Changelog` format, and Node 22 as the pinned runtime.

Open a PR adding the new maintainer to the table above with their GitHub handle and area(s) of responsibility. The lead approves and merges.

## Removing a maintainer

A maintainer steps down by opening a PR removing their row. The lead also removes a maintainer who has been inactive for six months or who violates the Code of Conduct, after a written notice period.

---

## Escalation paths

If you cannot reach the relevant area maintainer within a reasonable window:

| Situation | Escalate to |
|---|---|
| Active security incident | `security@dankest.llc` (per `SECURITY.md`) |
| A tampered or forged snapshot/stream that could feed divergent state to a validator | Open a public issue tagged `security` AND email `security@dankest.llc` |
| Code-of-conduct concern | `conduct@dankest.llc` (per `CODE_OF_CONDUCT.md`) |
| PR has been open without review for 14+ days | Comment `@J-Dog` on the PR; if no response within 7 more days, open an issue tagged `governance` with the PR link |

---

## Decision-making

The lead makes final calls on:

- The replication protocol: replicated-table scope, snapshot format, and integrity/authentication of snapshot and stream endpoints.
- Database schema and migration changes.
- Release timing and version policy.
- Adopting a new heavy dependency.
- Code-of-conduct enforcement, and maintainer additions or removals.

Smaller calls (bug fixes, additions within an existing area, documentation, dependency bumps inside an existing minor) go through PR review by the area maintainer.

---

## Cross-project relationships

| Project | Relationship |
|---|---|
| [`xchain-indexer`](https://github.com/XChain-platform/xchain-indexer) | Primary source: sync replicates the indexer database (blocks, actions, ledger hashes) to validators |
| [`xchain-decoder`](https://github.com/XChain-platform/xchain-decoder) | Secondary source: sync also replicates the decoder database to validators |
| [`xchain-hub`](https://github.com/XChain-platform/xchain-hub) | Hub-backed tables are replicated; hub auto-discovery drives chain/network enumeration |
| [`xchain-documentation`](https://github.com/XChain-platform/xchain-documentation) | Protocol spec: sync wire format, dbType paths, replicated-table scope |
| [`xchain-node`](https://github.com/XChain-platform/xchain-node) | Installs and runs the sync service as a Docker container |

The sync maintainer is not automatically a maintainer of those sibling projects. Cross-project changes go through each project's own review process.
