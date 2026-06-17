# Security Policy

`xchain-sync` is the database replication service for XChain Platform. It delivers indexer and decoder state to validators via REST snapshots and real-time WebSocket streaming. A tampered or forged snapshot or stream could feed bad state to a validator node, causing it to diverge from consensus silently. We treat reports seriously and respond fast.

If you've found a security issue, please **do not open a public issue or pull request**. Use the private channels below.

---

## How to report

### Preferred: GitHub Private Vulnerability Reporting

Open a draft advisory at:

<https://github.com/XChain-platform/xchain-sync/security/advisories/new>

This is the fastest path. The advisory is private until we publish it.

### Alternative: Email

Email **security@dankest.llc** with:

- A description of the issue and the threat it poses.
- Reproduction steps or a proof-of-concept (a crafted payload, snapshot, or WebSocket event that triggers the bug).
- The affected version (see `CHANGELOG.md` and the version badge in `README.md`) and the mode you tested against (server / client, and which chain and network).
- Any patches or mitigations you'd like considered.

For sensitive reports, encrypt the email body to our PGP key. The fingerprint will be published alongside the first signed release artifact; until then, the email channel is acceptable for first contact and we will coordinate an encrypted exchange before you share proof-of-concept details.

We do not currently offer a paid bug bounty. We do offer public credit in release notes and the advisory itself, unless you prefer to remain anonymous.

---

## Response timeline

| Stage | Target |
|---|---|
| Initial acknowledgement | within 72 hours |
| Triage + severity assignment | within 7 days |
| Fix or mitigation in master | within 30 days for high/critical, 90 days for lower severities |
| Coordinated public disclosure | up to 90 days from initial report, or sooner if a fix has shipped and operators are protected |

If we cannot meet a timeline, we will tell you why and propose a new one. We will not silently let a report age.

---

## Scope

### In scope

- Integrity and authenticity of snapshot data served over the REST endpoints (full and incremental snapshots): a client must not be tricked into accepting tampered block data.
- Integrity and authenticity of the real-time WebSocket stream: forged or replayed events that could cause a client to apply wrong state or miss a reorg.
- Replication consistency: any path where a client can be induced to accept divergent state from a malicious or compromised server.
- Authentication and authorization on all snapshot and streaming endpoints.
- SQL identifier sanitization and DDL whitelisting in the client applier (a crafted server response must not execute arbitrary DDL on a replica database).
- Input validation on WebSocket event schemas and REST request parameters.
- Denial-of-service against a server (via crafted snapshot requests or WebSocket connections) or a client (via crafted events).
- The hash-chain verification and cross-source comparison logic that is the primary integrity defense for validator nodes.

### Out of scope

- Correctness of the source data being replicated (bad ledger data that originates in the indexer or decoder; report those against `xchain-indexer` or `xchain-decoder`).
- Vulnerabilities in the upstream coin nodes, the operator's TLS/reverse-proxy configuration, or network exposure of the sync port.
- Compromise of the operator's own MariaDB credentials or shell access to the host.
- Upstream npm dependency backdoors (we mitigate via audit + review; a backdoor in a dep is the dep author's incident, though we still want to hear about it).

If you are unsure, send the report anyway and we will tell you whether it falls in scope.

---

## What we ask

- Give us a reasonable window to fix before disclosing publicly. The 90-day ceiling is firm; earlier is fine once a fix has shipped and operators are protected.
- Test against `regtest` or `testnet` where possible (the `xchain-regtest-miner` plus a local stack make this easy). Mainnet proofs-of-concept are accepted but should be the minimum needed.
- Do not run automated scanners against shared XChain infrastructure in a way that would impact availability for other operators.
- Do not access data, or attempt to access data, beyond what is needed to demonstrate the issue.

---

## What we will do

- Confirm receipt within the SLA above.
- Keep you informed as triage and remediation proceed.
- Credit you in the advisory and `CHANGELOG.md` entry, on request.
- Coordinate a CVE assignment when the severity warrants it.
- Publish a post-fix advisory describing the issue, the fix, and the affected version range.

---

## Versions covered

We ship security fixes against the latest release on `master`. Older releases are unsupported. The current version is recorded in `CHANGELOG.md` and the badge in `README.md`.

---

Last reviewed: 2026-06-16.
