# Contributing to XChain Sync

Thanks for considering a contribution. `xchain-sync` replicates authoritative ledger state from origin servers to validator nodes via REST snapshots and WebSocket streaming, so correctness and integrity of that pipeline matter on every commit.

If you're reporting a security issue, **stop here** and read [`SECURITY.md`](./SECURITY.md) instead. Security reports go through a private channel.

---

## Quick links

- Project overview: [`README.md`](./README.md)
- Full component docs: the [`xchain-documentation`](https://github.com/XChain-Platform/xchain-documentation/tree/master/components/sync) repository (architecture, configuration, operations, API reference)
- Disclosure policy: [`SECURITY.md`](./SECURITY.md)
- License: [`LICENSE.md`](./LICENSE.md) + [`NOTICE.md`](./NOTICE.md) (GNU Affero General Public License v3.0, dual-licensed)

---

## Repo layout in 30 seconds

```
xchain-sync/
├── src/                  sync service: server poller, snapshot builder, WebSocket broadcaster,
│                         client applier, hash verifier, hub client, transparency log, API
├── test/                 layered suites (unit, integration, e2e, fuzz, chaos, security, ...)
├── CHANGELOG.md          authoritative version history
├── SECURITY.md           private vulnerability disclosure
└── package.json          scripts + dependencies
```

---

## Setting up

### Prerequisites

- **Node.js 22** exactly. The platform pins Node 22 fleet-wide: the `mariadb` driver is ESM-only (Node 18 fails with `ERR_REQUIRE_ESM`), and newer majors are not validated against the stack. `engines.node` declares `>=22.0.0`; use 22.
- **MariaDB** reachable from the sync host for anything beyond unit tests.
- A running `xchain-hub` instance for hub auto-discovery (server mode) or a configured `SYNC_SOURCES` list (client mode). For local work, a regtest stack provides both.

### First-time install

```bash
git clone https://github.com/XChain-Platform/xchain-sync.git
cd xchain-sync
npm install
```

Create a `.env` (see [`README.md`](./README.md) for the full key list). **Never commit a `.env` or any real credential.** Secrets live only in the local `.env`, loaded at runtime; never hard-code them into source, tests, or scripts.

---

## Running it

```bash
npm run api        # start the sync service (server or client mode per SYNC_MODE)
```

---

## Tests

The sync service runs a layered suite. Pick the tier that matches your change:

| Tier | Command | Needs external services |
|---|---|---|
| Smoke | `npm run test:smoke` | No |
| Unit | `npm test` | No |
| Security | `npm run test:security` | No |
| CI (unit, fast gate) | `npm run ci` | No |
| Integration | `npm run test:integration` | MariaDB + running indexer |
| End-to-end | `npm run test:e2e` | Full stack |
| Fuzz | `npm run test:fuzz` (`:quick` for 100 iterations) | No |
| Chaos | `npm run test:chaos` | No |
| Performance | `npm run test:perf` (`:quick` for 50 blocks) | No |
| Mutation | `npm run test:mutate` (`:quick` or `:check` variants) | No |

Run the no-external-services tiers before every commit; the README documents the full script catalogue. Changes to snapshot serialization, WebSocket event handling, hash verification, or the client applier should include fuzz and security coverage, since a validator's view of consensus state depends on this pipeline.

---

## Coding style

- **Plain JavaScript**, no TypeScript. Raw parameterized SQL via the `mariadb` driver, no ORM.
- **No linter is configured.** Match the style of the surrounding file: naming, structure, and comment density.
- **Comments are rare on purpose.** Don't restate what well-named code already says. Do comment a *why* that isn't obvious: a hidden invariant, a validator-trust assumption, a workaround with a reference.
- **Never use the em-dash character** in code, comments, or docs. Rewrite the sentence (a comma, colon, or parentheses) instead.
- **Two trailing spaces** on consecutive bold-label markdown lines so CommonMark renders the line break instead of collapsing them.
- **Integrity matters.** The hash-chain verification and cross-source comparison are a validator's primary defense against tampered data. Changes in that path deserve extra scrutiny and test coverage.

---

## Commit messages

Match the existing log style: a concise subject line, then a short body explaining what changed and why.

- Branch off `master` and keep history linear (rebase, don't merge).
- One logical change per commit; don't batch unrelated work.
- **No `Co-Authored-By` trailers.** This is a project policy.
- **Never `--no-verify`.** If a hook fails, fix the cause; don't bypass it.

---

## Pull requests

CI is the smoke + unit gate. Before opening a PR:

1. Run the no-external-services tiers (`npm run ci`, `npm run test:security`) and confirm they pass.
2. Update `CHANGELOG.md` with a terse entry for your change.
3. Make sure `git status` is clean apart from intended changes (no `node_modules/`, no editor leftovers, no `.env`).
4. Open the PR with a clear title and a description of what changed and why.

For non-security bugs, open an issue at <https://github.com/XChain-Platform/xchain-sync/issues/new>. For security bugs, see [`SECURITY.md`](./SECURITY.md).

---

Last reviewed: 2026-06-16.
