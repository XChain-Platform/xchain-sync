/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************
 *
 * XChain Indexer Sync - Configuration
 *
 * This file reads environment variables and returns a config object
 *
 ********************************************************************/

const { coinTicker } = require('./consensus-constants');
const { parseCorsOrigin } = require('./corsOrigin');

// Parse an integer from an env var, returning defaultVal when the value is
// absent, empty, or non-numeric.  Unlike `parseInt(x) || default`, this
// correctly preserves 0 as a valid value.
function parseIntSafe(val, defaultVal){
    if(val === undefined || val === null || val === '') return defaultVal;
    let parsed = parseInt(val, 10);
    return isNaN(parsed) ? defaultVal : parsed;
}

// Parse a non-negative integer, clamping to 0 at minimum.
function parseIntMin0(val, defaultVal){
    return Math.max(0, parseIntSafe(val, defaultVal));
}

// Parse a positive integer (>= 1).
function parseIntMin1(val, defaultVal){
    return Math.max(1, parseIntSafe(val, defaultVal));
}

const BOOTSTRAP_DEPTH_PREFIX = 'SYNC_BOOTSTRAP_DEPTH_';

// Canonical SYNC_BOOTSTRAP_DEPTH map key, '<TICKER>:<NETWORK>' uppercased.
//
// Both sides of this lookup MUST build the key through here. They did not: this
// file keyed the env suffix verbatim ('DOGE:TESTNET') while ClientSync looked the
// chain up by `cfg.coin`, which the hub publishes as the full lowercase name, so
// the documented SYNC_BOOTSTRAP_DEPTH_DOGE_TESTNET produced 'DOGE:TESTNET' and the
// lookup asked for 'DOGECOIN:TESTNET'. The miss was silent and fell through to
// depth 0, which is the FULL-snapshot branch: the 2026-08-10 DOGE reseed began a
// full-history bootstrap of a 67M-block chain and had to be killed by hand.
// coinTicker folds ticker and full-name forms onto the ticker, so both
// SYNC_BOOTSTRAP_DEPTH_DOGE_TESTNET and SYNC_BOOTSTRAP_DEPTH_DOGECOIN_TESTNET now
// resolve to the same chain.
function bootstrapDepthKey(chain, network){
    if(chain === undefined || chain === null) return null;
    if(network === undefined || network === null) return null;
    let ticker = coinTicker(String(chain));
    if(ticker === undefined || ticker === null) return null;
    return String(ticker).toUpperCase() + ':' + String(network).toUpperCase();
}

// Canonical key for a SYNC_BOOTSTRAP_DEPTH_<CHAIN>_<NETWORK> env name, or null when
// the suffix has no CHAIN_NETWORK shape at all (e.g. SYNC_BOOTSTRAP_DEPTH_BADKEY).
// An unrecognized chain still yields a key: it is a real key that matches no chain,
// which is the assertBootstrapDepthChains case, not a parse failure.
function bootstrapDepthEnvKey(envKey){
    if(String(envKey).indexOf(BOOTSTRAP_DEPTH_PREFIX) !== 0) return null;
    let rest = String(envKey).slice(BOOTSTRAP_DEPTH_PREFIX.length); // e.g. DOGE_TESTNET
    let sep  = rest.lastIndexOf('_');
    if(sep <= 0 || sep >= rest.length - 1) return null; // need CHAIN_NETWORK
    return bootstrapDepthKey(rest.slice(0, sep), rest.slice(sep + 1));
}

// Every SYNC_BOOTSTRAP_DEPTH_* env name that names no discovered chain, each as
// { envKey, resolved } (resolved is null when the name has no CHAIN_NETWORK shape).
// `chains` is the discovered chain list: [{ coin, network }, ...].
function unmatchedBootstrapDepthKeys(envKeys, chains){
    let discovered = new Set();
    for(let c of (chains || [])){
        let key = bootstrapDepthKey((c.coin !== undefined) ? c.coin : c.chain, c.network);
        if(key) discovered.add(key);
    }
    let unmatched = [];
    for(let envKey of (envKeys || [])){
        let resolved = bootstrapDepthEnvKey(envKey);
        if(resolved === null || !discovered.has(resolved))
            unmatched.push({ envKey: envKey, resolved: resolved });
    }
    return unmatched;
}

// REFUSE a depth key that matches no discovered chain instead of letting it default
// to 0. Depth 0 is not a harmless "unset": it is the full-history snapshot branch,
// the exact branch the operator set the key to avoid, and on a fast chain that means
// an unbounded bootstrap nobody asked for. A typo'd or stale key is therefore a hard
// startup failure, not a warning.
function assertBootstrapDepthChains(config, chains){
    let envKeys   = (config && config['SYNC_BOOTSTRAP_DEPTH_ENV_KEYS']) || [];
    let unmatched = unmatchedBootstrapDepthKeys(envKeys, chains);
    if(unmatched.length === 0) return;

    let discovered = (chains || [])
        .map(c => bootstrapDepthKey((c.coin !== undefined) ? c.coin : c.chain, c.network))
        .filter(k => k);
    let detail = unmatched
        .map(u => u.envKey + (u.resolved ? ' (resolves to ' + u.resolved + ')' : ' (not CHAIN_NETWORK shaped)'))
        .join(', ');
    throw new Error(
        'SYNC_BOOTSTRAP_DEPTH refers to no discovered chain: ' + detail +
        '. Discovered chains: ' + (discovered.length ? [...new Set(discovered)].join(', ') : '(none)') +
        '. Refusing to start: an unmatched depth key would silently fall back to depth 0, ' +
        'which is the full-history snapshot branch.'
    );
}

module.exports = {

    bootstrapDepthKey,
    bootstrapDepthEnvKey,
    unmatchedBootstrapDepthKeys,
    assertBootstrapDepthChains,

    getConfig: function(){
        let config = {};

        config['SYNC_MODE']     = process.env.SYNC_MODE || 'server';

        config['SYNC_API_PORT'] = parseIntMin0(process.env.SYNC_API_PORT, 3006);

        // Hub connection (HUB_VALIDATORS takes priority over HUB_API_HOST:HUB_PORT)
        config['HUB_VALIDATORS'] = process.env.HUB_VALIDATORS || '';
        config['HUB_API_HOST']   = process.env.HUB_API_HOST;
        config['HUB_PORT']       = parseIntMin1(process.env.HUB_PORT, 10000);

        // Max time to wait for the hub at startup before giving up and
        // exiting non-zero (so a process supervisor can restart/alert).
        // Defaults to 5 minutes.
        config['MAX_HUB_WAIT_MS'] = parseIntMin0(process.env.MAX_HUB_WAIT_MS, 300000);

        // A comma-separated ALLOWLIST, not a single origin: handing `cors` the raw
        // string echoes it back verbatim to every caller, which is a multi-value
        // header no browser accepts. Parsed here rather than at the cors() call
        // because api.js only ever sees cfg. See src/corsOrigin.js.
        config['CORS_ORIGIN']   = parseCorsOrigin(process.env.CORS_ORIGIN);

        config['BLOCK_POLL_INTERVAL'] = parseIntMin0(process.env.BLOCK_POLL_INTERVAL, 3000);
        // A replica/validator follows EVERY chain a server hosts over its own WS, all
        // from one IP, and a single chain-node host alone serves 12 of those streams.
        // The old default of 3 closed 9 of them with 1008 "Too many connections", a
        // permanent reconnect storm that also broke gap-detection-driven catch-up.
        config['WS_MAX_PER_IP']       = parseIntMin1(process.env.WS_MAX_PER_IP, 100);
        // Per-resource (IP + chain/network/dbType) limits; see snapshotKey in api.js.
        // Defaults give a multi-chain replica headroom for bootstrap retries (full) and
        // frequent gap catch-ups (incremental) instead of a single global-per-IP bucket
        // that starves other chains.
        config['SNAPSHOT_RATE_FULL']  = parseIntMin0(process.env.SNAPSHOT_RATE_FULL, 12);
        config['SNAPSHOT_RATE_INCR']  = parseIntMin0(process.env.SNAPSHOT_RATE_INCR, 600);

        // Per-chain replication exclude (client mode). Comma-separated list of
        // `coin:network:dbType` keys (e.g. `DOGE:testnet:indexer`) that the client
        // must NOT replicate. A discovered chain whose key is listed is skipped in
        // _discoverChains, so no ClientSync is started for it and it can never
        // crash-loop the process. Used to drop a chain that cannot full-snapshot
        // bootstrap (fast chains with tens of millions of blocks) until the
        // start-from-recent-height bootstrap (SYNC_BOOTSTRAP_DEPTH_*) is deployed.
        // Trimmed + deduplicated; empty/unset -> [] (no chain excluded).
        config['SYNC_EXCLUDE'] = [...new Set(
            (process.env.SYNC_EXCLUDE || '')
                .split(',')
                .map(s => s.trim())
                .filter(s => s.length > 0)
        )];

        // Per-chain start-from-recent-height bootstrap (client mode). Opt-in via
        // SYNC_BOOTSTRAP_DEPTH_<CHAIN>_<NETWORK>=N (e.g.
        // SYNC_BOOTSTRAP_DEPTH_DOGE_TESTNET=50000). When set for a chain, an empty
        // replica bootstraps from (sourceTip - N) using one incremental snapshot
        // instead of a full-history snapshot. This is the only way to seed a fast
        // chain whose full snapshot (tens of millions of blocks) cannot be buffered
        // and applied in one pass on the client. Absent -> unchanged full bootstrap
        // with full history + full verification.
        //
        // A truncated replica CANNOT answer pre-base history and its aggregate
        // balances are recent-window only; it is acceptable ONLY for a non-consensus
        // explorer mirror, never a trusted validator. Parsed into a map keyed
        // '<TICKER>:<NETWORK>' by bootstrapDepthKey (so the ticker and full-name env
        // spellings land on one key, and ClientSync's lookup by hub `cfg.coin`
        // matches); values clamped to >= 1.
        //
        // The raw env names are carried alongside as SYNC_BOOTSTRAP_DEPTH_ENV_KEYS so
        // assertBootstrapDepthChains can refuse a key that matches no discovered chain
        // once discovery has run. Keys are recorded whatever their value: a key whose
        // value is 0 or garbage is still an operator naming a chain, and naming a chain
        // that does not exist is the misconfiguration worth failing on.
        let bootstrapDepth = {};
        let bootstrapDepthEnvKeys = [];
        for(let envKey in process.env){
            if(envKey.indexOf(BOOTSTRAP_DEPTH_PREFIX) !== 0) continue;
            bootstrapDepthEnvKeys.push(envKey);
            let chainKey = bootstrapDepthEnvKey(envKey);
            if(chainKey === null) continue; // not CHAIN_NETWORK shaped
            let depth = parseIntSafe(process.env[envKey], 0);
            if(depth >= 1) bootstrapDepth[chainKey] = depth;
        }
        config['SYNC_BOOTSTRAP_DEPTH'] = bootstrapDepth;
        config['SYNC_BOOTSTRAP_DEPTH_ENV_KEYS'] = bootstrapDepthEnvKeys;

        // Client mode settings
        config['SYNC_SOURCES']   = process.env.SYNC_SOURCES || '';
        config['VERIFY_HASHES']  = (process.env.VERIFY_HASHES || '').toLowerCase() !== 'false';
        config['REPLICA_DB_HOST'] = process.env.REPLICA_DB_HOST;
        config['REPLICA_DB_PORT'] = parseIntMin1(process.env.REPLICA_DB_PORT, 3306);
        config['REPLICA_DB_USER'] = process.env.REPLICA_DB_USER;
        config['REPLICA_DB_PASS'] = process.env.REPLICA_DB_PASS;
        // Serve from a replica this process must never write to. Set on a re-serving
        // tier whose replica is maintained by something else (MariaDB binlog
        // replication on the sync.xchain.io web tier), where the box is read_only=1
        // and sync_meta / merkle_epochs arrive already recorded by the upstream
        // server. Makes the transparency log serve-only; reads are unaffected.
        config['REPLICA_DB_READONLY'] = (process.env.REPLICA_DB_READONLY || '').toLowerCase() === 'true'
            || process.env.REPLICA_DB_READONLY === '1';

        // Hub re-poll interval (default 5 minutes; override via HUB_REPOLL_INTERVAL)
        config['HUB_REPOLL_INTERVAL'] = parseIntMin0(process.env.HUB_REPOLL_INTERVAL, 300000);

        // Merkle tree epoch size (blocks per epoch)
        config['MERKLE_EPOCH_SIZE'] = parseInt(process.env.MERKLE_EPOCH_SIZE) || 100;

        // Transparency endpoint rate limit (requests per minute per IP)
        config['TRANSPARENCY_RATE_LIMIT'] = parseInt(process.env.TRANSPARENCY_RATE_LIMIT) || 10;

        // WebSocket backpressure: a replica is dropped only when its send buffer
        // is genuinely stuck, not merely slow. MAX_BYTES caps per-peer server memory (a peer
        // accumulating past this is not draining); STALL_MS is how long the buffer may go
        // without making downward progress before the peer is dropped. This replaces the old
        // count-based WS_BACKPRESSURE_LIMIT, which dropped slow-but-draining replicas and
        // thrashed them into re-bootstraps.
        config['WS_BACKPRESSURE_MAX_BYTES'] = parseIntMin1(process.env.WS_BACKPRESSURE_MAX_BYTES, 16777216); // 16 MiB
        config['WS_BACKPRESSURE_STALL_MS']  = parseIntMin1(process.env.WS_BACKPRESSURE_STALL_MS, 30000);     // 30 s
        if(process.env.WS_BACKPRESSURE_LIMIT !== undefined)
            console.log('config: WS_BACKPRESSURE_LIMIT is retired and ignored; tune WS_BACKPRESSURE_MAX_BYTES / WS_BACKPRESSURE_STALL_MS instead.');

        // WebSocket status broadcast interval (default 60 seconds; override via WS_STATUS_INTERVAL)
        config['WS_STATUS_INTERVAL'] = parseIntMin0(process.env.WS_STATUS_INTERVAL, 60000);

        // WebSocket ping interval (default 30 seconds; override via WS_PING_INTERVAL)
        config['WS_PING_INTERVAL'] = parseIntMin0(process.env.WS_PING_INTERVAL, 30000);

        // Security: API key authentication (disabled when not set)
        config['SYNC_API_KEY'] = process.env.SYNC_API_KEY || '';

        // Security: Hub protocol (http or https)
        config['HUB_PROTOCOL'] = (process.env.HUB_PROTOCOL || '').toLowerCase() === 'https' ? 'https' : 'http';

        // Security: Trust x-forwarded-for header (only enable behind a reverse proxy)
        config['TRUST_PROXY'] = (process.env.TRUST_PROXY || '').toLowerCase() === 'true';

        // Security: Maximum rollback depth from a single source (blocks)
        config['MAX_ROLLBACK_DEPTH'] = parseIntMin1(process.env.MAX_ROLLBACK_DEPTH, 100);

        // Security: Reject blocks on cross-source verification timeout (instead of applying from primary)
        config['HASH_CONFIRM_STRICT'] = (process.env.HASH_CONFIRM_STRICT || '').toLowerCase() === 'true';

        // Multi-source Byzantine quorum. SOURCE_QUORUM is the M-of-N
        // agreement threshold the live cross-source path requires before it applies a
        // block: >= SOURCE_QUORUM sources must publish the SAME ledger/actions/contract
        // hash tuple. 0 (unset) selects the simple-majority default, computed from the
        // configured source count N in ClientSync: ceil((N+1)/2). That makes N=1 behave
        // as the single-source posture (quorum 1), N=2 demand both sources (a 1-1 split
        // has no majority and halts, exactly as the prior pairwise path), and N=3f+1
        // tolerate f Byzantine sources (e.g. N=4 -> quorum 3 = 2f+1). An explicit value
        // is clamped to [1, N]. Setting it below the majority is a footgun (f colluding
        // sources can then out-vote the honest set) and is the operator's deliberate
        // choice. The checkpoint-quorum anchor is the only defense against ALL sources
        // colluding; cross-source quorum only defends against a minority.
        config['SOURCE_QUORUM'] = parseIntMin0(process.env.SOURCE_QUORUM, 0);

        // Byzantine-source eviction. A source that dissents from the applied quorum
        // majority accrues a strike per block; once its strike count within the sliding
        // window reaches SOURCE_EVICT_THRESHOLD it is evicted from the active quorum
        // denominator (its WebSocket is closed and not reconnected, and an alert fires
        // on /status). Eviction preserves liveness against a Byzantine minority instead
        // of halting on every block it contests. Never evicts below two active sources
        // (that would collapse to a blind single-source posture); below that floor a
        // persistent dissenter is retained and per-block no-source-quorum halts guard
        // safety.
        config['SOURCE_EVICT_THRESHOLD'] = parseIntMin1(process.env.SOURCE_EVICT_THRESHOLD, 3);

        // Sliding-window size (in applied blocks) over which source strikes are counted
        // toward SOURCE_EVICT_THRESHOLD. Strikes older than this many blocks behind the
        // current block are pruned, so a source that misbehaved long ago but has since
        // been consistent is not evicted on stale strikes.
        config['SOURCE_STRIKE_WINDOW'] = parseIntMin1(process.env.SOURCE_STRIKE_WINDOW, 200);

        // Consensus safety: on a CONFIRMED cross-source hash divergence (two honest
        // sources committed different ledger/actions/contract hashes for the same
        // block; one is on a forked/Byzantine chain), HALT durably instead of just
        // logging and silently stalling. Default ON. Set HALT_ON_DIVERGENCE=false to
        // revert to log-only (not recommended for validators).
        config['HALT_ON_DIVERGENCE'] = (process.env.HALT_ON_DIVERGENCE || 'true').toLowerCase() !== 'false';

        // Validator track: independently RECOMPUTE each indexer block's consensus
        // hashes (ledger/actions/contract) from the replicated raw rows and confirm
        // they match the committed hash. Catches a replica whose DATA does not hash
        // to the committed hash (replication corruption / partial apply / a source
        // serving rows inconsistent with its committed hash), which the verbatim
        // hash comparison cannot. A mismatch HALTs durably. Default ON.
        //
        // VERIFY_RECOMPUTE=false is DECLARED UNSAFE (operator decision 2026-06-12)
        // for any consensus-relevant replica: the recompute is the ONLY mechanism
        // that verifies the catch-up JOIN block, so with it off a reorg that
        // happens while this client is disconnected/restarting is stitched onto
        // the orphaned pre-reorg tip and the replica SILENTLY follows the forked
        // chain, keeping the orphaned blocks forever (proven by the
        // parity-interleave e2e property suite). Disable only for throwaway,
        // read-only convenience mirrors whose state nothing downstream trusts.
        config['VERIFY_RECOMPUTE'] = (process.env.VERIFY_RECOMPUTE || 'true').toLowerCase() !== 'false';

        // VERIFY_STATE_HASH: apply-time recompute of the per-block state_hash (the
        // in-place mutations + backdated refund credits the three consensus hashes
        // can't cover). Default ON; a mismatch HALTs durably, the same as a recompute
        // divergence. Additive + fail-soft: a NULL state_hash (block indexed before the
        // source had the feature) is skipped, so enabling it can never false-halt a
        // back-level source. Set VERIFY_STATE_HASH=false only for throwaway mirrors.
        config['VERIFY_STATE_HASH'] = (process.env.VERIFY_STATE_HASH || 'true').toLowerCase() !== 'false';

        // VERIFY_STATE_COMMITMENT: apply-time recompute of the per-block light-client
        // SMT roots (SPV spec sec.4-5) over the replica, HALTing on divergence. Default
        // ON; NULL roots (blocks before the flag-day) are skipped (additive + fail-soft).
        // Phase 1 verifies balances_root + block_merkle_root only (stakes_root/state_root
        // are deferred until the BTC stake-weight query is ported). Set
        // VERIFY_STATE_COMMITMENT=false on truncated / incremental-bootstrapped replicas
        // (their balances history is incomplete, so the SMT full-build would be wrong)
        // and on throwaway mirrors.
        config['VERIFY_STATE_COMMITMENT'] = (process.env.VERIFY_STATE_COMMITMENT || 'true').toLowerCase() !== 'false';

        // INDEX_MAP_PARITY_CHECK: advisory id->address map parity. Default OFF, and
        // UNLIKE the VERIFY_* gates above it NEVER halts: a mismatch is logged + counted
        // only. It catches a replica whose index_addresses id->address map content
        // diverged from the source's (e.g. a local INSERT IGNORE that kept a colliding
        // id and dropped the source's row), which the resolved-string consensus hashes
        // and the row-count check structurally cannot see (equal count, different
        // content). Read on BOTH sides: a server (SYNC_MODE=server) publishes the
        // deterministic-subset checksum on /status; a client recomputes + compares in
        // _verifyAgainstSource. OFF by default because computing it scans the
        // deterministic subset of index_addresses (an index on block_index is advisable
        // before enabling on a high-volume chain).
        config['INDEX_MAP_PARITY_CHECK'] = (process.env.INDEX_MAP_PARITY_CHECK || '').toLowerCase() === 'true';

        // TABLE_CONTENT_PARITY_CHECK: advisory per-table CONTENT parity over
        // every replicated table the registry declares covered (src/tableLifecycle.js
        // CONTENT_PARITY_*). Same posture as INDEX_MAP_PARITY_CHECK and for the same
        // reason, one scope wider: the row counts published beside it prove only
        // cardinality, so an equal-count content substitution in a table no consensus
        // hash reads passed every check a follower ran. NEVER halts; a mismatch is
        // logged and durably counted. Read on BOTH sides (a server publishes the
        // checksums on /status, a client at the same height recomputes and compares).
        // OFF by default: it reads a window of ~93 indexer tables per status poll.
        config['TABLE_CONTENT_PARITY_CHECK'] = (process.env.TABLE_CONTENT_PARITY_CHECK || '').toLowerCase() === 'true';

        // TABLE_CONTENT_PARITY_WINDOW: how many blocks (and, for the append-only
        // lookups that carry no block column, how many ids) each content checksum
        // spans. Server-side setting: the source publishes the window it used and a
        // follower recomputes over THAT, so the two can never compare different spans
        // and an operator only has to tune the source. Clamped to [1, 10000]: 0 or a
        // negative would silently disable the check while it still reported passes,
        // and an unbounded value would read a table's whole history every poll.
        // Default 100, comfortably above the applied blocks between two verification
        // passes, so a divergence has to be caught rather than aged out.
        {
            let raw = parseInt(process.env.TABLE_CONTENT_PARITY_WINDOW, 10);
            config['TABLE_CONTENT_PARITY_WINDOW'] = Number.isFinite(raw) ? Math.min(10000, Math.max(1, raw)) : 100;
        }

        // VERIFY_CHECKPOINT_QUORUM (SPV): anchor the replica's INDEPENDENTLY-recomputed
        // state_root to the federation quorum instead of trusting the source's claimed
        // value. The client fetches the source's signed checkpoint, verifies its
        // signatures against an OUT-OF-BAND pinned validator set (pinnedValidators.js),
        // and HALTs if the quorum fails or the checkpoint's state_root disagrees with the
        // replica's own state_tree_roots row. Closes the single-source trust gap that the
        // recompute checks (which only compare against the same source) cannot.
        // Default OFF: consensus-sensitive and INERT without a pinned set. When on but no
        // pinned set is configured for the (chain, network), the step is skipped (never
        // bypassed). CHECKPOINT_VERIFY_INTERVAL bounds how often the /latest checkpoint is
        // probed (in applied blocks; default 50).
        //
        // FLAG-DAY COUPLING: this default is tied to pinnedValidators.js. It stays
        // OFF only while every pinned key is null, because a true-but-inert flag reads as
        // "protected" while nothing is actually verified. The change that populates a real
        // launch validator set must flip this default to ON in the same change (an operator
        // who has a trust root available and does not use it is still trusting the source);
        // an explicit VERIFY_CHECKPOINT_QUORUM=false remains the opt-out for throwaway
        // mirrors. test/unit/checkpointQuorumFlagDay.test.js enforces both halves, and the
        // launch activation runbook carries the deploy-side ordering (the federation must
        // be serving signed checkpoints first).
        config['VERIFY_CHECKPOINT_QUORUM'] = (process.env.VERIFY_CHECKPOINT_QUORUM || 'false').toLowerCase() === 'true';
        config['CHECKPOINT_VERIFY_INTERVAL'] = parseIntMin1(process.env.CHECKPOINT_VERIFY_INTERVAL, 50);

        // Out-of-band checkpoint anchor endpoint. The quorum verify is sound even when the
        // checkpoint is fetched from the audited source (a lying source cannot forge a
        // pinned-set quorum), but a single source can WITHHOLD: serve a stale-but-genuine
        // older checkpoint, or 404, so a forged tail past the last served checkpoint is
        // never anchored. Point this at the hub/federation (a different endpoint than the
        // streaming source) to close that withholding gap. Unset = fall back to sources[0].
        config['CHECKPOINT_ANCHOR_URL'] = process.env.CHECKPOINT_ANCHOR_URL || null;

        // Freshness bound (in applied blocks) for the checkpoint anchor. When the newest
        // quorum checkpoint trails the replica tip by more than this, the anchor cannot
        // catch a forged tail, so the gap is logged (advisory, never a halt: withholding is
        // not proof of forgery, and halting on absence would hand an attacker a DoS-halt).
        config['CHECKPOINT_FRESHNESS_BLOCKS'] = parseIntMin1(process.env.CHECKPOINT_FRESHNESS_BLOCKS, 500);

        // CHECKPOINT_FRESHNESS_STRICT: promote the freshness bound from
        // advisory to enforced. When on, a replica whose tip trails the newest
        // quorum-signed checkpoint by more than CHECKPOINT_FRESHNESS_BLOCKS refuses to
        // serve the unanchored tail and HALTs durably (reason `checkpoint-freshness-
        // stale`) instead of only logging. Default OFF: an always-on version would hand
        // a source that withholds fresh checkpoints a halt-DoS, so only replicas that
        // opt in accept that trade for the stronger guarantee. Only enforced once the
        // anchor has verified at least one checkpoint (the federation is demonstrably
        // live), so a replica that has never seen a checkpoint is not halted at startup.
        config['CHECKPOINT_FRESHNESS_STRICT'] = (process.env.CHECKPOINT_FRESHNESS_STRICT || '').toLowerCase() === 'true';

        // Security: WebSocket max incoming message size in bytes (default 1 MB)
        config['WS_MAX_PAYLOAD'] = parseIntMin1(process.env.WS_MAX_PAYLOAD, 1048576);

        // Security: Max HTTP response size for snapshot downloads in bytes (default 512 MB)
        config['SNAPSHOT_MAX_CONTENT'] = parseIntMin1(process.env.SNAPSHOT_MAX_CONTENT, 536870912);

        // Client reconnect delay (default 5 seconds; override via CLIENT_RECONNECT_DELAY)
        config['CLIENT_RECONNECT_DELAY'] = parseIntMin0(process.env.CLIENT_RECONNECT_DELAY, 5000);

        // Source-height freshness window (client mode). The server emits a `status`
        // heartbeat every WS_STATUS_INTERVAL (60s default), so a healthy live WS sees
        // an event at least that often. If no WS event arrives within this window the
        // client's lastKnownServerBlock is frozen and /status would otherwise report
        // lag_blocks 0 forever once the replica catches up to that stale value, hiding
        // a silently dropped WebSocket. Default 180s = 3 missed heartbeats.
        config['CLIENT_SOURCE_STALE_MS'] = parseIntMin1(process.env.CLIENT_SOURCE_STALE_MS, 180000);

        // Replication freshness ceiling (server mode). The client path has the window
        // above; a server fronting a native SQL replica had no equivalent, so a
        // stalled replica froze the source and served heights together and published
        // lag_blocks 0 on an hours-behind node. Seconds behind the source
        // above this reads stale. Needs the REPLICATION CLIENT grant to be readable.
        config['SYNC_REPLICA_MAX_LAG_S'] = parseIntMin1(process.env.SYNC_REPLICA_MAX_LAG_S, 120);

        // Bootstrap retry-with-backoff (client mode). A full snapshot bootstrap that
        // exhausts every configured source must NOT fall through to live-follow on an
        // empty replica; instead it retries the whole source rotation with bounded
        // exponential backoff, then propagates failure (start() throws, and the process
        // supervisor restarts the container). This is the only recovery for the
        // single-source topology, where there is no second source to rotate to.
        //   BOOTSTRAP_MAX_RETRIES: extra full-rotation rounds after the first (0 = no retry)
        //   BOOTSTRAP_RETRY_BASE_MS: first backoff delay; doubles each round
        //   BOOTSTRAP_RETRY_MAX_MS: backoff ceiling
        config['BOOTSTRAP_MAX_RETRIES']   = parseIntMin0(process.env.BOOTSTRAP_MAX_RETRIES, 5);
        config['BOOTSTRAP_RETRY_BASE_MS'] = parseIntMin1(process.env.BOOTSTRAP_RETRY_BASE_MS, 2000);
        config['BOOTSTRAP_RETRY_MAX_MS']  = parseIntMin1(process.env.BOOTSTRAP_RETRY_MAX_MS, 60000);

        // Hash confirm timeout for cross-source verification (default 5 seconds; override via HASH_CONFIRM_TIMEOUT)
        config['HASH_CONFIRM_TIMEOUT'] = parseIntMin0(process.env.HASH_CONFIRM_TIMEOUT, 5000);

        // Validator heartbeat TTL: entries not seen within this window (ms) are
        // transitioned to a 'stale' status (kept visible in /validator-status, not
        // hard-deleted) so a validator that restarted or briefly dropped off the
        // network remains observable rather than silently vanishing.
        config['VALIDATOR_HEARTBEAT_TTL'] = parseIntMin1(process.env.VALIDATOR_HEARTBEAT_TTL, 60000);

        // Optional expected-validator roster: a comma-separated list of validator
        // ids/pubkeys. When set, /validator-status reports an `expected_total`
        // denominator alongside the observed count and flags roster members that
        // have never reported a heartbeat as status 'absent'. This is the only
        // in-band signal for a validator that silently fell off the federation,
        // e.g. a replaced machine configured with the wrong sync server URL, or a
        // node network-partitioned before it ever POSTed. Empty/unset -> [] (the
        // service runs exactly as before, with no roster anchor). Deduplicated and
        // trimmed so the denominator is accurate.
        config['EXPECTED_VALIDATORS'] = [...new Set(
            (process.env.EXPECTED_VALIDATORS || '')
                .split(',')
                .map(s => s.trim())
                .filter(s => s.length > 0)
        )];

        return config;
    }
};
