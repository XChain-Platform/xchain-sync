/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
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
 * Light-client state commitment: FOLLOWER twin (SPV spec sec.4-5).
 *
 * Conformance twin of xchain-indexer/src/stateCommitment.js. The indexer
 * (SOURCE) computes + stores the per-block SMT roots inside its block txn,
 * driven by db._smtTouched (the keys its ledger choke point recorded). This
 * follower copy recomputes the roots over the REPLICA inside the apply txn and
 * compares to the source's committed roots; a mismatch HALTS the follower
 * (ClientSync VERIFY_STATE_COMMITMENT).
 *
 * Differences from the indexer twin, all consensus-neutral:
 *   - The follower has no write choke point, so computeFollowerRoots takes the
 *     touched (address, tick) set EXPLICITLY (ClientApplier derives it from the
 *     applied block event rows, which already carry merged cooldown refunds).
 *   - getNetBalance renders the DECIMAL sum to a minimal-decimal string in SQL
 *     (balance-helpers.minimalDecimal), instead of mathjs bcsub; canonicalAmount
 *     normalises both to the identical leaf value. xchain-sync has no mathjs
 *     dependency.
 *   - stakes_root IS recomputed here (BTC-only) from the ported _stakeWeightsSql +
 *     the frozen BTC capability config (consensus-constants.js), mirroring the
 *     indexer's gatherStakeEntries. Non-BTC chains commit the EMPTY_SMT_ROOT.
 *     ClientSync now verifies balances_root + block_merkle_root + state_root (the
 *     last folds stakes_root). The stake-weight query is a consensus surface, so
 *     db._stakeWeightsSql is locked byte-identical to the indexer by the cross-repo
 *     drift guard in test/unit/rollback-coverage.test.js.
 *
 * DbNodeStore, MemoryNodeStore, PersistentSMT.update / buildFull / prove, the leaf
 * encoders, buildFullBalancesRoot, and computeBlockMerkleRoot are byte-identical to
 * the indexer; merkle.js itself is a verbatim copy. The golden vectors + the
 * persistent-vs-reference fuzz test lock the equality, and the twin cases in
 * test/unit/blockhash-conformance-twin.test.js pin the bytes.
 *
 * ONE DECLARED DIVERGENCE, and it is the only one: the indexer's PersistentSMT
 * carries a bounded read-through node cache (SMT_NODE_CACHE_MAX, _nodeCache,
 * _cacheGet/_cachePut, wired into _descend's read and _putBatch's write) that this
 * copy does not. It is a transport fix on the indexer's own hot path, and it is
 * root-neutral by construction: positive entries only (a store MISS is never
 * cached, so the M-17 fail-loud absence signal still reaches the store every time),
 * content-addressed keys (node_hash = H(left||right), so an entry cannot go stale),
 * and instance-scoped so it dies with the call. The follower's consensus recompute
 * therefore reads the same rows and returns the same roots, one store round trip at
 * a time. Porting it here is an open decision, NOT an oversight: it buys the same
 * round-trip saving at the cost of a few hundred MB of transient heap on the
 * follower apply path, which nobody has measured on follower hardware. Do not
 * silently widen this divergence and do not silently close it: the shape is pinned
 * by 'PersistentSMT divergence is exactly the indexer node cache' in
 * test/unit/blockhash-conformance-twin.test.js, which fails on either move and
 * points back at this paragraph.
 *
 ********************************************************************/

'use strict';

const M = require('./merkle.js');
const CC = require('./consensus-constants.js');
const SUB = require('./state_subtree_activation.js');
const CST = require('./contractStateSubtree.js');
const ESC = require('./escrowLeafSubtree.js');
const { minimalDecimal } = require('./balance-helpers.js');

const EMPTY_ROOT_HEX = M.toHex(M.EMPTY_SMT_ROOT);   // root of an empty depth-256 SMT
const EMPTY0_HEX     = M.toHex(M.EMPTY[0]);

// ---- Node stores ------------------------------------------------------------
// Interface: async get(nodeHashHex) -> { left_hash, right_hash } | null ;
//            async put(nodeHashHex, leftHex, rightHex) -> void  (idempotent / INSERT IGNORE)
//            async putMany(nodes) -> void  (OPTIONAL fast path: same rows, one
//                                            statement per chunk)
//
// putMany is optional because stores are DECORATED as well as implemented: the
// bench harness in bin/ wraps an inner store to instrument put, and the subtree
// unit tests hand in bare {get, put} fakes. Requiring it would break every one of
// them at a call site far from the edit. PersistentSMT._putBatch is the single
// place that chooses, and the fallback writes the identical rows in the identical
// order, so a store without it is slow, never wrong.
//
// putMany exists because ONE key update writes SMT_DEPTH internal nodes
// and a value leaf's ancestors are never an empty subtree, so the write loop is
// always a full 256 rows. Issued one statement at a time that is 256 sequential
// round trips per key, which on the BTC regtest venue (49 stake keys rebuilt from
// an empty root every block) is 12,544 round trips and 25-66s of wall clock per
// block against LTC's 3-5s, failing five standing envelope tests as timeouts.
// The rows written are identical either way; only the statement count changes, so
// this is a transport fix and not a consensus one.

// MariaDB-backed content-addressed store over `state_tree_nodes`.
//
// M-17: every read and write in this file uses doQueryStrict. doQuery collapses
// a NON-transactional query error into [], and inside a transaction the two are
// identical, so this changes nothing on the block path. It changes the paths
// that run WITHOUT a transaction, where a fail-soft [] is not an error signal
// but a meaningful and WRONG answer.
//
// Which callers those are is deliberately NOT the criterion, because the answer
// drifts. seedSnapshotRoots is the case in point: both production callers
// (ClientApplier.applyFullSnapshot / applyIncrementalSnapshot) happen to hold a
// transaction, so its reads rethrow today by inheritance rather than by design,
// while the integration harnesses already call it untransacted. The posture is
// "must not DEPEND on a caller-held transaction", which is why the db.js helpers
// this file delegates to are strict too (getBlockLeafRows, getStateRootsRow,
// _applyStakeWeightCap, and the getStatusId behind the stake readers). The
// failure shapes:
//
//   DbNodeStore.get -> [] is "this subtree is empty", so _descend keeps
//     building against a truncated tree and emits a root that looks perfectly
//     valid. This is the worst of the set: nothing downstream can detect it.
//   DbNodeStore.put -> a swallowed write means the node is missing on a LATER
//     block, which then reads as an empty subtree by the same route.
//   buildFullBalancesRoot -> [] commits EMPTY_ROOT over a populated ledger.
//   the prior-root read -> [] degrades to a full rebuild: correct, expensive,
//     and strict anyway so no read here is left soft for a later edit to move.
//
// getNetBalance was already loud by accident (it indexes rows[0]); it is strict
// now by intent rather than by luck.
const DB_NODE_PUT_CHUNK = 128;

class DbNodeStore {
    constructor(db){ this.db = db; }
    async get(nodeHashHex){
        const rows = await this.db.doQueryStrict(
            'SELECT left_hash, right_hash FROM state_tree_nodes WHERE node_hash=? LIMIT 1', [nodeHashHex]);
        return rows.length ? rows[0] : null;
    }
    async put(nodeHashHex, leftHex, rightHex){
        await this.db.doQueryStrict(
            'INSERT IGNORE INTO state_tree_nodes (node_hash, left_hash, right_hash) VALUES (?, ?, ?)',
            [nodeHashHex, leftHex, rightHex]);
    }
    // Chunked so one statement stays far inside max_allowed_packet: 128 rows is
    // 384 bound 64-char hex params, ~25KB on the wire against a 16MB default.
    // Duplicate hashes WITHIN a chunk are safe by the same INSERT IGNORE rule
    // that makes the single-row form idempotent.
    async putMany(nodes){
        for(let i = 0; i < nodes.length; i += DB_NODE_PUT_CHUNK){
            const chunk  = nodes.slice(i, i + DB_NODE_PUT_CHUNK);
            const values = new Array(chunk.length).fill('(?, ?, ?)').join(', ');
            const args   = [];
            for(const n of chunk) args.push(n.hash, n.left, n.right);
            await this.db.doQueryStrict(
                'INSERT IGNORE INTO state_tree_nodes (node_hash, left_hash, right_hash) VALUES ' + values,
                args);
        }
    }
}

// In-memory store: used by the unit fuzz test and any caller that wants a
// throwaway tree. Same interface as DbNodeStore.
class MemoryNodeStore {
    constructor(){ this.map = new Map(); }
    async get(nodeHashHex){ return this.map.has(nodeHashHex) ? this.map.get(nodeHashHex) : null; }
    async put(nodeHashHex, leftHex, rightHex){
        if(!this.map.has(nodeHashHex)) this.map.set(nodeHashHex, { left_hash: leftHex, right_hash: rightHex });
    }
    async putMany(nodes){
        for(const n of nodes) await this.put(n.hash, n.left, n.right);
    }
    get size(){ return this.map.size; }
}

// ---- Persistent SMT engine --------------------------------------------------
class PersistentSMT {
    constructor(store){ this.store = store; }

    // Descend a key's path collecting the 256 siblings (hex, top-down). Returns
    // { siblings, oldLeaf } where oldLeaf is the current value leaf at the key
    // (EMPTY[0] hex if absent). Empty subtrees short-circuit: once a node has no
    // row it is an EMPTY constant and every remaining sibling is the EMPTY for
    // that level.
    async _descend(rootHex, keyBuf){
        const siblings = new Array(M.SMT_DEPTH);
        let cur = rootHex;
        let empty = false;
        for(let d = 0; d < M.SMT_DEPTH; d++){
            const sibEmptyHex = M.toHex(M.EMPTY[M.SMT_DEPTH - 1 - d]);
            if(empty){ siblings[d] = sibEmptyHex; continue; }
            const row = await this.store.get(cur);
            if(!row){ empty = true; siblings[d] = sibEmptyHex; continue; }
            const bit = M.bitAt(keyBuf, d);
            siblings[d] = (bit === 0) ? row.right_hash : row.left_hash;
            cur         = (bit === 0) ? row.left_hash  : row.right_hash;
        }
        return { siblings, oldLeaf: empty ? EMPTY0_HEX : cur };
    }

    // Persist a path's nodes, preferring the store's batch write. The fallback
    // is the pre-batching behaviour and exists only for stores that predate
    // putMany (bare {get, put} fakes and the bin/ instrumentation decorator);
    // it writes the same rows in the same order at one round trip each.
    async _putBatch(nodes){
        if(typeof this.store.putMany === 'function'){
            await this.store.putMany(nodes);
            return;
        }
        for(const n of nodes) await this.store.put(n.hash, n.left, n.right);
    }

    // Set (leafHex) or delete (null) a key, persisting new internal nodes. Returns
    // the new root hex. Apply keys sequentially: each call threads the updated root
    // so shared-prefix keys see prior inserts.
    async update(rootHex, keyBuf, newLeafHexOrNull){
        const { siblings } = await this._descend(rootHex, keyBuf);
        let cur = (newLeafHexOrNull == null) ? EMPTY0_HEX : newLeafHexOrNull;
        // Collect the path's nodes and write them in ONE batch after the climb
        // rather than a round trip per level. Deferring is safe because
        // the climb reads NOTHING: every parent is hashed from `cur` and the
        // sibling already captured by _descend, so no node written here is read
        // back before the flush. The flush is inside update() and not hoisted to
        // buildFull for exactly that reason in reverse: the NEXT update() descends
        // the root this one returns, so its nodes must be durable by then.
        const pending = [];
        for(let d = M.SMT_DEPTH - 1; d >= 0; d--){
            const bit  = M.bitAt(keyBuf, d);
            const sib  = siblings[d];
            const left  = (bit === 0) ? cur : sib;
            const right = (bit === 0) ? sib : cur;
            const parent = M.toHex(M.nodeHash(left, right));
            // Skip storing an all-empty subtree: its hash is an EMPTY constant with no row.
            if(parent !== M.toHex(M.EMPTY[M.SMT_DEPTH - d]))
                pending.push({ hash: parent, left, right });
            cur = parent;
        }
        if(pending.length) await this._putBatch(pending);
        return cur;
    }

    // Build a fresh tree from a full leaf set (key hex -> leaf hex). Used for the
    // flag-day initialization and the (small) BTC stakes subtree.
    async buildFull(entries){
        let root = EMPTY_ROOT_HEX;
        for(const [keyHex, leafHex] of entries)
            root = await this.update(root, M.toBuf(keyHex), leafHex);
        return root;
    }

    // Membership / non-membership proof as-of a given root (same shape as
    // merkle.js SparseMerkleTree.prove; verify with M.verifyCompressedSmtProof).
    async prove(rootHex, keyBuf){
        const { siblings, oldLeaf } = await this._descend(rootHex, keyBuf);
        const present = (oldLeaf !== EMPTY0_HEX);
        return {
            key:        M.toHex(keyBuf),
            leaf_value: present ? oldLeaf : null,
            siblings,
            compressed: M.compressSmtProof(siblings)
        };
    }
}

// Every EMPTY[h] constant, hex. A child hash equal to one of these has no row in
// state_tree_nodes (empty subtrees are never stored), so reachability marking skips it.
const EMPTY_CONSTANTS = (function(){
    const s = new Set();
    for(let h = 0; h <= M.SMT_DEPTH; h++) if(M.EMPTY[h]) s.add(M.toHex(M.EMPTY[h]));
    return s;
})();

// ---- Orphan-node observability (read-only; SPV spec §4.3) -------------------
// TWIN PAIR: xchain-indexer/src/stateCommitment.js and xchain-sync/src/
// stateCommitment.js each carry this comment + function; keep the whole block
// BYTE-IDENTICAL, comments included (drift-guarded in both repos by
// test/unit/blockhash-conformance-twin.test.js).
//
// Reports total vs reachable internal nodes in the content-addressed COW
// state_tree_nodes store so unbounded growth (reorg orphans + per-block stake-
// subtree buildFull churn) is measurable. Reachability marks from the UNION of
// EVERY retained state_tree_roots row's balances_root + stakes_root +
// contract_state_root: the explorer SPV proof server descends historical roots,
// so a node is live if ANY retained root reaches it. The extension column is
// NULL on every inert row and IS NOT NULL filters those out, so the union is
// unchanged until a slot arms; leaving it out instead would under-report
// reachability the moment one does, which is a reporting bug now and a
// correctness trap for any future sweep that trusts these numbers.
//
// Marks by a BATCHED FRONTIER WALK and never materializes the node table. Heap
// holds one hash per seen node plus the current batch, so it tracks the
// REACHABLE set rather than the whole store, and only reachable rows are read at
// all. Each frontier level resolves in one indexed `WHERE node_hash IN (...)`
// against uq_node_hash, capping round trips at ceil(maxNodes / batchSize); every
// one of those takes and releases its own pooled connection, so nothing is held
// across the walk. Mark semantics are unchanged from the in-memory DFS this
// replaced: a hash counts as reachable only when the store actually returned a
// row for it, EMPTY constants are skipped, and each hash is expanded once.
//
// Past maxNodes seen hashes the walk stops instead of growing without bound and
// sets reachabilityEstimated, which makes reachableNodes a LOWER bound and
// orphanCount an UPPER bound. totalNodes is the COUNT(*), a snapshot separate
// from the walk, so a concurrent insert can move the two apart by a few rows.
// This function is observability only and never feeds a consensus hash, so both
// that skew and a truncated estimate are acceptable here.
//
// Deliberately does NOT delete. A safe reclaiming sweep must serialize against
// block-root insertion: a content-addressed node orphaned by a reorg is commonly
// re-created by the new canonical chain (INSERT IGNORE is a no-op, the row keeps
// its old id), and deleting it after it is re-referenced would make the next
// incremental _descend read a missing row as an EMPTY subtree and fork the
// balances_root. Reclamation is deferred to a dedicated design paired with
// root-retention pruning (which is what would actually free the bulk that
// retained historical roots otherwise pin).
//
// `query(sql, args)` MUST run on a POOLED (non-transaction) connection so this
// never shares the caller's block-processing/apply transaction. Returns
// { totalNodes, reachableNodes, orphanCount, reachabilitySkipped }, plus
// reachabilityEstimated: true when the walk stopped at the cap.
async function reportOrphanStats(query, chain, network, opts){
    opts = opts || {};
    const maxNodes  = opts.maxNodes  || parseInt(process.env.STATE_TREE_METRIC_MAX_NODES, 10) || 2000000;
    // One placeholder per hash, so the batch must stay well inside max_allowed_packet
    // and the server's prepared-statement placeholder ceiling; 1000 CHAR(64) hashes is
    // ~66KB of SQL text and one uq_node_hash range scan.
    const batchSize = opts.batchSize || 1000;
    const cnt = await query('SELECT COUNT(*) AS c FROM state_tree_nodes', []);
    const totalNodes = cnt.length ? Number(cnt[0].c) : 0;
    if(totalNodes === 0) return { totalNodes: 0, reachableNodes: 0, orphanCount: 0, reachabilitySkipped: false };

    const rootRows = await query(
        'SELECT DISTINCT balances_root AS r FROM state_tree_roots WHERE chain=? AND network=? ' +
        'UNION SELECT DISTINCT stakes_root AS r FROM state_tree_roots WHERE chain=? AND network=? ' +
        'UNION SELECT DISTINCT contract_state_root AS r FROM state_tree_roots WHERE chain=? AND network=? AND contract_state_root IS NOT NULL',
        [chain, network, chain, network, chain, network]);

    // `seen` holds every hash queued or resolved and doubles as the dedupe guard, so
    // no hash is queried or expanded twice; reachableNodes counts only hashes the
    // store returned a row for, which is what the old in-memory `nodes.has(...)`
    // guards enforced. A queued hash with no row is simply never counted, which is
    // the normal case for the value leaf under a depth-255 node (leaves are not rows,
    // SPV spec §4.1): seen therefore runs to reachable + reachable leaves, still O(1)
    // per node and still what maxNodes is bounding, since seen IS the heap.
    const seen = new Set();
    let frontier = [];
    for(const rr of rootRows){
        const root = rr.r;
        if(root && !EMPTY_CONSTANTS.has(root) && !seen.has(root)){ seen.add(root); frontier.push(root); }
    }
    let reachableNodes = 0;
    let truncated = false;
    while(frontier.length){
        if(seen.size > maxNodes){ truncated = true; break; }
        const batch = frontier.splice(0, batchSize);
        const rows = await query(
            'SELECT node_hash, left_hash, right_hash FROM state_tree_nodes WHERE node_hash IN (' +
            batch.map(() => '?').join(',') + ')', batch);
        for(const row of rows){
            reachableNodes++;
            for(const child of [row.left_hash, row.right_hash]){
                if(child && !EMPTY_CONSTANTS.has(child) && !seen.has(child)){ seen.add(child); frontier.push(child); }
            }
        }
    }
    const stats = { totalNodes, reachableNodes, orphanCount: totalNodes - reachableNodes, reachabilitySkipped: false };
    if(truncated) stats.reachabilityEstimated = true;
    return stats;
}

// Assemble the top-level state_root from the two v1 sub-roots plus any RESERVED
// slot that its flag-day has armed (SPV spec §4.1, sub-tree extension design).
//
// `extraSubRoots` is the forward-compatible carrier for ownership_root /
// tokens_root / contract_state_root and MUST be the output of
// state_subtree_activation.gateSubRoots(), never a caller's raw candidates: the
// gate is what keeps an un-armed slot EMPTY. It is null today (every slot inert),
// and merkle.stateRoot() maps a null/absent/empty-root slot to the identical
// EMPTY_SMT_ROOT leaf, so this is byte-identical to the old two-argument
// assembly on every chain. The equality is asserted, not assumed, in
// test/unit/stateSubtreeActivation.test.js.
function assembleStateRoot(balancesRootHex, stakesRootHex, extraSubRoots){
    const subRoots = { balances_root: balancesRootHex, stakes_root: stakesRootHex };
    if(extraSubRoots){
        for(const name of SUB.RESERVED_SUBTREES)
            if(extraSubRoots[name]) subRoots[name] = extraSubRoots[name];
    }
    return M.toHex(M.stateRoot(subRoots));
}

// Persisted column value for one reserved slot, taken from the GATED sub-root
// object. Returns null (SQL NULL = EMPTY) when the slot is inert at this height
// or the gate dropped it, so a row's extension column always describes the same
// leaf set as the row's own state_root.
function extraSubRootColumn(extraSubRoots, slotName){
    return (extraSubRoots && extraSubRoots[slotName]) ? extraSubRoots[slotName] : null;
}

// Candidate reserved sub-roots for one block, before gating. Stage A's
// contract_state_root is derived here (contractStateSubtree.js, byte-identical
// across the twins); Stage B's escrow leaf is not a slot and does not appear.
// This is the single seam all three block paths share, which is why arming a
// slot stays a height change plus this function rather than a re-plumb of the
// commit path.
//
// The isSubtreeActive check below is a COST guard, NOT the consensus gate.
// gateSubRoots remains the only thing that decides what enters state_root, and
// it re-checks. This early return exists so that while every map is empty the
// fleet issues ZERO additional queries per block, which is what makes "nothing
// changes until a height is armed" provable by inspection instead of argued
// from the gate's behaviour. Do not delete gateSubRoots on the strength of it.
//
// Deriving BELOW an armed height (spec §7's shadow-compute window, where the
// candidate is computed and stored but not committed) is a deliberate future
// change to this one condition, and it is safe precisely because the column
// read is gated separately.
async function reservedSubRootCandidates(db, chain, network, blockIndex){
    if(!SUB.isSubtreeActive('contract_state_root', blockIndex, network, chain)) return null;
    const smt = new PersistentSMT(new DbNodeStore(db));
    // `shadow` is passed EXPLICITLY false, byte-for-byte with the indexer twin's
    // call site. It reads as a falsy ternary either way today, so this is not a
    // live divergence; it is closed because a future three-state `shadow` that
    // told undefined from false would fork the follower's contract_state_root
    // candidate here with nothing in either twin's tests to catch it.
    return { contract_state_root: await CST.resolveContractStateRoot(db, smt, chain, network, blockIndex, false) };
}

// Shadow-compute window (spec §7 step 1): the WOULD-BE sub-roots at a height where
// the slot is NOT committed. Returned separately from the candidates above and
// never handed to gateSubRoots, so there is no path by which a shadow value can
// reach state_root; it is persisted to its own column for cross-twin comparison.
// Null while nothing is shadowing, which is the fleet's state today, and the same
// zero-query rule applies: an inert chain does not read contract_state at all.
async function shadowSubRoots(db, chain, network, blockIndex){
    if(!SUB.isSubtreeShadowActive('contract_state_root', blockIndex, network, chain)) return null;
    const smt = new PersistentSMT(new DbNodeStore(db));
    return { contract_state_root: await CST.resolveContractStateRoot(db, smt, chain, network, blockIndex, true) };
}

// ---- Leaf value derivation (authoritative, never the balances cache) --------
// Per SPV spec sec.4.2 the leaf is the authoritative SUM(credits)-SUM(debits) at
// 18 dp, NOT the mutable balances cache. Rendered to the source indexer's
// minimal-decimal string form in SQL (balance-helpers.minimalDecimal) so the
// value canonicalAmount sees is byte-identical to the indexer's bcsub output.
const ZERO_CANON = M.canonicalAmount('0');

function _nz(amountStr){ return M.canonicalAmount(String(amountStr)); }

// Returns the value-leaf hex for an amount, or null when it is exactly zero
// (delete-on-zero, normative sec.4.2).
function _leafOrNull(amountStr){
    const canon = _nz(amountStr);
    return (canon === ZERO_CANON) ? null : M.toHex(M.leafHash(canon));
}

async function getNetBalance(db, address, tick){
    const rows = await db.doQueryStrict(
        `SELECT ${minimalDecimal(
            '( (SELECT COALESCE(SUM(CAST(c.amount AS DECIMAL(60,18))),0) FROM credits c'
          + '       INNER JOIN index_addresses a ON a.id=c.address_id'
          + '       INNER JOIN index_tickers   t ON t.id=c.tick_id'
          + '       WHERE a.address=? AND t.tick=?)'
          + ' - (SELECT COALESCE(SUM(CAST(d.amount AS DECIMAL(60,18))),0) FROM debits d'
          + '       INNER JOIN index_addresses a ON a.id=d.address_id'
          + '       INNER JOIN index_tickers   t ON t.id=d.tick_id'
          + '       WHERE a.address=? AND t.tick=?) )')} AS net`,
        [address, tick, address, tick]);
    return rows.length ? String(rows[0].net) : '0';
}

// Locked-escrow leaf (XCHAIN_ESC): BUILT and gated, no longer deferred (SPV
// sub-tree spec §3 Stage B), matching the indexer twin. The 2026-06-18
// finding that killed the naive derivation still stands and is why the journal
// exists: nine escrow release sites key to the recipient, so SUM(escrows) per
// (address, tick) does not net per key. The SOURCE's writer re-keys those rows
// to their locker; this follower REPLICATES the journal rows and derives the
// leaves from them (escrowLeafSubtree.js), applied inside balances_root when
// ESCROW_LOCKED_LEAF_ACTIVATION arms a height (and into the shadow column
// while ESCROW_LOCKED_LEAF_SHADOW does). Until then balances_root commits
// ONLY the net-spendable leaf, byte-identical to v1.

// ---- Block-content Merkle root (sec.5) --------------------------------------
// Leaves over the EXACT canonical rows + order the flat consensus hashes cover
// (db.getBlockLeafRows mirrors BlockHasher's SELECTs), in the frozen cross-kind
// total order: all ledger (credits, debits, escrows), then actions, then the six
// contract sub-tables. Null fields coerce to '' (matching actionsLeaf's tx_index).
//
// The ordering itself lives in merkle.blockMerkleLeaves (the twin-guarded module,
// pinned byte-identical across the repos) so the explorer proof server locates a
// row's leaf index with byte-identical logic; this just hashes the assembled
// vector, exactly as the indexer twin does. It was hand-copied inline here, which
// left the one consensus-critical ordering the twin guard does NOT cover: an edit
// to blockMerkleLeaves reached the indexer through its call and this copy only if
// someone remembered, with merkle.js's own twin-identity check still green.

// `network`/`coin` drive the state_key collation flag-day
// (state_key_collation_activation.js) inside getBlockLeafRows, mirroring
// BlockHasher; omitted -> legacy folding collation (pre-activation behavior).
async function computeBlockMerkleRoot(db, blockIndex, network, coin){
    const rows = await db.getBlockLeafRows(blockIndex, undefined, network, coin);
    return M.toHex(M.blockMerkleRoot(M.blockMerkleLeaves(rows)));
}

// ---- Full balances-tree initialization (flag-day cutover + snapshot seed) ----
// One-time at the activation boundary block (or snapshot bootstrap): seed the
// balances SMT from ALL pre-existing nonzero net balances (escrow leaf deferred
// from v1, see note above). Mirrors the indexer's buildFullBalancesRoot (CAST AS
// CHAR, normalised by canonicalAmount). Persists nodes.
async function buildFullBalancesRoot(db, chain, network, blockIndex, opts){
    const smt = new PersistentSMT(new DbNodeStore(db));
    let root = EMPTY_ROOT_HEX;
    const bals = await db.doQueryStrict(
        `SELECT a.address AS address, t.tick AS tick, CAST(SUM(s.amt) AS CHAR) AS net FROM (
            SELECT address_id, tick_id,  CAST(amount AS DECIMAL(60,18)) AS amt FROM credits
            UNION ALL
            SELECT address_id, tick_id, -CAST(amount AS DECIMAL(60,18)) AS amt FROM debits
         ) s
         INNER JOIN index_addresses a ON a.id=s.address_id
         INNER JOIN index_tickers   t ON t.id=s.tick_id
         GROUP BY s.address_id, s.tick_id
         HAVING SUM(s.amt) <> 0`, []);
    for(const r of bals){
        if(r.address == null || r.tick == null) continue;
        const leaf = _leafOrNull(r.net);
        if(leaf == null) continue;
        root = await smt.update(root, M.balanceKey(chain, network, r.address, r.tick), leaf);
    }
    // XCHAIN_ESC locked-balance leaves (Stage B), height-gated. Work item 2: this
    // function had NO height and three callers (activation-boundary init, the
    // indexer self-heal full recompute, seedSnapshotRoots), so after the escrow
    // leaf arms it could not decide whether locked leaves belong in the tree, and
    // the self-heal path would have silently rebuilt a locked-leaf-FREE
    // balances_root on the SOURCE. That is a quiet fork, the worst kind, so the
    // height is now a parameter and a caller that omits it gets the v1 leaf set.
    // opts.forceEscrowLeaves is the §7 shadow window's build of the SAME set at
    // heights where the leaf is not yet committed; only the shadow path passes it.
    if(SUB.isEscrowLockedLeafActive(blockIndex, network, chain) || (opts && opts.forceEscrowLeaves)){
        for(const e of await ESC.liveEscrowLeaves(db))
            root = await smt.update(root, M.escrowKey(chain, network, e.address, e.tick), e.leaf);
    }
    return root;
}

// ---- Stakes sub-tree (BTC-only, sec.4.1) ------------------------------------
// Mirror of the indexer's gatherStakeEntries: one leaf per (pubkey, capability)
// over the frozen BTC capability set, source-deduped weight via db
// getStakeWeightsByCapability (the ported _stakeWeightsSql), keyed by canonical
// pubkey+capability strings. Non-BTC chains commit the empty-SMT root (capability
// staking is BTC-only). The capability set + per-capability MIN_STAKE floors and
// the VALIDATOR_QUERY_LIMIT cap come from consensus-constants.js, mirrored from
// the indexer BTC config; any drift forks the stakes_root.
async function gatherStakeEntries(db, chain, network, blockIndex){
    if(chain !== 'BTC') return [];
    const caps  = CC.btcStakeCapabilities();
    const limit = CC.VALIDATOR_QUERY_LIMIT;
    const entries = [];
    for(const capability of Object.keys(caps)){
        // chain/network gate the SWQ source-cap (byte-mirrored to the indexer) so the
        // follower's stakes_root set is identical to the source at/after the activation height.
        const rows = await db.getStakeWeightsByCapability(capability, blockIndex, caps[capability], limit, chain, network);
        const seenSource = new Map();   // source -> weight (first wins; equal per source)
        for(const r of (rows || [])){
            if(!r || r.pubkey == null) continue;
            if(_nz(String(r.weight == null ? '0' : r.weight)) === ZERO_CANON) continue;   // zero cannot qualify
            const source = String(r.source);
            // Member leaf commits SOURCE + weight (validator-set proof, byte-identical
            // to the indexer's gatherStakeEntries so the stakes_root does not fork).
            entries.push([ M.toHex(M.stakeKey(String(r.pubkey), capability)),
                           M.toHex(M.stakeMemberLeaf(source, String(r.weight))) ]);
            if(!seenSource.has(source)) seenSource.set(source, String(r.weight));
        }
        // Total leaf: the source-deduped quorum denominator S (spec §7).
        const total = M.sumCanonicalAmounts(Array.from(seenSource.values()));
        if(_nz(total) !== ZERO_CANON)
            entries.push([ M.toHex(M.stakeKey(M.STAKE_TOTAL_PUBKEY, capability)),
                           M.toHex(M.stakeTotalLeaf(total)) ]);
    }
    return entries;
}

// Rebuild the stakes tree only when the stake set actually changed.
//
// buildFull writes SMT_DEPTH nodes per key, so this tree costs keys x 256 node
// writes on EVERY BTC block - 12,544 on the regtest venue's 49 keys - and the
// stake set changes on almost none of them. Every one of those writes is an
// INSERT IGNORE no-op, but each still probes a primary key far larger than the
// buffer pool, which is what put BTC regtest block parse at 25-66s against LTC's
// 3-5s for the same code (LTC commits the empty stakes root and never pays it).
//
// The memo is sound because buildFull is a PURE function of its entries: the node
// store is content-addressed, so the same entry set always yields the same root.
// stateCommitment.test.js pins both halves of that - equality with the merkle.js
// reference, and insert-order independence.
//
// Every way this can be wrong is a way it rebuilds. It shortcuts ONLY when the
// entries are identical to those of the block IMMEDIATELY BEFORE it, which this
// same process built and committed, and whose nodes are therefore already durable
// (state_tree_nodes is COW and rollback-exempt). Keyed on block CONTINUITY and not
// on the digest alone, so a reorg, a rollback, or a cold start lands on a block
// that is not the memo's successor and rebuilds. That direction matters: a cache
// in a consensus path may only ever fail toward the slow correct answer, and the
// one that failed the other way here did so because it was keyed on a
// MUTABLE dense id rather than on its own inputs.
let _stakesMemo = null;

function _stakeEntriesDigest(entries){
    // Sorted, because buildFull is order-independent: an entry set that merely
    // reordered must still hit. Length-prefixed and separator-joined so no pair
    // boundary can be forged by a value that happens to contain the separator.
    const pairs = entries.map(e => String(e[0]) + ':' + String(e[1])).sort();
    return M.toHex(M.sha256(Buffer.from(pairs.length + '|' + pairs.join('|'), 'utf8')));
}

// Exported for tests and for any caller that wipes the node store underneath a
// live process; forgetting the memo only ever costs one rebuild.
function resetStakesMemo(){ _stakesMemo = null; }

async function buildStakesRoot(smt, chain, network, blockIndex, entries){
    const digest = _stakeEntriesDigest(entries);
    const memo   = _stakesMemo;
    if(memo && memo.chain === chain && memo.network === network
            && memo.blockIndex === blockIndex - 1 && memo.digest === digest){
        // One indexed read weighed against 12,544 writes: proves the memoized tree
        // is still IN the store before trusting it. It does not prove every interior
        // node survived - only a prune could remove one, and reachability marking
        // keeps whatever a retained root reaches - so this is a cheap floor, stated
        // as such rather than sold as verification.
        if(memo.root === EMPTY_ROOT_HEX || await smt.store.get(memo.root)){
            _stakesMemo = { chain, network, blockIndex, digest, root: memo.root };
            return memo.root;
        }
    }
    const root = await smt.buildFull(entries);
    _stakesMemo = { chain, network, blockIndex, digest, root };
    return root;
}


// ---- Follower orchestrator --------------------------------------------------
// Compute + persist the per-block roots over the REPLICA, INSIDE the apply txn
// (ClientApplier runs this after the row inserts + balance rebuild, before
// commit). chain = COIN, network = NETWORK. touchedKeys is an array of
// { address, tick } strings the applied block event touched (credits/debits/
// escrows, cooldown refunds already merged by the source). On the activation
// boundary block, and on the escrow-leaf ARMING block, the touched set is ignored
// and the balances tree is fully initialized from pre-existing state.
//
// stakes_root is rebuilt fresh each block from the BTC capability stake set
// (gatherStakeEntries; EMPTY on non-BTC), so the returned state_root is fully
// verifiable; ClientSync compares balances_root + block_merkle_root + state_root.
async function computeFollowerRoots(db, chain, network, blockIndex, touchedKeys, isActivationBlock){
    const smt = new PersistentSMT(new DbNodeStore(db));

    // When the escrow leaf is SHADOWING, the incremental branch also collects
    // this block's spendable-leaf updates so the shadow thread can replay the
    // identical spendable set on its own root (null on the full branch, which
    // makes the shadow full-build too). The follower shadowing from REPLICATED
    // journal rows is the point of the window: it exercises writer, replication
    // and application end to end, and the two twins' shadow columns are the §7
    // cross-twin comparison.
    const escShadow = SUB.isEscrowLockedLeafShadowActive(blockIndex, network, chain);
    // The ARMING BLOCK full-builds too, byte-for-byte with the source twin's gate in
    // xchain-indexer/src/stateCommitment.js. The incremental branch below
    // applies escrow leaves from journal rows stamped at THIS height only, while the
    // arming replay deliberately writes no row for a key whose locked total is
    // unchanged. After a §7 shadow window every still-unchanged live lock therefore has
    // no arming-height row, and it is not in the prior committed root either, so it
    // never enters the newly committed balances_root - while a snapshot-bootstrapped
    // node rebuilds the same block from the full live escrow set and includes it. Both
    // twins must take the full branch on this one block or the divergence simply moves
    // from source-vs-snapshot to source-vs-follower.
    const armingBlock = SUB.isEscrowLockedLeafActive(blockIndex, network, chain) &&
                        !SUB.isEscrowLockedLeafActive(blockIndex - 1, network, chain);
    let shadowBalanceUpdates = null;
    let balancesRoot;
    // Read the prior row BEFORE the gate, because a MISSING one is a third
    // full-build trigger, matching the source twin's `!prior.length`. A follower
    // reaches this with no block-1 row after a snapshot bootstrap, a rollback that
    // took the row below this height, or a resume over a data gap. Threading from
    // EMPTY_ROOT_HEX there commits a balances_root covering only THIS block's
    // touched keys, which is the fork the indexer twin's own comment says not to
    // substitute; buildFullBalancesRoot derives the root from the whole current
    // net-balance set, so at this point in the apply it yields the root the
    // incremental thread would have produced. Self-healing instead of a false halt.
    // balances_root is NOT NULL in state_tree_roots, so the row test and the source's
    // row-count test select the same blocks.
    const prior = isActivationBlock ? null : await db.getStateRootsRow(chain, network, blockIndex - 1);
    const noPriorRoot = !(prior && prior.balances_root);
    if(isActivationBlock || armingBlock || noPriorRoot){
        if(!isActivationBlock && !armingBlock)
            console.warn('stateCommitment: no prior state_tree_roots row for ' + chain + '/' + network +
                ' block ' + (blockIndex - 1) + '; full-recomputing balances_root for block ' + blockIndex +
                ' instead of threading from the empty root (snapshot-bootstrap or activation rolled below this height)');
        balancesRoot = await buildFullBalancesRoot(db, chain, network, blockIndex);
    } else {
        let root = prior.balances_root;
        if(escShadow) shadowBalanceUpdates = [];
        for(const entry of (touchedKeys || [])){
            const address = entry.address, tick = entry.tick;
            if(address == null || tick == null || tick === '') continue;
            const balLeaf = _leafOrNull(await getNetBalance(db, address, tick));
            const balKey  = M.balanceKey(chain, network, address, tick);
            root = await smt.update(root, balKey, balLeaf);
            if(shadowBalanceUpdates) shadowBalanceUpdates.push({ key: balKey, leaf: balLeaf });
        }
        // XCHAIN_ESC locked-balance leaves for this block (Stage B), height-gated.
        // Applied AFTER the spendable leaves and driven by its OWN touched set: an
        // order match writes the escrows release row against the recipient
        // GET_ADDRESS while the leaf that moves is the LOCKER's, so the balance
        // touched set is the wrong input and reusing it would update the wrong key
        // and miss the right one on every match. The journal answers per locker.
        if(SUB.isEscrowLockedLeafActive(blockIndex, network, chain)){
            root = await ESC.applyEscrowLeaves(db, smt, root, chain, network, blockIndex);
        }
        balancesRoot = root;
    }

    // stakes_root: BTC-only; non-BTC commit the empty-SMT root. Rebuilt fresh from
    // the authoritative capability stake set (small, bounded by VALIDATOR_QUERY_LIMIT),
    // matching the indexer's gatherStakeEntries.
    let stakesRoot = EMPTY_ROOT_HEX;
    if(chain === 'BTC'){
        const stakeEntries = await gatherStakeEntries(db, chain, network, blockIndex);
        stakesRoot = await buildStakesRoot(smt, chain, network, blockIndex, stakeEntries);
    }

    const extraSubRoots   = SUB.gateSubRoots(await reservedSubRootCandidates(db, chain, network, blockIndex), blockIndex, network, chain);
    const stateRoot       = assembleStateRoot(balancesRoot, stakesRoot, extraSubRoots);
    // Extension columns are read back OUT of the gated object, never off the
    // candidate: the column and the state_root it must reassemble to are then
    // written by one statement from one value, so no rewrite path (reorg,
    // self-heal, ON DUPLICATE KEY UPDATE) can leave the column stale against
    // its own root. NULL means EMPTY, which is why historical rows need no
    // backfill and why an inert chain keeps writing NULL forever.
    const contractStateRoot = extraSubRootColumn(extraSubRoots, 'contract_state_root');
    // Shadow-compute window (spec §7): derived at heights where the slot is NOT
    // committed, written to its OWN column, and never routed through gateSubRoots,
    // so there is no path by which it can reach state_root. Null on every chain
    // today, and an inert chain does not query contract_state at all.
    const contractStateShadow = extraSubRootColumn(
        await shadowSubRoots(db, chain, network, blockIndex), 'contract_state_root');
    // Stage B's shadow (spec §7, amended): the would-be balances_root with the
    // locked leaves applied, threading through its own column. Never routed
    // anywhere near assembleStateRoot, so no committed root can move; null on
    // every chain today (the shadow map is empty).
    const balancesEscrowShadow = !escShadow ? null :
        await ESC.resolveShadowBalancesRoot(db, smt, chain, network, blockIndex, shadowBalanceUpdates,
            () => buildFullBalancesRoot(db, chain, network, blockIndex, { forceEscrowLeaves: true }));
    const blockMerkleRoot = await computeBlockMerkleRoot(db, blockIndex, network, chain);

    await db.doQueryStrict(
        `INSERT INTO state_tree_roots
            (chain, network, block_index, balances_root, stakes_root, state_root, block_merkle_root, contract_state_root, contract_state_root_shadow, balances_root_escrow_shadow)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
            balances_root=VALUES(balances_root), stakes_root=VALUES(stakes_root),
            state_root=VALUES(state_root), block_merkle_root=VALUES(block_merkle_root),
            contract_state_root=VALUES(contract_state_root),
            contract_state_root_shadow=VALUES(contract_state_root_shadow),
            balances_root_escrow_shadow=VALUES(balances_root_escrow_shadow)`,
        [chain, network, blockIndex, balancesRoot, stakesRoot, stateRoot, blockMerkleRoot, contractStateRoot, contractStateShadow, balancesEscrowShadow]);

    return { balances_root: balancesRoot, stakes_root: stakesRoot, state_root: stateRoot,
             block_merkle_root: blockMerkleRoot, contract_state_root: contractStateRoot };
}

// Seed the SMT at a snapshot-bootstrap height H (no per-block touched set is
// available): full balances/escrow build + empty stakes placeholder. A snapshot
// copies the full tables, so block H's content rows are present and its
// block_merkle_root is computed for real (state_tree_roots.block_merkle_root is
// NOT NULL). Persists the state_tree_roots[H] row so live block H+1's incremental
// update has a prior balances_root to thread. The seed row is never compared to
// the source (snapshots are not live block events); it only carries balances_root
// forward.
async function seedSnapshotRoots(db, chain, network, blockHeight){
    const balancesRoot    = await buildFullBalancesRoot(db, chain, network, blockHeight);
    let   stakesRoot      = EMPTY_ROOT_HEX;
    if(chain === 'BTC'){
        const smt = new PersistentSMT(new DbNodeStore(db));
        stakesRoot = await smt.buildFull(await gatherStakeEntries(db, chain, network, blockHeight));
    }
    const extraSubRoots   = SUB.gateSubRoots(await reservedSubRootCandidates(db, chain, network, blockHeight), blockHeight, network, chain);
    const stateRoot       = assembleStateRoot(balancesRoot, stakesRoot, extraSubRoots);
    // Extension columns are read back OUT of the gated object, never off the
    // candidate: the column and the state_root it must reassemble to are then
    // written by one statement from one value, so no rewrite path (reorg,
    // self-heal, ON DUPLICATE KEY UPDATE) can leave the column stale against
    // its own root. NULL means EMPTY, which is why historical rows need no
    // backfill and why an inert chain keeps writing NULL forever.
    const contractStateRoot = extraSubRootColumn(extraSubRoots, 'contract_state_root');
    // Shadow-compute window (spec §7): derived at heights where the slot is NOT
    // committed, written to its OWN column, and never routed through gateSubRoots,
    // so there is no path by which it can reach state_root. Null on every chain
    // today, and an inert chain does not query contract_state at all.
    const contractStateShadow = extraSubRootColumn(
        await shadowSubRoots(db, chain, network, blockHeight), 'contract_state_root');
    const blockMerkleRoot = await computeBlockMerkleRoot(db, blockHeight, network, chain);
    await db.doQueryStrict(
        `INSERT INTO state_tree_roots
            (chain, network, block_index, balances_root, stakes_root, state_root, block_merkle_root, contract_state_root, contract_state_root_shadow)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
            balances_root=VALUES(balances_root), stakes_root=VALUES(stakes_root),
            state_root=VALUES(state_root), block_merkle_root=VALUES(block_merkle_root),
            contract_state_root=VALUES(contract_state_root),
            contract_state_root_shadow=VALUES(contract_state_root_shadow)`,
        [chain, network, blockHeight, balancesRoot, stakesRoot, stateRoot, blockMerkleRoot, contractStateRoot, contractStateShadow]);
    return { balances_root: balancesRoot, stakes_root: stakesRoot, state_root: stateRoot,
             block_merkle_root: blockMerkleRoot, contract_state_root: contractStateRoot };
}

module.exports = {
    EMPTY_ROOT_HEX,
    DbNodeStore,
    MemoryNodeStore,
    PersistentSMT,
    assembleStateRoot,
    extraSubRootColumn,
    reservedSubRootCandidates,
    shadowSubRoots,
    getNetBalance,
    gatherStakeEntries,
    buildStakesRoot,
    resetStakesMemo,
    computeBlockMerkleRoot,
    buildFullBalancesRoot,
    computeFollowerRoots,
    seedSnapshotRoots,
    reportOrphanStats
};
