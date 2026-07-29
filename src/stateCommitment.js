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
 * The PersistentSMT / DbNodeStore / MemoryNodeStore engine, the leaf encoders,
 * buildFullBalancesRoot, and computeBlockMerkleRoot are byte-identical to the
 * indexer; merkle.js itself is a verbatim copy. The golden vectors + the
 * persistent-vs-reference fuzz test lock the equality.
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

// MariaDB-backed content-addressed store over `state_tree_nodes`.
//
// M-17: every read and write in this file uses doQueryStrict. doQuery collapses
// a NON-transactional query error into [], and inside a transaction the two are
// identical, so this changes nothing on the block path. It changes the paths
// that run WITHOUT a transaction (seedSnapshotRoots on the follower, and the
// bin/ harnesses), where a fail-soft [] is not an error signal but a meaningful
// and WRONG answer:
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
}

// In-memory store: used by the unit fuzz test and any caller that wants a
// throwaway tree. Same interface as DbNodeStore.
class MemoryNodeStore {
    constructor(){ this.map = new Map(); }
    async get(nodeHashHex){ return this.map.has(nodeHashHex) ? this.map.get(nodeHashHex) : null; }
    async put(nodeHashHex, leftHex, rightHex){
        if(!this.map.has(nodeHashHex)) this.map.set(nodeHashHex, { left_hash: leftHex, right_hash: rightHex });
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

    // Set (leafHex) or delete (null) a key, persisting new internal nodes. Returns
    // the new root hex. Apply keys sequentially: each call threads the updated root
    // so shared-prefix keys see prior inserts.
    async update(rootHex, keyBuf, newLeafHexOrNull){
        const { siblings } = await this._descend(rootHex, keyBuf);
        let cur = (newLeafHexOrNull == null) ? EMPTY0_HEX : newLeafHexOrNull;
        for(let d = M.SMT_DEPTH - 1; d >= 0; d--){
            const bit  = M.bitAt(keyBuf, d);
            const sib  = siblings[d];
            const left  = (bit === 0) ? cur : sib;
            const right = (bit === 0) ? sib : cur;
            const parent = M.toHex(M.nodeHash(left, right));
            // Skip storing an all-empty subtree: its hash is an EMPTY constant with no row.
            if(parent !== M.toHex(M.EMPTY[M.SMT_DEPTH - d]))
                await this.store.put(parent, left, right);
            cur = parent;
        }
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
// { totalNodes, reachableNodes, orphanCount, reachabilitySkipped }.
async function reportOrphanStats(query, chain, network, opts){
    opts = opts || {};
    const maxNodes = opts.maxNodes || parseInt(process.env.STATE_TREE_METRIC_MAX_NODES, 10) || 2000000;
    const cnt = await query('SELECT COUNT(*) AS c FROM state_tree_nodes', []);
    const totalNodes = cnt.length ? Number(cnt[0].c) : 0;
    if(totalNodes === 0) return { totalNodes: 0, reachableNodes: 0, orphanCount: 0, reachabilitySkipped: false };
    // Above the in-memory mark ceiling, do not go silent. Load a bounded, deterministic
    // sample (ORDER BY node_hash LIMIT maxNodes) and mark reachability WITHIN that sample
    // from the same retained root set, so a rough orphan ratio and growth stay observable
    // without an unbounded in-memory mark. Reported as an estimate (reachabilityEstimated)
    // scoped to sampledNodes; this whole function is observability only and never feeds a
    // consensus hash, so a sampled figure is safe here.
    if(totalNodes > maxNodes){
        const sampleRows = await query('SELECT node_hash, left_hash, right_hash FROM state_tree_nodes ORDER BY node_hash LIMIT ?', [maxNodes]);
        const sampleNodes = new Map();
        for(const r of sampleRows) sampleNodes.set(r.node_hash, { l: r.left_hash, r: r.right_hash });
        const sampleRootRows = await query(
            'SELECT DISTINCT balances_root AS r FROM state_tree_roots WHERE chain=? AND network=? ' +
            'UNION SELECT DISTINCT stakes_root AS r FROM state_tree_roots WHERE chain=? AND network=? ' +
            'UNION SELECT DISTINCT contract_state_root AS r FROM state_tree_roots WHERE chain=? AND network=? AND contract_state_root IS NOT NULL',
            [chain, network, chain, network, chain, network]);
        const sampleVisited = new Set();
        const sampleStack = [];
        for(const rr of sampleRootRows){
            const root = rr.r;
            if(root && !EMPTY_CONSTANTS.has(root) && sampleNodes.has(root)) sampleStack.push(root);
        }
        while(sampleStack.length){
            const h = sampleStack.pop();
            if(sampleVisited.has(h)) continue;
            sampleVisited.add(h);
            const row = sampleNodes.get(h);
            if(!row) continue;
            for(const child of [row.l, row.r]){
                if(child && !EMPTY_CONSTANTS.has(child) && !sampleVisited.has(child) && sampleNodes.has(child)) sampleStack.push(child);
            }
        }
        const sampledNodes = sampleNodes.size;
        const sampledReachable = sampleVisited.size;
        return { totalNodes, reachableNodes: sampledReachable, orphanCount: sampledNodes - sampledReachable, reachabilitySkipped: false, reachabilityEstimated: true, sampledNodes };
    }

    const rows = await query('SELECT node_hash, left_hash, right_hash FROM state_tree_nodes', []);
    const nodes = new Map();
    for(const r of rows) nodes.set(r.node_hash, { l: r.left_hash, r: r.right_hash });

    const rootRows = await query(
        'SELECT DISTINCT balances_root AS r FROM state_tree_roots WHERE chain=? AND network=? ' +
        'UNION SELECT DISTINCT stakes_root AS r FROM state_tree_roots WHERE chain=? AND network=? ' +
        'UNION SELECT DISTINCT contract_state_root AS r FROM state_tree_roots WHERE chain=? AND network=? AND contract_state_root IS NOT NULL',
        [chain, network, chain, network, chain, network]);

    // Iterative DFS from every retained root; only push hashes that actually have a
    // row (EMPTY constants and absent children are skipped). visited == reachable set.
    const visited = new Set();
    const stack = [];
    for(const rr of rootRows){
        const root = rr.r;
        if(root && !EMPTY_CONSTANTS.has(root) && nodes.has(root)) stack.push(root);
    }
    while(stack.length){
        const h = stack.pop();
        if(visited.has(h)) continue;
        visited.add(h);
        const row = nodes.get(h);
        if(!row) continue;
        for(const child of [row.l, row.r]){
            if(child && !EMPTY_CONSTANTS.has(child) && !visited.has(child) && nodes.has(child)) stack.push(child);
        }
    }
    const reachableNodes = visited.size;
    return { totalNodes: nodes.size, reachableNodes, orphanCount: nodes.size - reachableNodes, reachabilitySkipped: false };
}

// Assemble the top-level state_root from the two v1 sub-roots plus any RESERVED
// slot that its flag-day has armed (SPV spec §4.1; design in
// claude/specs/spv-state-subtree-extension.md).
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
    return { contract_state_root: await CST.resolveContractStateRoot(db, smt, chain, network, blockIndex) };
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
// sub-tree spec §3 Stage B, ), matching the indexer twin. The 2026-06-18
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
function _c(x){ return (x == null) ? '' : x; }

// `network`/`coin` drive the state_key collation flag-day
// (state_key_collation_activation.js) inside getBlockLeafRows, mirroring
// BlockHasher; omitted -> legacy folding collation (pre-activation behavior).
async function computeBlockMerkleRoot(db, blockIndex, network, coin){
    const rows = await db.getBlockLeafRows(blockIndex, undefined, network, coin);
    const leaves = [];
    for(const kind of ['credit', 'debit', 'escrow']){
        const arr = rows.ledger[kind + 's'] || [];   // credits / debits / escrows
        for(const r of arr)
            leaves.push(M.ledgerLeaf({ kind, action_index: r.action_index,
                address: _c(r.address), tick: _c(r.tick), amount: r.amount }));
    }
    for(const r of (rows.actions || []))
        leaves.push(M.actionsLeaf({ action_index: r.action_index, tx_index: r.tx_index, action: _c(r.action) }));
    const C = rows.contracts;
    for(const r of (C.contracts || []))
        leaves.push(M.contractLeaf('contracts', [r.action_index, _c(r.source_address), _c(r.code_hash), _c(r.status)]));
    for(const r of (C.state || []))
        leaves.push(M.contractLeaf('state', [r.contract_index, _c(r.state_key), _c(r.state_value)]));
    for(const r of (C.executions || []))
        leaves.push(M.contractLeaf('executions', [r.action_index, r.contract_index, _c(r.caller_address), _c(r.gas_used), _c(r.status), _c(r.emitted_count)]));
    for(const r of (C.emissions || []))
        leaves.push(M.contractLeaf('emissions', [r.execution_index, _c(r.emitted_action), r.action_index, r.position]));
    for(const r of (C.deposits || []))
        leaves.push(M.contractLeaf('deposits', [r.action_index, r.contract_index, _c(r.source_address), _c(r.tick), r.amount, _c(r.status)]));
    for(const r of (C.withdrawals || []))
        leaves.push(M.contractLeaf('withdrawals', [r.action_index, r.contract_index, _c(r.source_address), _c(r.tick), r.amount, _c(r.status)]));
    return M.toHex(M.blockMerkleRoot(leaves));
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

// ---- Follower orchestrator --------------------------------------------------
// Compute + persist the per-block roots over the REPLICA, INSIDE the apply txn
// (ClientApplier runs this after the row inserts + balance rebuild, before
// commit). chain = COIN, network = NETWORK. touchedKeys is an array of
// { address, tick } strings the applied block event touched (credits/debits/
// escrows, cooldown refunds already merged by the source). On the activation
// boundary block the touched set is ignored and the balances tree is fully
// initialized from pre-existing state.
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
    let shadowBalanceUpdates = null;
    let balancesRoot;
    if(isActivationBlock){
        balancesRoot = await buildFullBalancesRoot(db, chain, network, blockIndex);
    } else {
        const prior = await db.getStateRootsRow(chain, network, blockIndex - 1);
        let root = (prior && prior.balances_root) ? prior.balances_root : EMPTY_ROOT_HEX;
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
        stakesRoot = await smt.buildFull(stakeEntries);
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
    computeBlockMerkleRoot,
    buildFullBalancesRoot,
    computeFollowerRoots,
    seedSnapshotRoots,
    reportOrphanStats
};
