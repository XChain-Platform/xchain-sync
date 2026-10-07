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
 * XChain Sync - Cooldown-maturity refund credits (source side, forward)
 *
 * When a capability/contract UNSTAKE cooldown matures, the indexer's
 * processCooldownCompletions writes a refund credit that REUSES the unstake's
 * own (earlier-block) action_index and carries NO block_index, and flips the
 * surviving unstake row's status_id to 'completed' in place at the maturity
 * block (= cooldown_end_block under sequential processing).
 *
 * Because the credit's only block placement is the unstake's original (much
 * earlier) block, none of the three forward channels reach it on their own:
 *   - the per-block stream's action-scoped join selects credits by the action's
 *     block, so it misses the credit at the maturity block (wrong block) and at
 *     the original block (the credit did not exist yet);
 *   - the updated_rows channel is keyed by a UNIQUE action_index and applied
 *     with ON DUPLICATE KEY UPDATE; credits has no unique key, so it cannot
 *     ride that channel;
 *   - the incremental snapshot scopes credits by action_index >= cursor, and
 *     the backdated credit sits below the cursor.
 * A follower (which does not run processCooldownCompletions) therefore stays
 * permanently short by every matured refund: steady-state, hash-blind balance
 * divergence (the credit is in no per-block ledger hash at the maturity height).
 *
 * This selects those credits by MATURITY block (cooldown_end_block) so they can be
 * merged into the normal `credits` payload. It is the exact FORWARD mirror of
 * ClientRollback's reverse delete: same join keys, same cooldown_end_block and
 * status='completed' predicate, GAS tick for the capability refund. They then flow
 * through the existing credits apply path (the replica's row insert followed by its
 * balance rebuild), idempotent at the block/window boundary exactly as normal
 * action-scoped credits are. Indexer dbType only.
 *
 * The same maturity writes a SECOND row under the same backdated action_index:
 * the negative `escrows` row that releases the bond the stake locked (escrows has
 * no block_index either, so it misses the same three channels). Without it the
 * follower's escrows table stays short by every release, and the supply its
 * rollback recomputes from credits - debits + escrows comes out too high.
 * collectMaturedCooldownEscrows forwards that row by the same keys. The source's
 * cooldown-maturity rollback removes only the credit, so ClientRollback mirrors it
 * and leaves the release in place too: the replica keeps the rows the source keeps.
 *
 * Everything above describes the LEGACY attribution era only. Once the indexer's
 * UNSTAKE_COOLDOWN_COMPLETION_ACTION protocol change is active (genesis on testnet
 * and regtest, a timestamp on mainnet), processCooldownCompletions writes the credit
 * and the escrow release under a fresh synthetic UNSTAKE (FORMAT 2) action minted AT
 * the maturity block (xchain-indexer/src/utility.js completionAttribution). Those
 * rows ride the ordinary action-scoped channels (the per-block stream and the
 * incremental snapshot's action_index cursor), and the unstake-keyed joins here match
 * nothing, so an empty result for a post-activation maturity is correct, not a miss.
 * The collectors stay for mainnet's pre-activation history; mergeMaturedRows' dedup
 * on the logical identity keeps a row both channels reach from landing twice.
 *
 ********************************************************************/

const { gasTickSymbol } = require('../consensus-constants');

// The two Database finders behind each half of a maturity: the refund credit
// and the escrow release written beside it under the same action_index.
const FINDERS = {
    credits: { capability: 'findMaturedCapabilityCooldownCredits', contract: 'findMaturedContractCooldownCredits' },
    escrows: { capability: 'findMaturedCapabilityCooldownEscrows', contract: 'findMaturedContractCooldownEscrows' },
};

// The logical identity of a backdated ledger row (credits and escrows have no
// unique key, so every dedup here is explicit and keys on this triple).
function ledgerRowKey(r){
    return r.action_index + ':' + r.address_id + ':' + r.tick_id;
}

// Select the matured cooldown rows of one ledger table (`kind`, credits or
// escrows) whose maturity block (cooldown_end_block) falls in the inclusive
// window [fromBlock, toBlock]. Returns raw rows (action_index, address_id,
// tick_id, amount), deduped by their logical identity. The caller merges these
// into the block / snapshot array of the same table (and dedups the union).
//
//   db        the source Database (indexer dbType only; callers must gate)
//   fromBlock inclusive lower maturity-block bound
//   toBlock   inclusive upper maturity-block bound (for a single live block,
//             pass fromBlock === toBlock)
//   conn      optional connection (so a snapshot's REPEATABLE READ view reads
//             these at the same height as the rest of the payload)
async function collectMatured(kind, db, fromBlock, toBlock, conn){
    let from = Number(fromBlock);
    let to   = Number(toBlock);
    let finders = FINDERS[kind];

    let completedStatusId = await db.getStatusId('completed');
    if(completedStatusId === null || completedStatusId === undefined) return [];

    // An unstake created AND matured inside the same incremental window can be
    // reached both here and by the caller's action-scoped selection; the caller
    // dedups the union on the same triple.
    let acc = new Map();
    function add(rows){
        for(let r of (rows || [])){
            if(r && r.action_index != null && r.address_id != null && r.tick_id != null)
                acc.set(ledgerRowKey(r), r);
        }
    }

    // Capability maturity: paid in GAS, keyed by the unstake's action_index.
    // Forward mirror of ClientRollback's capability reverse delete (same join
    // keys + cooldown_end_block/status predicate); the GAS tick is the frozen
    // consensus constant, never a hub poll.
    let gasTick = gasTickSymbol();
    if(gasTick){
        try {
            add(await db[finders.capability](gasTick, completedStatusId, from, to, conn));
        } catch(e){
            // Skip ONLY a schema gap (errno 1146/1054): an error with no numeric errno would
            // otherwise broadcast the block short of its refund rows (poller's isSchemaGapError rule).
            if(!(e && typeof e.errno === 'number' && (e.errno === 1146 || e.errno === 1054))) throw e;
            // Table/column may not exist on older source schemas; skip.
        }
    }

    // Contract maturity: paid in the unstake's own tick. Forward mirror of
    // ClientRollback's contract reverse delete.
    try {
        add(await db[finders.contract](completedStatusId, from, to, conn));
    } catch(e){
        // Same schema-gap-only skip as the capability leg above.
        if(!(e && typeof e.errno === 'number' && (e.errno === 1146 || e.errno === 1054))) throw e;
        // Table may not exist on older source schemas; skip.
    }

    return Array.from(acc.values());
}

// The matured refund credits in the window (see collectMatured).
async function collectMaturedCooldownCredits(db, fromBlock, toBlock, conn){
    return await collectMatured('credits', db, fromBlock, toBlock, conn);
}

// The matured escrow releases in the window, the negative row paired with each
// refund credit (see collectMatured).
async function collectMaturedCooldownEscrows(db, fromBlock, toBlock, conn){
    return await collectMatured('escrows', db, fromBlock, toBlock, conn);
}

// Append `matured` rows to a payload's `rows` for the same table, skipping any
// identity already there; returns `rows` unchanged when there is nothing to add,
// so a payload never gains an empty table entry it did not have.
function mergeMaturedRows(rows, matured){
    if(!matured || matured.length === 0) return rows;
    let out = rows || [];
    let seen = new Set(out.map(ledgerRowKey));
    for(let r of matured){
        let k = ledgerRowKey(r);
        if(!seen.has(k)){ seen.add(k); out.push(r); }
    }
    return out;
}

module.exports = { collectMaturedCooldownCredits, collectMaturedCooldownEscrows, mergeMaturedRows };
