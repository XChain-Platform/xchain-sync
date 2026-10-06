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
 * XChain Sync - Decoder block linkage check (replica side)
 *
 * A decoder `blocks` row carries previous_block_hash_id, an id into
 * index_transactions, and the source ships the index_transactions rows those ids
 * name with every block. A block that arrives on top of the committed tip but
 * names a different parent proves the tip was replaced by a reorg this replica
 * never saw (the source's `reorg` broadcast is not replayed after a reconnect).
 * The check only ever answers "broken" on proof: anything it cannot resolve
 * reads as linked, so missing data can never trigger a rewind.
 *
 ********************************************************************/

// Return the parent hash a decoder block row names, read from the payload's
// index_transactions rows first and the replica's own copy second, or null when
// neither resolves it (or it resolves to the decoder's seeded empty hash).
async function previousBlockHash(blockRow, indexTxRows, db){
    let prevId = blockRow ? blockRow.previous_block_hash_id : null;
    if(prevId === null || prevId === undefined) return null;
    let hit = (indexTxRows || []).find(r => r && String(r.id) === String(prevId));
    if(!hit && db && typeof db.findIndexTransactionsByIds === 'function'){
        let rows = await db.findIndexTransactionsByIds([prevId]);
        hit = (rows || []).find(r => r && String(r.id) === String(prevId));
    }
    return (hit && hit.hash) ? String(hit.hash) : null;
}

// True only when `blockRow` provably does not build on the committed tip whose
// hash is `tipHash`: its parent hash resolves and differs from the tip's.
async function decoderLinkBroken(blockRow, indexTxRows, tipHash, db){
    if(!tipHash || !blockRow) return false;
    let prevHash = await previousBlockHash(blockRow, indexTxRows, db);
    return prevHash !== null && prevHash !== String(tipHash);
}

// Strict catch-up verdict: 'linked' or 'broken' only on proof, 'unresolved' when the
// committed tip hash or a named parent lookup row is unavailable. A block naming no
// parent, or a parent row whose hash is the seeded empty value, proves nothing and
// reads as linked; the caller must abort and retry on 'unresolved', never apply.
async function decoderLinkState(blockRow, indexTxRows, tipHash, db){
    if(!blockRow) return 'linked';
    let prevId = blockRow.previous_block_hash_id;
    if(prevId === null || prevId === undefined) return 'linked';
    if(!tipHash) return 'unresolved';
    let hit = (indexTxRows || []).find(r => r && String(r.id) === String(prevId));
    if(!hit && db && typeof db.findIndexTransactionsByIds === 'function'){
        let rows = await db.findIndexTransactionsByIds([prevId]);
        hit = (rows || []).find(r => r && String(r.id) === String(prevId));
    }
    if(!hit) return 'unresolved';
    if(!hit.hash) return 'linked';
    return String(hit.hash) === String(tipHash) ? 'linked' : 'broken';
}

// Reason string when a decoder replica's transactions.data column cannot hold a
// 4-byte UTF-8 payload, or null when it can. `row` is one information_schema.columns
// row carrying CHARACTER_SET_NAME (either case). An absent column or an unreadable
// charset returns null: a missing table is the schema layer's problem and an answer
// we could not read must not halt a chain. A non-null reason is meant to fail the
// startup check for that chain only.
function transactionsDataWidthReason(row){
    if(!row) return null;
    let raw = row.CHARACTER_SET_NAME != null ? row.CHARACTER_SET_NAME : row.character_set_name;
    if(raw == null) return null;
    let charset = String(raw).toLowerCase();
    if(charset === 'utf8mb4' || charset === 'binary') return null;
    return 'transactions.data is ' + charset + ' but a decoder replica needs utf8mb4 ' +
           'to store a 4-byte payload; widen the column before running this chain.';
}

// Reads the replica's transactions.data charset and throws the width reason when it is
// too narrow. Decoder replicas only; any other dbType returns. The read rethrows so a
// transient driver fault surfaces instead of reading as an absent column and passing.
// The caller runs it per chain so one narrow replica halts only its own chain.
async function assertTransactionsDataWidth(db){
    if(!db || db.dbType !== 'decoder') return;
    let rows = await db.readTransactionsDataCharset();
    if(!rows || rows.length === 0) return;
    let reason = transactionsDataWidthReason(rows[0]);
    if(reason) throw new Error(reason);
}

module.exports = {
    previousBlockHash, decoderLinkBroken, decoderLinkState,
    transactionsDataWidthReason, assertTransactionsDataWidth
};
