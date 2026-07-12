// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const crypto = require('crypto');
const BlockHasher = require('../../../src/BlockHasher');
const Utility = require('../../../src/utility');
const { rebuildBalances } = require('../../../src/balance-helpers');

const _util = new Utility();

function blockHash(blockIndex, label) {
    return crypto.createHash('sha256').update(label + ':' + blockIndex).digest('hex');
}

// Compute the REAL consensus-hash triple for a block being seeded (from the
// raw rows already inserted, chained off the previous block's committed
// hashes (exactly what the indexer commits and what VERIFY_RECOMPUTE
// re-derives), insert the three hashes into index_transactions, and return
// their ids. Fixtures used to fabricate these hashes, which made the data
// hash-INCONSISTENT: any client running the production-default
// VERIFY_RECOMPUTE would halt on fixture data, so the whole verification
// track was untestable end-to-end.
//
// Ordering contract: BlockHasher reads only the block's raw rows (via
// transactions joins) and the PREVIOUS block's blocks row, so the caller
// seeds raw rows first, calls this, and inserts the blocks row LAST, fully
// formed. The blocks row is what makes a block visible to ServerPoller
// (getLastBlock), so visibility is atomic: a poller may never observe a
// block whose hash links are still NULL (that broadcast would carry null
// committed hashes and trip every client's recompute halt). Seeding must run
// in ascending block order (the chain folds in the previous block's hashes).
async function computeAndInsertBlockHashes(db, blockIndex, conn) {
    // When called inside a fixture block transaction, every read/write must ride
    // that transaction's connection or the hasher can't see the block's own
    // uncommitted rows. BlockHasher only needs doQuery, so a thin facade pins it.
    let hasherDb = conn ? { doQuery: (q, a) => db.doQuery(q, a, conn) } : db;
    let computed = await new BlockHasher(hasherDb, _util).computeBlockHashes(blockIndex);
    let ids = {};
    for (let [field, hash] of [['ledger_hash_id', computed.ledger_hash],
                               ['actions_hash_id', computed.actions_hash],
                               ['contract_hash_id', computed.contract_hash]]) {
        let res = await db.doQuery(
            "INSERT INTO index_transactions (hash) VALUES (?) ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)",
            [hash], conn);
        ids[field] = Number(res.insertId);
    }
    return { computed, ids };
}

// Get-or-create an index_actions row, mirroring the real indexer's db.createAction.
// index_actions carries only a NON-unique index on `action`, so a blind
// `INSERT IGNORE` does NOT dedup; it inserts one duplicate row per call. The real
// indexer SELECTs first and inserts only when absent, so a given action type is a
// single row referenced by actions.action_id. Without this, the fixtures pile up an
// unreferenced duplicate index_actions row per block (nothing points at their id);
// those rows can only sync via snapshot, so a live-synced replica legitimately ends
// up with fewer index_actions than the source and the integrity check fails.
async function ensureIndexAction(db, action, conn) {
    let rows = await db.doQuery("SELECT id FROM index_actions WHERE action = ? LIMIT 1", [action], conn);
    if (rows.length === 0)
        await db.doQuery("INSERT INTO index_actions (action) VALUES (?)", [action], conn);
}

function buildBlock(blockIndex, opts = {}) {
    let txIndex     = opts.txIndex || blockIndex * 10;
    let actionIndex = opts.actionIndex || blockIndex * 100;
    let blockTime   = opts.blockTime || 1700000000 + blockIndex;
    let sourceAddr  = opts.sourceAddr || 'addr_' + blockIndex;
    let tickName    = opts.tickName || 'TOKEN';
    let creditAmt   = opts.creditAmount || '1000';
    let debitAmt    = opts.debitAmount || '0';

    // Only the tx hash is fabricated (it is an opaque identifier). The
    // ledger/actions/contract hashes are COMPUTED after the block's rows land
    // (commitBlockHashes) so fixture data is hash-consistent. The txSalt keeps
    // a reorg-replacement block's tx hash distinct from the orphaned
    // original's (real replacement transactions are different transactions).
    let txHash = blockHash(blockIndex, 'tx' + (opts.txSalt ? ':' + opts.txSalt : ''));

    return {
        index_addresses: [{ address: sourceAddr }],
        index_transactions: [
            { hash: txHash }
        ],
        index_tickers: [{ tick: tickName }],
        index_actions: [{ action: 'SEND' }],
        blocks: [{
            block_index: blockIndex,
            block_time: blockTime,
            ledger_hash_id: null,
            actions_hash_id: null,
            contract_hash_id: null
        }],
        transactions: [{
            tx_index: txIndex,
            block_index: blockIndex,
            tx_hash_id: null,
            source_id: null
        }],
        actions: [{
            action_index: actionIndex,
            block_index: blockIndex,
            tx_index: txIndex,
            tx_vout: 0,
            action_id: 1,
            action_format: 0
        }],
        credits: [{
            action_index: actionIndex,
            address_id: null,
            tick_id: null,
            amount: creditAmt
        }],
        debits: debitAmt !== '0' ? [{
            action_index: actionIndex,
            address_id: null,
            tick_id: null,
            amount: debitAmt
        }] : [],
        _meta: {
            blockIndex, txIndex, actionIndex, blockTime,
            sourceAddr, tickName,
            txHash
            // ledgerHash/actionsHash/contractHash land here after
            // commitBlockHashes computes them in seedBlocks.
        }
    };
}

async function seedBlocks(db, startBlock, endBlock, opts = {}) {
    // indexOffset shifts the tx/action index space. Production tx_index /
    // action_index are globally monotonic counters that are NEVER reused: a
    // chain that reorgs and re-mines block N assigns FRESH indexes to the
    // replacement transactions. Reorg scenarios that re-seed a block range
    // must pass a distinct offset, or the replacement rows would collide with
    // (and silently overwrite) the orphaned originals, hiding exactly the
    // divergence those scenarios exist to create.
    let off = opts.indexOffset || 0;
    let blocks = [];
    for (let i = startBlock; i <= endBlock; i++) {
        blocks.push(buildBlock(i, {
            txIndex: i * 10 + off * 10,
            actionIndex: i * 100 + off * 100,
            txSalt: off || undefined,
            sourceAddr: opts.sourceAddr || 'addr_' + i,
            tickName: opts.tickName || 'TOKEN',
            creditAmount: opts.creditAmount || '1000',
            debitAmount: opts.debitAmount || '0'
        }));
    }

    for (let block of blocks) {
        let meta = block._meta;

        // Idempotent seed: skip a block that already exists. The chaos suites
        // reuse one long-lived source DB across describe blocks and re-seed
        // overlapping ranges; a plain re-INSERT of blocks/transactions would
        // throw a duplicate-key and abort the test. Reorg tests call
        // deleteBlocksFrom() first, so a genuinely-rolled-back block is absent
        // here and is re-inserted with fresh content as intended.
        let existing = await db.doQuery("SELECT 1 FROM blocks WHERE block_index = ? LIMIT 1", [meta.blockIndex]);
        if (existing.length > 0) continue;

        // One transaction per block, like the production indexer: a REPEATABLE
        // READ snapshot (poller batch, /snapshot stream) pinned at any instant
        // must see either the whole block or none of it. The prior autocommit-
        // per-statement seed left a window where a snapshot captured a block's
        // transactions/actions/credits WITHOUT its blocks row; the incremental
        // snapshot's unbounded `block_index >= since` scans then shipped those
        // stray rows above block_height, and the later live apply of the same
        // block hit duplicate-key errors and wedged the replica .
        await db.withTransaction(async (conn) => {
            for (let addr of block.index_addresses)
                await db.doQuery("INSERT IGNORE INTO index_addresses (address) VALUES (?)", [addr.address], conn);
            for (let tx of block.index_transactions)
                await db.doQuery("INSERT IGNORE INTO index_transactions (hash) VALUES (?)", [tx.hash], conn);
            for (let tick of block.index_tickers)
                await db.doQuery("INSERT IGNORE INTO index_tickers (tick) VALUES (?)", [tick.tick], conn);
            for (let act of block.index_actions)
                await ensureIndexAction(db, act.action, conn);

            // Atomic upsert-and-return-id (LAST_INSERT_ID(id) on duplicate): a separate
            // SELECT can land on a pooled connection holding an older REPEATABLE READ
            // snapshot and miss the row just inserted on another connection, which
            // surfaced on CI as `addrRows[0]` being undefined.
            let addrRes = await db.doQuery(
                "INSERT INTO index_addresses (address) VALUES (?) ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)",
                [meta.sourceAddr], conn);
            let addrId = Number(addrRes.insertId);

            let txHashRes = await db.doQuery(
                "INSERT INTO index_transactions (hash) VALUES (?) ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)",
                [meta.txHash], conn);
            let txHashId = Number(txHashRes.insertId);

            let tickRes = await db.doQuery(
                "INSERT INTO index_tickers (tick) VALUES (?) ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)",
                [meta.tickName], conn);
            let tickId = Number(tickRes.insertId);

            // Raw rows first; the blocks row goes in LAST, fully formed (see
            // computeAndInsertBlockHashes' ordering contract).
            await db.doQuery(
                "INSERT INTO transactions (tx_index, block_index, tx_hash_id, source_id) VALUES (?, ?, ?, ?)",
                [meta.txIndex, meta.blockIndex, txHashId, addrId], conn
            );

            await db.doQuery(
                "INSERT INTO actions (action_index, block_index, tx_index, tx_vout, action_id, action_format) VALUES (?, ?, ?, ?, ?, ?)",
                [meta.actionIndex, meta.blockIndex, meta.txIndex, 0, 1, 0], conn
            );

            if (block.credits.length > 0) {
                await db.doQuery(
                    "INSERT INTO credits (action_index, address_id, tick_id, amount) VALUES (?, ?, ?, ?)",
                    [meta.actionIndex, addrId, tickId, block.credits[0].amount], conn
                );
            }

            if (block.debits.length > 0) {
                await db.doQuery(
                    "INSERT INTO debits (action_index, address_id, tick_id, amount) VALUES (?, ?, ?, ?)",
                    [meta.actionIndex, addrId, tickId, block.debits[0].amount], conn
                );
            }

            let { computed, ids } = await computeAndInsertBlockHashes(db, meta.blockIndex, conn);
            meta.ledgerHash   = computed.ledger_hash;
            meta.actionsHash  = computed.actions_hash;
            meta.contractHash = computed.contract_hash;

            // Blocks row last: atomic visibility to the poller.
            await db.doQuery(
                "INSERT INTO blocks (block_index, block_time, ledger_hash_id, actions_hash_id, contract_hash_id) VALUES (?, ?, ?, ?, ?)",
                [meta.blockIndex, meta.blockTime, ids.ledger_hash_id, ids.actions_hash_id, ids.contract_hash_id], conn
            );
        });
    }

    // Rebuild balances with the SAME shared SQL the applier and rollback use
    // (DECIMAL(65,18) summing + minimal-decimal rendering) so the source's
    // derived aggregate is byte-identical to what a replica rebuilds.
    await rebuildBalances(db);

    return blocks;
}

async function deleteBlocksFrom(db, blockIndex) {
    // One transaction for the whole rollback, like the production indexer's
    // reorg rewind: a snapshot pinned mid-delete must never see a blocks row
    // whose actions/credits are already gone (that partial state recomputes to
    // divergent hashes on the replica and forces an illegitimate halt).
    await db.withTransaction(async (conn) => {
        let actionRows = await db.doQuery(
            "SELECT action_index FROM actions WHERE block_index >= ? ORDER BY action_index ASC LIMIT 1",
            [blockIndex], conn
        );
        let firstActionIndex = actionRows.length > 0 ? actionRows[0].action_index : null;

        if (firstActionIndex !== null) {
            let actionTables = ['credits', 'debits', 'sends', 'issues', 'tokens', 'destroys', 'actions'];
            for (let table of actionTables) {
                try {
                    await db.doQuery("DELETE FROM `" + table + "` WHERE action_index >= ?", [firstActionIndex], conn);
                } catch (e) {}
            }
        }

        await db.doQuery("DELETE FROM transactions WHERE block_index >= ?", [blockIndex], conn);
        await db.doQuery("DELETE FROM blocks WHERE block_index >= ?", [blockIndex], conn);
        try {
            await db.doQuery("DELETE FROM sync_meta WHERE block_index >= ?", [blockIndex], conn);
        } catch (e) {}
    });

    try {
        await rebuildBalances(db);
    } catch (e) {}
}

// Seed a large block with many actions for volume testing
async function seedLargeBlock(db, blockIndex, actionCount) {
    let blockTime  = 1700000000 + blockIndex;
    let sourceAddr = 'addr_large_' + blockIndex;
    let tickName   = 'TOKEN';

    let txHash = blockHash(blockIndex, 'tx');

    // Same per-block atomicity contract as seedBlocks (see comment there).
    await db.withTransaction(async (conn) => {
        await db.doQuery("INSERT IGNORE INTO index_addresses (address) VALUES (?)", [sourceAddr], conn);
        await db.doQuery("INSERT IGNORE INTO index_transactions (hash) VALUES (?)", [txHash], conn);
        await db.doQuery("INSERT IGNORE INTO index_tickers (tick) VALUES (?)", [tickName], conn);
        await ensureIndexAction(db, 'SEND', conn);

        let addrRows = await db.doQuery("SELECT id FROM index_addresses WHERE address = ?", [sourceAddr], conn);
        let addrId = addrRows[0].id;
        let txHashRows = await db.doQuery("SELECT id FROM index_transactions WHERE hash = ?", [txHash], conn);
        let tickRows = await db.doQuery("SELECT id FROM index_tickers WHERE tick = ?", [tickName], conn);
        let tickId = tickRows[0].id;

        // The blocks row is inserted LAST, fully formed (see
        // computeAndInsertBlockHashes' ordering contract).
        let txIndex = blockIndex * 10;
        await db.doQuery(
            "INSERT INTO transactions (tx_index, block_index, tx_hash_id, source_id) VALUES (?, ?, ?, ?)",
            [txIndex, blockIndex, txHashRows[0].id, addrId], conn
        );

        // Batch insert actions and credits
        let batchSize = 100;
        for (let i = 0; i < actionCount; i += batchSize) {
            let batch = Math.min(batchSize, actionCount - i);
            let actionValues = [];
            let actionArgs = [];
            let creditValues = [];
            let creditArgs = [];

            for (let j = 0; j < batch; j++) {
                let actionIndex = blockIndex * 100000 + i + j;
                actionValues.push('(?, ?, ?, ?, ?, ?)');
                actionArgs.push(actionIndex, blockIndex, txIndex, 0, 1, 0);
                creditValues.push('(?, ?, ?, ?)');
                creditArgs.push(actionIndex, addrId, tickId, '10');
            }

            await db.doQuery(
                "INSERT INTO actions (action_index, block_index, tx_index, tx_vout, action_id, action_format) VALUES " + actionValues.join(','),
                actionArgs, conn
            );
            await db.doQuery(
                "INSERT INTO credits (action_index, address_id, tick_id, amount) VALUES " + creditValues.join(','),
                creditArgs, conn
            );
        }

        let { ids } = await computeAndInsertBlockHashes(db, blockIndex, conn);
        await db.doQuery(
            "INSERT INTO blocks (block_index, block_time, ledger_hash_id, actions_hash_id, contract_hash_id) VALUES (?, ?, ?, ?, ?)",
            [blockIndex, blockTime, ids.ledger_hash_id, ids.actions_hash_id, ids.contract_hash_id], conn
        );
    });

    // Rebuild balances with the shared applier/rollback SQL (byte-parity).
    await rebuildBalances(db);
}

module.exports = {
    blockHash,
    buildBlock,
    seedBlocks,
    deleteBlocksFrom,
    seedLargeBlock,
    computeAndInsertBlockHashes
};
