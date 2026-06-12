/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available —
 * contact legal@dankest.llc.
 *
 **********************************************************************
 *
 * decoderFixtures — seed/inspect helpers for a decoder-shaped MariaDB.
 *
 * Companion to fixtures.js (which seeds the indexer schema). Each block
 * here gets one transaction with one output, all from a single source
 * address — enough to round-trip through the decoder sync path.
 *
 ********************************************************************/

const fs   = require('fs');
const path = require('path');
const crypto = require('crypto');
const { splitSqlStatements } = require('../../../src/sqlUtil');

// Apply the decoder schema (from xchain-decoder/src/sql/) to a database.
// Skips mempool_transactions per the xchain-sync decoder decisions (not synced).
async function seedDecoderSchema(db){
    // Same convention as the indexer repo's CI: XCHAIN_DECODER_SQL_PATH points at
    // a checked-out xchain-decoder/src/sql when the sibling path doesn't exist.
    let sqlDir = process.env.XCHAIN_DECODER_SQL_PATH
        || path.join(__dirname, '..', '..', '..', '..', 'xchain-decoder', 'src', 'sql');
    let files = fs.readdirSync(sqlDir).filter(f => f.endsWith('.sql')).sort();

    // index_* first (referenced by blocks/transactions)
    let indexFiles = files.filter(f => f.startsWith('index_'));
    let otherFiles = files.filter(f => !f.startsWith('index_'));
    let ordered    = [...indexFiles, ...otherFiles];

    for(let file of ordered){
        let data = fs.readFileSync(path.join(sqlDir, file), 'utf8');
        let queries = splitSqlStatements(data);
        for(let q of queries){
            try { await db.doQuery(q); } catch(e){}
        }
    }
}

function h(label, blockIndex){
    return crypto.createHash('sha256').update(label + ':' + blockIndex).digest('hex');
}

// Insert a contiguous range of decoder blocks. Each block produces:
//   - 1 index_addresses row (source)
//   - 3 index_transactions rows (block_hash, previous_block_hash, tx_hash)
//   - 1 blocks row
//   - 1 transactions row
//   - 1 transaction_outputs row
//   - 1 events row (operational log, not block-scoped)
async function seedDecoderBlocks(db, startBlock, endBlock, opts){
    opts = opts || {};
    let prefix = opts.addressPrefix || 'addr_dec_';

    for(let i = startBlock; i <= endBlock; i++){
        let blockTime = 1700000000 + i;
        let sourceAddr = prefix + i;
        let destAddr   = prefix + 'dest_' + i;
        let txIndex    = i * 10;

        let blockHash = h('blockhash', i);
        let prevHash  = (i === startBlock) ? null : h('blockhash', i - 1);
        let txHash    = h('txhash', i);

        // index_addresses
        await db.doQuery('INSERT IGNORE INTO index_addresses (address) VALUES (?)', [sourceAddr]);
        await db.doQuery('INSERT IGNORE INTO index_addresses (address) VALUES (?)', [destAddr]);
        let srcRows = await db.doQuery('SELECT id FROM index_addresses WHERE address = ?', [sourceAddr]);
        let dstRows = await db.doQuery('SELECT id FROM index_addresses WHERE address = ?', [destAddr]);
        let srcId = Number(srcRows[0].id);
        let dstId = Number(dstRows[0].id);

        // index_transactions
        await db.doQuery('INSERT IGNORE INTO index_transactions (hash) VALUES (?)', [blockHash]);
        await db.doQuery('INSERT IGNORE INTO index_transactions (hash) VALUES (?)', [txHash]);
        let blkRows = await db.doQuery('SELECT id FROM index_transactions WHERE hash = ?', [blockHash]);
        let txhRows = await db.doQuery('SELECT id FROM index_transactions WHERE hash = ?', [txHash]);
        let blkId = Number(blkRows[0].id);
        let txhId = Number(txhRows[0].id);

        let prevId = null;
        if(prevHash){
            await db.doQuery('INSERT IGNORE INTO index_transactions (hash) VALUES (?)', [prevHash]);
            let prevRows = await db.doQuery('SELECT id FROM index_transactions WHERE hash = ?', [prevHash]);
            prevId = Number(prevRows[0].id);
        }

        // blocks
        await db.doQuery(
            'INSERT INTO blocks (block_index, block_time, block_hash_id, previous_block_hash_id) VALUES (?, ?, ?, ?)',
            [i, blockTime, blkId, prevId]
        );

        // transactions
        await db.doQuery(
            'INSERT INTO transactions (tx_index, tx_hash_id, block_index, source_id, destination_id, amount, fee) VALUES (?, ?, ?, ?, ?, ?, ?)',
            [txIndex, txhId, i, srcId, dstId, 100000, 250]
        );

        // transaction_outputs
        await db.doQuery(
            'INSERT INTO transaction_outputs (tx_index, vout, destination_id, amount) VALUES (?, ?, ?, ?)',
            [txIndex, 0, dstId, '0.001']
        );

        // events (operational, not block-scoped — fine if it drifts in count)
        await db.doQuery(
            "INSERT INTO events (time, code, data) VALUES (FROM_UNIXTIME(?), ?, ?)",
            [blockTime, 'BLOCK_PARSED', JSON.stringify({ block_index: i })]
        );
    }
}

// Reset every decoder table between tests. Uses DELETE rather than TRUNCATE
// because MariaDB refuses to TRUNCATE a table that is referenced by a foreign
// key (e.g. pubkeys.address_id → index_addresses.id) even when the referencing
// table is empty. SET FOREIGN_KEY_CHECKS=0 doesn't help here: each db.doQuery
// borrows a fresh pool connection, so the session-scoped flag wouldn't carry.
// Order is child-table-first so referenced rows can be removed cleanly.
async function truncateAll(db){
    let tables = ['transaction_outputs', 'dispensers', 'pubkeys',
                  'mempool_transactions',
                  'transactions', 'blocks',
                  'events',
                  'index_addresses', 'index_transactions'];
    for(let t of tables){
        try { await db.doQuery('DELETE FROM `' + t + '`'); } catch(e){}
    }
    // Restore the canonical empty rows (id=1) the decoder schema seeds.
    try { await db.doQuery("INSERT INTO index_addresses (id, address) VALUES (1, '')"); } catch(e){}
    try { await db.doQuery("INSERT INTO index_transactions (id, hash) VALUES (1, '')"); } catch(e){}
    // Reset AUTO_INCREMENT so subsequent seeds start from id=2 deterministically.
    try { await db.doQuery('ALTER TABLE index_addresses    AUTO_INCREMENT = 2'); } catch(e){}
    try { await db.doQuery('ALTER TABLE index_transactions AUTO_INCREMENT = 2'); } catch(e){}
}

module.exports = {
    seedDecoderSchema,
    seedDecoderBlocks,
    truncateAll
};
