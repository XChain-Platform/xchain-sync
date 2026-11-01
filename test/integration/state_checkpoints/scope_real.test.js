// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Against a REAL MariaDB: the three checkpoint reads over a state_checkpoints table that
// holds every chain's rows, the way the hub mirror fills each indexer DB. The heights are
// the live testnet shape (BTC ~150k, LTC ~4.9M, DOGE ~68M), so an unscoped "newest" would
// be the DOGE row. A BTC row at the same block_index as an LTC row, with a far higher
// seq, proves the range subquery's MAX(checkpoint_seq) is taken within the chain.

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');
const { splitSqlStatements } = require('../../../src/db/sql_util');
const { createDatabase, dropDatabase } = require('../helpers/testDb');

const DB = 'xchain_sync_test_state_checkpoints_scope';

// [chain, network, block_index, checkpoint_seq]. LTC 4950000 carries two seqs (a
// superseded height), and BTC shares LTC's 4900000 height with a higher seq.
const ROWS = [
    ['BTC',  'testnet', 150000,   9000], ['BTC',  'testnet', 4900000, 9001],
    ['LTC',  'testnet', 4870000,  10],   ['LTC',  'testnet', 4900000, 11],
    ['LTC',  'testnet', 4950000,  12],   ['LTC',  'testnet', 4950000, 13],
    ['LTC',  'mainnet', 9000000,  14],
    ['DOGE', 'testnet', 68034819, 500]
];

// The indexer's own schema file, so the keys and collation are the ones served from.
function stateCheckpointsDdl(){
    let sqlDir = process.env.XCHAIN_INDEXER_SQL_PATH
        || path.join(__dirname, '..', '..', '..', '..', 'xchain-indexer', 'src', 'sql');
    return splitSqlStatements(fs.readFileSync(path.join(sqlDir, 'state_checkpoints.sql'), 'utf8'));
}

async function seed(db){
    await db.doQuery('DROP TABLE IF EXISTS state_checkpoints');
    for(let stmt of stateCheckpointsDdl()) await db.doQuery(stmt);
    for(let [chain, network, height, seq] of ROWS){
        await db.doQuery('INSERT INTO state_checkpoints (chain, network, block_index, block_hash, ledger_hash, ' +
            'actions_hash, contract_hash, checkpoint_seq, snapshot_block, validator_signatures) ' +
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [chain, network, height, 'h' + seq, 'l', 'a', 'c', seq, 150000, '[]']);
    }
}

const pick = (r) => [r.chain, r.network, Number(r.block_index), Number(r.checkpoint_seq)];

describe('state_checkpoints reads over a table mirroring every chain @integration', function(){
    this.timeout(0);
    let db;

    before(async function(){
        db = await createDatabase(DB);
        await seed(db);
    });

    after(async function(){
        try { await db.pool.end(); } catch(e){}
        await dropDatabase(DB);
    });

    it('getLatestCheckpoint returns the LTC testnet newest row, not the higher DOGE one', async function(){
        let rows = await db.getLatestCheckpoint('LTC', 'testnet');
        assert.deepStrictEqual(rows.map(pick), [['LTC', 'testnet', 4950000, 13]]);
    });

    it('findCheckpointsInRange returns one LTC testnet row per height, keeping the LTC row where BTC shares the height', async function(){
        let rows = await db.findCheckpointsInRange('LTC', 'testnet', 0, 70000000, 2000);
        assert.deepStrictEqual(rows.map(pick), [
            ['LTC', 'testnet', 4870000, 10], ['LTC', 'testnet', 4900000, 11], ['LTC', 'testnet', 4950000, 13]
        ]);
    });

    it('getCheckpointAtHeight returns the LTC row at a height another chain also checkpointed', async function(){
        let rows = await db.getCheckpointAtHeight('LTC', 'testnet', 4900000);
        assert.deepStrictEqual(rows.map(pick), [['LTC', 'testnet', 4900000, 11]]);
    });

    it('a chain with no rows on that network reads empty rather than another chain', async function(){
        assert.deepStrictEqual(await db.getLatestCheckpoint('DOGE', 'mainnet'), []);
        assert.deepStrictEqual(await db.getCheckpointAtHeight('BTC', 'testnet', 68034819), []);
    });
});
