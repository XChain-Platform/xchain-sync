// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Against a REAL MariaDB: the two /status height labels hold when another connection
// commits while they are being read. table_counts_height goes null when a block
// commits mid-count, and the poller's lookup id ceiling, read inside its pinned batch
// snapshot, excludes a lookup row committed after that snapshot opened.

const assert     = require('assert');
const sinon      = require('sinon');
const proxyquire = require('proxyquire');
const Database   = require('../../src/db');
const Utility    = require('../../src/util');
const ServerPoller = require('../../src/server/poller');
const {
    TEST_DB_HOST, TEST_DB_PORT, TEST_DB_USER, TEST_DB_PASS,
    createDatabase, dropDatabase
} = require('./helpers/testDb');

const DB = 'xchain_sync_test_status_height_labels';

function loadServerApi(){
    let prior = process.env.SYNC_MODE;
    process.env.SYNC_MODE = 'server';
    let api = proxyquire('../../src/api', {});
    if(prior === undefined) delete process.env.SYNC_MODE; else process.env.SYNC_MODE = prior;
    return api;
}

function serverSyncService(polledBlock){
    return {
        getBroadcaster: () => ({
            getStatus: () => ({ block_height: polledBlock, source_block_height: polledBlock }),
            getSubscribers: () => []
        }),
        getSnapshotBuilder: () => ({ snapshotsServed: 0, snapshotsRejected: 0 }),
        getPoller: () => null
    };
}

describe('/status height labels against a real database @integration', function(){
    this.timeout(0);
    let db, helperDb, nextBlock;

    before(async function(){
        helperDb = await createDatabase(DB);
        await helperDb.doQuery('DROP TABLE IF EXISTS blocks');
        await helperDb.doQuery('DROP TABLE IF EXISTS index_statuses');
        await helperDb.doQuery('CREATE TABLE blocks (block_index BIGINT PRIMARY KEY, block_time BIGINT)');
        await helperDb.doQuery('CREATE TABLE index_statuses (id INT AUTO_INCREMENT PRIMARY KEY, status VARCHAR(32))');
        for(let i = 1; i <= 5; i++)
            await helperDb.doQuery('INSERT INTO blocks (block_index, block_time) VALUES (?, ?)', [i, 1000 + i]);
        for(let s of ['valid', 'invalid', 'pending'])
            await helperDb.doQuery('INSERT INTO index_statuses (status) VALUES (?)', [s]);
        nextBlock = 6;
        db = new Database(TEST_DB_HOST, TEST_DB_PORT, DB, TEST_DB_USER, TEST_DB_PASS, new Utility(), 'indexer');
    });

    after(async function(){
        try { await db.close(); } catch(e){}
        try { await helperDb.close(); } catch(e){}
        await dropDatabase(DB);
    });

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    // Commit one more block from a separate connection, as the co-located indexer does.
    async function commitBlock(){
        await helperDb.doQuery('INSERT INTO blocks (block_index, block_time) VALUES (?, ?)', [nextBlock, 1000 + nextBlock]);
        nextBlock++;
    }

    it('labels the counts with the database tip, not the broadcast height', async function(){
        let { buildStatusRow } = loadServerApi();
        let tip = nextBlock - 1;
        let row = await buildStatusRow(serverSyncService(tip - 1), db, 'indexer', 'bitcoin', 'mainnet');
        assert.strictEqual(row.block_height, tip - 1);
        assert.strictEqual(row.table_counts_height, tip);
        assert.strictEqual(row.table_counts.blocks, tip);
    });

    it('publishes a null label when a block commits while the counts are read', async function(){
        let { buildStatusRow } = loadServerApi();
        let racing = Object.create(db);
        let fired = false;
        racing.getTableCount = async function(table, conn){
            if(!fired){ fired = true; await commitBlock(); }
            return db.getTableCount(table, conn);
        };
        let row = await buildStatusRow(serverSyncService(nextBlock - 1), racing, 'indexer', 'bitcoin', 'mainnet');
        assert.strictEqual(fired, true, 'the race must actually have run inside the count loop');
        assert.strictEqual(row.table_counts_height, null);
    });

    it('reads the lookup id ceiling from the pinned batch view, excluding a later commit', async function(){
        let poller = new ServerPoller('bitcoin', 'mainnet', db,
            { broadcast(){}, updateStatus(){}, getSubscribers: () => [], getSubscriberCount: () => 0 },
            null, { TABLE_CONTENT_PARITY_CHECK: true }, new Utility());
        let snap = await db.beginReadSnapshot();
        let before;
        try {
            before = await db.getMaxRowId('index_statuses', snap);
            // A later block interns a status after the batch view was pinned.
            await helperDb.doQuery('INSERT INTO index_statuses (status) VALUES (?)', ['expired']);
            await poller.recordContentParityIdCeiling(snap, 5);
        } finally {
            await db.commitReadSnapshot(snap);
        }
        let ceiling = poller.getContentParityIdCeiling();
        let live = await db.getMaxRowId('index_statuses');
        assert.strictEqual(live, before + 1, 'control: the live read sees the later row');
        assert.strictEqual(ceiling.block, 5);
        assert.strictEqual(ceiling.ids.index_statuses, before, 'the ceiling describes the pinned height only');
    });
});
