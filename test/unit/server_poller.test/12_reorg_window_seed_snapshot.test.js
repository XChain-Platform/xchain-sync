// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

'use strict';

const assert = require('assert');
const sinon = require('sinon');
const ServerPoller = require('../../../src/server/poller');
const seedReorgWindow = require('../../../src/server/poller/reorg_window_seed');
const { withDbMixins } = require('../../helpers/db_mixins');

function hashRow(blockIndex, generation){
    return {
        block_index: blockIndex,
        block_time: blockIndex * 10,
        block_hash: generation + '-' + blockIndex
    };
}

function broadcaster(){
    return {
        broadcast: sinon.stub(),
        updateStatus: sinon.stub(),
        getSubscriberCount: sinon.stub().returns(0)
    };
}

describe('ServerPoller restart reorg-window snapshot @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'warn');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    it('seeds one consistent range read and resolves a mid-seed reorg at the true fork', async function(){
        const cursor = 100;
        const fork = 80;
        let generation = 'old';
        const snapshot = { id: 'seed-snapshot' };
        const db = {
            dbType: 'decoder',
            getLastBlock: sinon.stub().resolves(cursor),
            getBlockHashRow: sinon.stub().callsFake(async blockIndex => hashRow(blockIndex,
                generation === 'new' && blockIndex >= fork ? 'new' : 'old')),
            beginReadSnapshot: sinon.stub().resolves(snapshot),
            commitReadSnapshot: sinon.stub().resolves(),
            rollbackReadSnapshot: sinon.stub().resolves(),
            doQuery: sinon.stub().callsFake(async (query, args, conn) => {
                assert.match(query, /FROM blocks b/);
                assert.deepStrictEqual(args, [70, cursor]);
                assert.strictEqual(conn, snapshot);
                const rows = [];
                for(let blockIndex = args[0]; blockIndex <= args[1]; blockIndex++)
                    rows.push({ block_index: blockIndex, hash: 'old-' + blockIndex });
                generation = 'new';
                return rows;
            })
        };
        const sink = broadcaster();
        const poller = new ServerPoller('bitcoin', 'mainnet', db, sink, null,
            { BLOCK_POLL_INTERVAL: 0 }, { sleep: sinon.stub().resolves() });
        poller.lastPolledBlock = cursor;
        poller.recentHashCap = 31;

        poller.lastPolledBlockHash = await seedReorgWindow(poller, { warn: sinon.spy() });

        assert.strictEqual(db.doQuery.callCount, 1);
        assert.strictEqual(db.beginReadSnapshot.callCount, 1);
        assert.strictEqual(db.commitReadSnapshot.callCount, 1);
        assert.strictEqual(db.rollbackReadSnapshot.callCount, 0);
        assert.strictEqual(db.getBlockHashRow.callCount, 0);
        assert.strictEqual(poller.lastPolledBlockHash, 'old-100');

        await poller.poll();

        const event = sink.broadcast.getCalls()
            .map(call => call.args[2])
            .find(payload => payload && payload.type === 'reorg');
        assert.strictEqual(event.block_index, fork);
        assert.strictEqual(poller.lastPolledBlock, fork - 1);
    });

    it('uses one consistent range statement when snapshot helpers are unavailable', async function(){
        const cursor = 100;
        const fork = 80;
        let generation = 'old';
        const db = {
            dbType: 'decoder',
            getLastBlock: sinon.stub().resolves(cursor),
            getBlockHashRow: sinon.stub().callsFake(async blockIndex => hashRow(blockIndex,
                generation === 'new' && blockIndex >= fork ? 'new' : 'old')),
            doQuery: sinon.stub().callsFake(async (query, args, conn) => {
                assert.match(query, /FROM blocks b/);
                assert.deepStrictEqual(args, [70, cursor]);
                assert.strictEqual(conn, undefined);
                const rows = [];
                for(let blockIndex = args[0]; blockIndex <= args[1]; blockIndex++)
                    rows.push({ block_index: blockIndex, hash: 'old-' + blockIndex });
                generation = 'new';
                return rows;
            })
        };
        const sink = broadcaster();
        const poller = new ServerPoller('bitcoin', 'mainnet', db, sink, null,
            { BLOCK_POLL_INTERVAL: 0 }, { sleep: sinon.stub().resolves() });
        poller.lastPolledBlock = cursor;
        poller.recentHashCap = 31;

        poller.lastPolledBlockHash = await seedReorgWindow(poller, { warn: sinon.spy() });

        assert.strictEqual(db.doQuery.callCount, 1);
        assert.strictEqual(db.getBlockHashRow.callCount, 0);
        assert.strictEqual(poller.lastPolledBlockHash, 'old-100');

        await poller.poll();

        const event = sink.broadcast.getCalls()
            .map(call => call.args[2])
            .find(payload => payload && payload.type === 'reorg');
        assert.strictEqual(event.block_index, fork);
        assert.strictEqual(poller.lastPolledBlock, fork - 1);
    });

    it('seeds indexer hashes from the durable log range after a pre-seed reorg', async function(){
        const cursor = 100;
        const fork = 80;
        const snapshot = { id: 'log-seed-snapshot' };
        const sourceDb = {
            dbType: 'indexer',
            getLastBlock: sinon.stub().resolves(cursor),
            getBlockHashRow: sinon.stub().callsFake(async blockIndex => ({
                block_index: blockIndex,
                ledger_hash: blockIndex >= fork ? 'new-' + blockIndex : 'old-' + blockIndex
            })),
            doQuery: sinon.stub().rejects(new Error('source range must not seed recorded hashes'))
        };
        const logDb = withDbMixins({
            beginReadSnapshot: sinon.stub().resolves(snapshot),
            commitReadSnapshot: sinon.stub().resolves(),
            rollbackReadSnapshot: sinon.stub().resolves(),
            doQuery: sinon.stub().callsFake(async (query, args, conn) => {
                assert.match(query, /FROM sync_meta/);
                assert.deepStrictEqual(args, [70, cursor]);
                assert.strictEqual(conn, snapshot);
                const rows = [];
                for(let blockIndex = args[0]; blockIndex <= args[1]; blockIndex++){
                    rows.push({
                        block_index: blockIndex,
                        ledger_hash: 'old-' + blockIndex,
                        actions_hash: null,
                        contract_hash: null
                    });
                }
                return rows;
            })
        });
        const transparencyLog = {
            db: logDb,
            getRecordedHash: sinon.stub().resolves(null),
            pruneFrom: sinon.stub().resolves()
        };
        const sink = broadcaster();
        const poller = new ServerPoller('bitcoin', 'mainnet', sourceDb, sink, transparencyLog,
            { BLOCK_POLL_INTERVAL: 0 }, { sleep: sinon.stub().resolves() });
        poller.lastPolledBlock = cursor;
        poller.recentHashCap = 31;
        sinon.stub(poller, 'updateStatus').resolves();

        poller.lastPolledBlockHash = await seedReorgWindow(poller, { warn: sinon.spy() });

        assert.strictEqual(logDb.doQuery.callCount, 1);
        assert.strictEqual(sourceDb.doQuery.callCount, 0);
        assert.strictEqual(transparencyLog.getRecordedHash.callCount, 0);
        assert.strictEqual(logDb.beginReadSnapshot.callCount, 1);
        assert.strictEqual(logDb.commitReadSnapshot.callCount, 1);
        assert.strictEqual(logDb.rollbackReadSnapshot.callCount, 0);
        assert.strictEqual(poller.lastPolledBlockHash, 'old-100');

        await poller.poll();

        const event = sink.broadcast.getCalls()
            .map(call => call.args[2])
            .find(payload => payload && payload.type === 'reorg');
        assert.strictEqual(event.block_index, fork);
        assert.strictEqual(poller.lastPolledBlock, fork - 1);
        assert.strictEqual(transparencyLog.pruneFrom.calledOnceWith(fork), true);
    });

    it('retries an unstable seed when the adapter has no range-query primitive', async function(){
        const cursor = 100;
        const fork = 80;
        let generation = 'old';
        let seeding = true;
        const db = {
            dbType: 'decoder',
            getLastBlock: sinon.stub().resolves(cursor),
            getBlockHashRow: sinon.stub().callsFake(async blockIndex => {
                const row = hashRow(blockIndex,
                    generation !== 'old' && blockIndex >= fork ? generation : 'old');
                if(seeding && generation === 'old' && blockIndex === 90)
                    generation = 'new';
                return row;
            })
        };
        const sink = broadcaster();
        const poller = new ServerPoller('bitcoin', 'mainnet', db, sink, null,
            { BLOCK_POLL_INTERVAL: 0 }, { sleep: sinon.stub().resolves() });
        poller.lastPolledBlock = cursor;
        poller.recentHashCap = 31;

        poller.lastPolledBlockHash = await seedReorgWindow(poller, { warn: sinon.spy() });

        assert.strictEqual(db.getBlockHashRow.callCount, 66);
        assert.strictEqual(poller.lastPolledBlockHash, 'new-100');
        assert.deepStrictEqual([...poller.recentBroadcastHashes.entries()].slice(0, 2), [
            [70, 'old-70'], [71, 'old-71']
        ]);
        assert.deepStrictEqual([...poller.recentBroadcastHashes.entries()].slice(-2), [
            [99, 'new-99'], [100, 'new-100']
        ]);

        seeding = false;
        generation = 'latest';
        await poller.poll();

        const event = sink.broadcast.getCalls()
            .map(call => call.args[2])
            .find(payload => payload && payload.type === 'reorg');
        assert.strictEqual(event.block_index, fork);
        assert.strictEqual(poller.lastPolledBlock, fork - 1);
    });
});
