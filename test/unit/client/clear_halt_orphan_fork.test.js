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

// A max-depth halt refuses a source-reported reorg and leaves the orphaned blocks
// committed. Clearing that halt must not resume on the orphaned tip, where the replica
// reads halted:false while every catch-up fails on the fork's rows: the clear re-runs
// the refused rewind, and an apply that keeps colliding on a fork row halts by name.

const assert = require('assert');
const sinon = require('sinon');
const axios = require('axios');
const zlib = require('zlib');
const ClientSync = require('../../../src/client/sync');
const ClientApplier = require('../../../src/client/applier');
const Utility = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');
const { withDbMixins } = require('../../helpers/db_mixins.js');

// Replica committed through 13; blocks 10..13 are on a fork the source has replaced.
function makeSync(maxDepth){
    const state = { tip: 13 };
    const db = {
        dbName: 'test_db',
        dbType: 'indexer',
        getLastBlock: sinon.stub().callsFake(async () => state.tip),
        getBlockHashRow: sinon.stub().resolves(null),
        getActiveHalt: sinon.stub().resolves(null),
        recordHalt: sinon.stub().resolves({ block_index: 10 }),
        clearHalt: sinon.stub().resolves(1),
        getSyncState: sinon.stub().resolves(null),
        setSyncState: sinon.stub().resolves(true),
        deleteSyncState: sinon.stub().resolves(true),
        doQuery: sinon.stub().resolves([])
    };
    const rollback = {
        rollback: sinon.stub().callsFake(async blockIndex => { state.tip = blockIndex - 1; })
    };
    const applier = {
        applyBlock: sinon.stub().resolves(),
        applyFullSnapshot: sinon.stub().resolves(),
        applyIncrementalSnapshot: sinon.stub().resolves()
    };
    const config = {
        SYNC_SOURCES: 'http://source:3006',
        MAX_ROLLBACK_DEPTH: maxDepth,
        MAX_ROLLBACK_DEPTH_EXPLICIT: true,
        VERIFY_RECOMPUTE: false
    };
    const sync = new ClientSync('bitcoin', 'regtest', db, applier, rollback,
        new HashVerifier(), config, new Utility());
    sync.running = true;
    sync.lastAppliedBlock = 13;
    return { sync, db, rollback, applier, state };
}

function dupTxIndex(entry){
    let msg = "Duplicate entry '" + entry + "' for key 'tx_index'";
    return Object.assign(new Error(msg), { errno: 1062, sqlMessage: msg });
}

function logged(stub){
    return stub.getCalls().map(call => call.args.map(String).join(' ')).join('\n');
}

describe('ClientSync clear of a max-rollback-depth halt on an orphaned fork @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'warn');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('re-runs the refused rewind on clear, so the replica leaves the orphaned tip', async function(){
        // The halting run: depth 4 against a limit of 2.
        const halting = makeSync(2);
        await halting.sync.handleReorg({ block_index: 10 });
        assert.strictEqual(halting.sync.getHaltInfo().reason, 'max-rollback-depth-exceeded');
        assert.strictEqual(halting.rollback.rollback.called, false);

        // Restarted with the default limit; the durable halt row comes back at startup.
        const { sync, rollback, state } = makeSync(100);
        sync.installStartupHalt({ block_index: 10, reason: 'max-rollback-depth-exceeded',
            mismatches: '[]', sources: '[]', detected_at: 'x' });

        await sync.clearHalt();

        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(rollback.rollback.calledOnceWith(10), true, 'orphaned blocks 10..13 are rewound');
        assert.strictEqual(state.tip, 9);
        assert.strictEqual(sync.lastAppliedBlock, 9, 'never left reporting healthy on orphaned block 13');
    });

    it('halts again by name when the depth limit still refuses the rewind', async function(){
        const { sync, rollback } = makeSync(2);
        await sync.handleReorg({ block_index: 10 });
        assert.strictEqual(sync.isHalted(), true);

        await sync.clearHalt();

        assert.strictEqual(rollback.rollback.called, false);
        assert.strictEqual(sync.isHalted(), true, 'never halted:false on the orphaned tip');
        assert.strictEqual(sync.getHaltInfo().reason, 'max-rollback-depth-exceeded');
        assert.strictEqual(sync.lastAppliedBlock, 13);
    });

    it('does not rewind on the clear of an unrelated halt', async function(){
        const { sync, rollback } = makeSync(100);
        await sync.haltOnDivergence(10, [], [], 'cross-source-divergence');

        await sync.clearHalt();

        assert.strictEqual(rollback.rollback.called, false);
        assert.strictEqual(sync.lastAppliedBlock, 13);
    });
});

describe('ClientSync incremental catch-up colliding on an orphaned fork row @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'warn');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('tags a failing plain transactions insert with its table', async function(){
        const db = withDbMixins({ doQuery: sinon.stub().rejects(dupTxIndex('371')) });
        const applier = new ClientApplier(db, new Utility());
        const err = await applier.insertRows('transactions', [{ tx_index: 371, tx_hash: 'aa', block_index: 14 }])
            .then(() => null, e => e);
        assert.ok(err, 'the duplicate key propagates');
        assert.strictEqual(err.applyTable, 'transactions');
        assert.strictEqual(err.upsertTable, undefined);
    });

    it('retries once, then halts when the same tx_index collides twice with no apply between', async function(){
        const { sync, db, applier } = makeSync(100);
        sinon.stub(sync, 'prepareCatchUpFromTip').resolves(true);
        const gz = zlib.gzipSync(JSON.stringify({ schema_version: 'x', block_height: 18, tables: {} }));
        sinon.stub(axios, 'get').resolves({ data: gz });
        applier.applyIncrementalSnapshot.rejects(Object.assign(dupTxIndex('371'), { applyTable: 'transactions' }));

        await sync.runIncrementalCatchUp();
        assert.strictEqual(sync.isHalted(), false, 'the first collision retries');
        assert.match(logged(console.warn) + logged(console.log), /retrying once before halting/);

        await sync.runIncrementalCatchUp();

        assert.strictEqual(sync.isHalted(), true);
        const info = sync.getHaltInfo();
        assert.strictEqual(info.reason, 'apply-duplicate-key');
        assert.deepStrictEqual(info.mismatches, [{ table: 'transactions', entry: '371', key: 'tx_index' }]);
        assert.strictEqual(db.recordHalt.callCount, 1);
        assert.match(logged(console.error), /re-bootstrap this replica/);
    });
});
