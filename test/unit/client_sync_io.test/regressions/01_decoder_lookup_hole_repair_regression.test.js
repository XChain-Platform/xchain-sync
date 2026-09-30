// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert     = require('assert');
const sinon      = require('sinon');
const axios      = require('axios');
const ClientSync = require('../../../../src/client/sync');
const Utility    = require('../../../../src/util');
const HashVerifier = require('../../../../src/client/hash_verifier');

const SOURCE = 'http://src1:3006';

// Build a decoder replica at block 100 whose local lookup counts read 9.
function makeDecoderSync(){
    let db = {
        dbName:        'decoder_db',
        dbType:        'decoder',
        getLastBlock:  sinon.stub().resolves(null),
        getActiveHalt: sinon.stub().resolves(null),
        getTableCount: sinon.stub().resolves(9),
        setSyncState:  sinon.stub().resolves()
    };
    let applier = { applyBlock: sinon.stub().resolves(), applyIncrementalSnapshot: sinon.stub().resolves() };
    let config  = { SYNC_SOURCES: SOURCE, COMPLETENESS_CHECK_INTERVAL: 60000 };
    let sync = new ClientSync('bitcoin', 'mainnet', db, applier, { rollback: sinon.stub().resolves() },
        new HashVerifier(), config, new Utility());
    sync.lastAppliedBlock = 100;
    let pager = sinon.stub(sync, 'syncLookupTablesPaged').resolves();
    return { sync, db, pager };
}

// Run one periodic sweep against a source reporting `status`.
async function sweep(sync, status){
    axios.get.resolves({ data: status });
    sync._lastCompletenessSweepAt = 0;
    await sync.maybeVerifyCompleteness(SOURCE, 100);
}

// The indexer sweep repairs a short lookup from id 0; the decoder sweep only
// reported it, so a decoder hole below MAX(id) stayed until a forced snapshot.
describe('ClientSync decoder lookup-hole repair @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
        sinon.stub(axios, 'get');
    });
    afterEach(function(){ sinon.restore(); });

    it('re-pages a short decoder lookup from id 0 at equal height and records the attempt', async function(){
        let { sync, pager } = makeDecoderSync();
        await sweep(sync, { block_height: 100, table_counts: { index_addresses: 10 } });
        assert.strictEqual(pager.callCount, 1);
        assert.deepStrictEqual([...pager.firstCall.args[1].fromZero], ['index_addresses']);
        assert.strictEqual(sync._replicaGaps.get('index_addresses').repairAttempts, 1);
    });

    it('does not repair a shortfall in a non-lookup table', async function(){
        let { sync, pager } = makeDecoderSync();
        await sweep(sync, { block_height: 100, table_counts: { blocks: 10 } });
        assert.strictEqual(pager.called, false);
        assert.strictEqual(sync._replicaGaps.get('blocks').repairAttempts, 0);
    });

    it('never re-pages the events operational log', async function(){
        let { sync, pager } = makeDecoderSync();
        await sweep(sync, { block_height: 100, table_counts: { events: 415 } });
        assert.strictEqual(pager.called, false);
        let tried = await sync.repairShortLookups(SOURCE, [{ table: 'events', delta: 1 }]);
        assert.strictEqual(tried.size, 0, 'events is excluded even when handed in directly');
        assert.strictEqual(pager.called, false);
    });

    it('keeps sweeping and ages the gap when the repair pass fails', async function(){
        let { sync, pager } = makeDecoderSync();
        pager.rejects(new Error('ECONNRESET'));
        await sweep(sync, { block_height: 100, table_counts: { index_transactions: 10 } });
        assert.strictEqual(pager.callCount, 1);
        assert.strictEqual(sync._replicaGaps.get('index_transactions').sweeps, 1);
    });

    it('treats a source that moved past the replica as no reading: no repair, no aging', async function(){
        let { sync, pager } = makeDecoderSync();
        await sweep(sync, { block_height: 101, table_counts: { index_addresses: 10 } });
        assert.strictEqual(pager.called, false);
        assert.strictEqual(sync._replicaGaps.size, 0);
    });

    it('never repairs from the bootstrap caller of verifyDecoderCompleteness', async function(){
        let { sync, pager } = makeDecoderSync();
        axios.get.resolves({ data: { block_height: 101, table_counts: { index_addresses: 10 } } });
        let shortfalls = await sync.verifyDecoderCompleteness(SOURCE, 100);
        assert.strictEqual(shortfalls.length, 1, 'bootstrap still reports without the height guard');
        assert.strictEqual(pager.called, false);
    });
});
