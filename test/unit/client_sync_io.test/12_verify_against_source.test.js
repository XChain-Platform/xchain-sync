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
const ClientSync = require('../../../src/client/sync');
const Utility    = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');
const { withDbMixins } = require('../../helpers/db_mixins.js');

function createMockDb(overrides){
    return Object.assign({
        dbName:           'test_db',
        dbType:           'indexer',
        getLastBlock:     sinon.stub().resolves(null),
        getBlockHashRow:  sinon.stub().resolves(null),
        doQuery:          sinon.stub().resolves([]),
        getActiveHalt:    sinon.stub().resolves(null),
        getTableCount:    sinon.stub().resolves(0),
        addMissingColumns: sinon.stub().resolves(),
        recordHalt:       sinon.stub().resolves({ block_index: 0 }),
        clearHalt:        sinon.stub().resolves(1)
    }, overrides || {});
}

function createMockApplier(){
    return {
        applyBlock:               sinon.stub().resolves(),
        applyFullSnapshot:        sinon.stub().resolves(),
        applyIncrementalSnapshot: sinon.stub().resolves()
    };
}

function createMockRollback(){
    return { rollback: sinon.stub().resolves() };
}

function makeSync(configOverrides, dbOverrides){
    let db      = createMockDb(dbOverrides);
    let applier = createMockApplier();
    let rb      = createMockRollback();
    let hv      = new HashVerifier();
    let util    = new Utility();
    let config  = Object.assign({
        SYNC_SOURCES:          'http://src1:3006',
        VERIFY_HASHES:         false,
        CLIENT_RECONNECT_DELAY: 5000,
        HASH_CONFIRM_TIMEOUT:  5000,
        SNAPSHOT_MAX_CONTENT:  200 * 1024 * 1024,
        WS_MAX_PAYLOAD:        50 * 1024 * 1024,
        MAX_ROLLBACK_DEPTH:    10,
        GAP_LOG_INTERVAL_MS:   30000
    }, configOverrides || {});
    let sync = new ClientSync('bitcoin', 'mainnet', withDbMixins(db), applier, rb, hv, config, util);
    return { sync, db, applier, rb, hv, util, config };
}
describe('ClientSync: verifyAgainstSource', function(){
    let sync, db;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('returns early when dbType is decoder', async function(){
        ({ sync, db } = makeSync({}, { dbType: 'decoder' }));
        sinon.stub(axios, 'get');

        await sync.verifyAgainstSource('http://src1:3006', 100);

        assert.strictEqual(axios.get.called, false);
    });

    it('logs "Hash verification passed" when hashes match', async function(){
        ({ sync, db } = makeSync({ VERIFY_HASHES: true }));
        db.getBlockHashRow.resolves({
            ledger_hash:   'lh1',
            actions_hash:  'ah1',
            contract_hash: 'ch1'
        });
        sinon.stub(axios, 'get').resolves({ data: {
            ledger_hash:   'lh1',
            actions_hash:  'ah1',
            contract_hash: 'ch1',
            table_counts:  null
        }});

        await sync.verifyAgainstSource('http://src1:3006', 100);

        let logCalls = console.log.getCalls().map(c => c.args[0]);
        assert.ok(logCalls.some(m => m && m.indexOf('Hash verification passed') !== -1));
    });

    it('logs "HASH MISMATCH" when hashes differ', async function(){
        ({ sync, db } = makeSync({ VERIFY_HASHES: true }));
        db.getBlockHashRow.resolves({
            ledger_hash:   'lh-local',
            actions_hash:  'ah-local',
            contract_hash: 'ch-local'
        });
        sinon.stub(axios, 'get').resolves({ data: {
            ledger_hash:   'lh-remote',
            actions_hash:  'ah-remote',
            contract_hash: 'ch-remote',
            table_counts:  null
        }});

        await sync.verifyAgainstSource('http://src1:3006', 100);

        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('HASH MISMATCH') !== -1));
    });
});

describe('ClientSync: verifyAgainstSource', function(){
    let sync, db;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });



    it('returns early when localHashes is null', async function(){
        ({ sync, db } = makeSync({ VERIFY_HASHES: true }));
        db.getBlockHashRow.resolves(null);
        sinon.stub(axios, 'get').resolves({ data: {
            ledger_hash:   'lh1',
            actions_hash:  'ah1',
            contract_hash: 'ch1',
            table_counts:  null
        }});

        await sync.verifyAgainstSource('http://src1:3006', 100);

        // No hash comparison logging
        let logCalls = console.log.getCalls().map(c => c.args[0]);
        assert.ok(!logCalls.some(m => m && m.indexOf('Hash verification passed') !== -1));
    });

    it('calls haltOnDivergence when VERIFY_RECOMPUTE=true and recompute has mismatches', async function(){
        ({ sync, db } = makeSync({ VERIFY_HASHES: true, VERIFY_RECOMPUTE: true }));
        db.getBlockHashRow.resolves({
            ledger_hash:   'lh1',
            actions_hash:  'ah1',
            contract_hash: 'ch1'
        });
        sinon.stub(axios, 'get').resolves({ data: {
            ledger_hash:   'lh1',
            actions_hash:  'ah1',
            contract_hash: 'ch1',
            table_counts:  null
        }});
        sinon.stub(sync, 'verifyRecompute').resolves([{ field: 'ledger_hash', computed: 'X', committed: 'lh1' }]);
        sinon.stub(sync, 'haltOnDivergence').resolves();

        await sync.verifyAgainstSource('http://src1:3006', 100);

        assert.ok(sync.haltOnDivergence.calledOnce, 'haltOnDivergence must be called on recompute mismatch');
    });
});

describe('ClientSync: verifyAgainstSource', function(){
    let sync, db;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });



    it('logs TABLE_COUNT_MISMATCH when source has more rows', async function(){
        ({ sync, db } = makeSync({ VERIFY_HASHES: true }));
        db.getBlockHashRow.resolves({
            ledger_hash:   'lh1',
            actions_hash:  'ah1',
            contract_hash: 'ch1'
        });
        db.getTableCount.resolves(5); // local has 5
        sinon.stub(axios, 'get').resolves({ data: {
            ledger_hash:   'lh1',
            actions_hash:  'ah1',
            contract_hash: 'ch1',
            table_counts:  { blocks: 100 } // remote has 100
        }});

        await sync.verifyAgainstSource('http://src1:3006', 100);

        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('TABLE_COUNT_MISMATCH') !== -1));
    });

    it('logs "Table-count verification passed" when counts match', async function(){
        ({ sync, db } = makeSync({ VERIFY_HASHES: true }));
        db.getBlockHashRow.resolves({
            ledger_hash:   'lh1',
            actions_hash:  'ah1',
            contract_hash: 'ch1'
        });
        db.getTableCount.resolves(100); // local same as remote
        sinon.stub(axios, 'get').resolves({ data: {
            ledger_hash:   'lh1',
            actions_hash:  'ah1',
            contract_hash: 'ch1',
            table_counts:  { blocks: 100 }
        }});

        await sync.verifyAgainstSource('http://src1:3006', 100);

        let logCalls = console.log.getCalls().map(c => c.args[0]);
        assert.ok(logCalls.some(m => m && m.indexOf('Table-count verification passed') !== -1));
    });
});

describe('ClientSync: verifyAgainstSource', function(){
    let sync, db;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });



    it('logs "Hash verification failed" when axios.get rejects', async function(){
        ({ sync, db } = makeSync({ VERIFY_HASHES: true }));
        sinon.stub(axios, 'get').rejects(new Error('net fail'));

        await sync.verifyAgainstSource('http://src1:3006', 100);

        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('Hash verification failed') !== -1));
    });

    it('skips the cross-source hash check on tip skew (no spurious HASH MISMATCH, no halt)', async function(){
        ({ sync, db } = makeSync({ VERIFY_HASHES: true, HALT_ON_DIVERGENCE: true }));
        db.getBlockHashRow.resolves({
            ledger_hash:   'lh-local',
            actions_hash:  'ah-local',
            contract_hash: 'ch-local'
        });
        // Source tip (height 105) is AHEAD of the bootstrap height (100); its hashes
        // describe height 105, so a comparison at 100 would be spurious.
        sinon.stub(axios, 'get').resolves({ data: {
            block_height:  105,
            ledger_hash:   'lh-remote',
            actions_hash:  'ah-remote',
            contract_hash: 'ch-remote',
            table_counts:  null
        }});
        sinon.stub(sync, 'haltOnDivergence').resolves();

        await sync.verifyAgainstSource('http://src1:3006', 100);

        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(!errCalls.some(m => m && m.indexOf('HASH MISMATCH') !== -1),
            'tip skew must not raise a HASH MISMATCH');
        assert.strictEqual(sync.haltOnDivergence.called, false,
            'tip skew must not halt');
    });
});

describe('ClientSync: verifyAgainstSource', function(){
    let sync, db;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });



    it('halts on a confirmed same-height cross-source hash mismatch when HALT_ON_DIVERGENCE is on', async function(){
        ({ sync, db } = makeSync({ VERIFY_HASHES: true, HALT_ON_DIVERGENCE: true }));
        db.getBlockHashRow.resolves({
            ledger_hash:   'lh-local',
            actions_hash:  'ah-local',
            contract_hash: 'ch-local'
        });
        sinon.stub(axios, 'get').resolves({ data: {
            block_height:  100,
            ledger_hash:   'lh-remote',
            actions_hash:  'ah-remote',
            contract_hash: 'ch-remote',
            table_counts:  null
        }});
        sinon.stub(sync, 'haltOnDivergence').resolves();

        await sync.verifyAgainstSource('http://src1:3006', 100);

        assert.ok(sync.haltOnDivergence.calledOnce,
            'same-height mismatch must halt like the live dual-source path');
        assert.strictEqual(sync.haltOnDivergence.firstCall.args[3], 'cross-source-divergence');
    });
});
