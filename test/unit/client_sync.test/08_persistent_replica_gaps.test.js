// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const {
    assert, sinon, axios, ClientSync, createMockDb, registerClientSyncHooks
} = require('./support');

let sync, db, applier, rollback, hashVerifier, config, util;

function assignState(state){
    ({ sync, db, applier, rollback, hashVerifier, config, util } = state);
}

let warn;

// Drive one completed equal-height sweep. The throttle stamp is cleared each
// time so consecutive sweeps are what the test controls, not the clock.
async function sweep(tableCounts){
    axios.get.resolves({ data: { block_height: 100, table_counts: tableCounts } });
    sync._lastCompletenessSweepAt = 0;
    await sync.maybeVerifyCompleteness('http://source1:3006', 100);
}

function errorLines(){
    return console.error.getCalls().map(c => String(c.args[0])).join('\n');
}

function registerPersistentReplicaGapHooks(){
    beforeEach(function(){
        config.COMPLETENESS_CHECK_INTERVAL = 60000;
        sync.lastAppliedBlock = 100;
        db.getTableCount   = sinon.stub().resolves(9);
        db.setSyncState    = sinon.stub().resolves();
        db.getSyncState    = sinon.stub().resolves(null);
        sinon.stub(axios, 'get');
        warn = sinon.stub(console, 'warn');
    });
}

function registerPersistentReplicaGapsGroup1Tests(){
    describe('persistent replica gaps', function(){
        registerPersistentReplicaGapHooks();

        it('does not escalate a shortfall seen on a single sweep', async function(){
            // One sighting can be the local count racing the source /status read.
            await sweep({ blocks: 10 });

            let logged = errorLines();
            assert.ok(/TABLE_COUNT_MISMATCH/.test(logged), 'the detection still fires');
            assert.ok(!/REPLICA_GAP_PERSISTENT/.test(logged),
                'one sighting is not yet evidence of a permanent gap');
            assert.deepStrictEqual(sync.getReplicaGaps(), []);
        });
    });
}

function registerPersistentReplicaGapsGroup2Tests(){
    describe('persistent replica gaps', function(){
        registerPersistentReplicaGapHooks();

        it('escalates a shortfall that survives consecutive equal-height sweeps', async function(){
            await sweep({ blocks: 10 });
            console.error.resetHistory();
            await sweep({ blocks: 10 });

            let logged = errorLines();
            assert.ok(/REPLICA_GAP_PERSISTENT/.test(logged),
                'a gap repeated sweeps do not close must raise louder than the detection line');
            assert.ok(/blocks short 1 row\(s\)/.test(logged), 'names the table and the delta');
            assert.ok(/2 equal-height sweep\(s\)/.test(logged), 'names how long it has survived');

            let gaps = sync.getReplicaGaps();
            assert.strictEqual(gaps.length, 1);
            assert.strictEqual(gaps[0].table, 'blocks');
            assert.strictEqual(gaps[0].delta, 1);
            assert.strictEqual(gaps[0].source_count, 10);
            assert.strictEqual(gaps[0].local_count, 9);
            assert.strictEqual(gaps[0].sweeps, 2);
            assert.strictEqual(gaps[0].last_block, 100);
        });
    });
}

function registerPersistentReplicaGapsGroup3Tests(){
    describe('persistent replica gaps', function(){
        registerPersistentReplicaGapHooks();

        it('records the persistent gap durably and clears it when the gap closes', async function(){
            await sweep({ blocks: 10 });
            await sweep({ blocks: 10 });

            let written = db.setSyncState.getCalls().map(c => [c.args[0], c.args[1]]);
            assert.ok(written.some(([k, v]) => k === 'replica_gap_tables:indexer' && v === 'blocks:1'),
                'a monitor must be able to read the gap without scraping logs');
            assert.ok(written.some(([k, v]) => k === 'replica_gap_sweeps:indexer' && v === '2'));

            db.setSyncState.resetHistory();
            db.getTableCount.resolves(10);           // source and replica agree
            await sweep({ blocks: 10 });

            assert.deepStrictEqual(sync.getReplicaGaps(), []);
            assert.ok(warn.getCalls().some(c => /REPLICA_GAP_CLOSED/.test(String(c.args[0]))),
                'recovery is reported at the same volume as the alert');
            let cleared = db.setSyncState.getCalls().map(c => [c.args[0], c.args[1]]);
            assert.ok(cleared.some(([k, v]) => k === 'replica_gap_tables:indexer' && v === ''),
                'the durable record must not outlive the gap it described');
        });
    });
}

function registerPersistentReplicaGapsGroup4Tests(){
    describe('persistent replica gaps', function(){
        registerPersistentReplicaGapHooks();

        it('rate-limits the alert but re-raises immediately when the gap grows', async function(){
            await sweep({ blocks: 10 });
            await sweep({ blocks: 10 });          // escalation 1
            console.error.resetHistory();
            await sweep({ blocks: 10 });          // inside the repeat window, same delta
            assert.ok(!/REPLICA_GAP_PERSISTENT/.test(errorLines()),
                'an unchanged known gap must not rebuild the wall of repeated lines');

            console.error.resetHistory();
            await sweep({ blocks: 20 });          // delta 1 -> 11 is a new fault
            let logged = errorLines();
            assert.ok(/REPLICA_GAP_PERSISTENT/.test(logged), 'a growing gap re-alerts inside the window');
            assert.ok(/GROWING from 1/.test(logged), 'the alert states the trend');
        });
    });
}

function registerPersistentReplicaGapsGroup5Tests(){
    describe('persistent replica gaps', function(){
        registerPersistentReplicaGapHooks();

        it('reports that the client self-repair pass failed to close a short lookup', async function(){
            // The shape that went unnoticed: the client detects a short append-only
            // lookup, re-pages it, and the gap is still there on the next sweep.
            sinon.stub(sync, 'syncLookupTablesPaged').resolves();
            await sweep({ index_transactions: 10 });
            await sweep({ index_transactions: 10 });

            let logged = errorLines();
            assert.ok(/REPLICA_GAP_PERSISTENT/.test(logged));
            assert.ok(/self-repair pass\(es\) did NOT close it/.test(logged),
                'an exhausted repair path is the loudest part of the verdict');
        });
    });
}

function registerPersistentReplicaGapsGroup6Tests(){
    describe('persistent replica gaps', function(){
        registerPersistentReplicaGapHooks();

        it('does not clear a tracked gap on a sweep that never completed', async function(){
            await sweep({ blocks: 10 });
            await sweep({ blocks: 10 });
            assert.strictEqual(sync.getReplicaGaps().length, 1);

            axios.get.rejects(new Error('ECONNREFUSED'));
            sync._lastCompletenessSweepAt = 0;
            await sync.maybeVerifyCompleteness('http://source1:3006', 100);

            assert.strictEqual(sync.getReplicaGaps().length, 1,
                'an unreachable source is not evidence the gap closed');
        });
    });
}

function registerPersistentReplicaGapsGroup7Tests(){
    describe('persistent replica gaps', function(){
        registerPersistentReplicaGapHooks();

        it('ages decoder shortfalls from the periodic path only', async function(){
            let decoderDb = createMockDb();
            decoderDb.dbType        = 'decoder';
            decoderDb.getTableCount = sinon.stub().resolves(9);
            decoderDb.setSyncState  = sinon.stub().resolves();
            let decoderSync = new ClientSync('bitcoin', 'mainnet', decoderDb, applier, rollback,
                hashVerifier, config, util);
            decoderSync.dbType = 'decoder';
            decoderSync.lastAppliedBlock = 100;
            axios.get.resolves({ data: { block_height: 100, table_counts: { blocks: 10 } } });

            // Bootstrap-time checks run the same comparison mid-dump: they must not
            // count toward persistence.
            await decoderSync.verifyDecoderCompleteness('http://source1:3006', 100);
            await decoderSync.verifyDecoderCompleteness('http://source1:3006', 100);
            assert.deepStrictEqual(decoderSync.getReplicaGaps(), []);

            decoderSync._lastCompletenessSweepAt = 0;
            await decoderSync.maybeVerifyCompleteness('http://source1:3006', 100);
            decoderSync._lastCompletenessSweepAt = 0;
            await decoderSync.maybeVerifyCompleteness('http://source1:3006', 100);
            assert.strictEqual(decoderSync.getReplicaGaps().length, 1);
            assert.ok(/REPLICA_GAP_PERSISTENT/.test(errorLines()));
        });
    });
}

describe('ClientSync', function(){
    registerClientSyncHooks(assignState);
    registerPersistentReplicaGapsGroup1Tests();
    registerPersistentReplicaGapsGroup2Tests();
    registerPersistentReplicaGapsGroup3Tests();
    registerPersistentReplicaGapsGroup4Tests();
    registerPersistentReplicaGapsGroup5Tests();
    registerPersistentReplicaGapsGroup6Tests();
    registerPersistentReplicaGapsGroup7Tests();
});
