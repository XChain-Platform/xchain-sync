// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// A source's /status labels block_height with its poller's broadcast height, while
// table_counts are counted on the live database. The counts carry their own height
// (table_counts_height), and every follower count comparison gates on that label.

const {
    assert, sinon, axios, ClientSync, createMockDb, registerClientSyncHooks
} = require('../support');

let sync, db, applier, rollback, hashVerifier, config, util;

function assignState(state){
    ({ sync, db, applier, rollback, hashVerifier, config, util } = state);
}

function errorLines(){
    return console.error.getCalls().map(c => String(c.args[0])).join('\n');
}

function infoLines(){
    return console.log.getCalls().map(c => String(c.args[0])).join('\n');
}

// Drive one completed indexer sweep at local height 100 against the given status.
async function sweep(status){
    axios.get.resolves({ data: status });
    sync._lastCompletenessSweepAt = 0;
    await sync.maybeVerifyCompleteness('http://source1:3006', 100);
}

// Seed one tracked gap so a sweep that wrongly ran (or wrongly cleared) is visible.
function seedGap(){
    sync._replicaGaps.set('blocks', {
        table: 'blocks', firstSeenAt: 1, firstDelta: 1, worstDelta: 1, sweeps: 1,
        alerts: 0, lastAlertAt: 0, repairAttempts: 0, lastDelta: 1, sourceCount: 10, localCount: 9
    });
}

function registerHooks(){
    beforeEach(function(){
        config.COMPLETENESS_CHECK_INTERVAL = 60000;
        sync.lastAppliedBlock = 100;
        db.getTableCount = sinon.stub().resolves(9);
        db.setSyncState  = sinon.stub().resolves();
        db.getSyncState  = sinon.stub().resolves(null);
        sinon.stub(axios, 'get');
        sinon.stub(console, 'warn');
    });
}

function registerHeightReadingTests(){
    describe('remoteTableCountsHeight', function(){
        registerHooks();

        it('reads the published label, and null when the source saw the tip move mid-count', function(){
            assert.strictEqual(sync.remoteTableCountsHeight({ block_height: 99, table_counts_height: 100 }), 100);
            assert.strictEqual(sync.remoteTableCountsHeight({ block_height: 100, table_counts_height: null }), null);
        });

        it('trusts an unlabelled source at block_height only while its own tip equals it', function(){
            assert.strictEqual(sync.remoteTableCountsHeight({ block_height: 100 }), 100);
            assert.strictEqual(sync.remoteTableCountsHeight({ block_height: 100, source_height: 100 }), 100);
            assert.strictEqual(sync.remoteTableCountsHeight({ block_height: 100, source_height: 103 }), null);
            assert.strictEqual(sync.remoteTableCountsHeight({ table_counts: {} }), null);
        });
    });
}

function registerIndexerSweepTests(){
    describe('indexer completeness sweep and the counts label', function(){
        registerHooks();

        it('skips counts read past the follower height and leaves the tracked gap untouched', async function(){
            seedGap();
            let pager = sinon.stub(sync, 'syncLookupTablesPaged').resolves();
            await sweep({ block_height: 100, table_counts_height: 103, table_counts: { blocks: 10 } });

            assert.ok(!/TABLE_COUNT_MISMATCH/.test(errorLines()),
                'counts taken three blocks ahead are lag, not a hole');
            assert.strictEqual(pager.called, false, 'no lookup re-page from id 0 on a skipped sweep');
            assert.strictEqual(sync._replicaGaps.get('blocks').sweeps, 1, 'a skipped sweep neither ages nor clears');
        });

        it('skips counts the source could not label (a block landed mid-count)', async function(){
            seedGap();
            await sweep({ block_height: 100, table_counts_height: null, table_counts: { blocks: 10 } });
            assert.ok(!/TABLE_COUNT_MISMATCH/.test(errorLines()));
            assert.strictEqual(sync._replicaGaps.get('blocks').sweeps, 1);
        });

        it('compares and ages the gap when the counts were read at the follower height', async function(){
            // Negative control for the two skips above: the same seeded gap and payload
            // with an honest label must report and age, or the skips prove nothing.
            seedGap();
            await sweep({ block_height: 100, table_counts_height: 100, table_counts: { blocks: 10 } });
            assert.ok(/TABLE_COUNT_MISMATCH/.test(errorLines()));
            assert.strictEqual(sync._replicaGaps.get('blocks').sweeps, 2);
        });

        it('skips an unlabelled source whose own tip is past its broadcast height', async function(){
            seedGap();
            await sweep({ block_height: 100, source_height: 102, table_counts: { blocks: 10 } });
            assert.ok(!/TABLE_COUNT_MISMATCH/.test(errorLines()));
            assert.strictEqual(sync._replicaGaps.get('blocks').sweeps, 1);
        });

        it('still compares an unlabelled source that publishes no source_height', async function(){
            await sweep({ block_height: 100, table_counts: { blocks: 10 } });
            assert.ok(/TABLE_COUNT_MISMATCH/.test(errorLines()));
        });

        it('reports replica-ahead only against counts read at the follower height', async function(){
            db.getTableCount = sinon.stub().callsFake(async t => (t === 'validator_rewards' ? 12 : 10));
            await sweep({ block_height: 100, table_counts_height: 100, table_counts: { validator_rewards: 10 } });
            assert.ok(/TABLE_COUNT_REPLICA_AHEAD/.test(errorLines()), 'control: equal labels report it');

            console.error.resetHistory();
            let mismatches = await sync.verifyCompletenessCounts(
                { block_height: 100, table_counts_height: 98, table_counts: { validator_rewards: 10 } });
            assert.deepStrictEqual(mismatches, [], 'a source rollback the poller has not seen is not replica-ahead');
        });
    });
}

function registerVerifyAgainstSourceTests(){
    describe('verifyAgainstSource and the counts label', function(){
        registerHooks();

        beforeEach(function(){
            db.getBlockHashRow = sinon.stub().resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
        });

        it('skips counts read at another height, with no shortfall and no pass line', async function(){
            axios.get.resolves({ data: { block_height: 100, table_counts_height: 101,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c', table_counts: { blocks: 10 } } });
            await sync.verifyAgainstSource('http://source1:3006', 100);
            assert.ok(!/TABLE_COUNT_MISMATCH/.test(errorLines()));
            assert.ok(!/Table-count verification passed/.test(infoLines()), 'a skip must not read as a pass');
            assert.ok(/Table-count check against http:\/\/source1:3006 skipped/.test(infoLines()));
        });

        it('compares counts read at the follower height', async function(){
            axios.get.resolves({ data: { block_height: 100, table_counts_height: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c', table_counts: { blocks: 10 } } });
            await sync.verifyAgainstSource('http://source1:3006', 100);
            assert.ok(/TABLE_COUNT_MISMATCH/.test(errorLines()));
        });
    });
}

function registerDecoderSweepTests(){
    describe('decoder completeness sweep and the counts label', function(){
        registerHooks();

        function decoderClient(){
            let decoderDb = createMockDb();
            decoderDb.dbType        = 'decoder';
            decoderDb.getTableCount = sinon.stub().resolves(9);
            decoderDb.setSyncState  = sinon.stub().resolves();
            let decoderSync = new ClientSync('bitcoin', 'mainnet', decoderDb, applier, rollback,
                hashVerifier, config, util);
            decoderSync.dbType = 'decoder';
            decoderSync.lastAppliedBlock = 100;
            return decoderSync;
        }

        it('returns no reading for counts read past the replica height', async function(){
            let decoderSync = decoderClient();
            axios.get.resolves({ data: { block_height: 100, table_counts_height: 104, table_counts: { blocks: 10 } } });
            let res = await decoderSync.verifyDecoderCompleteness('http://source1:3006', 100,
                new Set(['dispensers']), { requireEqualHeight: true });
            assert.strictEqual(res, null, 'null keeps the periodic caller from aging or clearing gaps');
            assert.ok(/source counts read at 104, replica at 100/.test(infoLines()));
        });

        it('returns the shortfalls for counts read at the replica height', async function(){
            let decoderSync = decoderClient();
            axios.get.resolves({ data: { block_height: 100, table_counts_height: 100, table_counts: { blocks: 10 } } });
            let res = await decoderSync.verifyDecoderCompleteness('http://source1:3006', 100,
                new Set(['dispensers']), { requireEqualHeight: true });
            assert.ok(Array.isArray(res) && res.length === 1 && res[0].table === 'blocks');
        });
    });
}

describe('ClientSync', function(){
    registerClientSyncHooks(assignState);
    registerHeightReadingTests();
    registerIndexerSweepTests();
    registerVerifyAgainstSourceTests();
    registerDecoderSweepTests();
});
