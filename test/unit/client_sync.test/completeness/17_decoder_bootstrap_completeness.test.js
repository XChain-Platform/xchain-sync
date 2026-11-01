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
} = require('../support');

let sync, db, applier, rollback, hashVerifier, config, util;

function assignState(state){
    ({ sync, db, applier, rollback, hashVerifier, config, util } = state);
}

// Regression guard: a truncated/stale decoder full snapshot must not be
// accepted silently. The indexer-only hash path (verifyAgainstSource)
// short-circuits for decoder, so bootstrap must run a row-count cross-check
// against the second source independent of the VERIFY_HASHES flag.

function decoderSync(cfg){
    let decoderDb = createMockDb();
    decoderDb.dbType = 'decoder';
    let s = new ClientSync('bitcoin', 'mainnet', decoderDb, applier, rollback, hashVerifier, cfg, util);
    return { s, decoderDb };
}

function registerVerifyDecoderCompletenessTests(){
    describe('decoder bootstrap completeness', function(){
        describe('verifyDecoderCompleteness', function(){
            it('flags a truncated snapshot loudly when the source has more rows', async function(){
                let { s, decoderDb } = decoderSync(config);
                decoderDb.getTableCount = async (t) => ({ blocks: 100, transactions: 0 })[t];
                sinon.stub(axios, 'get').resolves({ data: { table_counts: { blocks: 100, transactions: 4200 } } });

                await s.verifyDecoderCompleteness('http://source2:3006', 500);

                // The shortfall must surface as a loud TABLE_COUNT_MISMATCH, not be swallowed.
                let logged = console.error.getCalls().some(c =>
                    typeof c.args[0] === 'string' && c.args[0].indexOf('TABLE_COUNT_MISMATCH') !== -1);
                assert.strictEqual(logged, true);
            });

            it('passes quietly when the follower is complete', async function(){
                let { s, decoderDb } = decoderSync(config);
                decoderDb.getTableCount = async (t) => ({ blocks: 100, transactions: 4200 })[t];
                sinon.stub(axios, 'get').resolves({ data: { table_counts: { blocks: 100, transactions: 4200 } } });

                await s.verifyDecoderCompleteness('http://source2:3006', 500);

                let mismatch = console.error.getCalls().some(c =>
                    typeof c.args[0] === 'string' && c.args[0].indexOf('TABLE_COUNT_MISMATCH') !== -1);
                assert.strictEqual(mismatch, false);
            });

            it('is a no-op for non-decoder dbType', async function(){
                // sync is the default indexer instance from the outer beforeEach.
                sinon.stub(axios, 'get').resolves({ data: { table_counts: { blocks: 9 } } });
                await sync.verifyDecoderCompleteness('http://source2:3006', 500);
                assert.strictEqual(axios.get.called, false);
            });
        });
    });
}

function registerBootstrapFromSnapshotWiringTests(){
    describe('decoder bootstrap completeness', function(){
        describe('bootstrapFromSnapshot wiring', function(){
            it('runs the decoder completeness check even when VERIFY_HASHES is false', async function(){
                let cfg = Object.assign({}, config, { VERIFY_HASHES: false });
                let { s } = decoderSync(cfg);
                sinon.stub(s, 'fetchAndApplySchema').resolves();
                sinon.stub(s, 'verifyDecoderCompleteness').resolves();
                sinon.stub(s, 'verifyAgainstSource').resolves();
                sinon.stub(axios, 'get').resolves({ data: Buffer.from(JSON.stringify({ block_height: 500 })) });

                await s.bootstrapFromSnapshot();

                assert.strictEqual(s.verifyDecoderCompleteness.calledOnce, true,
                    'decoder completeness check must run regardless of VERIFY_HASHES');
                assert.strictEqual(s.verifyDecoderCompleteness.firstCall.args[0], 'http://source2:3006');
                assert.strictEqual(s.verifyDecoderCompleteness.firstCall.args[1], 500);
                assert.deepStrictEqual([...s.verifyDecoderCompleteness.firstCall.args[2]], ['dispensers']);
                // The indexer-only hash path must never run for decoder.
                assert.strictEqual(s.verifyAgainstSource.called, false);
            });

            it('ignores pre-reconcile dispenser drift during snapshot verification', async function(){
                let { s, decoderDb } = decoderSync(config);
                s.lastAppliedBlock = 500;
                decoderDb.getTableCount = async (table) => ({ blocks: 100, dispensers: 10 })[table];
                sinon.stub(axios, 'get').resolves({ data: {
                    block_height: 500, table_counts: { blocks: 100, dispensers: 12 }
                } });

                assert.strictEqual(await s.bootstrapVerifySnapshot(), true);
                const logged = console.error.getCalls().some(call =>
                    String(call.args[0]).includes('TABLE_COUNT_MISMATCH'));
                assert.strictEqual(logged, false);
            });

            it('still reports non-dispenser drift during snapshot verification', async function(){
                let { s, decoderDb } = decoderSync(config);
                s.lastAppliedBlock = 500;
                decoderDb.getTableCount = async (table) => ({ blocks: 100, transactions: 9 })[table];
                sinon.stub(axios, 'get').resolves({ data: {
                    block_height: 500, table_counts: { blocks: 100, transactions: 10 }
                } });

                assert.strictEqual(await s.bootstrapVerifySnapshot(), true);
                const logged = console.error.getCalls().some(call =>
                    String(call.args[0]).includes('TABLE_COUNT_MISMATCH'));
                assert.strictEqual(logged, true);
            });
        });
    });
}

function registerBootstrapFromSnapshotFallbackTests(){
    describe('decoder bootstrap completeness', function(){
        describe('bootstrapFromSnapshot wiring', function(){

            it('tries the next decoder source when an alternate cannot be read', async function(){
                const cfg = Object.assign({}, config, {
                    SYNC_SOURCES: 'http://source1:3006,http://source2:3006,http://source3:3006'
                });
                const { s } = decoderSync(cfg);
                s.lastAppliedBlock = 500;
                const verify = sinon.stub(s, 'verifyDecoderCompleteness');
                verify.onFirstCall().resolves(null);
                verify.onSecondCall().resolves([]);

                assert.strictEqual(await s.bootstrapVerifySnapshot(1), true);
                assert.deepStrictEqual(verify.getCalls().map(call => call.args[0]),
                    ['http://source3:3006', 'http://source1:3006']);
            });

            it('does not run the decoder check in single-source mode', async function(){
                let cfg = Object.assign({}, config, { SYNC_SOURCES: 'http://source1:3006' });
                let { s } = decoderSync(cfg);
                sinon.stub(s, 'fetchAndApplySchema').resolves();
                sinon.stub(s, 'verifyDecoderCompleteness').resolves();
                sinon.stub(axios, 'get').resolves({ data: Buffer.from(JSON.stringify({ block_height: 500 })) });

                await s.bootstrapFromSnapshot();

                assert.strictEqual(s.verifyDecoderCompleteness.called, false);
            });

            it('takes the indexer hash path (not the decoder check) for indexer dbType', async function(){
                sinon.stub(sync, 'fetchAndApplySchema').resolves();
                sinon.stub(sync, 'verifyAgainstSource').resolves();
                sinon.stub(sync, 'verifyDecoderCompleteness').resolves();
                sinon.stub(axios, 'get').resolves({ data: Buffer.from(JSON.stringify({ block_height: 500 })) });

                await sync.bootstrapFromSnapshot();

                assert.strictEqual(sync.verifyAgainstSource.calledOnce, true);
                assert.strictEqual(sync.verifyDecoderCompleteness.called, false);
            });
        });
    });
}

describe('ClientSync', function(){
    registerClientSyncHooks(assignState);
    registerVerifyDecoderCompletenessTests();
    registerBootstrapFromSnapshotWiringTests();
    registerBootstrapFromSnapshotFallbackTests();
});
