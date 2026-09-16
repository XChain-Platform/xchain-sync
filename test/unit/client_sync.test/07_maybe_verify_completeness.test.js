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

function registerMaybeVerifyCompletenessTests(){
    describe('maybeVerifyCompleteness', function(){
        beforeEach(function(){
            config.COMPLETENESS_CHECK_INTERVAL = 60000;
            sync.lastAppliedBlock = 100;
            db.getTableCount = sinon.stub().resolves(9);
            sinon.stub(axios, 'get').resolves({
                data: { block_height: 100, table_counts: { blocks: 10 } }
            });
        });

        it('reports a shortfall against the primary source at equal heights', async function(){
            await sync.maybeVerifyCompleteness('http://source1:3006', 100);

            assert.strictEqual(axios.get.calledOnce, true);
            assert.ok(/\/status\/indexer\/bitcoin\/mainnet$/.test(axios.get.firstCall.args[0]));
            let logged = console.error.getCalls().map(c => String(c.args[0])).join('\n');
            assert.ok(/TABLE_COUNT_MISMATCH/.test(logged),
                'a follower short rows the hashes cannot cover must be reported');
        });

        it('does not sweep while the replica is behind the source', async function(){
            // A shortfall while behind is ordinary lag: reporting it would train
            // operators to ignore the one signal this check exists to give them.
            await sync.maybeVerifyCompleteness('http://source1:3006', 140);
            assert.strictEqual(axios.get.called, false);
        });

        it('throttles to COMPLETENESS_CHECK_INTERVAL', async function(){
            await sync.maybeVerifyCompleteness('http://source1:3006', 100);
            await sync.maybeVerifyCompleteness('http://source1:3006', 100);
            assert.strictEqual(axios.get.callCount, 1, 'the second tick inside the window must not re-sweep');
        });

        it('is inert when the interval is 0', async function(){
            config.COMPLETENESS_CHECK_INTERVAL = 0;
            await sync.maybeVerifyCompleteness('http://source1:3006', 100);
            assert.strictEqual(axios.get.called, false);
        });

        it('does not sweep once halted on a divergence', async function(){
            sync._halted = { blockIndex: 100, reason: 'test' };
            await sync.maybeVerifyCompleteness('http://source1:3006', 100);
            assert.strictEqual(axios.get.called, false);
        });

        it('logs and continues when the source is unreachable', async function(){
            axios.get.rejects(new Error('ECONNREFUSED'));
            await sync.maybeVerifyCompleteness('http://source1:3006', 100);
            let logged = console.error.getCalls().map(c => String(c.args[0])).join('\n');
            assert.ok(/Periodic completeness sweep failed/.test(logged));
        });
    });
}

describe('ClientSync', function(){
    registerClientSyncHooks(assignState);
    registerMaybeVerifyCompletenessTests();
});
