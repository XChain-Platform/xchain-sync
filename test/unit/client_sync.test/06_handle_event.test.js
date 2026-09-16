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


// The server publishes its own replication verdict on every status tick. Dropping
// it left this follower's lag_blocks certifying a server whose SQL replica had
// stopped applying: its heights freeze together, so we catch up to the frozen tip
// and the heartbeats keep source_height_stale false.
function registerUpstreamReplicationHook(){
    beforeEach(function(){
        sync.lastAppliedBlock = 100;
        sinon.stub(sync, 'maybeVerifyCompleteness').resolves();
    });
}

function registerHandleEventGroup1Tests(){
    describe('handleEvent', function(){
        it('routes block events to handleBlock', async function(){
            sinon.stub(sync, 'handleBlock').resolves();
            await sync.handleEvent({ type: 'block', block_index: 10 }, 0);
            assert.strictEqual(sync.handleBlock.calledOnce, true);
        });

        it('routes reorg events to handleReorg', async function(){
            sinon.stub(sync, 'handleReorg').resolves();
            await sync.handleEvent({ type: 'reorg', block_index: 10 }, 0);
            assert.strictEqual(sync.handleReorg.calledOnce, true);
        });

        it('detects gap on status event and triggers catch-up', async function(){
            sync.lastAppliedBlock = 50;
            sinon.stub(sync, 'incrementalCatchUp').resolves();
            await sync.handleEvent({ type: 'status', block_height: 55 }, 0);
            assert.strictEqual(sync.incrementalCatchUp.calledOnce, true);
            assert.strictEqual(sync.incrementalCatchUp.firstCall.args[0], 51);
        });

        it('does not trigger catch-up when no gap', async function(){
            sync.lastAppliedBlock = 50;
            sinon.stub(sync, 'incrementalCatchUp').resolves();
            await sync.handleEvent({ type: 'status', block_height: 51 }, 0);
            assert.strictEqual(sync.incrementalCatchUp.called, false);
        });

        it('does not trigger catch-up when lastAppliedBlock is null', async function(){
            sync.lastAppliedBlock = null;
            sinon.stub(sync, 'incrementalCatchUp').resolves();
            await sync.handleEvent({ type: 'status', block_height: 100 }, 0);
            assert.strictEqual(sync.incrementalCatchUp.called, false);
        });

        it('runs the completeness sweep against the source that sent the status tick @regression', async function(){
            // The status tick is the one recurring signal from the client's OWN primary,
            // and the bootstrap caller's loop over sources[1..] never reaches that source,
            // so a single-source replica runs no completeness check without this wiring.
            config.COMPLETENESS_CHECK_INTERVAL = 60000;
            sync.lastAppliedBlock = 100;
            sinon.stub(sync, 'maybeVerifyCompleteness').resolves();

            await sync.handleEvent({ type: 'status', block_height: 100 }, 0);

            assert.strictEqual(sync.maybeVerifyCompleteness.calledOnce, true);
            assert.strictEqual(sync.maybeVerifyCompleteness.firstCall.args[0], 'http://source1:3006');
        });
    });
}

function registerHandleEventGroup2Tests(){
    describe('handleEvent', function(){
        describe('upstream replication evidence', function(){
            registerUpstreamReplicationHook();
            it('is unknown, not fresh, before any status event', function(){
                assert.deepStrictEqual(sync.getUpstreamReplicaState(),
                    { stale: null, secondsBehind: null, sourceHeight: null });
            });

            it('keeps the source height, staleness verdict and lag from a status event', async function(){
                await sync.handleEvent({
                    type: 'status', block_height: 100, source_block_height: 140,
                    replica_stale: true, replica_seconds_behind: 900
                }, 0);
                assert.deepStrictEqual(sync.getUpstreamReplicaState(),
                    { stale: true, secondsBehind: 900, sourceHeight: 140 });
            });

            it('re-reads the verdict on a status tick that does not advance the height', async function(){
                await sync.handleEvent({
                    type: 'status', block_height: 100, source_block_height: 100,
                    replica_stale: false, replica_seconds_behind: 2
                }, 0);
                assert.strictEqual(sync.getUpstreamReplicaState().stale, false);

                // The upstream replica stops applying: its height never moves again.
                await sync.handleEvent({
                    type: 'status', block_height: 100, source_block_height: 100,
                    replica_stale: true, replica_seconds_behind: null
                }, 0);
                assert.strictEqual(sync.getUpstreamReplicaState().stale, true);
            });
        });
    });
}

function registerHandleEventGroup3Tests(){
    describe('handleEvent', function(){
        describe('upstream replication evidence', function(){
            registerUpstreamReplicationHook();
            it('reads a server older than the fields as unknown rather than fresh', async function(){
                await sync.handleEvent({ type: 'status', block_height: 100 }, 0);
                assert.deepStrictEqual(sync.getUpstreamReplicaState(),
                    { stale: null, secondsBehind: null, sourceHeight: null });
            });

            it('takes the worst verdict across sources and ignores an evicted one', async function(){
                await sync.handleEvent({
                    type: 'status', block_height: 100, source_block_height: 100,
                    replica_stale: false, replica_seconds_behind: 1
                }, 0);
                await sync.handleEvent({
                    type: 'status', block_height: 100, source_block_height: 130,
                    replica_stale: true, replica_seconds_behind: 700
                }, 1);
                assert.deepStrictEqual(sync.getUpstreamReplicaState(),
                    { stale: true, secondsBehind: 700, sourceHeight: 130 });

                sync._evictedSources.add(1);
                assert.deepStrictEqual(sync.getUpstreamReplicaState(),
                    { stale: false, secondsBehind: 1, sourceHeight: 100 });
            });
        });
    });
}

describe('ClientSync', function(){
    registerClientSyncHooks(assignState);
    registerHandleEventGroup1Tests();
    registerHandleEventGroup2Tests();
    registerHandleEventGroup3Tests();
});
