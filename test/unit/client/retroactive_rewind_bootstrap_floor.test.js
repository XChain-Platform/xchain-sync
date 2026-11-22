// Copyright © 2025-2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

const assert = require('assert');
const sinon = require('sinon');
const ClientSync = require('../../../src/client/sync');
const Utility = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');

const RULES = 'a'.repeat(64);

function createMockDb(lastAppliedBlock){
    return {
        dbName: 'test_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(lastAppliedBlock),
        getBlockHashRow: sinon.stub().resolves(null),
        getActiveHalt: sinon.stub().resolves(null),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        getSyncState: sinon.stub().resolves(null),
        setSyncState: sinon.stub().resolves(true),
        doQuery: sinon.stub().resolves([])
    };
}

function makeSync(lastAppliedBlock = 500, bootstrapDepth = 0){
    const db = createMockDb(lastAppliedBlock);
    const rollback = { rollback: sinon.stub().resolves() };
    const config = {
        SYNC_SOURCES: 'source-a,source-b,source-c', SOURCE_QUORUM: 2,
        VERIFY_HASHES: true, VERIFY_RECOMPUTE: true,
        MAX_ROLLBACK_DEPTH: 10, MAX_ROLLBACK_DEPTH_EXPLICIT: true,
        RETROACTIVE_REWIND_CAP_HOURS: 72, HASH_CONFIRM_TIMEOUT: 50,
        SYNC_BOOTSTRAP_DEPTH: bootstrapDepth ? { 'BTC:MAINNET': bootstrapDepth } : {}
    };
    const sync = new ClientSync('bitcoin', 'mainnet', db,
        { applyBlock: sinon.stub().resolves(), applyFullSnapshot: sinon.stub().resolves() },
        rollback, new HashVerifier(), config, new Utility());
    sync.lastAppliedBlock = lastAppliedBlock;
    return { sync, db, rollback };
}

function retroactiveEvent(target){
    return { type: 'reorg', block_index: target, retroactive: { target, rules_to: RULES } };
}

async function deliverQuorum(sync, target){
    const event = retroactiveEvent(target);
    await sync.handleEvent(event, 0);
    await sync.handleEvent(event, 1);
}

function setBootstrapBase(sync, base){
    Object.defineProperty(sync, '_bootstrapBase', { value: base, writable: true });
}

describe('ClientSync truncated rewind floor', function(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'warn');
        sinon.stub(console, 'error');
    });

    afterEach(function(){ sinon.restore(); });

    for(const target of [420, 450]){
        it('halts before changing rows when target ' + target + ' is not above the floor', async function(){
            const { sync, db, rollback } = makeSync();
            setBootstrapBase(sync, 450);
            await deliverQuorum(sync, target);
            assert.strictEqual(rollback.rollback.called, false);
            assert.strictEqual(db.setSyncState.called, false);
            assert.strictEqual(sync.lastAppliedBlock, 500);
            assert.strictEqual(sync.getHaltInfo().reason, 'rewind-below-bootstrap-floor');
        });
    }

    it('applies a target immediately above the held floor', async function(){
        const { sync, rollback } = makeSync();
        setBootstrapBase(sync, 450);
        await deliverQuorum(sync, 451);
        assert.strictEqual(rollback.rollback.calledOnceWithExactly(451), true);
        assert.strictEqual(sync.lastAppliedBlock, 450);
        assert.strictEqual(sync.isHalted(), false);
    });

    it('keeps full-history retroactive rewind behavior', async function(){
        const { sync, rollback } = makeSync();
        await deliverQuorum(sync, 420);
        assert.strictEqual(rollback.rollback.calledOnceWithExactly(420), true);
        assert.strictEqual(sync.lastAppliedBlock, 419);
    });

    it('warns only when the configured held window is within the retroactive cap', function(){
        makeSync(500, 20);
        assert.strictEqual(console.warn.getCalls().some(c =>
            c.args.join(' ').includes('rewind-below-bootstrap-floor')), true);
        console.warn.resetHistory();
        makeSync(500, 500);
        assert.strictEqual(console.warn.called, false);
    });
});
