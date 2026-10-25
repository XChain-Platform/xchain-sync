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
const ClientSync = require('../../../src/client/sync');
const Utility = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');

const RULES_A = 'a'.repeat(64);
const RULES_B = 'b'.repeat(64);

function createMockDb(){
    return {
        dbName: 'test_db',
        dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getActiveHalt: sinon.stub().resolves(null),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        getSyncState: sinon.stub().resolves(null),
        setSyncState: sinon.stub().resolves(true),
        doQuery: sinon.stub().resolves([])
    };
}

function retroactiveEvent(target, rulesTo = RULES_A){
    return {
        type: 'reorg',
        block_index: target,
        retroactive: { target, rules_to: rulesTo }
    };
}

function makeSync(lastAppliedBlock = 500){
    const db = createMockDb();
    db.getSyncState.resolves(String(lastAppliedBlock));
    const rollback = { rollback: sinon.stub().resolves() };
    const config = {
        SYNC_SOURCES: 'http://a:3006,http://b:3006,http://c:3006',
        SOURCE_QUORUM: 2,
        VERIFY_HASHES: true,
        VERIFY_RECOMPUTE: true,
        MAX_ROLLBACK_DEPTH: 10,
        MAX_ROLLBACK_DEPTH_EXPLICIT: true,
        RETROACTIVE_REWIND_CAP_HOURS: 72,
        HASH_CONFIRM_TIMEOUT: 50
    };
    const sync = new ClientSync('bitcoin', 'mainnet', db,
        { applyBlock: sinon.stub().resolves(), applyFullSnapshot: sinon.stub().resolves() },
        rollback, new HashVerifier(), config, new Utility());
    sync.lastAppliedBlock = lastAppliedBlock;
    return { sync, db, rollback };
}

describe('ClientSync retroactive rewind admission @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'warn');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    it('applies an in-cap quorum-matched rewind after lowering verified_tip', async function(){
        const { sync, db, rollback } = makeSync();
        const event = retroactiveEvent(100);

        await sync.handleEvent(event, 0);
        await sync.handleEvent(event, 1);

        assert.strictEqual(rollback.rollback.calledOnceWithExactly(100), true);
        assert.strictEqual(sync.lastAppliedBlock, 99);
        assert.deepStrictEqual(db.setSyncState.firstCall.args,
            ['verified_tip:indexer', '99', { rethrow: true }]);
        assert.strictEqual(db.setSyncState.firstCall.calledBefore(rollback.rollback.firstCall), true);
    });
});

describe('ClientSync retroactive rewind admission @regression', function(){
    let clock;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'warn');
        sinon.stub(console, 'error');
        clock = sinon.useFakeTimers();
    });

    afterEach(function(){
        clock.restore();
        sinon.restore();
    });

    it('leaves a lone in-cap retroactive report unapplied after the normal timeout', async function(){
        const { sync, rollback } = makeSync();

        await sync.handleEvent(retroactiveEvent(100), 0);
        await clock.tickAsync(100);

        assert.strictEqual(rollback.rollback.called, false);
        assert.strictEqual(sync.lastAppliedBlock, 500);
        assert.strictEqual(sync.isHalted(), false);
    });

    it('leaves reports with mismatched rules_to unapplied', async function(){
        const { sync, rollback } = makeSync();

        await sync.handleEvent(retroactiveEvent(100, RULES_A), 0);
        await sync.handleEvent(retroactiveEvent(100, RULES_B), 1);
        await clock.tickAsync(100);

        assert.strictEqual(rollback.rollback.called, false);
        assert.strictEqual(sync.lastAppliedBlock, 500);
        assert.strictEqual(sync.isHalted(), false);
    });

    it('halts an over-cap quorum-matched rewind under the existing max-depth reason', async function(){
        const { sync, rollback } = makeSync(1000);
        const event = retroactiveEvent(100);

        await sync.handleEvent(event, 0);
        await sync.handleEvent(event, 1);

        assert.strictEqual(rollback.rollback.called, false);
        assert.strictEqual(sync.lastAppliedBlock, 1000);
        assert.strictEqual(sync.getHaltInfo().reason, 'max-rollback-depth-exceeded');
        assert.strictEqual(sync.getHaltInfo().mismatches[0].depth, 901);
    });
});
