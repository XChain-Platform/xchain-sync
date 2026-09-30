/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 ********************************************************************/

'use strict';

const assert = require('assert');
const sinon  = require('sinon');
const ClientSync = require('../../../src/client/sync');
const Utility = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');
const { classDigests } = require('../../../src/client/state_hash_classes');

function createMockDb(){
    return {
        dbName: 'test_db',
        dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        getActiveHalt: sinon.stub().resolves(null),
        clearHalt: sinon.stub().resolves(1)
    };
}

let sync, db, applier, util;

async function recordsEveryLocalPreimageClass(){
    let preimage = {
        credits: [{ action_index: 1 }, { action_index: 2 }],
        poll_finalize: [],
        block_index: 200,
        state_hash_version: 1
    };
    sync.blockHasher.computeStateHash = sinon.stub().resolves('LOCAL_STATE');
    sync.blockHasher.computeStateHashPreimage = sinon.stub().resolves(preimage);

    await sync.applyBlockEvent({ block_index: 200, block_time: 1, state_hash: 'SOURCE_STATE' });

    assert.strictEqual(sync.isHalted(), true);
    assert.ok(db.recordHalt.calledOnce);
    let mismatch = db.recordHalt.firstCall.args[3][0];
    assert.deepStrictEqual(mismatch.local_classes,
        Object.keys(preimage).map(key => ({
            class: key,
            rows: Array.isArray(preimage[key]) ? preimage[key].length : null,
            digest: util.getDataHash({ class: key, value: preimage[key] }).slice(0, 16)
        })));
    for(let detail of mismatch.local_classes)
        assert.match(detail.digest, /^[0-9a-f]{16}$/);
    let classLogs = console.error.getCalls().filter(call =>
        String(call.args[0]).startsWith('state_hash local class '));
    assert.strictEqual(classLogs.length, Object.keys(preimage).length);
}

function keepsClassDigestsStable(){
    let first = { alpha: [{ id: 1 }], beta: [{ id: 2 }] };
    let same = { alpha: [{ id: 1 }], beta: [{ id: 2 }] };
    let changed = { alpha: [{ id: 1 }], beta: [{ id: 3 }] };
    let hash = data => util.getDataHash(data);

    let a = classDigests(first, hash);
    let b = classDigests(same, hash);
    let c = classDigests(changed, hash);

    assert.deepStrictEqual(a, b);
    assert.strictEqual(a[0].digest, c[0].digest);
    assert.notStrictEqual(a[1].digest, c[1].digest);
}

async function haltsWithoutClassesWhenPreimageThrows(){
    sync.blockHasher.computeStateHash = sinon.stub().resolves('LOCAL_STATE');
    sync.blockHasher.computeStateHashPreimage = sinon.stub().rejects(new Error('detail read failed'));

    await sync.applyBlockEvent({ block_index: 200, block_time: 1, state_hash: 'SOURCE_STATE' });

    assert.strictEqual(sync.isHalted(), true);
    assert.ok(db.recordHalt.calledOnce);
    let mismatch = db.recordHalt.firstCall.args[3][0];
    assert.deepStrictEqual(mismatch,
        { field: 'state_hash', a: 'SOURCE_STATE', b: 'LOCAL_STATE' });
    assert.ok(console.error.calledWithMatch('state_hash local class detail failed at block 200:'));
}

async function skipsDetailWhenStateHashMatches(){
    sync.blockHasher.computeStateHash = sinon.stub().resolves('AGREED_STATE');
    sync.blockHasher.computeStateHashPreimage = sinon.stub().resolves({});

    await sync.applyBlockEvent({ block_index: 200, block_time: 1, state_hash: 'AGREED_STATE' });

    assert.strictEqual(sync.isHalted(), false);
    assert.strictEqual(sync.lastAppliedBlock, 200);
    assert.strictEqual(sync.blockHasher.computeStateHashPreimage.called, false);
    assert.strictEqual(db.recordHalt.called, false);
}

describe('ClientSync: state_hash halt class digests @regression', function(){
    beforeEach(function(){
        db = createMockDb();
        applier = {
            applyBlock: sinon.stub().resolves(),
            applyFullSnapshot: sinon.stub().resolves(),
            applyIncrementalSnapshot: sinon.stub().resolves()
        };
        util = new Utility();
        sync = new ClientSync('bitcoin', 'mainnet', db, applier,
            { rollback: sinon.stub().resolves() }, new HashVerifier(), {
                SYNC_SOURCES: 'http://a:3006',
                VERIFY_HASHES: true,
                HASH_CONFIRM_TIMEOUT: 5000,
                HALT_ON_DIVERGENCE: true,
                VERIFY_RECOMPUTE: false
            }, util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){ sinon.restore(); });

    it('records every local preimage class on a state_hash mismatch', recordsEveryLocalPreimageClass);
    it('keeps class digests stable and changes only the modified class digest', keepsClassDigestsStable);
    it('still halts without local_classes when the detail preimage throws', haltsWithoutClassesWhenPreimageThrows);
    it('does not halt or build detail when the state_hash matches', skipsDetailWhenStateHashMatches);
});
