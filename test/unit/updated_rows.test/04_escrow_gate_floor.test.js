// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.
//
// The escrow-gate re-derive on the apply side: the pre-floor preserve rule and the
// fail-closed bootstrap floor read.

'use strict';

const assert = require('assert');
const sinon  = require('sinon');
const ClientApplier = require('../../../src/client/applier');
const ClientRollback = require('../../../src/client/rollback');
const Utility = require('../../../src/util');
const { fakeDb } = require('./helpers/fake_db.js');

// Shared with the describe below; each case assigns them from fakeDb.
let db, applier;

// Bootstrap floor reads fail closed: a read fault or schema gap aborts the apply rather than skipping the derive.
function bootstrapFloorReadCases(){
    it('reads the bootstrap floor fail-closed before the apply-side re-derive', async function(){
        db = fakeDb([{ match: 'escrow_action_index IS NOT NULL', rows: [] }]);
        db.getSyncState = sinon.stub().resolves('50');
        db.getFirstActionIndex = sinon.stub().resolves(500);
        applier = new ClientApplier(db, new Utility());
        await applier.maybeRederiveEscrow({ orders: [{}] });
        assert.ok(db.getSyncState.calledWith('bootstrap_base:indexer', { rethrow: true }));
        assert.ok(db.getFirstActionIndex.calledWith(50, null, { rethrow: true }));

        db.getSyncState.rejects(new Error('sync state read failed'));
        await assert.rejects(() => applier.maybeRederiveEscrow({ orders: [{}] }), /sync state read failed/);
    });

    it('a schema-gap errno on the bootstrap floor read aborts the apply instead of skipping the derive', async function(){
        let isEscrowQuery = (c) => c.sql.indexOf('escrow_action_index IS NOT NULL') !== -1;
        for(let errno of [1146, 1054]){
            let gap = Object.assign(new Error('gap'), { errno });
            db = fakeDb([{ match: 'escrow_action_index IS NOT NULL', rows: [] }]);
            db.getSyncState = sinon.stub().rejects(gap);
            applier = new ClientApplier(db, new Utility());
            await assert.rejects(() => applier.maybeRederiveEscrow({ orders: [{}] }), /gap/);
            assert.strictEqual(db.calls.some(isEscrowQuery), false);

            db.getSyncState = sinon.stub().resolves('50');
            db.getFirstActionIndex = sinon.stub().rejects(gap);
            await assert.rejects(() => applier.maybeRederiveEscrow({ orders: [{}] }), /gap/);
        }
    });
}

describe('ClientApplier in-place updated-rows apply', function(){

    beforeEach(function(){
        db = fakeDb([]);
        applier = new ClientApplier(db, new Utility());
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(() => sinon.restore());

    it('preserves a pre-floor escrow gate without local proof that its offer closed', async function(){
        db = fakeDb([
            { match: 'escrow_action_index IS NOT NULL', rows: [{ tick: 'PRE' }] },
            { match: 'SELECT t.escrow_action_index', rows: [{ escrow_action_index: 90 }] },
            { match: 'SELECT status FROM (', rows: [] }
        ]);
        await ClientRollback.rederiveEscrowGate(db, { floorActionIndex: 100 });
        assert.strictEqual(db.calls.some(c => c.sql.indexOf('UPDATE tokens SET escrow_action_index') !== -1), false);
    });

    it('clears a proven-closed pre-floor gate and still opens an in-window gate', async function(){
        for(let testCase of [
            { tick: 'CLOSED', current: 90, status: [{ status: 'cancelled' }], offers: [], expected: null },
            { tick: 'OPEN', current: null, status: [], offers: [{ action_index: 105 }], expected: 105 }
        ]){
            db = fakeDb([
                { match: 'escrow_action_index IS NOT NULL', rows: [{ tick: testCase.tick }] },
                { match: 'SELECT t.escrow_action_index', rows: [{ escrow_action_index: testCase.current }] },
                { match: 'SELECT status FROM (', rows: testCase.status },
                { match: 'SELECT o.action_index FROM orders', rows: testCase.offers }
            ]);
            await ClientRollback.rederiveEscrowGate(db, { floorActionIndex: 100 });
            let update = db.calls.find(c => c.sql.indexOf('UPDATE tokens SET escrow_action_index') !== -1);
            assert.deepStrictEqual(update.args, [testCase.expected, testCase.tick]);
        }
    });

    bootstrapFloorReadCases();
});
