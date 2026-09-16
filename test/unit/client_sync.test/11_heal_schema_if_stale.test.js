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

function registerHealSchemaIfStaleTests(){
    describe('healSchemaIfStale', function(){
        let heal;
        beforeEach(function(){
            heal = sinon.stub(sync, 'fetchAndApplySchema').resolves();
        });

        it('heals on missing table (1146) and missing column (1054)', async function(){
            assert.strictEqual(await sync.healSchemaIfStale({ errno: 1146 }), true);
            sync._lastSchemaHeal = null; // reset the debounce between cases
            assert.strictEqual(await sync.healSchemaIfStale({ errno: 1054 }), true);
            assert.strictEqual(heal.callCount, 2);
        });

        it('ignores non-schema errors and null errors', async function(){
            assert.strictEqual(await sync.healSchemaIfStale({ errno: 1062 }), false);
            assert.strictEqual(await sync.healSchemaIfStale(new Error('plain')), false);
            assert.strictEqual(await sync.healSchemaIfStale(null), false);
            assert.strictEqual(heal.called, false);
        });

        it('debounces to one heal per minute', async function(){
            assert.strictEqual(await sync.healSchemaIfStale({ errno: 1146 }), true);
            assert.strictEqual(await sync.healSchemaIfStale({ errno: 1146 }), false);
            assert.strictEqual(heal.callCount, 1);
        });
    });
}

describe('ClientSync', function(){
    registerClientSyncHooks(assignState);
    registerHealSchemaIfStaleTests();
});
