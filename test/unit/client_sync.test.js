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
} = require('./client_sync.test/support');

let sync, db, applier, rollback, hashVerifier, config, util;

function assignState(state){
    ({ sync, db, applier, rollback, hashVerifier, config, util } = state);
}

function registerConstructorTests(){
    describe('constructor', function(){
        it('parses SYNC_SOURCES into array', function(){
            assert.strictEqual(sync.sources.length, 2);
            assert.strictEqual(sync.sources[0], 'http://source1:3006');
            assert.strictEqual(sync.sources[1], 'http://source2:3006');
        });

        it('handles empty SYNC_SOURCES', function(){
            config.SYNC_SOURCES = '';
            let s = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            assert.strictEqual(s.sources.length, 0);
        });

        it('trims whitespace from sources', function(){
            config.SYNC_SOURCES = ' http://a:3006 , http://b:3006 ';
            let s = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            assert.strictEqual(s.sources[0], 'http://a:3006');
            assert.strictEqual(s.sources[1], 'http://b:3006');
        });
    });
}

describe('ClientSync', function(){
    registerClientSyncHooks(assignState);
    registerConstructorTests();
});
