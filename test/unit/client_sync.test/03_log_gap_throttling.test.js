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

function registerLogGapThrottlingTests(){
    describe('logGap throttling', function(){
        // On a fast chain (e.g. Dogecoin testnet) the replica trails the tip and
        // would log a gap line per block (thousands/min). logGap collapses that
        // into one line per window, folding in a suppressed count.
        it('logs the first occurrence immediately', function(){
            sync._gapLogIntervalMs = 30000;
            sync.logGap('gap', 1000);
            assert.strictEqual(console.log.calledOnce, true);
            assert.match(console.log.firstCall.args[0], /^gap/);
        });

        it('suppresses repeats within the window, then emits one summary with the count', function(){
            sync._gapLogIntervalMs = 30000;
            sync.logGap('gap', 1000);           // emits
            sync.logGap('gap', 5000);           // suppressed
            sync.logGap('gap', 10000);          // suppressed
            assert.strictEqual(console.log.callCount, 1);

            sync.logGap('gap', 40000);          // past window → emits summary
            assert.strictEqual(console.log.callCount, 2);
            assert.match(console.log.secondCall.args[0], /\+2 similar/);
        });

        it('resets the suppressed count after emitting a summary', function(){
            sync._gapLogIntervalMs = 30000;
            sync.logGap('gap', 1000);           // emits
            sync.logGap('gap', 5000);           // suppressed (+1)
            sync.logGap('gap', 40000);          // emits with (+1)
            sync.logGap('gap', 80000);          // emits, no leftover count
            assert.strictEqual(console.log.callCount, 3);
            assert.doesNotMatch(console.log.thirdCall.args[0], /similar/);
        });
    });
}

describe('ClientSync', function(){
    registerClientSyncHooks(assignState);
    registerLogGapThrottlingTests();
});
