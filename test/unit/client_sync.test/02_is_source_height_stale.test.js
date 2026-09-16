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

function registerIsSourceHeightStaleTests(){
    describe('isSourceHeightStale', function(){
        // lastKnownServerBlock only advances on live WS events, so after a silent
        // disconnect it freezes and lag_blocks settles to 0 once the replica catches
        // up to the stale tip. isSourceHeightStale exposes that the live signal has
        // gone quiet so /status can flag the lag figure as computed against a stale tip.
        let clock;
        beforeEach(function(){
            clock = sinon.useFakeTimers();
            sync.config.CLIENT_SOURCE_STALE_MS = 180000;
        });
        afterEach(function(){
            clock.restore();
        });

        it('returns null before any WS event is seen', function(){
            assert.strictEqual(sync.isSourceHeightStale(), null);
        });

        it('returns false immediately after an event', function(){
            sync._lastWsEventAt = Date.now();
            assert.strictEqual(sync.isSourceHeightStale(), false);
        });

        it('stays fresh within the staleness window', function(){
            sync._lastWsEventAt = Date.now();
            clock.tick(179000);
            assert.strictEqual(sync.isSourceHeightStale(), false);
        });

        it('reports stale once the window elapses with no new event', function(){
            sync._lastWsEventAt = Date.now();
            clock.tick(180001);
            assert.strictEqual(sync.isSourceHeightStale(), true);
        });
    });
}

describe('ClientSync', function(){
    registerClientSyncHooks(assignState);
    registerIsSourceHeightStaleTests();
});
