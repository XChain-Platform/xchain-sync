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

function makeSync(){
    const db = {
        dbName: 'test_db',
        dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getActiveHalt: sinon.stub().resolves(null),
        recordHalt: sinon.stub().resolves({ block_index: 100 }),
        getSyncState: sinon.stub().resolves('1000'),
        setSyncState: sinon.stub().resolves(true),
        doQuery: sinon.stub().resolves([])
    };
    const rollback = { rollback: sinon.stub().resolves() };
    const config = {
        SYNC_SOURCES: 'http://a:3006,http://b:3006',
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
    sync.lastAppliedBlock = 1000;
    return { sync, rollback };
}

describe('ClientSync retroactive rewind refusal reason @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'warn');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    it('logs an over-cap reason and the requested depth against the 72-hour BTC cap', async function(){
        const { sync, rollback } = makeSync();
        const event = {
            type: 'reorg',
            block_index: 100,
            retroactive: { target: 100, rules_to: 'a'.repeat(64) }
        };

        await sync.handleEvent(event, 0);
        await sync.handleEvent(event, 1);

        const errors = console.error.getCalls().map(call => String(call.args[0])).join('\n');
        assert.match(errors,
            /Refusing deep rewind at block 100: reason=over_cap, depth=901 blocks, retroactive cap=432 blocks/);
        assert.strictEqual(rollback.rollback.called, false);
        assert.strictEqual(sync.getHaltInfo().reason, 'max-rollback-depth-exceeded');
    });
});
