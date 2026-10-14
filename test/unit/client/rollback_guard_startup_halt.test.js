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

const KEY = 'rollback_guard:indexer';

function makeSync(getSyncState){
    const db = {
        dbName: 'test_db',
        dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getActiveHalt: sinon.stub().resolves(null),
        clearHalt: sinon.stub().resolves(1),
        getSyncState,
        setSyncState: sinon.stub().resolves(true),
        deleteSyncState: sinon.stub().resolves(true),
        doQuery: sinon.stub().resolves([])
    };
    const config = {
        SYNC_SOURCES: 'http://source:3006',
        MAX_ROLLBACK_DEPTH: 5,
        MAX_ROLLBACK_DEPTH_EXPLICIT: true,
        VERIFY_RECOMPUTE: false
    };
    const sync = new ClientSync('bitcoin', 'mainnet', db, {}, {}, new HashVerifier(), config, new Utility());
    sync.running = true;
    return { sync, db };
}

function stubConsole(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });
}

describe('ClientSync rollback guard startup hold', function(){
    stubConsole();

    it('holds in a synthetic halt with doubling backoff while the state is unreadable, then resumes', async function(){
        const read = sinon.stub();
        read.onCall(0).rejects(new Error('down'));
        read.onCall(1).rejects(new Error('down'));
        read.onCall(2).resolves(JSON.stringify({ peak: 100, low: 98 }));
        const { sync } = makeSync(read);
        const delays = [];
        const reasons = [];
        sync.util.sleep = async ms => { delays.push(ms); reasons.push(sync._halted && sync._halted.reason); };

        assert.strictEqual(await sync.holdForRollbackGuardState(), 'continue');

        assert.deepStrictEqual(delays, [5000, 10000]);
        assert.deepStrictEqual(reasons, ['rollback-guard-state-unreadable', 'rollback-guard-state-unreadable']);
        assert.strictEqual(sync._halted, null);
        assert.deepStrictEqual(sync.rollbackGuardState(), { peak: 100, low: 98 });
    });

    it('caps the retry backoff at 60 seconds', async function(){
        const read = sinon.stub().rejects(new Error('down'));
        const { sync } = makeSync(read);
        const delays = [];
        sync.util.sleep = async ms => {
            delays.push(ms);
            if(delays.length === 7) sync.running = false;
        };

        assert.strictEqual(await sync.holdForRollbackGuardState(), 'stop');

        assert.deepStrictEqual(delays, [5000, 10000, 20000, 40000, 60000, 60000, 60000]);
        assert.strictEqual(sync.getHaltInfo().reason, 'rollback-guard-state-unreadable');
    });

    it('does not apply any block while the state stays unreadable', async function(){
        const { sync } = makeSync(sinon.stub().callsFake(async key => {
            if(key === KEY) throw new Error('down');
            return null;
        }));
        sinon.stub(sync, 'synchronizeStoredReplica').resolves();
        sync.util.sleep = async () => { sync.running = false; };

        await sync.start();

        assert.strictEqual(sync.synchronizeStoredReplica.called, false);
    });
});

describe('ClientSync rollback guard startup hold', function(){
    stubConsole();

    it('halts on an invalid saved row until an operator clear deletes it', async function(){
        let row = '{"peak":5,"low":9}';
        const read = sinon.stub().callsFake(async () => row);
        const { sync, db } = makeSync(read);
        db.deleteSyncState.callsFake(async () => { row = null; return true; });
        let sleeps = 0;
        sync.util.sleep = async () => {
            sleeps++;
            if(sleeps === 2){
                assert.strictEqual(sync.getHaltInfo().reason, 'rollback-guard-state-invalid');
                await sync.clearHalt();
            }
        };

        assert.strictEqual(await sync.holdForRollbackGuardState(), 'continue');

        assert.strictEqual(sleeps, 2);
        assert.ok(db.deleteSyncState.calledOnceWith(KEY));
        assert.strictEqual(db.clearHalt.called, false);
        assert.strictEqual(sync._halted, null);
    });

    it('returns stop when the sync is stopped during an invalid-row halt', async function(){
        const { sync } = makeSync(sinon.stub().resolves('not json'));
        sync.util.sleep = async () => { sync.running = false; };

        assert.strictEqual(await sync.holdForRollbackGuardState(), 'stop');
        assert.strictEqual(sync.getHaltInfo().reason, 'rollback-guard-state-invalid');
    });
});
