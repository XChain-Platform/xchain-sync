// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const EventEmitter = require('events');
const sinon = require('sinon');
const ClientSync = require('../../../../src/client/sync');
const Utility = require('../../../../src/util');
const HashVerifier = require('../../../../src/client/hash_verifier');

function makeSync(sources){
    const db = {
        dbName: 'test_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getActiveHalt: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        recordHalt: sinon.stub().resolves()
    };
    const applier = { applyBlock: sinon.stub().resolves(), applyFullSnapshot: sinon.stub().resolves() };
    const config = {
        SYNC_SOURCES: sources || 'http://a:3006', VERIFY_HASHES: false,
        VERIFY_RECOMPUTE: false, VERIFY_STATE_HASH: false, VERIFY_STATE_COMMITMENT: false,
        CLIENT_RECONNECT_DELAY: 100, HASH_CONFIRM_TIMEOUT: 5000,
        MAX_ROLLBACK_DEPTH: 10, WS_MAX_PAYLOAD: 1024, SNAPSHOT_MAX_CONTENT: 1024
    };
    return new ClientSync('bitcoin', 'mainnet', db, applier,
        { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
}

function socket(){
    const ws = new EventEmitter();
    ws.close = sinon.stub();
    return ws;
}

describe('ClientSync WebSocket lifecycle integrity @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log'); sinon.stub(console, 'error'); sinon.stub(console, 'warn');
    });
    afterEach(function(){ sinon.restore(); });

    it('reconnect recovery uses the committed tip rather than the verified marker', async function(){
        const sync = makeSync();
        const ws = socket();
        sync.running = true;
        sync.lastAppliedBlock = 10;
        sync['_verifiedTip'] = 7;
        sync.wsConns[0] = ws;
        sync['_sourcesNeedingRecovery'].add(0);
        sinon.stub(sync, 'armStreamWatchdog');
        sinon.stub(sync, 'incrementalCatchUp').resolves();

        sync.handleWebSocketOpen('http://a:3006', 0, ws);
        await Promise.resolve();
        await Promise.resolve();

        assert.ok(sync.incrementalCatchUp.calledOnceWithExactly(11));
        assert.strictEqual(sync['_sourcesNeedingRecovery'].has(0), false);
        const messages = console.log.getCalls().map(call => String(call.args[0]));
        assert.strictEqual(messages.some(message => message.includes('last verified block')), false);
    });

    it('does not re-add an evicted source when its own close event arrives', function(){
        const sync = makeSync('http://a:3006,http://b:3006,http://c:3006,http://d:3006');
        const ws = socket();
        sync.running = true;
        sync.wsConns[3] = ws;
        sync.registerWebSocketHandlers(ws, 'http://d:3006', 3);

        sync.evictSource(3);
        ws.emit('close');

        assert.strictEqual(sync['_sourcesNeedingRecovery'].has(3), false);
        assert.strictEqual(sync.wsConns[3], null);
        assert.strictEqual(sync['_reconnectTimers'].has(3), false);
        assert.strictEqual(Object.hasOwn(ws, '_xchainEvicted'), false);
    });

    it('does not re-add an evicted source when its closing socket errors', function(){
        const sync = makeSync('http://a:3006,http://b:3006,http://c:3006,http://d:3006');
        const ws = socket();
        sync.running = true;
        sync.wsConns[3] = ws;
        sync.registerWebSocketHandlers(ws, 'http://d:3006', 3);

        sync.evictSource(3);
        ws.emit('error', { message: 'closed' });

        assert.strictEqual(sync['_sourcesNeedingRecovery'].has(3), false);
        assert.strictEqual(sync['_reconnectTimers'].has(3), false);
    });
});
