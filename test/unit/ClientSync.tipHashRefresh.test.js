/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md.
 *
 **********************************************************************
 * ClientSync: lastAppliedBlock / lastHashes are a PAIR describing ONE block.
 *
 * _handleBlock's fork-at-head guard treats a re-delivery at blockIndex ===
 * lastAppliedBlock whose hashes differ from lastHashes as a lost 1-block reorg.
 * The snapshot-apply paths advanced only the height, so after a catch-up
 * lastHashes still held the PRE-catch-up tip's hashes: the next delivery of the
 * new tip (a second source serving the same height, or a WS reconnect replaying
 * it) compared an honest block against the wrong block's hashes, always
 * mismatched, and turned the one log line that means "a reorg event was lost"
 * into routine noise plus a redundant catch-up.
 ********************************************************************/

'use strict';

const assert = require('assert');
const sinon  = require('sinon');
const axios  = require('axios');
const ClientSync   = require('../../src/ClientSync');
const Utility      = require('../../src/utility');
const HashVerifier = require('../../src/HashVerifier');

const HASHES = {
    100: { block_index: 100, ledger_hash: 'aa'.repeat(32), actions_hash: 'bb'.repeat(32), contract_hash: 'cc'.repeat(32) },
    101: { block_index: 101, ledger_hash: '11'.repeat(32), actions_hash: '22'.repeat(32), contract_hash: '33'.repeat(32) }
};

function eventFor(blockIndex){
    let h = HASHES[blockIndex];
    return { type: 'block', block_index: blockIndex, ledger_hash: h.ledger_hash,
             actions_hash: h.actions_hash, contract_hash: h.contract_hash, data: {} };
}

function makeSync(){
    // The replica's own DB: committed tip 100 before the catch-up, 101 after the
    // snapshot applies. getBlockHashRow answers from the same table, so a refresh
    // that reads the DB gets block 101 and one that never runs keeps block 100.
    let db = {
        dbName: 'test_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(100),
        getBlockHashRow: sinon.stub().callsFake(async (i) => HASHES[i] || null),
        doQuery: sinon.stub().resolves([]),
        getActiveHalt: sinon.stub().resolves(null),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        listExistingTables: sinon.stub().resolves(new Set())
    };
    let applier = {
        applyBlock: sinon.stub().resolves(),
        applyIncrementalSnapshot: sinon.stub().callsFake(async () => {
            db.getLastBlock.resolves(101);           // the apply committed block 101
        })
    };
    let config = { SYNC_SOURCES: 'http://a:3006', VERIFY_RECOMPUTE: false,
                   SNAPSHOT_MAX_CONTENT: 1024 * 1024 };
    let sync = new ClientSync('BTC', 'regtest', db, applier,
        { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
    sync.lastAppliedBlock = 100;
    sync.lastHashes       = HASHES[100];
    return { sync, db, applier };
}

describe('ClientSync: snapshot catch-up repairs the tip hash pair @regression', function(){
    let getStub, errorStub;

    beforeEach(function(){
        // The snapshot fetch: a raw (ungzipped) JSON buffer, which the gunzip attempt
        // rejects and the parse then reads directly.
        getStub = sinon.stub(axios, 'get').callsFake(async () => ({
            data: Buffer.from(JSON.stringify({ block_height: 101, since_block: 101, data: {} }), 'utf8')
        }));
        sinon.stub(console, 'log');
        sinon.stub(console, 'warn');
        errorStub = sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('leaves lastHashes describing the new tip, not the pre-catch-up tip', async function(){
        let { sync } = makeSync();
        await sync._runIncrementalCatchUp();
        assert.strictEqual(sync.lastAppliedBlock, 101, 'catch-up advanced the height');
        assert.strictEqual(sync.lastHashes.ledger_hash, HASHES[101].ledger_hash,
            'lastHashes must describe block 101, not the pre-catch-up tip 100');
        assert.strictEqual(sync.lastHashes.block_index, 101);
    });

    it('does not report a bogus fork when the new tip is re-delivered after a catch-up', async function(){
        let { sync } = makeSync();
        await sync._runIncrementalCatchUp();

        let catchUp = sinon.stub(sync, '_incrementalCatchUp').resolves();
        errorStub.resetHistory();
        await sync._handleBlock(eventFor(101), 0);

        let forkLines = errorStub.getCalls().map(c => String(c.args[0]))
            .filter(l => l.indexOf('fork at head block') !== -1);
        assert.deepStrictEqual(forkLines, [],
            'an honest re-delivery of the tip must not log a fork-at-head continuity error');
        assert.strictEqual(catchUp.called, false, 'and must not fire a redundant catch-up');
    });

    it('still reports a REAL fork at the head (the guard is not disarmed)', async function(){
        // Negative control for the two assertions above: with genuinely different
        // hashes at the same height the guard must still fire, so a green result on
        // the previous test means "no bogus alarm", not "the guard stopped working".
        let { sync } = makeSync();
        await sync._runIncrementalCatchUp();

        let catchUp = sinon.stub(sync, '_incrementalCatchUp').resolves();
        errorStub.resetHistory();
        let forked = eventFor(101);
        forked.ledger_hash = 'ff'.repeat(32);
        await sync._handleBlock(forked, 0);

        let forkLines = errorStub.getCalls().map(c => String(c.args[0]))
            .filter(l => l.indexOf('fork at head block') !== -1);
        assert.strictEqual(forkLines.length, 1, 'a genuine head fork must still be reported');
        assert.strictEqual(catchUp.calledWith(102), true, 'and must still trigger the catch-up');
    });
});
