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
const sinon  = require('sinon');
const ServerPoller = require('../../../src/server/poller');
const ClientSync = require('../../../src/client/sync');
const HashVerifier = require('../../../src/client/hash_verifier');
const Utility = require('../../../src/util');
const { withDbMixins } = require('../../helpers/db_mixins.js');

const BURST = [100, 101, 102];
const VIEW_TIP = 102;

function createSourceDb(){
    let rows = {};
    for(let b of BURST){
        rows[b] = {
            block_index: b, block_time: 1700000000 + b,
            ledger_hash: 'lh' + b, actions_hash: 'ah' + b, contract_hash: 'ch' + b,
            state_hash: 'SOURCE_STATE_' + b
        };
    }
    return withDbMixins({
        getBlockHashRow: sinon.stub().callsFake(async (b) => rows[b] || null),
        getBlockScopedRows: sinon.stub().resolves([]),
        getTxScopedRows: sinon.stub().resolves([]),
        getActionScopedRows: sinon.stub().resolves([]),
        getEmissionRowsForBlock: sinon.stub().resolves([]),
        getStateRootsRow: sinon.stub().resolves(null),
        getTransactions: sinon.stub().resolves([]),
        getActions: sinon.stub().resolves([]),
        getStatusId: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([])
    });
}

function createReplica(){
    let db = {
        dbName: 'test_db',
        dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        getActiveHalt: sinon.stub().resolves(null),
        clearHalt: sinon.stub().resolves(1)
    };
    let applier = { applyBlock: sinon.stub().resolves(), applyFullSnapshot: sinon.stub().resolves(), applyIncrementalSnapshot: sinon.stub().resolves() };
    let config = { SYNC_SOURCES: 'http://a:3006', VERIFY_HASHES: true, HASH_CONFIRM_TIMEOUT: 5000, HALT_ON_DIVERGENCE: true, VERIFY_RECOMPUTE: false };
    let sync = new ClientSync('bitcoin', 'mainnet', db, applier, { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
    return { sync, db, applier };
}

describe('ServerPoller: catch-up burst state_hash reaches a follower @regression', function(){
    let poller, payloads;

    beforeEach(async function(){
        let broadcaster = { broadcast: sinon.stub(), updateStatus: sinon.stub(), getSubscribers: sinon.stub().returns([]), getSubscriberCount: sinon.stub().returns(0) };
        poller = new ServerPoller('bitcoin', 'mainnet', createSourceDb(), broadcaster, null, { BLOCK_POLL_INTERVAL: 3000 }, new Utility());
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        payloads = [];
        for(let b of BURST) payloads.push(await poller.buildBlockPayload(b, null, VIEW_TIP));
    });

    afterEach(function(){ sinon.restore(); });

    it('ships NULL below the view tip and a full state_hash on the batch-tip block', function(){
        assert.deepStrictEqual(payloads.map(p => p.state_hash), [null, null, 'SOURCE_STATE_102']);
    });

    it('a follower applying the burst skips the check below the tip and halts on a seeded tip-state mismatch', async function(){
        let { sync, db } = createReplica();
        sync.blockHasher.computeStateHash = sinon.stub().callsFake(async (b) => (b === VIEW_TIP ? 'SEEDED_WRONG_STATE' : 'SOURCE_STATE_' + b));

        for(let p of payloads){
            if(sync.isHalted()) break;
            await sync.applyBlockEvent(p);
        }

        assert.deepStrictEqual(sync.blockHasher.computeStateHash.args.map(a => a[0]), [VIEW_TIP],
            'only the batch-tip block is recomputed');
        assert.strictEqual(sync.isHalted(), true, 'the tip-state mismatch halts');
        assert.strictEqual(sync.getHaltInfo().reason, 'state-hash-divergence');
        assert.strictEqual(sync.getHaltInfo().blockIndex, VIEW_TIP);
        assert.strictEqual(sync.lastAppliedBlock, 101, 'must not advance past the divergent tip block');
        assert.ok(db.recordHalt.calledOnce);
    });

    it('a follower whose tip state matches applies the whole burst without halting', async function(){
        let { sync } = createReplica();
        sync.blockHasher.computeStateHash = sinon.stub().callsFake(async (b) => 'SOURCE_STATE_' + b);
        for(let p of payloads) await sync.applyBlockEvent(p);
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(sync.lastAppliedBlock, VIEW_TIP);
    });
});
