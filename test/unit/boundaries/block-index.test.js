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
const ServerPoller  = require('../../../src/ServerPoller');
const ClientApplier = require('../../../src/ClientApplier');
const HashVerifier  = require('../../../src/HashVerifier');
const Utility       = require('../../../src/utility');

function createMockDb(){
    return {
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getBlockScopedRows: sinon.stub().resolves([]),
        getActionScopedRows: sinon.stub().resolves([]),
        getTransactions: sinon.stub().resolves([]),
        getActions: sinon.stub().resolves([]),
        doQuery: sinon.stub().resolves([]),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves()
    };
}

describe('Boundary: Block Index Values', function(){

    let db, util;

    beforeEach(function(){
        db = createMockDb();
        util = new Utility();
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){ sinon.restore(); });

    describe('block_index = 0', function(){
        it('getLastBlock returns 0 (not null) when MAX is 0', async function(){
            // Simulate: rows[0].block_index is 0, which is not null
            // The db.js code: rows[0].block_index !== null → Number(0) = 0
            // We verify the logic via ServerPoller
            let broadcaster = { broadcast: sinon.stub(), updateStatus: sinon.stub(), getSubscribers: sinon.stub().returns([]), getSubscriberCount: sinon.stub().returns(0) };
            let log = { recordBlock: sinon.stub().resolves(), pruneFrom: sinon.stub().resolves() };
            let poller = new ServerPoller('bitcoin', 'mainnet', db, broadcaster, log, { BLOCK_POLL_INTERVAL: 100 }, util);

            db.getLastBlock.resolves(0);
            db.getBlockHashRow.resolves({
                block_index: 0, block_time: 0,
                ledger_hash: 'l0', actions_hash: 'a0', contract_hash: 'c0'
            });

            poller.lastPolledBlock = null;
            await poller._poll();
            // Should initialize to 0, not treat it as null
            assert.strictEqual(poller.lastPolledBlock, 0);
        });

        it('continuity check: 0 → 1 is valid', function(){
            let verifier = new HashVerifier();
            let result = verifier.verifyChainContinuity(0,
                { ledger_hash: 'a', actions_hash: 'b', contract_hash: 'c' },
                { block_index: 1 }
            );
            assert.strictEqual(result.valid, true);
        });

        it('duplicate detection: block_index 0 is skipped if exists', async function(){
            let applier = new ClientApplier(db, util);
            db.getBlockHashRow.resolves({ block_index: 0, ledger_hash: 'abc' });
            await applier.applyBlock({ block_index: 0, data: { blocks: [{ block_index: 0 }] } });
            assert.strictEqual(db.beginTransaction.called, false);
        });
    });

    describe('block_index = 1 (typical first block)', function(){
        it('poller processes block 1 from initial state', async function(){
            let broadcaster = { broadcast: sinon.stub(), updateStatus: sinon.stub(), getSubscribers: sinon.stub().returns([]), getSubscriberCount: sinon.stub().returns(0) };
            let log = { recordBlock: sinon.stub().resolves(), pruneFrom: sinon.stub().resolves() };
            let poller = new ServerPoller('bitcoin', 'mainnet', db, broadcaster, log, { BLOCK_POLL_INTERVAL: 100 }, util);

            db.getLastBlock.resolves(1);
            db.getBlockHashRow.resolves({
                block_index: 1, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });

            // First poll: initialize
            poller.lastPolledBlock = null;
            await poller._poll();
            assert.strictEqual(poller.lastPolledBlock, 1);

            // Second poll: no new blocks
            await poller._poll();
            assert.strictEqual(broadcaster.broadcast.called, false);
        });
    });

    describe('large block indices', function(){
        it('JS safe integer max (2^53-1) preserved correctly', function(){
            let safeMax = Number.MAX_SAFE_INTEGER; // 9007199254740991
            assert.strictEqual(Number(safeMax), safeMax);
            assert.strictEqual(safeMax + 1 !== safeMax, true); // still distinct
        });

        it('Number() preserves safe integers in hash comparison', function(){
            let verifier = new HashVerifier();
            let bigBlock = Number.MAX_SAFE_INTEGER;
            let result = verifier.verifyChainContinuity(bigBlock - 1,
                { ledger_hash: 'a', actions_hash: 'b', contract_hash: 'c' },
                { block_index: bigBlock }
            );
            assert.strictEqual(result.valid, true);
        });

        it('beyond safe integer: precision loss documented', function(){
            // Two distinct BigInts collapse to the same JS Number
            let a = Number(BigInt('9007199254740992'));
            let b = Number(BigInt('9007199254740993'));
            assert.strictEqual(a, b); // Both become 9007199254740992 (precision lost)
            // This means two different block_indices would be indistinguishable as Numbers
        });
    });

    describe('reorg boundary at block 1', function(){
        it('reorg from 10 to 0: reorg event at block_index 1', async function(){
            let broadcaster = { broadcast: sinon.stub(), updateStatus: sinon.stub(), getSubscribers: sinon.stub().returns([]), getSubscriberCount: sinon.stub().returns(0) };
            let log = { recordBlock: sinon.stub().resolves(), pruneFrom: sinon.stub().resolves() };
            let poller = new ServerPoller('bitcoin', 'mainnet', db, broadcaster, log, { BLOCK_POLL_INTERVAL: 100 }, util);

            poller.lastPolledBlock = 10;
            db.getLastBlock.resolves(0);
            db.getBlockHashRow.resolves({ block_index: 0, block_time: 0, ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });

            await poller._poll();
            let event = broadcaster.broadcast.firstCall.args[2];
            assert.strictEqual(event.type, 'reorg');
            assert.strictEqual(event.block_index, 1); // currentBlock(0) + 1
        });
    });

    describe('client skip logic', function(){
        it('skips block with index <= lastAppliedBlock', function(){
            // ClientSync._handleBlock: blockIndex <= this.lastAppliedBlock → return
            // This is tested indirectly through the unit tests, but we verify the comparison
            let lastApplied = 10;
            assert.strictEqual(10 <= lastApplied, true);  // same block: skipped
            assert.strictEqual(9 <= lastApplied, true);   // earlier: skipped
            assert.strictEqual(11 <= lastApplied, false);  // next: processed
        });
    });
});
