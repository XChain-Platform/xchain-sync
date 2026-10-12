// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.
//
// The three indexer hashes chain independently and the ledger preimage carries no
// actions, so a replacement block can keep its ledger_hash while its actions or
// contract hash changes (an invalid action still writes transaction and action rows).
// Reorg detection must compare all three, or the orphaned rows stay on every replica
// and the transparency log keeps serving proofs over the old block.

const assert = require('assert');
const sinon  = require('sinon');
const ServerPoller = require('../../../src/server/poller');
const TransparencyLog = require('../../../src/server/transparency_log');
const Utility = require('../../../src/util');
const { withDbMixins } = require('../../helpers/db_mixins.js');

let poller, db, broadcaster, log;

function createMockDb(){
    return withDbMixins({
        getLastBlock: sinon.stub().resolves(100),
        getBlockHashRow: sinon.stub().resolves(null),
        getStatusId: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        beginReadSnapshot: sinon.stub().resolves({ mockSnapshotConn: true }),
        commitReadSnapshot: sinon.stub().resolves(),
        rollbackReadSnapshot: sinon.stub().resolves()
    });
}

function createMockLog(){
    return {
        epochSize:        100,
        recordBlock:      sinon.stub().resolves(),
        pruneFrom:        sinon.stub().resolves(),
        getHighWaterMark: sinon.stub().resolves(null),
        getRecordedHash:  sinon.stub().resolves(null),
        findGaps:         sinon.stub().resolves([]),
        recommitEpoch:    sinon.stub().resolves()
    };
}

function registerHooks(){
    beforeEach(function(){
        db = createMockDb();
        broadcaster = { broadcast: sinon.stub(), updateStatus: sinon.stub(),
            getSubscribers: sinon.stub().returns([]), getSubscriberCount: sinon.stub().returns(0) };
        log = createMockLog();
        poller = new ServerPoller('bitcoin', 'mainnet', db, broadcaster, log, { BLOCK_POLL_INTERVAL: 3000 }, new Utility());
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });
}

function row(block_index, ledger_hash, actions_hash, contract_hash, state_hash){
    return { block_index, block_time: block_index, ledger_hash, actions_hash, contract_hash,
        state_hash: state_hash === undefined ? null : state_hash };
}

// Record a block exactly as the forward pass does, then forget the broadcast.
function published(r){
    poller.publishBlockPayload(Object.assign({}, r), r.block_index);
    poller.lastPolledBlock = r.block_index;
    broadcaster.broadcast.resetHistory();
}

function reorgEvents(){
    return broadcaster.broadcast.getCalls().map(c => c.args[2]).filter(p => p && p.type === 'reorg');
}

describe('ServerPoller ledger-neutral reorg identity: net-forward @regression', function(){
    registerHooks();

    it('fires a net-forward reorg when only the actions hash changed', async function(){
        published(row(99, 'l99', 'a99', 'c99'));
        published(row(100, 'l100', 'a100-old', 'c100'));
        db.getBlockHashRow.withArgs(99).resolves(row(99, 'l99', 'a99', 'c99'));
        db.getBlockHashRow.withArgs(100).resolves(row(100, 'l100', 'a100-new', 'c100'));

        await poller.poll();

        assert.deepStrictEqual(reorgEvents().map(e => e.block_index), [100]);
        assert.ok(log.pruneFrom.calledOnceWithExactly(100));
        assert.ok(log.pruneFrom.calledBefore(broadcaster.broadcast), 'prune precedes the reorg broadcast');
        assert.strictEqual(poller.lastPolledBlock, 99);
    });

    it('fires a net-forward reorg when only the contract hash changed', async function(){
        published(row(99, 'l99', 'a99', 'c99'));
        published(row(100, 'l100', 'a100', 'c100-old'));
        db.getBlockHashRow.withArgs(99).resolves(row(99, 'l99', 'a99', 'c99'));
        db.getBlockHashRow.withArgs(100).resolves(row(100, 'l100', 'a100', 'c100-new'));

        await poller.poll();

        assert.deepStrictEqual(reorgEvents().map(e => e.block_index), [100]);
        assert.ok(log.pruneFrom.calledOnceWithExactly(100));
    });

    it('walks the fork point down through a height whose ledger hash did not change', async function(){
        published(row(98, 'l98', 'a98', 'c98'));
        published(row(99, 'l99', 'a99-old', 'c99'));
        published(row(100, 'l100-old', 'a100', 'c100'));
        db.getBlockHashRow.withArgs(98).resolves(row(98, 'l98', 'a98', 'c98'));
        db.getBlockHashRow.withArgs(99).resolves(row(99, 'l99', 'a99-new', 'c99'));
        db.getBlockHashRow.withArgs(100).resolves(row(100, 'l100-new', 'a100', 'c100'));

        await poller.poll();

        assert.deepStrictEqual(reorgEvents().map(e => e.block_index), [99]);
        assert.ok(log.pruneFrom.calledOnceWithExactly(99));
        assert.strictEqual(poller.lastPolledBlock, 98);
    });
});

describe('ServerPoller ledger-neutral reorg identity: restart and catch-up @regression', function(){
    registerHooks();

    it('fires on the first poll after a restart when the recorded actions hash is pre-reorg', async function(){
        const logDb = withDbMixins({
            doQuery: sinon.stub().callsFake(async (sql, args) => {
                if(/FROM sync_meta WHERE block_index=\?/.test(sql) && args[0] === 100)
                    return [{ ledger_hash: 'l100', actions_hash: 'a100-old', contract_hash: 'c100' }];
                return [];
            })
        });
        const realLog = new TransparencyLog(logDb, 100);
        sinon.stub(realLog, 'pruneFrom').resolves();
        poller = new ServerPoller('bitcoin', 'mainnet', db, broadcaster, realLog, { BLOCK_POLL_INTERVAL: 3000 }, new Utility());
        db.getBlockHashRow.withArgs(100).resolves(row(100, 'l100', 'a100-new', 'c100'));

        poller.lastPolledBlock = 100;
        poller.lastPolledBlockHash = await poller.seedReorgGuardHash(100);
        await poller.poll();

        assert.deepStrictEqual(reorgEvents().map(e => e.block_index), [100]);
        assert.ok(realLog.pruneFrom.calledOnceWithExactly(100));
    });

    it('does not fire when a catch-up payload carried a null state_hash the source row has', async function(){
        published(row(99, 'l99', 'a99', 'c99', null));
        published(row(100, 'l100', 'a100', 'c100', null));
        db.getBlockHashRow.withArgs(99).resolves(row(99, 'l99', 'a99', 'c99', 's99'));
        db.getBlockHashRow.withArgs(100).resolves(row(100, 'l100', 'a100', 'c100', 's100'));

        await poller.poll();

        assert.deepStrictEqual(reorgEvents(), []);
        assert.strictEqual(log.pruneFrom.called, false);
    });

    it('does not fire when a NULL contract hash was recorded and the source still has none', async function(){
        published(row(100, 'l100', 'a100', null));
        db.getBlockHashRow.withArgs(100).resolves(row(100, 'l100', 'a100', undefined));

        await poller.poll();

        assert.deepStrictEqual(reorgEvents(), []);
    });
});
