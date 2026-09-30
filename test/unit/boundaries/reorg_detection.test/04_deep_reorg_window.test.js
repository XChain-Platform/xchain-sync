// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers the per-chain fork-point window. One part of reorg_detection.test.js.
const assert = require('assert');
const sinon  = require('sinon');
const ServerPoller = require('../../../../src/server/poller');
const Utility = require('../../../../src/util');
const { createMockDb } = require('./helpers/reorg_detection_suite');

const TIP = 700;
const FORK = 300;

// Stream TIP blocks, then rewrite every height at or above FORK and poll once.
async function deepReorg(chain, network){
    let db = createMockDb();
    let reorged = false;
    db.getLastBlock.resolves(TIP);
    db.getBlockHashRow.callsFake(async (i) => ({ block_index: i, block_time: i * 10,
        ledger_hash: (reorged && i >= FORK ? 'post' : 'pre') + i, actions_hash: 'a', contract_hash: 'c' }));
    let broadcaster = { broadcast: sinon.stub(), updateStatus: sinon.stub(), getSubscribers: sinon.stub().returns([]), getSubscriberCount: sinon.stub().returns(0) };
    let log = { recordBlock: sinon.stub().resolves(), pruneFrom: sinon.stub().resolves() };
    let poller = new ServerPoller(chain, network, db, broadcaster, log, { BLOCK_POLL_INTERVAL: 100 }, new Utility());
    poller.lastPolledBlock = 0;
    while(poller.lastPolledBlock < TIP) await poller.poll();
    reorged = true;
    broadcaster.broadcast.resetHistory();
    await poller.poll();
    return broadcaster.broadcast.getCalls().map((c) => c.args[2]).find((e) => e && e.type === 'reorg');
}

describe('Boundary: Reorg Detection', function(){
    beforeEach(function(){ sinon.stub(console, 'log'); sinon.stub(console, 'error'); });
    afterEach(function(){ sinon.restore(); });

    it('sizes the recorded-hash window from the chain reorg ceiling', function(){
        const cap = (chain, network) => new ServerPoller(chain, network, createMockDb(), {}, {}, {}, new Utility()).recentHashCap;
        assert.strictEqual(cap('bitcoin', 'mainnet'), 256);
        assert.strictEqual(cap('dogecoin', 'testnet'), 256);
        assert.ok(cap('litecoin', 'testnet') >= 5006);
        assert.ok(cap('LTC', 'testnet') >= 5006);
    });

    it('resolves a 400-deep LTC testnet reorg to its true fork point', async function(){
        this.timeout(20000);
        const event = await deepReorg('litecoin', 'testnet');
        assert.strictEqual(event.block_index, FORK);
    });

    it('stops at the 256-height floor on a chain whose ceiling is lower', async function(){
        this.timeout(20000);
        const event = await deepReorg('bitcoin', 'mainnet');
        assert.strictEqual(event.block_index, TIP - 256);
    });
});
