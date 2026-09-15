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
const { registerHooks } = require('./poll_limit.test/helpers/poll_limit_suite');

describe('Boundary: Poll Loop Limit (100 blocks)', function(){
    let poller, db, broadcaster, log;
    registerHooks(function(context){ ({ poller, db, broadcaster, log } = context); });

    it('processes 0 blocks when at current (no-op)', async function(){
        poller.lastPolledBlock = 50;
        db.getLastBlock.resolves(50);
        await poller.poll();
        assert.strictEqual(broadcaster.broadcast.callCount, 0);
        assert.strictEqual(poller.lastPolledBlock, 50);
    });

    it('processes 1 new block', async function(){
        poller.lastPolledBlock = 49;
        db.getLastBlock.resolves(50);
        await poller.poll();
        assert.strictEqual(broadcaster.broadcast.callCount, 1);
        assert.strictEqual(poller.lastPolledBlock, 50);
    });

    it('processes exactly 99 blocks in one poll', async function(){
        poller.lastPolledBlock = 1;
        db.getLastBlock.resolves(100);
        await poller.poll();
        assert.strictEqual(broadcaster.broadcast.callCount, 99);
        assert.strictEqual(poller.lastPolledBlock, 100);
    });

    it('processes exactly 100 blocks in one poll (at limit)', async function(){
        poller.lastPolledBlock = 0;
        db.getLastBlock.resolves(100);
        await poller.poll();
        assert.strictEqual(broadcaster.broadcast.callCount, 100);
        assert.strictEqual(poller.lastPolledBlock, 100);
    });
});
