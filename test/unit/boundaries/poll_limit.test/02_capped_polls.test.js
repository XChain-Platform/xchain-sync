// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers capped poll batches. One part of poll_limit.test.js.
const assert = require('assert');
const { registerHooks } = require('./helpers/poll_limit_suite');

describe('Boundary: Poll Loop Limit (100 blocks)', function(){
    let poller, db, broadcaster, log;
    registerHooks(function(context){ ({ poller, db, broadcaster, log } = context); });

    it('caps at 100 blocks when 101 available', async function(){
        poller.lastPolledBlock = 0;
        db.getLastBlock.resolves(101);
        await poller.poll();
        assert.strictEqual(broadcaster.broadcast.callCount, 100);
        assert.strictEqual(poller.lastPolledBlock, 100);
    });

    it('processes remaining 1 block on second poll after cap', async function(){
        poller.lastPolledBlock = 0;
        db.getLastBlock.resolves(101);
        await poller.poll(); // processes 1–100
        assert.strictEqual(poller.lastPolledBlock, 100);

        broadcaster.broadcast.resetHistory();
        await poller.poll(); // processes 101
        assert.strictEqual(broadcaster.broadcast.callCount, 1);
        assert.strictEqual(poller.lastPolledBlock, 101);
    });

    it('caps at 100 blocks when 200 available', async function(){
        poller.lastPolledBlock = 0;
        db.getLastBlock.resolves(200);
        await poller.poll();
        assert.strictEqual(broadcaster.broadcast.callCount, 100);
        assert.strictEqual(poller.lastPolledBlock, 100);
    });
});
