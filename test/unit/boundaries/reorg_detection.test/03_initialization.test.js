// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers empty and initial poll states. One part of reorg_detection.test.js.
const assert = require('assert');
const { registerHooks } = require('./helpers/reorg_detection_suite');

describe('Boundary: Reorg Detection', function(){
    let poller, db, broadcaster;
    registerHooks(function(context){ ({ poller, db, broadcaster } = context); });

    it('currentBlock = null (all blocks deleted): early return', async function(){
        poller.lastPolledBlock = 10;
        db.getLastBlock.resolves(null);
        await poller.poll();
        assert.strictEqual(broadcaster.broadcast.called, false);
        assert.strictEqual(poller.lastPolledBlock, 10); // unchanged
    });

    it('first poll (lastPolledBlock = null): initializes without processing', async function(){
        poller.lastPolledBlock = null;
        db.getLastBlock.resolves(50);
        await poller.poll();
        assert.strictEqual(poller.lastPolledBlock, 50);
        assert.strictEqual(broadcaster.broadcast.called, false); // no block broadcasts
        assert.strictEqual(broadcaster.updateStatus.calledOnce, true);
    });

    it('first poll with empty DB: remains null', async function(){
        poller.lastPolledBlock = null;
        db.getLastBlock.resolves(null);
        await poller.poll();
        assert.strictEqual(poller.lastPolledBlock, null);
        assert.strictEqual(broadcaster.broadcast.called, false);
    });

    it('same-height non-detection: no event when data changes at same block', async function(){
        poller.lastPolledBlock = 10;
        db.getLastBlock.resolves(10);
        // Even if underlying data changed at block 10, poller does not detect it
        await poller.poll();
        assert.strictEqual(broadcaster.broadcast.called, false);
    });
});
