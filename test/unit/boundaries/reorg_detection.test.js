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
const { registerHooks } = require('./reorg_detection.test/helpers/reorg_detection_suite');

describe('Boundary: Reorg Detection', function(){
    let poller, db, broadcaster;
    registerHooks(function(context){ ({ poller, db, broadcaster } = context); });

    it('no change (currentBlock === lastPolledBlock): no-op', async function(){
        poller.lastPolledBlock = 10;
        db.getLastBlock.resolves(10);
        await poller.poll();
        assert.strictEqual(broadcaster.broadcast.called, false);
        assert.strictEqual(poller.lastPolledBlock, 10);
    });

    it('one new block: no reorg', async function(){
        poller.lastPolledBlock = 10;
        db.getLastBlock.resolves(11);
        await poller.poll();
        let event = broadcaster.broadcast.firstCall.args[2];
        assert.strictEqual(event.type, 'block');
        assert.strictEqual(poller.lastPolledBlock, 11);
    });

    it('one block rollback: reorg detected', async function(){
        poller.lastPolledBlock = 10;
        db.getLastBlock.resolves(9);
        await poller.poll();
        let event = broadcaster.broadcast.firstCall.args[2];
        assert.strictEqual(event.type, 'reorg');
        assert.strictEqual(event.block_index, 10); // currentBlock + 1
        assert.strictEqual(poller.lastPolledBlock, 9);
    });

    it('deep rollback (10 blocks): reorg at correct index', async function(){
        poller.lastPolledBlock = 100;
        db.getLastBlock.resolves(90);
        await poller.poll();
        let event = broadcaster.broadcast.firstCall.args[2];
        assert.strictEqual(event.type, 'reorg');
        assert.strictEqual(event.block_index, 91); // currentBlock + 1
        assert.strictEqual(poller.lastPolledBlock, 90);
    });
});
