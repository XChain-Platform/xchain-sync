// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers mid-rewrite fork discovery. One part of reorg_detection.test.js.
const assert = require('assert');
const { registerHooks } = require('./helpers/reorg_detection_suite');

describe('Boundary: Reorg Detection', function(){
    let poller, db, broadcaster;
    registerHooks(function(context){ ({ poller, db, broadcaster } = context); });

    it('mid-rewrite height drop: walks back to the true fork point', async function(){
        // The source is observed mid-rewrite: the tip dropped from 10 to 8, but the rewrite
        // actually forked at 5, so blocks 5-8 are already replacements. A height-only check
        // would broadcast reorg@9 (too shallow), leaving followers on stale blocks 5-8 until a
        // later poll caught the deeper rewrite, so the walk-back must resolve fork=5 in this poll.
        poller.lastPolledBlock = 10;
        db.getLastBlock.resolves(8);
        // Recorded broadcast hashes: 4 matches the live source ('l'), 5..10 were broadcast pre-reorg with a different content hash.
        poller.recentBroadcastHashes.set(4, 'l');
        for(let bi = 5; bi <= 10; bi++) poller.recentBroadcastHashes.set(bi, 'pre-reorg');
        await poller.poll();
        let event = broadcaster.broadcast.firstCall.args[2];
        assert.strictEqual(event.type, 'reorg');
        assert.strictEqual(event.block_index, 5);
        assert.strictEqual(poller.lastPolledBlock, 4);
        // Guard re-seeded from the recorded (still matching) hash at the fork parent.
        assert.strictEqual(poller.lastPolledBlockHash, 'l');
        assert.strictEqual(poller.transparencyLog.pruneFrom.calledOnceWithExactly(5), true);
    });
});
