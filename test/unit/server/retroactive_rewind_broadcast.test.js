// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

'use strict';

const assert = require('assert');
const sinon = require('sinon');
const ServerPoller = require('../../../src/server/poller');

const RULES_TO = 'cd'.repeat(32);

function createPoller(marker){
    const db = {
        getLastBlock: sinon.stub().resolves(90),
        getBlockHashRow: sinon.stub().resolves(null),
        getSourceReplayMarker: sinon.stub().resolves(marker)
    };
    const broadcaster = {
        broadcast: sinon.stub(),
        updateStatus: sinon.stub(),
        getSubscriberCount: sinon.stub().returns(0)
    };
    const transparencyLog = { pruneFrom: sinon.stub().resolves() };
    const poller = new ServerPoller('bitcoin', 'mainnet', db, broadcaster,
        transparencyLog, { BLOCK_POLL_INTERVAL: 100 }, { sleep: sinon.stub().resolves() });
    poller.lastPolledBlock = 100;
    poller.recentHashCap = 5;
    return { poller, db, broadcaster, transparencyLog };
}

describe('ServerPoller retroactive rewind broadcast @regression', function(){
    afterEach(function(){ sinon.restore(); });

    it('broadcasts a marked height drop beyond the walk-back window at the marker target', async function(){
        const marker = {
            target: 70,
            replay_to: 100,
            rules_from: 'ab'.repeat(32),
            rules_to: RULES_TO,
            state: 'replaying'
        };
        const context = createPoller(marker);

        await context.poller.handleHeightDropReorg(90);

        assert.ok(context.db.getSourceReplayMarker.calledOnceWithExactly());
        assert.ok(context.transparencyLog.pruneFrom.calledOnceWithExactly(70));
        assert.ok(context.broadcaster.broadcast.calledOnceWithExactly(
            'bitcoin', 'mainnet', {
                type: 'reorg',
                chain: 'bitcoin',
                network: 'mainnet',
                dbType: 'indexer',
                block_index: 70,
                retroactive: { target: 70, rules_to: RULES_TO }
            }
        ));
        assert.strictEqual(context.poller.lastPolledBlock, 69);
    });

    it('broadcasts an unmarked height drop exactly as before', async function(){
        const context = createPoller(null);

        await context.poller.handleHeightDropReorg(90);

        assert.ok(context.transparencyLog.pruneFrom.calledOnceWithExactly(91));
        assert.ok(context.broadcaster.broadcast.calledOnceWithExactly(
            'bitcoin', 'mainnet', {
                type: 'reorg',
                chain: 'bitcoin',
                network: 'mainnet',
                dbType: 'indexer',
                block_index: 91
            }
        ));
        assert.strictEqual(context.poller.lastPolledBlock, 90);
    });
});
