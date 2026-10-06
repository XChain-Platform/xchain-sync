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
const ServerPoller = require('../../../src/server/poller');
const Utility = require('../../../src/util');
const { activationDelayBlocks } = require('../../../src/consensus-constants');

function createPoller(chain, dbType){
    return new ServerPoller(chain, 'mainnet', { dbType }, {}, null,
        { BLOCK_POLL_INTERVAL: 3000 }, new Utility());
}

// An indexer poller for a coin with no frozen activation delay would broadcast blocks
// with no deactivation_block updated rows, so it refuses to construct, as ClientRollback does.
describe('ServerPoller activation delay for an unrecognized coin', function(){
    it('throws for an indexer poller whose coin the bundle does not recognize', function(){
        assert.throws(() => createPoller('NOT-A-COIN', 'indexer'), /unrecognized coin "NOT-A-COIN"/);
    });

    it('keeps a decoder poller on the null delay, since decoders collect no updated rows', function(){
        assert.strictEqual(createPoller('NOT-A-COIN', 'decoder').activationDelay, null);
    });

    it('keeps an omitted coin on the null no-op path', function(){
        assert.strictEqual(createPoller(null, 'indexer').activationDelay, null);
    });

    it('resolves a known full-name coin to its frozen integer delay', function(){
        const delay = createPoller('bitcoin', 'indexer').activationDelay;
        assert.ok(Number.isSafeInteger(delay) && delay >= 0);
        assert.strictEqual(delay, activationDelayBlocks('BTC'));
    });
});
