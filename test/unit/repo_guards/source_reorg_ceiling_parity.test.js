/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************/

// test/unit/repo_guards/source_reorg_ceiling_parity.test.js
//
// Ties this repo's hand-copied source reorg ceiling to the code it mirrors: the decoder's
// dispenser-expiry safe depth and the utxo-tracker's undo ceiling. The ceiling feeds the
// MAX_ROLLBACK_DEPTH warning, the poller's walk-back cap and the sync_meta retention floor.

'use strict';

const assert = require('assert');
const path   = require('path');
const config = require('../../../src/config');
const coins  = require('../../../src/coins');
const { siblingCheckout, skipOrFail, siblingsRequired } = require('../../helpers/sibling_checkout.js');

const DECODER_CONSTANTS = path.join(__dirname, '..', '..', '..', '..', 'xchain-decoder', 'src', 'XChainDecoder', 'constants.js');
const TRACKER_UNDO      = path.join(__dirname, '..', '..', '..', '..', 'xchain-utxo-tracker', 'src', 'chain', 'undo_blocks.js');

// Every (ticker, network) this repo keys a source undo window for, so a new chain is covered.
function chains(){
    return Object.keys(config.SOURCE_UNDO_WINDOW).map(key => {
        const [ticker, net] = key.split('_');
        return { ticker, net: net.toLowerCase(), fullname: coins.COIN_FULL_NAME[ticker] };
    });
}

// Require a present sibling module; a load failure fails under strict siblings, else skips.
function requireSibling(ctx, abs, what){
    const verdict = siblingCheckout(__dirname, abs);
    if(!skipOrFail(ctx, verdict, what)) return null;
    try {
        return require(verdict.path);
    } catch(err){
        if(siblingsRequired()) throw err;
        ctx.skip();
        return null;
    }
}

describe('source reorg ceiling parity with the decoder and utxo-tracker', function(){
    let decoder = null, tracker = null;

    before(function(){
        decoder = requireSibling(this, DECODER_CONSTANTS, 'the source reorg ceiling parity with xchain-decoder');
        tracker = requireSibling(this, TRACKER_UNDO, 'the source reorg ceiling parity with xchain-utxo-tracker');
    });

    it('knows the full name of every chain it keys a source undo window for', function(){
        const list = chains();
        assert.ok(list.length >= 9, 'SOURCE_UNDO_WINDOW lost chains: ' + list.length);
        for(const c of list)
            assert.ok(c.fullname, 'no full coin name for ' + c.ticker + '; add it to src/coins before this guard can check it');
    });

    // Strict equality, so a decoder depth moved in either direction fails here by chain.
    it('rollbackDepthSafeCeiling equals the decoder dispenser-expiry safe depth for every chain', function(){
        for(const c of chains()){
            const want = decoder.resolveDispenserExpireSafeDepth(c.ticker, c.net);
            for(const form of [c.ticker, c.fullname]){
                assert.strictEqual(config.rollbackDepthSafeCeiling(form, c.net), want,
                    c.fullname + '/' + c.net + ': sync ceiling ' + config.rollbackDepthSafeCeiling(form, c.net) +
                    ' (as ' + form + ') differs from xchain-decoder resolveDispenserExpireSafeDepth ' + want +
                    '; change SOURCE_SAFE_REORG_DEPTH / LTC_TESTNET_SOURCE_SAFE_REORG_DEPTH in src/config.js together with it');
            }
        }
        assert.strictEqual(config.rollbackDepthSafeCeiling('BTC', 'mainnet'), decoder.DISPENSER_EXPIRE_SAFE_DEPTH);
    });

    it('rollbackDepthSafeCeiling equals the utxo-tracker undo ceiling for every chain', function(){
        for(const c of chains()){
            const name = c.fullname + '-' + c.net;
            assert.strictEqual(tracker.coinFromNetwork(name), c.ticker,
                'xchain-utxo-tracker does not resolve ' + name + ' to ' + c.ticker + ', so its ceiling would fall through');
            assert.strictEqual(config.rollbackDepthSafeCeiling(c.ticker, c.net), tracker.safeUndoBlocksCeiling(name),
                name + ': sync ceiling ' + config.rollbackDepthSafeCeiling(c.ticker, c.net) +
                ' differs from xchain-utxo-tracker safeUndoBlocksCeiling ' + tracker.safeUndoBlocksCeiling(name));
        }
        assert.strictEqual(config.rollbackDepthSafeCeiling('BTC', 'mainnet'), tracker.MAX_SAFE_UNDO_BLOCKS);
    });

    it('the poller walk-back reach covers the decoder depth for every chain', function(){
        for(const c of chains()){
            const depth = decoder.resolveDispenserExpireSafeDepth(c.ticker, c.net);
            assert.ok(config.reorgWalkBackDepth(c.ticker, c.net) >= depth,
                c.fullname + '/' + c.net + ': reorgWalkBackDepth ' + config.reorgWalkBackDepth(c.ticker, c.net) +
                ' is below the decoder depth ' + depth + ', so the poller would prune a reorg the source recovers');
        }
    });
});
