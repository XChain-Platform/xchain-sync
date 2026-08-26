'use strict';

// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Transport-integrity cross-check on the hub's additive coin_consensus_hashes
// field. The hub serves { network: { COIN: sha256 } } of its OWN bundled coin
// files on getallconfigs; sync derives consensus values only from its vendored
// bundle, so the field is compared and logged, NEVER applied. Before this the
// field was destructured away in _applyConfigResult and a hub running a
// divergent bundle reached sync only later, as a recompute-divergence halt.

const assert = require('assert');
const sinon  = require('sinon');

const HubClient = require('../../src/HubClient');
const coins     = require('../../src/coins');

function envelope(hashes){
    return {
        configs:   { bitcoin: { testnet: { 'xchain-indexer': { DB_NAME: 'x' } } } },
        seq:       7,
        watermark: 1234,
        coin_consensus_hashes: hashes
    };
}

function trueHashes(){
    const out = {};
    for(const net of coins.NETWORKS) out[net] = coins.consensusHashes(net);
    return out;
}

function driftedHashes(tick, network){
    const out = trueHashes();
    out[network] = Object.assign({}, out[network], { [tick]: 'f'.repeat(64) });
    return out;
}

describe('HubClient hub-vs-bundle consensus-hash cross-check', function(){

    let client, errors;

    beforeEach(function(){
        client = new HubClient(['http://hub.invalid:10000']);
        errors = [];
        sinon.stub(console, 'error').callsFake((...a) => errors.push(a.join(' ')));
    });

    afterEach(() => sinon.restore());

    it('logs when the hub serves a consensus hash this build does not bundle', function(){
        client._applyConfigResult(envelope(driftedHashes('BTC', 'testnet')));
        assert.strictEqual(errors.length, 1, 'a drifted hub must report exactly once');
        assert.match(errors[0], /CONSENSUS HASH MISMATCH/);
        assert.match(errors[0], /BTC\/testnet/);
        assert.match(errors[0], /never applied/);
    });

    it('says nothing when every served hash matches the bundle', function(){
        client._applyConfigResult(envelope(trueHashes()));
        assert.deepStrictEqual(errors, []);
    });

    it('says nothing when the hub predates the field', function(){
        const e = envelope(undefined);
        delete e.coin_consensus_hashes;
        client._applyConfigResult(e);
        assert.deepStrictEqual(errors, []);
    });

    it('says nothing for an old hub that returns the bare config map', function(){
        client._applyConfigResult({ bitcoin: { testnet: { 'xchain-indexer': { DB_NAME: 'x' } } } });
        assert.deepStrictEqual(errors, []);
    });

    it('does not re-log an unchanged mismatch on the next poll', function(){
        const drifted = driftedHashes('LTC', 'regtest');
        client._applyConfigResult(envelope(drifted));
        client._applyConfigResult(envelope(drifted));
        client._applyConfigResult(envelope(drifted));
        assert.strictEqual(errors.length, 1, 'a poll loop must not flood the log');
    });

    it('reports again when the mismatch set widens', function(){
        client._applyConfigResult(envelope(driftedHashes('BTC', 'testnet')));
        const wider = driftedHashes('BTC', 'testnet');
        wider.regtest = Object.assign({}, wider.regtest, { DOGE: 'e'.repeat(64) });
        client._applyConfigResult(envelope(wider));
        assert.strictEqual(errors.length, 2);
        assert.match(errors[1], /DOGE\/regtest/);
    });

    it('ignores a coin the hub serves that this build does not bundle', function(){
        const extra = trueHashes();
        extra.testnet = Object.assign({}, extra.testnet, { XYZ: 'a'.repeat(64) });
        client._applyConfigResult(envelope(extra));
        assert.deepStrictEqual(errors, []);
    });

    it('leaves the returned config tree and the cursor untouched', function(){
        const e    = envelope(driftedHashes('BTC', 'testnet'));
        const tree = client._applyConfigResult(e);
        assert.deepStrictEqual(tree, e.configs);
        assert.strictEqual(client.lastSeq, 7);
        assert.strictEqual(client.lastWatermark, 1234);
    });
});
