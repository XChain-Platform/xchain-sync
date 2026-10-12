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
const HubClient = require('../../../src/hub/client');

const MARK = 'pollutedByHubTest';

// Drop any marker a failing case leaked, so one red case cannot poison the rest.
function scrubMarkers(){
    delete Object.prototype[MARK];
    delete Object[MARK];
    delete Object.prototype.toString[MARK];
}

function mergeDelta(json){
    const hub = new HubClient([]);
    hub.configs = { bitcoin: { mainnet: { 'xchain-indexer': { A: '1' } } } };
    hub.lastWatermark = 1000;
    return hub.applyConfigResult({ configs: JSON.parse(json), seq: 2, watermark: 2000 });
}

describe('HubClient applyConfigResult hostile keys', function(){
    afterEach(scrubMarkers);

    it('ignores __proto__ and constructor keys and keeps the legitimate rows', function(){
        const result = mergeDelta('{"__proto__":{"' + MARK + '":{"m":{"p":1}}},' +
            '"constructor":{"' + MARK + '":{"m":{"p":1}}},' +
            '"bitcoin":{"mainnet":{"xchain-indexer":{"B":"2","__proto__":{"' + MARK + '":1}}}}}');
        assert.strictEqual(({})[MARK], undefined);
        assert.strictEqual(Object[MARK], undefined);
        assert.deepStrictEqual(result.bitcoin.mainnet['xchain-indexer'], { A: '1', B: '2' });
        assert.deepStrictEqual(Object.keys(result), ['bitcoin']);
    });

    it('gives an inherited name such as toString its own plain level', function(){
        const result = mergeDelta('{"toString":{"' + MARK + '":{"m":{"p":1}}}}');
        assert.strictEqual(Object.prototype.toString[MARK], undefined);
        assert.ok(Object.prototype.hasOwnProperty.call(result, 'toString'));
        assert.deepStrictEqual(result.toString[MARK], { m: { p: 1 } });
    });
});
