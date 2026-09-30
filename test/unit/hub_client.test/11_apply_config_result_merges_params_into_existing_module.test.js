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

describe('HubClient applyConfigResult', function(){
    it('merges changed params into an existing cached module', function(){
        const hub = new HubClient([]);
        hub.configs = {
            bitcoin: { mainnet: { 'xchain-indexer': { A: '1', B: '2' } } }
        };
        hub.lastWatermark = 1000;

        const result = hub.applyConfigResult({
            configs: {
                bitcoin: { mainnet: { 'xchain-indexer': { A: '9', C: '3' } } }
            },
            seq: 2,
            watermark: 2000
        });

        assert.strictEqual(result, hub.configs);
        assert.deepStrictEqual(result.bitcoin.mainnet['xchain-indexer'], {
            A: '9',
            B: '2',
            C: '3'
        });
        assert.strictEqual(hub.lastSeq, 2);
        assert.strictEqual(hub.lastWatermark, 2000);
    });
});
