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
const sinon  = require('sinon');
const HubClient = require('../../../src/hub/client');

describe('HubClient', function(){

    let hub;

    beforeEach(function(){
        hub = new HubClient('localhost', 10000);
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    describe('hubConfigRegressed', function(){
        it('is false when there is no prior watermark to regress against', function(){
            assert.strictEqual(hub.hubConfigRegressed({ configs: {}, seq: 1, watermark: 100 }), false);
        });

        it('is true when watermark drops below the last-seen value', function(){
            hub.lastWatermark = 5000;
            hub.lastSeq = 5;
            assert.strictEqual(hub.hubConfigRegressed({ configs: {}, seq: 5, watermark: 100 }), true);
        });

        it('is true when seq drops below the last-seen value even if watermark is unchanged', function(){
            hub.lastWatermark = 5000;
            hub.lastSeq = 5;
            assert.strictEqual(hub.hubConfigRegressed({ configs: {}, seq: 1, watermark: 5000 }), true);
        });

        it('is false for a bare-map payload with no seq/configs wrapper', function(){
            hub.lastWatermark = 5000;
            assert.strictEqual(hub.hubConfigRegressed({ btc: { main: {} } }), false);
        });
    });
});
