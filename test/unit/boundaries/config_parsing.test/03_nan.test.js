// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers invalid numeric inputs. One part of config_parsing.test.js.
const assert = require('assert');
const config = require('../../../../src/config');
const { registerHooks } = require('./helpers/config_parsing_suite');

describe('Boundary: Config Parsing', function(){
    registerHooks();
    describe('NaN inputs default gracefully', function(){
        it('SYNC_API_PORT=abc defaults to 3006', function(){
            process.env.SYNC_API_PORT = 'abc';
            assert.strictEqual(config.getConfig().SYNC_API_PORT, 3006);
        });
        it('WS_MAX_PER_IP=xyz defaults to 100', function(){
            process.env.WS_MAX_PER_IP = 'xyz';
            assert.strictEqual(config.getConfig().WS_MAX_PER_IP, 100);
        });
        it('HUB_PORT="" defaults to 10000', function(){
            process.env.HUB_PORT = '';
            assert.strictEqual(config.getConfig().HUB_PORT, 10000);
        });
        it('BLOCK_POLL_INTERVAL=undefined defaults to 3000', function(){
            delete process.env.BLOCK_POLL_INTERVAL;
            assert.strictEqual(config.getConfig().BLOCK_POLL_INTERVAL, 3000);
        });
    });
});
