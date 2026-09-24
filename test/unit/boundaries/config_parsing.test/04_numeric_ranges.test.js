// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers float and large values. One part of config_parsing.test.js.
const assert = require('assert');
const config = require('../../../../src/config');
const { registerHooks } = require('./helpers/config_parsing_suite');

describe('Boundary: Config Parsing', function(){
    registerHooks();
    describe('float strings truncated', function(){
        it('WS_MAX_PER_IP=3.7 becomes 3', function(){
            process.env.WS_MAX_PER_IP = '3.7';
            assert.strictEqual(config.getConfig().WS_MAX_PER_IP, 3);
        });
        it('SYNC_API_PORT=3006.5 becomes 3006', function(){
            process.env.SYNC_API_PORT = '3006.5';
            assert.strictEqual(config.getConfig().SYNC_API_PORT, 3006);
        });
    });
    describe('large values', function(){
        it('SNAPSHOT_RATE_FULL=999999 preserved', function(){
            process.env.SNAPSHOT_RATE_FULL = '999999';
            assert.strictEqual(config.getConfig().SNAPSHOT_RATE_FULL, 999999);
        });
        it('BLOCK_POLL_INTERVAL=2147483647 preserved', function(){
            process.env.BLOCK_POLL_INTERVAL = '2147483647';
            assert.strictEqual(config.getConfig().BLOCK_POLL_INTERVAL, 2147483647);
        });
    });
});
