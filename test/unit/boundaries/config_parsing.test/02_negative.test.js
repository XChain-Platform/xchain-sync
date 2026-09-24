// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers negative values. One part of config_parsing.test.js.
const assert = require('assert');
const config = require('../../../../src/config');
const { registerHooks } = require('./helpers/config_parsing_suite');

describe('Boundary: Config Parsing', function(){
    registerHooks();
    describe('negative values clamped', function(){
        it('clamps WS_MAX_PER_IP=-1 to 1', function(){
            process.env.WS_MAX_PER_IP = '-1';
            assert.strictEqual(config.getConfig().WS_MAX_PER_IP, 1);
        });
        it('clamps HUB_PORT=-1 to 1', function(){
            process.env.HUB_PORT = '-1';
            assert.strictEqual(config.getConfig().HUB_PORT, 1);
        });
        it('clamps REPLICA_DB_PORT=-1 to 1', function(){
            process.env.REPLICA_DB_PORT = '-1';
            assert.strictEqual(config.getConfig().REPLICA_DB_PORT, 1);
        });
        it('clamps BLOCK_POLL_INTERVAL=-100 to 0', function(){
            process.env.BLOCK_POLL_INTERVAL = '-100';
            assert.strictEqual(config.getConfig().BLOCK_POLL_INTERVAL, 0);
        });
        it('clamps SNAPSHOT_RATE_FULL=-5 to 0', function(){
            process.env.SNAPSHOT_RATE_FULL = '-5';
            assert.strictEqual(config.getConfig().SNAPSHOT_RATE_FULL, 0);
        });
    });
});
