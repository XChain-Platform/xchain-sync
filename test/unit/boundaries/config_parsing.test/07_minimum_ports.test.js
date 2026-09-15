// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers minimum port values. One part of config_parsing.test.js.
const assert = require('assert');
const config = require('../../../../src/config');
const { registerHooks } = require('./helpers/config_parsing_suite');

describe('Boundary: Config Parsing', function(){
    registerHooks();
    describe('minimum-one clamping for ports', function(){
        it('WS_MAX_PER_IP=0 clamps to 1', function(){
            process.env.WS_MAX_PER_IP = '0';
            assert.strictEqual(config.getConfig().WS_MAX_PER_IP, 1);
        });
        it('WS_MAX_PER_IP=1 stays 1', function(){
            process.env.WS_MAX_PER_IP = '1';
            assert.strictEqual(config.getConfig().WS_MAX_PER_IP, 1);
        });
        it('HUB_PORT=0 clamps to 1', function(){
            process.env.HUB_PORT = '0';
            assert.strictEqual(config.getConfig().HUB_PORT, 1);
        });
        it('REPLICA_DB_PORT=0 clamps to 1', function(){
            process.env.REPLICA_DB_PORT = '0';
            assert.strictEqual(config.getConfig().REPLICA_DB_PORT, 1);
        });
    });
});
