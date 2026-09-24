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
const config = require('../../../src/config');
const { registerHooks } = require('./config_parsing.test/helpers/config_parsing_suite');

describe('Boundary: Config Parsing', function(){
    registerHooks();

    describe('zero values (falsy-zero)', function(){
        it('preserves SYNC_API_PORT=0', function(){
            process.env.SYNC_API_PORT = '0';
            assert.strictEqual(config.getConfig().SYNC_API_PORT, 0);
        });

        it('preserves BLOCK_POLL_INTERVAL=0', function(){
            process.env.BLOCK_POLL_INTERVAL = '0';
            assert.strictEqual(config.getConfig().BLOCK_POLL_INTERVAL, 0);
        });

        it('preserves SNAPSHOT_RATE_FULL=0', function(){
            process.env.SNAPSHOT_RATE_FULL = '0';
            assert.strictEqual(config.getConfig().SNAPSHOT_RATE_FULL, 0);
        });

        it('preserves SNAPSHOT_RATE_INCR=0', function(){
            process.env.SNAPSHOT_RATE_INCR = '0';
            assert.strictEqual(config.getConfig().SNAPSHOT_RATE_INCR, 0);
        });
    });
});
