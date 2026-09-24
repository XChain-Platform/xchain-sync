// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers hash verification parsing. One part of config_parsing.test.js.
const assert = require('assert');
const config = require('../../../../src/config');
const { registerHooks } = require('./helpers/config_parsing_suite');

describe('Boundary: Config Parsing', function(){
    registerHooks();
    describe('VERIFY_HASHES case insensitivity', function(){
        it('"false" disables', function(){
            process.env.VERIFY_HASHES = 'false';
            assert.strictEqual(config.getConfig().VERIFY_HASHES, false);
        });
        it('"FALSE" disables', function(){
            process.env.VERIFY_HASHES = 'FALSE';
            assert.strictEqual(config.getConfig().VERIFY_HASHES, false);
        });
        it('"False" disables', function(){
            process.env.VERIFY_HASHES = 'False';
            assert.strictEqual(config.getConfig().VERIFY_HASHES, false);
        });
        it('"0" does NOT disable (not the word false)', function(){
            process.env.VERIFY_HASHES = '0';
            assert.strictEqual(config.getConfig().VERIFY_HASHES, true);
        });
        it('unset defaults to true', function(){
            delete process.env.VERIFY_HASHES;
            assert.strictEqual(config.getConfig().VERIFY_HASHES, true);
        });
    });
});
