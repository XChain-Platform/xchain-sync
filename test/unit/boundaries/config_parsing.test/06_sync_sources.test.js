// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers sync source parsing. One part of config_parsing.test.js.
const assert = require('assert');
const config = require('../../../../src/config');
const { registerHooks } = require('./helpers/config_parsing_suite');

describe('Boundary: Config Parsing', function(){
    registerHooks();
    describe('SYNC_SOURCES parsing boundaries', function(){
        it('empty string yields empty after getConfig', function(){
            process.env.SYNC_SOURCES = '';
            assert.strictEqual(config.getConfig().SYNC_SOURCES, '');
        });
        it('single URL preserved', function(){
            process.env.SYNC_SOURCES = 'http://server1';
            assert.strictEqual(config.getConfig().SYNC_SOURCES, 'http://server1');
        });
        it('trailing comma preserved in raw config (parsed by ClientSync)', function(){
            process.env.SYNC_SOURCES = 'http://s1,';
            assert.strictEqual(config.getConfig().SYNC_SOURCES, 'http://s1,');
        });
        it('whitespace preserved in raw config (parsed by ClientSync)', function(){
            process.env.SYNC_SOURCES = ' http://s1 , http://s2 ';
            assert.strictEqual(config.getConfig().SYNC_SOURCES, ' http://s1 , http://s2 ');
        });
    });
});
