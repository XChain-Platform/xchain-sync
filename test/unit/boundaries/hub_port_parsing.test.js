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
const { registerHooks } = require('./hub_port_parsing.test/helpers/hub_port_parsing_suite');

describe('Boundary: HubClient Port Parsing', function(){
    registerHooks();
    describe('parsePort static method', function(){
        it('valid port: returns as-is', function(){
            assert.strictEqual(HubClient.parsePort('3306', undefined), 3306);
        });
        it('zero: preserved (not treated as falsy)', function(){
            assert.strictEqual(HubClient.parsePort('0', undefined), 0);
        });
        it('falls back to secondary when primary is null', function(){
            assert.strictEqual(HubClient.parsePort(null, '5432'), 5432);
        });
        it('falls back to secondary when primary is undefined', function(){
            assert.strictEqual(HubClient.parsePort(undefined, '5432'), 5432);
        });
        it('falls back to secondary when primary is empty string', function(){
            assert.strictEqual(HubClient.parsePort('', '5432'), 5432);
        });
        it('defaults to 3306 when both are absent', function(){
            assert.strictEqual(HubClient.parsePort(undefined, undefined), 3306);
        });
        it('defaults to 3306 when both are null', function(){
            assert.strictEqual(HubClient.parsePort(null, null), 3306);
        });
    });
});
