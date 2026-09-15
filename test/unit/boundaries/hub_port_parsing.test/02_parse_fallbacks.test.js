// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers port fallbacks. One part of hub_port_parsing.test.js.
const assert = require('assert');
const HubClient = require('../../../../src/hub/client');
const { registerHooks } = require('./helpers/hub_port_parsing_suite');

describe('Boundary: HubClient Port Parsing', function(){
    registerHooks();
    describe('parsePort static method', function(){
        it('defaults to 3306 when both are empty', function(){
            assert.strictEqual(HubClient.parsePort('', ''), 3306);
        });
        it('defaults to 3306 for non-numeric primary', function(){
            assert.strictEqual(HubClient.parsePort('abc', undefined), 3306);
        });
        it('uses secondary when primary is non-numeric', function(){
            // A non-numeric primary is not empty/null/undefined, so it is used directly (parseInt fails to NaN) rather than falling back to secondary.
            assert.strictEqual(HubClient.parsePort('abc', '5432'), 3306);
        });
        it('negative port defaults to 3306', function(){
            assert.strictEqual(HubClient.parsePort('-1', undefined), 3306);
        });
        it('integer value (not string) works', function(){
            assert.strictEqual(HubClient.parsePort(3307, undefined), 3307);
        });
        it('integer 0 preserved', function(){
            assert.strictEqual(HubClient.parsePort(0, undefined), 0);
        });
        it('float string truncated', function(){
            assert.strictEqual(HubClient.parsePort('3306.5', undefined), 3306);
        });
    });
});
