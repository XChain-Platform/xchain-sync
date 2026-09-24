// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers the client reconcile and paging knobs. One part of config_parsing.test.js.
const assert = require('assert');
const config = require('../../../../src/config');

const KNOBS = ['DISPENSERS_RECONCILE_EVERY', 'DISPENSERS_RECONCILE_MAX_INTERVAL_MS',
    'LOOKUP_PAGE_SIZE', 'GAP_LOG_INTERVAL_MS'];

// Clear the knobs around each test so an ambient value cannot leak in.
function registerKnobHooks(){
    let saved = {};
    beforeEach(function(){
        for(let key of KNOBS){ saved[key] = process.env[key]; delete process.env[key]; }
    });
    afterEach(function(){
        for(let key of KNOBS){
            if(saved[key] !== undefined) process.env[key] = saved[key];
            else delete process.env[key];
        }
    });
}

describe('Boundary: Config Parsing', function(){
    registerKnobHooks();
    describe('client reconcile and paging knobs reach the live config', function(){
        it('keep the reader defaults when unset', function(){
            let cfg = config.getConfig();
            assert.strictEqual(cfg.DISPENSERS_RECONCILE_EVERY, 20);
            assert.strictEqual(cfg.DISPENSERS_RECONCILE_MAX_INTERVAL_MS, 1800000);
            assert.strictEqual(cfg.LOOKUP_PAGE_SIZE, 50000);
            assert.strictEqual(cfg.GAP_LOG_INTERVAL_MS, 30000);
        });
        it('pass a set value through', function(){
            process.env.DISPENSERS_RECONCILE_EVERY = '5';
            process.env.DISPENSERS_RECONCILE_MAX_INTERVAL_MS = '60000';
            process.env.LOOKUP_PAGE_SIZE = '1000';
            process.env.GAP_LOG_INTERVAL_MS = '5000';
            let cfg = config.getConfig();
            assert.strictEqual(cfg.DISPENSERS_RECONCILE_EVERY, 5);
            assert.strictEqual(cfg.DISPENSERS_RECONCILE_MAX_INTERVAL_MS, 60000);
            assert.strictEqual(cfg.LOOKUP_PAGE_SIZE, 1000);
            assert.strictEqual(cfg.GAP_LOG_INTERVAL_MS, 5000);
        });
        it('keeps DISPENSERS_RECONCILE_MAX_INTERVAL_MS=0 as the documented disable', function(){
            process.env.DISPENSERS_RECONCILE_MAX_INTERVAL_MS = '0';
            assert.strictEqual(config.getConfig().DISPENSERS_RECONCILE_MAX_INTERVAL_MS, 0);
        });
        it('fall back to the default on a non-numeric or out-of-range value', function(){
            process.env.DISPENSERS_RECONCILE_EVERY = 'later';
            process.env.DISPENSERS_RECONCILE_MAX_INTERVAL_MS = '-1';
            process.env.LOOKUP_PAGE_SIZE = '0';
            process.env.GAP_LOG_INTERVAL_MS = '0';
            let cfg = config.getConfig();
            assert.strictEqual(cfg.DISPENSERS_RECONCILE_EVERY, 20);
            assert.strictEqual(cfg.DISPENSERS_RECONCILE_MAX_INTERVAL_MS, 1800000, 'a negative value must not read as the disable');
            assert.strictEqual(cfg.LOOKUP_PAGE_SIZE, 50000, '0 must not become a one-row page');
            assert.strictEqual(cfg.GAP_LOG_INTERVAL_MS, 30000);
        });
    });
});
