// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// TRUST_PROXY decides whether sync keys its rate limits on the forwarded client
// or on the reverse proxy's own address. A value the parser does not recognize
// must never fall silently into the shared-bucket mode.

const assert = require('assert');
const config = require('../../src/config');
const { getLogger } = require('../../src/observability');

const logger = getLogger();

function loadWith(value){
    const previous = process.env.TRUST_PROXY;
    if(value === undefined) delete process.env.TRUST_PROXY;
    else process.env.TRUST_PROXY = value;
    const originalWarn = logger.warn;
    const warnings = [];
    logger.warn = (msg) => { warnings.push(String(msg)); };
    try {
        const cfg = config.getConfig();
        return { trustProxy: cfg.TRUST_PROXY, warnings: warnings.filter((w) => w.includes('TRUST_PROXY')) };
    } finally {
        logger.warn = originalWarn;
        if(previous === undefined) delete process.env.TRUST_PROXY;
        else process.env.TRUST_PROXY = previous;
    }
}

describe('config TRUST_PROXY parsing', function(){
    it('turns on for true in any case', function(){
        for(const v of ['true', 'TRUE', 'True', ' true ']){
            const r = loadWith(v);
            assert.strictEqual(r.trustProxy, true, v);
            assert.deepStrictEqual(r.warnings, [], v);
        }
    });

    it('turns on for 1, the one-hop spelling', function(){
        const r = loadWith('1');
        assert.strictEqual(r.trustProxy, true);
        assert.deepStrictEqual(r.warnings, []);
    });

    it('stays off with no warning when unset, empty, false or 0', function(){
        for(const v of [undefined, '', 'false', 'FALSE', '0']){
            const r = loadWith(v);
            assert.strictEqual(r.trustProxy, false, String(v));
            assert.deepStrictEqual(r.warnings, [], String(v));
        }
    });

    it('stays off but warns for a value it does not recognize', function(){
        for(const v of ['yes', '2', 'loopback', 'ture']){
            const r = loadWith(v);
            assert.strictEqual(r.trustProxy, false, v);
            assert.strictEqual(r.warnings.length, 1, v);
        }
    });
});
