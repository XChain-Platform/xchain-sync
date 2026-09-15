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

const HashVerifier = require('../../../src/client/hash_verifier');

describe('Advisory table-content parity', function(){

    describe('compareTableContent', function(){

        const hv = new HashVerifier();

        it('case 2: identical maps match', function(){
            let m = { window: 10, block: 5, tables: { sends: { n: 2, h: 'aaa' }, issues: { n: 1, h: 'bbb' } } };
            let res = hv.compareTableContent(5, m, m);
            assert.strictEqual(res.match, true);
            assert.strictEqual(res.compared, 2);
            assert.deepStrictEqual(res.mismatches, []);
        });

        it('case 3: equal count + different digest is the reported mismatch', function(){
            let res = hv.compareTableContent(5,
                { tables: { sends: { n: 2, h: 'local' } } },
                { tables: { sends: { n: 2, h: 'remote' } } });
            assert.strictEqual(res.match, false);
            assert.deepStrictEqual(res.mismatches, [{ table: 'sends', rows: 2, local: 'local', remote: 'remote' }]);
        });

        it('case 6: a row-count difference is skipped, never reported as divergence', function(){
            // That is completeness (the /status count check owns it) or a source that
            // has simply advanced a block inside the window. Either way, not a fork.
            let res = hv.compareTableContent(5,
                { tables: { sends: { n: 1, h: 'x' } } },
                { tables: { sends: { n: 2, h: 'y' } } });
            assert.strictEqual(res.match, true);
            assert.strictEqual(res.compared, 0);
            assert.deepStrictEqual(res.skipped, [{ table: 'sends', reason: 'row-count-differs', local: 1, remote: 2 }]);
        });

        it('a table present on one side only is skipped with its own reason', function(){
            let res = hv.compareTableContent(5,
                { tables: { sends: { n: 1, h: 'x' } } },
                { tables: { issues: { n: 1, h: 'y' } } });
            assert.strictEqual(res.match, true);
            assert.deepStrictEqual(res.skipped.map(s => s.table + ':' + s.reason).sort(),
                ['issues:absent-locally', 'sends:absent-on-source']);
        });

        it('tolerates a missing or empty payload on either side', function(){
            assert.strictEqual(hv.compareTableContent(5, null, null).match, true);
            assert.strictEqual(hv.compareTableContent(5, {}, { tables: {} }).match, true);
        });

        it('compares numeric counts across the JSON round-trip (string n must not read as a difference)', function(){
            let res = hv.compareTableContent(5,
                { tables: { sends: { n: 2, h: 'same' } } },
                { tables: { sends: { n: '2', h: 'same' } } });
            assert.strictEqual(res.compared, 1);
            assert.strictEqual(res.match, true);
        });
    });
});
