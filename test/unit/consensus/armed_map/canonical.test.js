'use strict';

// Copyright © 2025-2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC - https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// The canonical value serialisation is the one place two processes could
// disagree about bytes while agreeing about meaning, or the reverse. Every
// expected string below is written out literally and every hash is recomputed
// here with crypto, so a change to the serialiser cannot also move what the
// test expects.

const assert = require('assert');
const crypto = require('crypto');
const fs     = require('fs');
const path   = require('path');
const C = require('../../../../src/consensus/armed_map/canonical');
const { siblingCheckout, skipOrFail } = require('../../../helpers/sibling_checkout.js');

const sha = (text) => crypto.createHash('sha256').update(text, 'utf8').digest('hex');
const refuses = (value) => assert.throws(() => C.canonicalValue(value), C.ArmedMapCanonicalError);

describe('armed map v2: canonical value serialisation', function () {

    describe('scalars', function () {
        it('writes null, booleans, strings and finite numbers the way JSON does', function () {
            assert.strictEqual(C.canonicalValue(null), 'null');
            assert.strictEqual(C.canonicalValue(true), 'true');
            assert.strictEqual(C.canonicalValue(false), 'false');
            assert.strictEqual(C.canonicalValue(145000), '145000');
            assert.strictEqual(C.canonicalValue(1.5), '1.5');
            assert.strictEqual(C.canonicalValue(1e21), '1e+21');
            assert.strictEqual(C.canonicalValue('a"b'), '"a\\"b"');
        });

        it('writes -0 as 0, so one height has one spelling', function () {
            assert.strictEqual(C.canonicalValue(-0), '0');
            assert.strictEqual(C.canonicalValue(-0), C.canonicalValue(0));
        });

        it('keeps NOT-YET-PINNED (null) distinct from the UNARMED sentinel', function () {
            assert.notStrictEqual(C.canonicalValue({ 'BTC:testnet': null }), C.canonicalValue({ 'BTC:testnet': 9999999999 }));
        });

        it('refuses non-finite numbers, BigInt, undefined, symbols and functions', function () {
            for (const value of [NaN, Infinity, -Infinity, 10n, undefined, Symbol('x'), () => 1]) refuses(value);
        });
    });

    describe('containers', function () {
        it('keeps array order, because an ordered list is part of the meaning', function () {
            assert.strictEqual(C.canonicalValue([1, 'a', null]), '[1,"a",null]');
            assert.notStrictEqual(C.canonicalValue([1, 2]), C.canonicalValue([2, 1]));
        });

        it('sorts object keys by UTF-16 code unit, so declaration order never moves the value', function () {
            assert.strictEqual(C.canonicalValue({ b: 1, a: 2, B: 3 }), '{"B":3,"a":2,"b":1}');
            assert.strictEqual(C.canonicalValue({ a: 2, B: 3, b: 1 }), C.canonicalValue({ b: 1, B: 3, a: 2 }));
        });

        it('treats an undefined property as absent, but a null property as a value', function () {
            assert.strictEqual(C.canonicalValue({ a: 1, b: undefined }), C.canonicalValue({ a: 1 }));
            assert.notStrictEqual(C.canonicalValue({ a: 1, b: null }), C.canonicalValue({ a: 1 }));
        });

        it('serialises a null-prototype object exactly like the same literal', function () {
            const bare = Object.assign(Object.create(null), { a: 1 });
            assert.strictEqual(C.canonicalValue(bare), '{"a":1}');
        });

        it('writes a RegExp as source and flags, so different patterns never collide', function () {
            assert.strictEqual(C.canonicalValue(/a\.b/g), 're:"a\\\\.b":"g"');
            const spellings = [/a/, /a/g, /b/g].map((re) => C.canonicalValue(re));
            assert.strictEqual(new Set(spellings).size, 3);
        });

        it('refuses Map, Set, Date, class instances and a function nested in data', function () {
            class Gate { constructor() { this.height = 1; } }
            const nested = [new Map(), new Set([1]), new Date(0), new Gate(), { a: () => 1 }, [1, () => 2], { a: { b: 10n } }];
            for (const value of nested) refuses(value);
        });
    });

    describe('preimage and fingerprint', function () {
        const ROWS = [['b_mod.X', 1], ['a_mod.Y', { k: null }], ['A_mod.Z', 's']];

        it('is DOMAIN followed by key=VCS lines in code-unit key order', function () {
            assert.strictEqual(C.DOMAIN, 'xchain-armed-map/v2\n');
            assert.strictEqual(C.preimage(ROWS), 'xchain-armed-map/v2\nA_mod.Z="s"\na_mod.Y={"k":null}\nb_mod.X=1\n');
        });

        it('hashes exactly the preimage and gives a per-row hash of each VCS', function () {
            const fp = C.fingerprint(ROWS);
            assert.strictEqual(fp.hex, sha('xchain-armed-map/v2\nA_mod.Z="s"\na_mod.Y={"k":null}\nb_mod.X=1\n'));
            assert.deepStrictEqual(fp.rows, { 'A_mod.Z': sha('"s"'), 'a_mod.Y': sha('{"k":null}'), 'b_mod.X': sha('1') });
            assert.strictEqual(fp.count, 3);
        });

        it('does not depend on the order rows are listed in', function () {
            assert.strictEqual(C.fingerprint(ROWS.slice().reverse()).hex, C.fingerprint(ROWS).hex);
        });

        it('moves the fingerprint and exactly one row hash when one value changes', function () {
            const before = C.fingerprint(ROWS);
            const after = C.fingerprint([['b_mod.X', 2], ROWS[1], ROWS[2]]);
            assert.notStrictEqual(after.hex, before.hex);
            const moved = Object.keys(before.rows).filter((k) => before.rows[k] !== after.rows[k]);
            assert.deepStrictEqual(moved, ['b_mod.X']);
        });

        it('refuses a duplicate key rather than letting one row shadow another', function () {
            assert.throws(() => C.fingerprint([['a_mod.X', 1], ['a_mod.X', 2]]), C.ArmedMapCanonicalError);
        });

        it('refuses keys outside the grammar', function () {
            for (const key of ['nodot', 'has space.X', '.X', 'mod.', 'mod..X', 'mod.X-Y', 'möd.X', 42]) {
                assert.throws(() => C.preimage([[key, 1]]), C.ArmedMapCanonicalError, 'accepted ' + String(key));
            }
        });

        it("accepts today's stem spellings, including camelCase, hyphenated and nested stems", function () {
            const keys = ['stateHash.DEACTIVATION_TABLES', 'consensus-constants.GAS_TICK',
                'protocol/constants.XBRIDGE_MAX_PER_BLOCK', 'protocol_changes.changes.NAME'];
            assert.strictEqual(C.fingerprint(keys.map((k) => [k, 1])).count, keys.length);
        });
    });

    describe('twin discipline', function () {
        it('requires nothing but crypto, so the same bytes serve both repos', function () {
            const src = fs.readFileSync(path.join(__dirname, '../../../../src/consensus/armed_map/canonical.js'), 'utf8');
            const required = Array.from(src.matchAll(/require\(\s*['"]([^'"]+)['"]\s*\)/g), (m) => m[1]);
            assert.deepStrictEqual(required, ['crypto']);
        });

        it('is byte-identical to the xchain-indexer canonicaliser', function () {
            const verdict = siblingCheckout(__dirname, '../../../../../xchain-indexer/src/consensus/armed_map/canonical.js');
            if (!skipOrFail(this, verdict, 'the armed-map canonicaliser twin guard')) return;
            const ours = fs.readFileSync(path.join(__dirname, '../../../../src/consensus/armed_map/canonical.js'));
            const theirs = fs.readFileSync(verdict.path);
            assert.ok(ours.equals(theirs), 'src/consensus/armed_map/canonical.js differs from ' + verdict.path +
                '; the indexer copy is canonical, so re-copy it here and never edit this one');
        });
    });
});
