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

// The registry-backed manifest is only useful if the registry, its shims and
// the independent W1 key census still name the same rows. This suite scans the
// literal get() and copy() calls in every shim and compares all three populations.
//
// It resolves src/ relative to itself, so the falsification suite can copy it
// into a mutated temp tree and watch it go red there.

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');

const SRC = path.join(__dirname, '../../../../src');
const { ENTRIES, EXPECTED_KEYS, collectRows } = require(path.join(SRC, 'consensus/armed_map/manifest'));
const { KEY_RE } = require(path.join(SRC, 'consensus/armed_map/canonical'));
const registry = require(path.join(SRC, 'consensus/gate_registry'));

/** {file: Set(registry keys)} for every shim under srcDir. */
function scanShims(srcDir) {
    const shims = new Map();
    (function walk(dir, rel) {
        for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
            if (e.name === 'node_modules') continue;
            const r = rel ? rel + '/' + e.name : e.name;
            if (e.isDirectory()) { walk(path.join(dir, e.name), r); continue; }
            if (!e.name.endsWith('.js')) continue;
            const text = fs.readFileSync(path.join(dir, e.name), 'utf8');
            const keys = new Set(Array.from(text.matchAll(/\b(?:get|copy)\(['"]([^'"]+)['"]\)/g), (m) => m[1]));
            if (keys.size && text.includes('gate_registry')) shims.set(r, keys);
        }
    })(srcDir, '');
    return shims;
}

const shims = scanShims(SRC);
const shimKeys = new Set(Array.from(shims.values()).flatMap((keys) => Array.from(keys)));
const manifestKeys = ENTRIES.map(([key]) => key);

describe('armed map v2: manifest completeness over src/', function () {

    it('the shim scan finds all twelve gate files and all 39 rows', function () {
        assert.strictEqual(shims.size, 12, 'the shim scan found ' + shims.size + ' files');
        assert.strictEqual(shimKeys.size, 39, 'the shim scan found ' + shimKeys.size + ' keys');
    });

    it('the shim keys equal the independent expected-key census', function () {
        assert.deepStrictEqual(Array.from(shimKeys).sort(), Array.from(EXPECTED_KEYS));
    });

    it('the registry and manifest carry exactly the expected keys', function () {
        const expected = new Set(EXPECTED_KEYS);
        const syncKeys = registry.keys().filter((key) => expected.has(key));
        assert.deepStrictEqual(syncKeys.slice().sort(), Array.from(EXPECTED_KEYS));
        assert.deepStrictEqual(manifestKeys.slice().sort(), Array.from(EXPECTED_KEYS));
    });

    it('keys are unique and every one is in the key grammar', function () {
        assert.strictEqual(new Set(manifestKeys).size, ENTRIES.length, 'duplicate manifest key');
        assert.deepStrictEqual(manifestKeys.filter((key) => !KEY_RE.test(key)), []);
    });

    it('every row resolves to the registry value, with no refusal', function () {
        const collected = collectRows();
        assert.strictEqual(collected.ok, true, collected.reason);
        assert.strictEqual(collected.rows.length, ENTRIES.length);
        for (const [key, value] of collected.rows) assert.deepStrictEqual(value, registry.get(key), key);
    });

    it('sync has no ProtocolChanges table, so it owes no protocol_changes.changes rows', function () {
        // The indexer hashes that table through a stub construction. If sync ever
        // gains the registry, this goes red until the manifest carries its rows.
        assert.strictEqual(fs.existsSync(path.join(SRC, 'protocol_changes.js')), false);
    });
});
