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

// The v2 manifest is an explicit list, and an explicit list is only as good as
// the check that it is complete. This suite is that check: it finds every file
// under src/ that declares an activation map (by declaration shape, the same
// two patterns the code-structure gate and the v1 guard use) and every
// *_activation.js at any depth, and fails when a data export of one of them is
// not a manifest row. A carrier the manifest missed would let a replica on a
// stale copy publish the same v2 as a correctly-armed peer.
//
// It resolves src/ relative to itself, so the falsification suite can copy it
// into a mutated temp tree and watch it go red there.

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');

const SRC = path.join(__dirname, '../../../../src');
const { ENTRIES, collectRows } = require(path.join(SRC, 'consensus/armed_map/manifest'));
const { KEY_RE } = require(path.join(SRC, 'consensus/armed_map/canonical'));

// The ACTIVATION_MAP rule the platform's code-structure gate grades with, and
// the CARRIER_DECL of test/unit/consensus/armed_map/armed_map_fingerprint.test.js, each with a
// capture for the name.
const ACTIVATION_MAP = /\b([A-Z][A-Z0-9_]*_ACTIVATION)\s*=\s*\{/g;
const CARRIER_DECL = /^\s*(?:const|let|var)\s+([A-Z0-9_]*ACTIVATIONS?[A-Z0-9_]*)\s*=\s*(?:Object\.freeze\()?\{/gm;

/** {stem: Set(declared map names)} for every carrier under srcDir. */
function scanCarriers(srcDir) {
    const carriers = new Map();
    (function walk(dir, rel) {
        for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
            if (e.name === 'node_modules') continue;
            const r = rel ? rel + '/' + e.name : e.name;
            if (e.isDirectory()) { walk(path.join(dir, e.name), r); continue; }
            if (!e.name.endsWith('.js')) continue;
            const text = fs.readFileSync(path.join(dir, e.name), 'utf8');
            const names = new Set([...text.matchAll(ACTIVATION_MAP), ...text.matchAll(CARRIER_DECL)].map((m) => m[1]));
            if (names.size || e.name.endsWith('_activation.js')) carriers.set(r.replace(/\.js$/, ''), names);
        }
    })(srcDir, '');
    return carriers;
}

const carriers = scanCarriers(SRC);
const keys = new Set(ENTRIES.map(([key]) => key));

describe('armed map v2: manifest completeness over src/', function () {

    it('the carrier scan finds a real population, not a reassuring near-empty one', function () {
        // Sync carries eight *_activation.js files and four carriers outside
        // that convention today. Fix the scan if this trips, not the bound.
        assert.ok(carriers.size >= 12, 'the carrier scan found ' + carriers.size + ' files');
    });

    it('every data export of every carrier is a manifest row', function () {
        const missing = [];
        for (const [stem, declared] of carriers) {
            const mod = require(path.join(SRC, stem + '.js'));
            for (const name of declared) {
                if (!Object.prototype.hasOwnProperty.call(mod, name)) missing.push(stem + '.js declares ' + name + ' but does not export it');
            }
            for (const name of Object.keys(mod)) {
                if (typeof mod[name] !== 'function' && !keys.has(stem + '.' + name)) missing.push(stem + '.' + name);
            }
        }
        assert.deepStrictEqual(missing, [], 'not in src/consensus/armed_map/manifest.js, so a replica on a stale ' +
            'copy of these is invisible to v2: ' + missing.join(', '));
    });

    it('every manifest row names a carrier the scan found', function () {
        const strays = Array.from(keys).filter((key) => !carriers.has(key.slice(0, key.lastIndexOf('.'))));
        assert.deepStrictEqual(strays, []);
    });

    it('keys are unique and every one is in the key grammar', function () {
        assert.strictEqual(keys.size, ENTRIES.length, 'duplicate manifest key');
        assert.deepStrictEqual(Array.from(keys).filter((key) => !KEY_RE.test(key)), []);
    });

    it('every row resolves to the very value its carrier exports, with no refusal', function () {
        const collected = collectRows();
        assert.strictEqual(collected.ok, true, collected.reason);
        assert.strictEqual(collected.rows.length, ENTRIES.length);
        for (const [key, value] of collected.rows) {
            const dot = key.lastIndexOf('.');
            assert.strictEqual(value, require(path.join(SRC, key.slice(0, dot) + '.js'))[key.slice(dot + 1)], key);
        }
    });

    it('sync has no ProtocolChanges table, so it owes no protocol_changes.changes rows', function () {
        // The indexer hashes that table through a stub construction. If sync ever
        // gains the registry, this goes red until the manifest carries its rows.
        assert.strictEqual(fs.existsSync(path.join(SRC, 'protocol_changes.js')), false);
    });
});
