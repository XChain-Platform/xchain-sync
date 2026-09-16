'use strict';

// Copyright © 2025-2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC - https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md.

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');

const ROOT = path.join(__dirname, '../../..');
const REGISTRY_PATH = path.join(ROOT, 'src/consensus/gate_registry.js');
const PARTS_DIR = path.join(ROOT, 'src/consensus/gate_registry');
const PARTS = ['shared_rows_1.js', 'shared_rows_2.js', 'shared_rows_3.js', 'shared_rows_4.js', 'shared_rows_5.js'];
const registry = require(REGISTRY_PATH);
const manifest = require(path.join(ROOT, 'src/consensus/armed_map/manifest.js'));

describe('consensus gate registry', function () {
    it('throws on a duplicate key', function () {
        const { createRegistry } = require(path.join(PARTS_DIR, 'core.js'));
        const writable = createRegistry();
        writable.addGate('scratch.KEY', 'constant', 1);
        assert.throws(() => writable.addGate('scratch.KEY', 'constant', 1), /duplicate key scratch\.KEY/);
    });

    it('throws a RegistryMissError that names the missing key', function () {
        assert.throws(() => registry.get('missing_gate.VALUE'),
            (error) => error instanceof registry.RegistryMissError && error.message.includes('missing_gate.VALUE'));
    });

    it('keeps the SHARED block data-only', function () {
        assert.ok(!fs.readFileSync(REGISTRY_PATH, 'utf8').includes('// SHARED-GATES BEGIN'));
        for (const part of PARTS) {
            const source = fs.readFileSync(path.join(PARTS_DIR, part), 'utf8');
            const block = source.slice(source.indexOf('// SHARED-GATES BEGIN'), source.indexOf('// SHARED-GATES END'));
            assert.ok(!/\brequire\s*\(/.test(block), part + ' SHARED block contains require()');
        }
    });

    it('has exactly the manifest row count', function () {
        const expected = new Set(manifest.EXPECTED_KEYS);
        const syncRows = registry.rows().filter(([key]) => expected.has(key));
        assert.deepStrictEqual(syncRows.map(([key]) => key).sort(), [...expected].sort());
        assert.strictEqual(syncRows.length, manifest.ENTRIES.length);
    });

    it('stores frozen rows but gives each shim a mutable copy', function () {
        const key = 'checkpoint_commitment_activation.CHECKPOINT_COMMITMENT_ACTIVATION';
        const stored = registry.get(key);
        const copy = registry.copy(key);
        assert.ok(Object.isFrozen(stored));
        copy.regtest = 10;
        assert.strictEqual(stored.regtest, 0);
    });
});
