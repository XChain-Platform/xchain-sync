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
const Module = require('module');
const path   = require('path');

const ROOT = path.join(__dirname, '../../..');
const REGISTRY_PATH = path.join(ROOT, 'src/consensus/gate_registry.js');
const registry = require(REGISTRY_PATH);
const manifest = require(path.join(ROOT, 'src/consensus/armed_map/manifest.js'));

function registryWithWriter() {
    const source = fs.readFileSync(REGISTRY_PATH, 'utf8')
        .replace('addGate: undefined,', 'addGate,');
    const loaded = new Module(REGISTRY_PATH, module);
    loaded.filename = REGISTRY_PATH;
    loaded.paths = Module._nodeModulePaths(path.dirname(REGISTRY_PATH));
    loaded._compile(source, REGISTRY_PATH);
    return loaded.exports;
}

describe('consensus gate registry', function () {
    it('throws on a duplicate key', function () {
        const writable = registryWithWriter();
        assert.throws(() => writable.addGate(registry.keys()[0], 'constant', 1),
            /duplicate gate registry key/);
    });

    it('throws a RegistryMissError that names the missing key', function () {
        assert.throws(() => registry.get('missing_gate.VALUE'),
            (error) => error instanceof registry.RegistryMissError && error.message.includes('missing_gate.VALUE'));
    });

    it('keeps the SHARED block data-only', function () {
        const source = fs.readFileSync(REGISTRY_PATH, 'utf8');
        const block = source.slice(source.indexOf('// SHARED-GATES BEGIN'), source.indexOf('// SHARED-GATES END'));
        assert.ok(!/\brequire\s*\(/.test(block), 'SHARED block contains require()');
    });

    it('has exactly the manifest row count', function () {
        assert.strictEqual(registry.rows().length, manifest.ENTRIES.length);
    });

    it('stores frozen rows but gives each shim a mutable copy', function () {
        const key = 'checkpoint_commitment_activation.CHECKPOINT_COMMITMENT_ACTIVATION';
        const stored = new Map(registry.rows()).get(key);
        const copy = registry.get(key);
        assert.ok(Object.isFrozen(stored));
        copy.regtest = 10;
        assert.strictEqual(stored.regtest, 0);
    });
});
