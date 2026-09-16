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

// The claims v2 makes, each tested by breaking a copy of src/ and reading the
// value a fresh process computes there (design section 7, P4):
//   moves     when a committed height changes, or NOT-YET-PINNED becomes UNARMED
//   holds     under a comment, a reformat, a rename and a move
//   poisons   to UNREADABLE when a registry row vanishes
//   is caught by the completeness suite when a registry row is deleted
// Each case edits text that must exist, so a drifted anchor fails loudly
// instead of silently testing an unmodified tree.

const assert = require('assert');
const fs     = require('fs');
const os     = require('os');
const path   = require('path');
const { spawnSync } = require('child_process');

const ROOT = path.join(__dirname, '../../../..');
const COMPLETENESS = 'test/unit/consensus/armed_map/completeness.test.js';
const VENUE_ENV = { XC_ROLLCALL_REGTEST_ACTIVATION: 'armed', XC_ROLLCALL_GATES_REGTEST_ACTIVATION: 'armed' };

const READ_V2 = 'const r = require(process.argv[1]).computeArmedMapFingerprintV2();' +
    'process.stdout.write(JSON.stringify({ hex: r.hex, count: r.count, rows: r.rows, reason: r.reason }));';

const roots = [];

function cleanEnv(extra) {
    const env = { ...process.env, ...extra };
    delete env.NODE_PATH;
    return env;
}

/** A copy of src/ (and the completeness suite) with node_modules linked unless told not to. */
function tree({ nodeModules = true } = {}) {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'armed-map-v2-'));
    roots.push(root);
    fs.cpSync(path.join(ROOT, 'src'), path.join(root, 'src'), { recursive: true });
    fs.mkdirSync(path.join(root, path.dirname(COMPLETENESS)), { recursive: true });
    fs.copyFileSync(path.join(ROOT, COMPLETENESS), path.join(root, COMPLETENESS));
    if (nodeModules) fs.symlinkSync(fs.realpathSync(path.join(ROOT, 'node_modules')), path.join(root, 'node_modules'), 'dir');
    return root;
}

function edit(root, rel, from, to) {
    const file = path.join(root, rel);
    const text = fs.readFileSync(file, 'utf8');
    assert.ok(text.includes(from), rel + ' no longer contains the anchor ' + JSON.stringify(from));
    fs.writeFileSync(file, text.replace(from, to));
}

function readV2(root, env) {
    const res = spawnSync(process.execPath, ['-e', READ_V2, path.join(root, 'src/consensus/armed_map/fingerprint_v2.js')],
        { cwd: root, encoding: 'utf8', env: cleanEnv(env) });
    assert.strictEqual(res.status, 0, res.stderr);
    return JSON.parse(res.stdout);
}

function boot(root, rel) {
    return spawnSync(process.execPath, ['-e', 'require(process.argv[1])', path.join(root, rel)],
        { cwd: root, encoding: 'utf8', env: cleanEnv() });
}

function runCompleteness(root) {
    return spawnSync(process.execPath, [require.resolve('mocha/bin/mocha.js'), '--no-config', '--timeout', '30000', COMPLETENESS],
        { cwd: root, encoding: 'utf8', env: cleanEnv() });
}

const movedRows = (a, b) => Object.keys({ ...a.rows, ...b.rows }).filter((k) => a.rows[k] !== b.rows[k]).sort();

let baseline;

// The unmodified tree's reading, taken once and shared by both blocks of the suite.
function readBaseline() {
    if (baseline) return;
    baseline = readV2(tree());
    assert.match(baseline.hex, /^[0-9a-f]{64}$/, baseline.reason);
}

function removeTrees() {
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
}

// Declared once per section under one title, so each callback stays under the 60-line
// function limit and the pinned suite titles do not move.
describe('armed map v2: falsification on temp trees', function () {
    this.timeout(120000);
    before(readBaseline);
    after(removeTrees);

    it('a copied tree reads the same v2 as this checkout, so the harness measures the real thing', function () {
        const { computeArmedMapFingerprintV2 } = require(path.join(ROOT, 'src/consensus/armed_map/fingerprint_v2'));
        assert.strictEqual(baseline.hex, computeArmedMapFingerprintV2().hex);
    });

    it('does not move under the regtest venue arming environment (no sync carrier reads it)', function () {
        assert.strictEqual(readV2(tree(), VENUE_ENV).hex, baseline.hex);
    });

    it('moves when one committed height changes, and names that row alone', function () {
        const root = tree();
        edit(root, 'src/consensus/gate_registry.js', '    testnet: 146000,', '    testnet: 146001,');
        const after = readV2(root);
        assert.notStrictEqual(after.hex, baseline.hex);
        assert.deepStrictEqual(movedRows(baseline, after), ['checkpoint_commitment_activation.CHECKPOINT_COMMITMENT_ACTIVATION']);
    });

    it('moves when NOT-YET-PINNED (null) becomes the UNARMED sentinel', function () {
        const root = tree();
        edit(root, 'src/consensus/gate_registry.js', "    'BTC:testnet':  null,", "    'BTC:testnet':  9999999999,");
        const after = readV2(root);
        assert.notStrictEqual(after.hex, baseline.hex);
        assert.deepStrictEqual(movedRows(baseline, after), ['stake_weight_collation_activation.STAKE_WEIGHT_COLLATION_ACTIVATION']);
    });

    it('holds under a comment, a registry reformat, a carrier rename and a move', function () {
        const root = tree();
        fs.appendFileSync(path.join(root, 'src/stateHash.js'), '\n// a carrier comment\n');
        edit(root, 'src/consensus/gate_registry.js', '    testnet: 146000,', '    testnet:  146000,');
        fs.renameSync(path.join(root, 'src/train_activation.js'), path.join(root, 'src/rule_set_train.js'));
        fs.mkdirSync(path.join(root, 'src/activations'));
        fs.renameSync(path.join(root, 'src/state_key_collation_activation.js'),
            path.join(root, 'src/activations/state_key_collation_activation.js'));
        assert.strictEqual(readV2(root).hex, baseline.hex);
    });
});

describe('armed map v2: falsification on temp trees', function () {
    this.timeout(120000);
    before(readBaseline);
    after(removeTrees);

    it('a deleted registry row makes boot throw, v2 UNREADABLE and the completeness suite red', function () {
        const control = runCompleteness(tree());
        assert.strictEqual(control.status, 0, 'the completeness suite must pass on an unmodified copy first: ' + control.stdout);
        const root = tree();
        edit(root, 'src/consensus/gate_registry.js',
            "addGate('swq_source_cap_activation.STAKE_WEIGHT_MAX_SOURCES', 'constant', 1000);\n", '');
        const failedBoot = boot(root, 'src/swq_source_cap_activation.js');
        assert.notStrictEqual(failedBoot.status, 0, 'the shim booted with its registry row absent');
        assert.ok(failedBoot.stderr.includes('swq_source_cap_activation.STAKE_WEIGHT_MAX_SOURCES'), failedBoot.stderr);
        const after = readV2(root);
        assert.strictEqual(after.hex, 'UNREADABLE');
        assert.ok(after.reason.includes('swq_source_cap_activation.STAKE_WEIGHT_MAX_SOURCES'), after.reason);
        const red = runCompleteness(root);
        assert.notStrictEqual(red.status, 0, 'completeness stayed green with a row deleted');
        assert.ok(red.stdout.includes('swq_source_cap_activation.STAKE_WEIGHT_MAX_SOURCES'), red.stdout);
    });

    it('an unexpected registry row reads UNREADABLE and names the row', function () {
        const root = tree();
        edit(root, 'src/consensus/gate_registry.js', '// SHARED-GATES END',
            "addGate('unexpected_gate.VALUE', 'constant', 1);\n// SHARED-GATES END");
        const after = readV2(root);
        assert.strictEqual(after.hex, 'UNREADABLE');
        assert.ok(after.reason.startsWith('unexpected_gate.VALUE:'), after.reason);
    });

    it('reads the same value without node_modules because every registry row is local data', function () {
        assert.strictEqual(readV2(tree({ nodeModules: false })).hex, baseline.hex);
    });
});
