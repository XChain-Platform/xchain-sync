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
//   holds     under a comment, a reformat, a rename and a move with the manifest repointed
//   poisons   to UNREADABLE when an export vanishes, a value is refused, or
//             node_modules is missing, and never to a plausible hex
//   is caught by the completeness suite when a manifest row is deleted
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

function runCompleteness(root) {
    return spawnSync(process.execPath, [require.resolve('mocha/bin/mocha.js'), '--no-config', '--timeout', '30000', COMPLETENESS],
        { cwd: root, encoding: 'utf8', env: cleanEnv() });
}

const movedRows = (a, b) => Object.keys({ ...a.rows, ...b.rows }).filter((k) => a.rows[k] !== b.rows[k]).sort();

describe('armed map v2: falsification on temp trees', function () {
    this.timeout(120000);
    let baseline;

    before(function () {
        baseline = readV2(tree());
        assert.match(baseline.hex, /^[0-9a-f]{64}$/, baseline.reason);
    });

    after(function () {
        for (const root of roots) fs.rmSync(root, { recursive: true, force: true });
    });

    it('a copied tree reads the same v2 as this checkout, so the harness measures the real thing', function () {
        const { computeArmedMapFingerprintV2 } = require(path.join(ROOT, 'src/consensus/armed_map/fingerprint_v2'));
        assert.strictEqual(baseline.hex, computeArmedMapFingerprintV2().hex);
    });

    it('does not move under the regtest venue arming environment (no sync carrier reads it)', function () {
        assert.strictEqual(readV2(tree(), VENUE_ENV).hex, baseline.hex);
    });

    it('moves when one committed height changes, and names that row alone', function () {
        const root = tree();
        edit(root, 'src/state_commitment_activation.js', "'BTC:testnet':  145000,", "'BTC:testnet':  145001,");
        const after = readV2(root);
        assert.notStrictEqual(after.hex, baseline.hex);
        assert.deepStrictEqual(movedRows(baseline, after), ['state_commitment_activation.STATE_COMMITMENT_ACTIVATION']);
    });

    it('moves when NOT-YET-PINNED (null) becomes the UNARMED sentinel', function () {
        const root = tree();
        edit(root, 'src/stake_weight_collation_activation.js', "'BTC:testnet':  null,", "'BTC:testnet':  9999999999,");
        const after = readV2(root);
        assert.notStrictEqual(after.hex, baseline.hex);
        assert.deepStrictEqual(movedRows(baseline, after), ['stake_weight_collation_activation.STAKE_WEIGHT_COLLATION_ACTIVATION']);
    });

    it('holds under a comment, a reformat, a rename and a move with only the manifest repointed', function () {
        const root = tree();
        fs.appendFileSync(path.join(root, 'src/stateHash.js'), '\n// a comment v1 would have hashed\n');
        edit(root, 'src/state_commitment_activation.js', "'BTC:mainnet':  958500,", "'BTC:mainnet':958500,");
        fs.renameSync(path.join(root, 'src/train_activation.js'), path.join(root, 'src/rule_set_train.js'));
        fs.mkdirSync(path.join(root, 'src/activations'));
        fs.renameSync(path.join(root, 'src/state_key_collation_activation.js'),
            path.join(root, 'src/activations/state_key_collation_activation.js'));
        edit(root, 'src/consensus/armed_map/manifest.js', "require('../../train_activation')", "require('../../rule_set_train')");
        edit(root, 'src/consensus/armed_map/manifest.js', "require('../../state_key_collation_activation')",
            "require('../../activations/state_key_collation_activation')");
        assert.strictEqual(readV2(root).hex, baseline.hex);
    });

    it('a deleted manifest row moves v2 and turns the completeness suite red', function () {
        const control = runCompleteness(tree());
        assert.strictEqual(control.status, 0, 'the completeness suite must pass on an unmodified copy first: ' + control.stdout);
        const root = tree();
        edit(root, 'src/consensus/armed_map/manifest.js', "        'STAKE_WEIGHT_MAX_SOURCES',\n", '');
        const after = readV2(root);
        assert.notStrictEqual(after.hex, baseline.hex);
        assert.strictEqual(after.count, baseline.count - 1);
        const red = runCompleteness(root);
        assert.notStrictEqual(red.status, 0, 'completeness stayed green with a row deleted');
        assert.ok(red.stdout.includes('swq_source_cap_activation.STAKE_WEIGHT_MAX_SOURCES'), red.stdout);
    });

    it('an export that vanishes from a carrier reads UNREADABLE and names the row', function () {
        const root = tree();
        edit(root, 'src/train_activation.js', 'module.exports = {\n    TRAIN_ACTIVATION,\n', 'module.exports = {\n');
        const after = readV2(root);
        assert.strictEqual(after.hex, 'UNREADABLE');
        assert.ok(after.reason.startsWith('train_activation.TRAIN_ACTIVATION:'), after.reason);
    });

    it('a value the canonicaliser refuses reads UNREADABLE', function () {
        const root = tree();
        edit(root, 'src/consensus-constants.js', "const GAS_TICK = 'XCHAIN';", 'const GAS_TICK = new Map();');
        const after = readV2(root);
        assert.strictEqual(after.hex, 'UNREADABLE');
        assert.ok(after.reason.startsWith('consensus-constants.GAS_TICK:'), after.reason);
    });

    it('without node_modules reads UNREADABLE, never a plausible hex', function () {
        const after = readV2(tree({ nodeModules: false }));
        assert.strictEqual(after.hex, 'UNREADABLE');
        assert.strictEqual(after.count, undefined);
    });
});
