#!/usr/bin/env node
/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md.
 *
 **********************************************************************
 *
 * The identity pin: what this build IS, independent of what its tests say.
 *
 * WHY A SECOND PIN. The suite-title map proves the same tests still run. It
 * says nothing about the bytes a validator hashes, and a structural pass is
 * exactly the kind of change that can move those without failing a test. Two
 * populations carry that risk here:
 *
 *   the armed map   fingerprint v2 hashes every registry row by key and
 *                   resolved value. Carrier moves and formatting do not move
 *                   it, while a changed consensus value does.
 *   the vendored    src/coins/ is refreshed from the hub by a sync script.
 *   coin registry   A local edit here is drift that reddens every consumer.
 *   the carrier     bin/pins/carrier-logic.json hashes each carrier's TOKEN
 *   logic pin       stream, so it moves on a logic change and on nothing else.
 *
 * So the pin records the v2 meaning hash, the per-row hashes behind it, the
 * row count, the carrier logic digest, and the sha256 of each vendored coin
 * file.
 *
 * USAGE
 *   node bin/pin-identity.js                    human summary
 *   node bin/pin-identity.js --json             print the pin as JSON
 *   node bin/pin-identity.js --out <file>       write the pin as JSON
 *   node bin/pin-identity.js --compare <pin>    re-read the tree against a pin,
 *                                               exit 1 on any difference
 *
 ********************************************************************/

'use strict';

const fs     = require('fs');
const path   = require('path');
const crypto = require('crypto');

const REPO_ROOT = path.resolve(__dirname, '..');

// Vendored in from the hub by sync-coins.sh. Named rather than globbed: a
// globbed list would quietly absorb a file that appeared by accident, and the
// point of the pin is to notice exactly that.
const COIN_FILES = [
    'src/coins/BTC.js',
    'src/coins/DOGE.js',
    'src/coins/LTC.js',
    'src/coins/consensus_pin.js',
    'src/coins/index.js',
];

function sha256(rel) {
    return crypto.createHash('sha256').update(fs.readFileSync(path.join(REPO_ROOT, rel))).digest('hex');
}

/** The whole identity of this build, as the pin stores it. */
function buildPin() {
    const v2 = require(path.join(REPO_ROOT, 'src/consensus/armed_map/fingerprint.js')).computeArmedMapFingerprintV2();
    const logicPin = require(path.join(REPO_ROOT, 'bin/lib/carrier_logic_pin.js'));
    const coins = {};
    for (const rel of COIN_FILES) coins[rel] = sha256(rel);
    return {
        // The legacy field carries v2 and the version field says so; the _v2 alias of the
        // W1 to W4 window left the pin at W5 (activation-registry C4, D103).
        armed_map_fingerprint: v2.hex,
        armed_map_fingerprint_version: 2,
        armedMapRows: v2.rows || null,
        armed_map_rows: v2.count === undefined ? null : v2.count,
        carrier_logic_digest: logicPin.digest(logicPin.readPin(REPO_ROOT)),
        vendoredCoins: coins,
    };
}

/** Pin against tree, field by field, so a failure names the file that moved. */
function compare(pin, fresh) {
    const differences = [];
    for (const field of ['armed_map_fingerprint', 'armed_map_fingerprint_version', 'armed_map_rows', 'carrier_logic_digest']) {
        if (pin[field] !== fresh[field]) differences.push(`${field} ${pin[field]} became ${fresh[field]}`);
    }
    for (const group of ['armedMapRows', 'vendoredCoins']) {
        const names = Array.from(new Set(Object.keys(pin[group] || {}).concat(Object.keys(fresh[group] || {})))).sort();
        for (const name of names) {
            const before = (pin[group] || {})[name];
            const after = (fresh[group] || {})[name];
            if (before === after) continue;
            if (!before) differences.push(`${group}: ${name} appeared`);
            else if (!after) differences.push(`${group}: ${name} disappeared`);
            else differences.push(`${group}: ${name} changed bytes`);
        }
    }
    // A pin field this tool no longer writes (the W1 to W4 _v2 alias, or any future
    // retirement) is a pin taken by an older tool: it does not hold until re-pinned.
    for (const field of Object.keys(pin)) {
        if (!Object.prototype.hasOwnProperty.call(fresh, field)) differences.push(`${field} is no longer recorded; re-pin`);
    }
    return differences;
}

function parseArgs(argv) {
    const opts = {};
    for (let i = 0; i < argv.length; i += 1) {
        if (argv[i] === '--out') { opts.out = path.resolve(argv[i + 1]); i += 1; }
        else if (argv[i] === '--compare') { opts.compare = path.resolve(argv[i + 1]); i += 1; }
        else if (argv[i] === '--json') opts.json = true;
        else if (argv[i] === '--help' || argv[i] === '-h') opts.help = true;
    }
    return opts;
}

function main() {
    const opts = parseArgs(process.argv.slice(2));
    if (opts.help) {
        console.log(fs.readFileSync(__filename, 'utf8').split('*/')[0]);
        return;
    }
    const fresh = buildPin();

    if (opts.compare) {
        const pin = JSON.parse(fs.readFileSync(opts.compare, 'utf8'));
        const differences = compare(pin, fresh);
        if (!differences.length) {
            console.log(`identity holds against ${path.relative(REPO_ROOT, opts.compare)}`);
            return;
        }
        console.log(`${differences.length} difference(s) against ${path.relative(REPO_ROOT, opts.compare)}:`);
        for (const d of differences) console.log(`  ${d}`);
        process.exitCode = 1;
        return;
    }

    const text = `${JSON.stringify(fresh, null, 2)}\n`;
    if (opts.out) {
        fs.mkdirSync(path.dirname(opts.out), { recursive: true });
        fs.writeFileSync(opts.out, text);
    }
    if (opts.json) {
        console.log(text.trimEnd());
        return;
    }
    console.log(`armed-map fingerprint  ${fresh.armed_map_fingerprint}`);
    console.log(`armed-map version      ${fresh.armed_map_fingerprint_version}`);
    console.log(`armed-map v2 rows      ${fresh.armed_map_rows}`);
    console.log(`carrier logic digest   ${fresh.carrier_logic_digest}`);
    console.log(`vendored coin files    ${Object.keys(fresh.vendoredCoins).length}`);
    if (opts.out) console.log(`\nwritten to ${path.relative(REPO_ROOT, opts.out)}`);
}

if (require.main === module) main();

module.exports = { buildPin, compare, COIN_FILES };
