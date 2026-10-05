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

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const { siblingCheckout, skipOrFail } = require('../../helpers/sibling_checkout.js');

const ROW_KEY = 'stake_weight_collation_activation.STAKE_WEIGHT_COLLATION_ACTIVATION';

/** The text of one addGate() statement for `key`, from its opening line to the closing `});`. */
function extractRow(source, key) {
    const open = "addGate('" + key + "'";
    const start = source.indexOf(open);
    assert.notStrictEqual(start, -1, 'no addGate row for ' + key);
    assert.strictEqual(source.indexOf(open, start + 1), -1, 'more than one addGate row for ' + key);
    const end = source.indexOf('\n});', start);
    assert.notStrictEqual(end, -1, 'unterminated addGate row for ' + key);
    return source.slice(start, end + 4);
}

describe('stake weight collation activation row twin', function () {
    const oursPath = path.join(__dirname, '../../../src/consensus/gate_registry/shared_rows_3.js');

    it('carries the row in the local registry', function () {
        const row = extractRow(fs.readFileSync(oursPath, 'utf8'), ROW_KEY);
        assert.ok(/regtest:\s*0,/.test(row), 'local row lacks the regtest height');
    });

    it('is byte-identical to the xchain-indexer canonical row', function () {
        const verdict = siblingCheckout(__dirname, '../../../../xchain-indexer/src/protocol_changes/shared_rows_3.js');
        if (!skipOrFail(this, verdict, 'the stake weight collation row twin guard')) return;

        const ours = extractRow(fs.readFileSync(oursPath, 'utf8'), ROW_KEY);
        const theirs = extractRow(fs.readFileSync(verdict.path, 'utf8'), ROW_KEY);
        assert.strictEqual(ours, theirs, ROW_KEY + ' in ' + oursPath + ' differs from ' + verdict.path);
    });

    it('extractRow detects a drifted height', function () {
        const base = extractRow(fs.readFileSync(oursPath, 'utf8'), ROW_KEY);
        const drifted = base.replace("'BTC:testnet':  155001", "'BTC:testnet':  155002");
        assert.notStrictEqual(drifted, base);
        assert.notStrictEqual(extractRow(drifted, ROW_KEY), base);
    });
});
