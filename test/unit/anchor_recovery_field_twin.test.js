// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md.

'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const { siblingCheckout, skipOrFail } = require('../helpers/sibling_checkout');

const lifecycle = require('../../src/table_lifecycle');

const ARCHIVED_QUORUM_TABLES = [
    'bridge_transfers',
    'policy_snapshots',
    'state_checkpoints',
    'price_snapshots',
];

const TWIN_FILES = [
    ['src/table_lifecycle.js', 'src/hub/table_lifecycle.js'],
    ['src/table_lifecycle/block_and_special_tables.js', 'src/hub/table_lifecycle/block_and_special_tables.js'],
];

function indexerRoot() {
    return process.env.XCHAIN_INDEXER_SQL_PATH
        ? path.resolve(process.env.XCHAIN_INDEXER_SQL_PATH, '..', '..')
        : path.resolve(__dirname, '..', '..', '..', 'xchain-indexer');
}

describe('anchor recovery registry twin', function () {

    it('declares archive recovery for every added quorum table', function () {
        for (const table of ARCHIVED_QUORUM_TABLES)
            assert.strictEqual(lifecycle.entry(table).anchorRecovery, 'archive', table);
    });

    it('exports the archive recovery table derivation', function () {
        assert.strictEqual(typeof lifecycle.anchorRecoveryTables, 'function');

        const recoveryTables = lifecycle.anchorRecoveryTables();
        for (const table of ARCHIVED_QUORUM_TABLES)
            assert.ok(recoveryTables.includes(table), table);
    });

    for (const [own, canonical] of TWIN_FILES) {
        it('keeps ' + own + ' byte-identical to the indexer canonical', function () {
            const verdict = siblingCheckout(__dirname, indexerRoot());
            if (!skipOrFail(this, verdict, 'the ' + own + ' twin byte check')) return;

            const copy = fs.readFileSync(path.resolve(__dirname, '..', '..', own));
            const source = fs.readFileSync(path.join(indexerRoot(), canonical));
            assert.ok(copy.equals(source), own + ' drifted from xchain-indexer/' + canonical
                + '; edit the canonical and re-copy it, never hand-edit the twin');
        });
    }
});
