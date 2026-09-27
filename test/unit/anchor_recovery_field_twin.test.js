// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md.

'use strict';

const assert = require('assert');

const lifecycle = require('../../src/table_lifecycle');

const ARCHIVED_QUORUM_TABLES = [
    'bridge_transfers',
    'policy_snapshots',
    'state_checkpoints',
    'price_snapshots',
];

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
});
