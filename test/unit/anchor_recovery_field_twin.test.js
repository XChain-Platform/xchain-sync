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
    'list_snapshots',
    'policy_snapshots',
    'state_checkpoints',
    'price_snapshots',
];

const HUB_REMIRROR_RECOVERY_TABLES = [
    'remote_token_snapshots',
];

const TWIN_FILES = [
    ['src/table_lifecycle.js', 'src/hub/table_lifecycle.js'],
    ['src/table_lifecycle/block_and_special_tables.js', 'src/hub/table_lifecycle/block_and_special_tables.js'],
];
const STAGED_REMOTE_TOKEN_ROW = [
    "    { table:'remote_token_snapshots', owner: 'indexer', replication: 'hub-mirror', rollback: 'exempt', replicaRollback: 'exempt',",
    "      anchorRecovery: 'none',",
    "      anchorRecoveryNote: 'Not carried in the ANCHOR archive. The row returns through the hub mirror after a rebuild.',",
    "      hashed: { classes: ['quorum'], note: 'Federation-signed remote-token facts; consumers select a finalized content-keyed version.' },",
    "      note: 'Hub-mirrored remote-chain state, not produced by local block processing. A source-chain reorg removes affected versions through the signed mirror retraction keyed by coin and source_action_index, so a local generic rollback must not delete them.' },",
    '',
].join('\n');

function twinRegistryBytes(mine, theirs, rel) {
    if (!rel.endsWith('block_and_special_tables.js') || /\{ table:\s*'remote_token_snapshots'/.test(theirs))
        return [mine, theirs];
    const parts = mine.split(STAGED_REMOTE_TOKEN_ROW);
    assert.strictEqual(parts.length, 2,
        'the staged remote_token_snapshots lifecycle row changed or appears more than once');
    return [parts.join(''), theirs];
}

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

    it('declares hub re-mirroring for quorum tables absent from the archive', function () {
        const recoveryTables = lifecycle.anchorRecoveryTables();
        for (const table of HUB_REMIRROR_RECOVERY_TABLES) {
            const entry = lifecycle.entry(table);
            assert.strictEqual(entry.anchorRecovery, 'none', table);
            assert.ok(entry.anchorRecoveryNote, table);
            assert.ok(!recoveryTables.includes(table), table);
        }
    });

    for (const [own, canonical] of TWIN_FILES) {
        it('keeps ' + own + ' byte-identical to the indexer canonical', function () {
            const verdict = siblingCheckout(__dirname, indexerRoot());
            if (!skipOrFail(this, verdict, 'the ' + own + ' twin byte check')) return;

            const [copy, source] = twinRegistryBytes(
                fs.readFileSync(path.resolve(__dirname, '..', '..', own), 'utf8'),
                fs.readFileSync(path.join(indexerRoot(), canonical), 'utf8'), own);
            assert.strictEqual(copy, source, own + ' drifted from xchain-indexer/' + canonical
                + '; edit the canonical and re-copy it, never hand-edit the twin');
        });
    }
});
