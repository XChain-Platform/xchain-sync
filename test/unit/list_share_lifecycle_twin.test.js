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

const TWIN_FILES = [
    ['src/table_lifecycle/block_and_special_tables.js', 'src/hub/table_lifecycle/block_and_special_tables.js'],
    ['src/table_lifecycle/action_tables.js', 'src/hub/table_lifecycle/action_tables.js'],
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

describe('list sharing lifecycle registry twin', function () {

    it('keeps list_snapshots hub-mirrored and rollback-exempt', function () {
        const entry = lifecycle.entry('list_snapshots');
        assert.strictEqual(entry.replication, 'hub-mirror');
        assert.strictEqual(entry.rollback, 'exempt');
        assert.strictEqual(entry.replicaRollback, 'exempt');
        assert.strictEqual(entry.anchorRecovery, 'archive');
    });

    it('streams and rolls back list_share_mirrors by action', function () {
        const entry = lifecycle.entry('list_share_mirrors');
        assert.strictEqual(entry.replication, 'stream:action');
        assert.strictEqual(entry.rollback, 'action');
    });

    it('keeps remote_token_snapshots hub-mirrored and retraction-owned', function () {
        const entry = lifecycle.entry('remote_token_snapshots');
        assert.strictEqual(entry.replication, 'hub-mirror');
        assert.strictEqual(entry.rollback, 'exempt');
        assert.strictEqual(entry.replicaRollback, 'exempt');
        assert.strictEqual(entry.anchorRecovery, 'none');
        assert.ok(entry.anchorRecoveryNote);
        assert.deepStrictEqual(entry.hashed.classes, ['quorum']);
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
