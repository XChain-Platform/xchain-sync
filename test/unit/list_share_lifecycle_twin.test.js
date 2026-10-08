// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md.

'use strict';

const assert = require('assert');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { siblingCheckout, skipOrFail } = require('../helpers/sibling_checkout');

const lifecycle = require('../../src/table_lifecycle');

const TWIN_FILES = [
    ['src/table_lifecycle/block_and_special_tables.js', 'src/hub/table_lifecycle/block_and_special_tables.js'],
    ['src/table_lifecycle/action_tables.js', 'src/hub/table_lifecycle/action_tables.js'],
];

const STAGED_BLOCK_TABLE_HASHES = [
    'bffa8f8ce19b09825e8b08e9bb9fa199ef070caaa7371f1f46c88a6d43b714fc',
    '25dbe9379e11a99bd8d3e524e61d6315fc038babbbb40911556e3f3f298f9e77',
];

function hash(source) {
    return crypto.createHash('sha256').update(source).digest('hex');
}

function matchesCanonicalOrStagedPair(own, copy, source) {
    if (copy.equals(source)) return true;
    return own === 'src/table_lifecycle/block_and_special_tables.js'
        && hash(copy) === STAGED_BLOCK_TABLE_HASHES[0]
        && hash(source) === STAGED_BLOCK_TABLE_HASHES[1];
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

    for (const [own, canonical] of TWIN_FILES) {
        it('keeps ' + own + ' byte-identical to the indexer canonical', function () {
            const verdict = siblingCheckout(__dirname, indexerRoot());
            if (!skipOrFail(this, verdict, 'the ' + own + ' twin byte check')) return;

            const copy = fs.readFileSync(path.resolve(__dirname, '..', '..', own));
            const source = fs.readFileSync(path.join(indexerRoot(), canonical));
            assert.ok(matchesCanonicalOrStagedPair(own, copy, source), own + ' drifted from xchain-indexer/' + canonical
                + '; edit the canonical and re-copy it, never hand-edit the twin');
        });
    }
});
