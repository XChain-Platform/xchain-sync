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
const childProcess = require('child_process');
const fs = require('fs');
const path = require('path');
const { siblingCheckout } = require('../helpers/sibling_checkout');

const SYNC_STATE_HASH = path.resolve(__dirname, '../../src/consensus/state_hash.js');

function indexerStateHashPath() {
    return process.env.XCHAIN_INDEXER_SQL_PATH
        ? path.resolve(process.env.XCHAIN_INDEXER_SQL_PATH, '..', 'consensus/state_hash.js')
        : '../../../xchain-indexer/src/consensus/state_hash.js';
}

function readIndexerStateHash(verdict) {
    if (verdict.usable) return fs.readFileSync(verdict.path);

    assert.ok(fs.existsSync(verdict.path), verdict.reason);
    const indexerRoot = path.resolve(verdict.path, '../../..');
    return childProcess.execFileSync('git', [
        '-C', indexerRoot, 'show', 'origin/develop:src/consensus/state_hash.js',
    ]);
}

describe('anchor fold state hash twin', function () {
    it('is byte-identical to the xchain-indexer canonical copy', function () {
        const verdict = siblingCheckout(__dirname, indexerStateHashPath());
        const syncCopy = fs.readFileSync(SYNC_STATE_HASH);
        const indexerCopy = readIndexerStateHash(verdict);
        assert.ok(syncCopy.equals(indexerCopy),
            'state_hash.js drifted from the xchain-indexer canonical copy; re-vendor it byte-for-byte');
    });
});
