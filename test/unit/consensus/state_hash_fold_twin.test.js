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
const stateHash = require('../../../src/consensus/state_hash.js');
const Database = require('../../../src/db');
const { siblingCheckout, skipOrFail } = require('../../helpers/sibling_checkout.js');

describe('state hash archive fold twin', function () {
    it('is byte-identical to the xchain-indexer state hash module', function () {
        const verdict = siblingCheckout(__dirname, '../../../../xchain-indexer/src/consensus/state_hash.js');
        if (!skipOrFail(this, verdict, 'the state_hash twin guard')) return;

        const oursPath = path.join(__dirname, '../../../src/consensus/state_hash.js');
        const ours = fs.readFileSync(oursPath, 'utf8');
        const theirs = fs.readFileSync(verdict.path, 'utf8');
        assert.strictEqual(ours, theirs, oursPath + ' differs from ' + verdict.path);
    });

    // Hold every collector the follower recomputes with to the source's bytes, not only the entry module.
    it('keeps every state_hash/ collector byte-identical to the xchain-indexer copy', function () {
        const verdict = siblingCheckout(__dirname, '../../../../xchain-indexer/src/consensus/state_hash.js');
        if (!skipOrFail(this, verdict, 'the state_hash collector twin guard')) return;

        const oursDir = path.join(__dirname, '../../../src/consensus/state_hash');
        const theirsDir = path.join(path.dirname(verdict.path), 'state_hash');
        const ours = fs.readdirSync(oursDir).filter((f) => f.endsWith('.js')).sort();
        assert.deepStrictEqual(ours, fs.readdirSync(theirsDir).filter((f) => f.endsWith('.js')).sort(),
            oursDir + ' and ' + theirsDir + ' carry different collector files');
        assert.ok(ours.includes('tolerant_query.js'), 'the shared read wrapper must be among the compared files');
        for (const file of ours) {
            assert.strictEqual(fs.readFileSync(path.join(oursDir, file), 'utf8'),
                fs.readFileSync(path.join(theirsDir, file), 'utf8'), file + ' differs from the xchain-indexer copy');
        }
    });

    // The read wrapper falls back to doQuery only for a handle without doQueryStrict; the follower's has one.
    it('gives the recompute a Database with doQueryStrict', function () {
        assert.strictEqual(typeof Database.prototype.doQueryStrict, 'function');
    });

    it('exports the archive fold predicates with their canonical SQL', function () {
        const archiveHeadPredicate = stateHash.archiveHeadPredicate;
        const checkpointSectionPredicate = stateHash.checkpointSectionPredicate;
        assert.strictEqual(typeof archiveHeadPredicate, 'function');
        assert.strictEqual(typeof checkpointSectionPredicate, 'function');
        assert.strictEqual(archiveHeadPredicate('p'), 'p.match_batch_seq IS NOT NULL AND p.version <> 2');
        assert.strictEqual(checkpointSectionPredicate('a'), 'a.chain IS NOT NULL');
    });
});
