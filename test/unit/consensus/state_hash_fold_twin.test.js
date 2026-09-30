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

    it('exports the archive fold predicates with their canonical SQL', function () {
        const archiveHeadPredicate = stateHash.archiveHeadPredicate;
        const checkpointSectionPredicate = stateHash.checkpointSectionPredicate;
        assert.strictEqual(typeof archiveHeadPredicate, 'function');
        assert.strictEqual(typeof checkpointSectionPredicate, 'function');
        assert.strictEqual(archiveHeadPredicate('p'), 'p.match_batch_seq IS NOT NULL AND p.version <> 2');
        assert.strictEqual(checkpointSectionPredicate('a'), 'a.chain IS NOT NULL');
    });
});
