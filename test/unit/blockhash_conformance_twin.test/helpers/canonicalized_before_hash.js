/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************/

// test/unit/blockhash_conformance_twin.test/helpers/canonicalized_before_hash.js
//
// The special-address canonicalization check the conformance suite runs on every
// copy of the ledger gathering: BlockHasher, the indexer's getBlockHashLedgerRows and
// the follower's getLedgerLeafRows.

'use strict';

const assert = require('assert');
const { stripComments } = require('./twin_sources.js');

// Assert the three canonicalization loops run INSIDE fnSrc, after the escrows
// gather (afterText) and before hashing (beforeText), with nothing reassigning or
// reordering a ledger row set between the last loop and the hash.
function assertCanonicalizedBeforeHash(fnSrc, from, afterText, beforeText){
    const body  = stripComments(fnSrc);
    const start = body.indexOf(afterText);
    const end   = body.indexOf(beforeText, start);
    assert.ok(start !== -1 && end > start, from + ' no longer has `' + afterText + '` followed by `' + beforeText + '`');
    const loopRe = /for \(const row of ledger\.(credits|debits|escrows)\)\s+row\.address = canonicalizeHashAddress\(row\.address\);/g;
    const seen = new Set();
    let m, lastEnd = -1;
    while((m = loopRe.exec(body)) !== null){
        assert.ok(m.index > start && m.index < end, 'the ' + m[1] + ' canonicalization loop in ' + from +
            ' runs outside the window between the escrows gather and the hash');
        seen.add(m[1]);
        lastEnd = m.index + m[0].length;
    }
    assert.deepStrictEqual([...seen].sort(), ['credits', 'debits', 'escrows'],
        from + ' must canonicalize BURN/GAS/DONATE/REWARD addresses on all three ledger row sets ' +
        'before hashing; a missing loop leaks the per-chain address encoding into the hash on one side only');
    assert.doesNotMatch(body.slice(lastEnd, end), /ledger\.(credits|debits|escrows)\s*(=(?!=)|\.(sort|reverse|splice|map|filter)\()/,
        from + ' reassigns or reorders a canonicalized ledger row set before hashing it');
}

module.exports = { assertCanonicalizedBeforeHash };
