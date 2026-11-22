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

'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');

const TABLES = path.resolve(__dirname, '../../../src/db/tables.js');
const UPDATED_ROWS = path.resolve(__dirname, '../../../src/server/updated_rows.js');
const ROLLBACK = path.resolve(__dirname, '../../../src/client/rollback.js');

function normalize(src){
    return src.replace(/[`"']/g, ' ').replace(/\+/g, ' ').replace(/\s+/g, ' ').trim();
}

function extract(src, pattern, label){
    const hit = normalize(src).match(pattern);
    assert.ok(hit, 'could not extract ' + label + ' ATTEST batch-head statement');
    return hit[0];
}

function forwardStatement(src){
    const aliases = { ah: 'p', ac: 'c', aha: 'pa', aca: 'ca', ahs: 'ps', acs: 'cs' };
    return extract(src, /SELECT ah\.\* FROM attests ah .*? ac\.block_index BETWEEN \? AND \?/, 'forward')
        .replace(/\b(aha|aca|ahs|acs|ah|ac)\b/g, name => aliases[name])
        .replace(/\bheadVersion\b/g, 'ATTEST_BATCH_HEAD_VERSION')
        .replace(/\bcontinuationVersion\b/g, 'ATTEST_BATCH_CONTINUATION_VERSION');
}

function reverseStatement(src){
    return extract(src, /UPDATE attests p .*? p\.action_index < \?/, 'reverse');
}

function atoms(statement){
    return statement.replace(/BETWEEN \? AND \?/g, 'BETWEEN ? __BETWEEN_AND__ ?')
        .split(/\s+(?:JOIN|ON|AND|WHERE)\s+/)
        .map(atom => atom.replace(/__BETWEEN_AND__/g, 'AND').trim())
        .filter(Boolean);
}

function removeExactlyOnce(items, expected, side){
    assert.strictEqual(items.filter(item => item === expected).length, 1,
        side + ' statement must contain exactly one intentional atom: ' + expected);
    return items.filter(item => item !== expected);
}

function comparableAtoms(src, side){
    let out = atoms(side === 'forward' ? forwardStatement(src) : reverseStatement(src));
    const omitted = side === 'forward'
        ? ['SELECT p.* FROM attests p', 'c.block_index BETWEEN ? AND ?']
        : ['UPDATE attests p', 'c.action_index >= ?', 'index_statuses vs',
            'vs.status = valid SET p.status_id = vs.id', 'p.action_index < ?'];
    for(const atom of omitted) out = removeExactlyOnce(out, atom, side);
    return out.sort();
}

function parityDifference(forward, reverse){
    const left = comparableAtoms(forward, 'forward');
    const right = comparableAtoms(reverse, 'reverse');
    return {
        forwardOnly: left.filter(atom => !right.includes(atom)),
        reverseOnly: right.filter(atom => !left.includes(atom)),
    };
}

describe('Rollback coverage guard @regression', function(){
    it('ATTEST v5 batch-head forward carry and reverse reset have the same predicates', function(){
        const tables = fs.readFileSync(TABLES, 'utf8');
        const updatedRows = fs.readFileSync(UPDATED_ROWS, 'utf8');
        const rollback = fs.readFileSync(ROLLBACK, 'utf8');
        const call = /findFailedAttestBatchHeads\( ATTEST_BATCH_HEAD_VERSION, ATTEST_BATCH_CONTINUATION_VERSION, ATTEST_BATCH_COMPLETION_STAMP, from, to, conn\)/;
        const signature = /findFailedAttestBatchHeads\(headVersion, continuationVersion, completionStamp, from, to, conn\)/;

        assert.ok(call.test(normalize(updatedRows)), 'updated_rows must bind ATTEST head, continuation and stamp arguments in signature order');
        assert.ok(signature.test(normalize(tables)), 'findFailedAttestBatchHeads signature order changed without its caller');
        assert.ok(/\[ % completionStamp, from, to\]/.test(normalize(tables)),
            'forward carry must bind the completion stamp as a suffix LIKE');
        assert.ok(/\[ % ATTEST_BATCH_COMPLETION_STAMP, firstActionIndex, firstActionIndex\]/.test(normalize(rollback)),
            'reverse reset must bind the same completion stamp as a suffix LIKE');
        assert.deepStrictEqual(parityDifference(tables, rollback), { forwardOnly: [], reverseOnly: [] },
            'the forward carry and reverse reset ATTEST predicates must change together');

        const missingCrc = tables.replace('AND ac.batch_crc32 = ah.batch_crc32 ', '');
        assert.deepStrictEqual(parityDifference(missingCrc, rollback).reverseOnly,
            ['c.batch_crc32 = p.batch_crc32'], 'negative control must detect a forward-only predicate removal');
        const extraChunkGuard = tables.replace('AND ac.block_index BETWEEN ? AND ?',
            'AND ac.batch_chunk_index > 0 AND ac.block_index BETWEEN ? AND ?');
        assert.deepStrictEqual(parityDifference(extraChunkGuard, rollback).forwardOnly,
            ['c.batch_chunk_index > 0'], 'negative control must detect a forward-only predicate addition');
        const swappedCall = normalize(updatedRows).replace(
            'findFailedAttestBatchHeads( ATTEST_BATCH_HEAD_VERSION, ATTEST_BATCH_CONTINUATION_VERSION',
            'findFailedAttestBatchHeads( ATTEST_BATCH_CONTINUATION_VERSION, ATTEST_BATCH_HEAD_VERSION');
        assert.strictEqual(call.test(swappedCall), false, 'negative control must detect swapped call-site arguments');
    });
});
