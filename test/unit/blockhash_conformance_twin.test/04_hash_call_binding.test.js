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

// test/unit/blockhash_conformance_twin.test/04_hash_call_binding.test.js
//
// Static drift-lock for item 8 of ../blockhash_conformance_twin.test.js: the values each
// side hands to its hash fold. The tail case compares the fold body, whose identifiers are
// the indexer helper's PARAMETER names, so it cannot see the call that binds them.

'use strict';

const assert = require('assert');
const fs     = require('fs');
const { stripComments, extractFunction, syncFile, loadPair } = require('./helpers/twin_sources.js');

const SYNC_SIG = /async computeBlockHashes\(block_index, network, coin\)\{/;

// The previous-block fill both sides must write: each table chains to its OWN prior hash.
const PREV_HASH_FILLS = [
    "hashes['ledger'] = results[0].ledger;",
    "hashes['actions'] = results[0].actions;",
    "hashes['contracts'] = results[0].contracts;",
];

// Comment-stripped, whitespace-collapsed body of the function sigRe names.
function collapsedBody(src, sigRe, from){
    return stripComments(extractFunction(src, sigRe, from)).replace(/\s+/g, ' ');
}

// Count non-overlapping occurrences of text in a collapsed body.
function occurrences(body, text){
    return body.split(text).length - 1;
}

// Assert each statement appears exactly once, naming the one that does not.
function assertEachOnce(body, statements, from){
    for(const stmt of statements){
        assert.strictEqual(occurrences(body, stmt), 1,
            from + ' must contain exactly one `' + stmt + '`; the value it binds is a hash ' +
            'preimage input, so a one-sided change forks every follower\'s recomputed hash');
    }
}

// The previous-hash fill statements in source order, as written.
function prevHashFills(body){
    return body.match(/hashes\['\w+'\] = results\[0\]\.\w+;/g) || [];
}

describe('consensus block-hash conformance twins (static drift-lock) @regression', function(){
    // Reads this repo's own file, so it never skips when the indexer checkout is absent.
    it('BlockHasher gathers at block_index and chains to block_index - 1', function(){
        const fn = collapsedBody(fs.readFileSync(syncFile('src/client/block_hasher.js'), 'utf8'),
            SYNC_SIG, 'block_hasher.js');
        assertEachOnce(fn, [
            'const ledger = await getLedgerHashData.call(this, block_index);',
            'const actions = await getActionsHashData.call(this, block_index);',
            'const contracts_data = await getContractsHashData.call(this, block_index);',
            'const prev_block_index = block_index - 1;',
            'WHERE b.block_index=?`, [prev_block_index]);',
        ], 'BlockHasher.computeBlockHashes');
        assert.strictEqual(occurrences(fn, 'prev_block_index'), 2,
            'prev_block_index must be declared once and bound once, to the chaining query only');
        assert.deepStrictEqual(prevHashFills(fn), PREV_HASH_FILLS,
            'BlockHasher.computeBlockHashes cross-wires a previous-block hash into another table');
    });

    it('db.getBlockHashes hands the fold its rows in order and chains to block_index - 1', function(){
        const pair = loadPair(this, 'src/client/block_hasher.js', 'src/db/actions.js');
        if(!pair) return;
        const fn = collapsedBody(pair.indexer, /async getBlockHashes\(block_index\)\{/, 'db/actions.js');
        assertEachOnce(fn, [
            'const actions = await this.getBlockHashActionRows(block_index);',
            'const contracts_data = await this.getBlockHashContractData(block_index);',
            'const hashes = await this.getPreviousBlockHashes(block_index - 1);',
            'let info = this.foldBlockHashes(block_index, ledger, actions, contracts_data, hashes);',
            'this.foldBlockHashes(',
            'this.getPreviousBlockHashes(',
        ], 'db.getBlockHashes');
        assertEachOnce(stripComments(pair.indexer).replace(/\s+/g, ' '),
            ['foldBlockHashes(block_index, ledger, actions, contracts_data, hashes){'], 'db/actions.js');
    });

    it('db.getPreviousBlockHashes binds its argument and fills the same keys as BlockHasher', function(){
        const pair = loadPair(this, 'src/client/block_hasher.js', 'src/db/actions.js');
        if(!pair) return;
        const fn = collapsedBody(pair.indexer, /async getPreviousBlockHashes\(prev_block_index\)\{/, 'db/actions.js');
        assertEachOnce(fn, ['await this.doQuery(query, [prev_block_index]);'], 'db.getPreviousBlockHashes');
        assert.deepStrictEqual(prevHashFills(fn), prevHashFills(collapsedBody(pair.sync, SYNC_SIG, 'block_hasher.js')),
            'the previous-block hash fill drifted between db.getPreviousBlockHashes and BlockHasher');
        assert.deepStrictEqual(prevHashFills(fn), PREV_HASH_FILLS,
            'db.getPreviousBlockHashes cross-wires a previous-block hash into another table');
    });
});
