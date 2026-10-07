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
 **********************************************************************
 * test/unit/blockhash_conformance_twin.test/03_state_root_assembly_twin.test.js
 *
 * Static drift-lock for the state-root assembly functions the header of
 * src/state_commitment/index.js lists. The identical functions compare
 * code-for-code; each root-neutral shape difference is applied to the INDEXER's
 * code (every substitution asserts it fired) before the comparison, so a
 * one-sided edit anywhere else fails and points back at that header.
 *
 * Skips when the xchain-indexer sibling is absent and fails where
 * XCHAIN_REQUIRE_SIBLINGS=1.
 */

'use strict';

const assert = require('assert');

const { normalize, extractFunction, loadPair } = require('./helpers/twin_sources.js');

const HEADER = 'xchain-sync/src/state_commitment/index.js';

// Apply [find, replace] pairs to an indexer slice, asserting each one matched.
function applyDeclared(src, pairs, from){
    let out = src;
    for(const [find, replace] of pairs){
        assert.ok(out.includes(find),
            'the indexer code no longer has the shape this guard expects, in ' + from +
            '. Missing: ' + find + '\nRe-derive this case and the root-neutral differences ' +
            'listed in the ' + HEADER + ' header before trusting this guard again.');
        out = out.replace(find, replace);
    }
    return out;
}

describe('state-root assembly twins: identical functions @regression', function(){
    it('assembleStateRoot and extraSubRootColumn are identical to indexer state_root.js', function(){
        const pair = loadPair(this, 'src/state_commitment/index.js', 'src/state_commitment/state_root.js');
        if(!pair) return;
        for(const sig of [/function assembleStateRoot\(balancesRootHex, stakesRootHex, extraSubRoots\)\{/,
                          /function extraSubRootColumn\(extraSubRoots, slotName\)\{/]){
            assert.strictEqual(normalize(extractFunction(pair.sync, sig, HEADER)),
                normalize(extractFunction(pair.indexer, sig, 'xchain-indexer/src/state_commitment/state_root.js')),
                sig + ' differs between xchain-sync and xchain-indexer. Port the change, or move it ' +
                'out of the identical list in the ' + HEADER + ' header');
        }
    });

    it('reservedSubRootCandidates is identical to indexer state_commitment/index.js', function(){
        const pair = loadPair(this, 'src/state_commitment/index.js', 'src/state_commitment/index.js');
        if(!pair) return;
        const sig = /async function reservedSubRootCandidates\(db, chain, network, blockIndex\)\{/;
        assert.strictEqual(normalize(extractFunction(pair.sync, sig, HEADER)),
            normalize(extractFunction(pair.indexer, sig, 'xchain-indexer/src/state_commitment/index.js')),
            'reservedSubRootCandidates differs between xchain-sync and xchain-indexer. Port the ' +
            'change, or move it out of the identical list in the ' + HEADER + ' header');
    });
});

describe('state-root assembly twins: computeBlockMerkleRoot @regression', function(){
    it('differs only by the collation arguments (declared)', function(){
        const pair = loadPair(this, 'src/state_commitment/index.js', 'src/state_commitment/state_root.js');
        if(!pair) return;
        const from = 'xchain-indexer/src/state_commitment/state_root.js';
        const idx = applyDeclared(
            normalize(extractFunction(pair.indexer, /async function computeBlockMerkleRoot\(db, blockIndex\)\{/, from)),
            [['async function computeBlockMerkleRoot(db, blockIndex){',
              'async function computeBlockMerkleRoot(db, blockIndex, network, coin){'],
             ['db.getBlockLeafRows(blockIndex)', 'db.getBlockLeafRows(blockIndex, undefined, network, coin)']],
            from);
        assert.strictEqual(normalize(extractFunction(pair.sync,
            /async function computeBlockMerkleRoot\(db, blockIndex, network, coin\)\{/, HEADER)), idx,
            'computeBlockMerkleRoot differs by more than the declared collation arguments. Port the ' +
            'change, or extend the root-neutral differences in the ' + HEADER + ' header');
    });
});

describe('state-root assembly twins: buildFullBalancesRoot @regression', function(){
    it('differs only by the inlined ledger read (declared)', function(){
        const pair = loadPair(this, 'src/state_commitment/index.js', 'src/state_commitment/full_balances_root.js');
        if(!pair) return;
        const ledger = loadPair(this, 'src/state_commitment/index.js', 'src/db/state_commitment/ledger_reads.js');
        const from = 'xchain-indexer/src/state_commitment/full_balances_root.js';
        const read = normalize(extractFunction(ledger.indexer, /async function getNonzeroNetBalances\(db\)\{/,
            'xchain-indexer/src/db/state_commitment/ledger_reads.js'));
        const m = read.match(/^async function getNonzeroNetBalances\(db\)\{ return (db\.doQueryStrict\(.*\)); \}$/);
        assert.ok(m, 'getNonzeroNetBalances is no longer a single doQueryStrict read; re-derive this case');
        const idx = applyDeclared(
            normalize(extractFunction(pair.indexer,
                /async function buildFullBalancesRootWith\(smt, db, chain, network, blockIndex, opts\)\{/, from)),
            [['async function buildFullBalancesRootWith(smt, db, chain, network, blockIndex, opts){',
              'async function buildFullBalancesRoot(db, chain, network, blockIndex, opts){ ' +
              'const smt = new PersistentSMT(new DbNodeStore(db));'],
             ['LEDGER.getNonzeroNetBalances(db)', m[1]],
             ['leafOrNull(r.net)', '_leafOrNull(r.net)']],
            from);
        assert.strictEqual(normalize(extractFunction(pair.sync,
            /async function buildFullBalancesRoot\(db, chain, network, blockIndex, opts\)\{/, HEADER)), idx,
            'buildFullBalancesRoot differs by more than the declared inlined ledger read. Port the ' +
            'change, or extend the root-neutral differences in the ' + HEADER + ' header');
    });
});

describe('state-root assembly twins: leaf encoders @regression', function(){
    it('differ only by name (declared)', function(){
        const pair = loadPair(this, 'src/state_commitment/index.js', 'src/state_commitment/leaf_values.js');
        if(!pair) return;
        const from = 'xchain-indexer/src/state_commitment/leaf_values.js';
        const zero = 'const ZERO_CANON = M.canonicalAmount(\'0\');';
        assert.ok(pair.sync.includes(zero) && pair.indexer.includes(zero), 'ZERO_CANON is no longer ' + zero);
        for(const [idxSig, idxName, syncSig, syncName] of [
            [/function canonicalAmountOf\(amountStr\)\{/, 'canonicalAmountOf', /function _nz\(amountStr\)\{/, '_nz'],
            [/function leafOrNull\(amountStr\)\{/, 'leafOrNull', /function _leafOrNull\(amountStr\)\{/, '_leafOrNull']
        ]){
            const renames = [['function ' + idxName + '(', 'function ' + syncName + '(']];
            if(idxName === 'leafOrNull') renames.push(['canonicalAmountOf(amountStr)', '_nz(amountStr)']);
            const idx = applyDeclared(normalize(extractFunction(pair.indexer, idxSig, from)), renames, from);
            assert.strictEqual(normalize(extractFunction(pair.sync, syncSig, HEADER)), idx,
                syncName + ' differs from the indexer ' + idxName + ' by more than its name. Port the ' +
                'change, or extend the root-neutral differences in the ' + HEADER + ' header');
        }
    });
});
