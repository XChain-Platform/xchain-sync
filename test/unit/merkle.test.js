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
 *
 * Unit golden + behavioral lock for src/merkle.js (SPV primitives, spec §3-§5).
 * Reloads test/fixtures/merkle-vectors.json and asserts the live module
 * reproduces every committed root/proof, plus verify round-trips, delete-on-zero,
 * non-membership, compression, and the field-boundary injectivity guard.
 *
 ********************************************************************/

'use strict';

const assert = require('assert');
const path   = require('path');
const M      = require('../../src/merkle.js');
const V       = require('../fixtures/merkle-vectors.json');

const CHAIN = V.chain, NETWORK = V.network;
const balKey = (e) => M.balanceKey(CHAIN, NETWORK, e.address, e.tick);

function buildBalTree(entries){
    const t = new M.SparseMerkleTree();
    for(const e of entries) t.set(balKey(e), M.amountLeaf(e.amount));
    return t;
}

describe('merkle: SPV light-client primitives @regression', function(){

    describe('version + frozen constants', function(){
        it('scheme versions match the golden', function(){
            assert.strictEqual(M.MERKLE_VERSION,       V.merkle_version);
            assert.strictEqual(M.STATE_ROOT_VERSION,   V.state_root_version);
            assert.strictEqual(M.BLOCK_MERKLE_VERSION, V.block_merkle_version);
            assert.strictEqual(M.SMT_DEPTH,            V.smt_depth);
        });
        it('EMPTY_0 / EMPTY[h] recurrence is frozen', function(){
            assert.strictEqual(M.toHex(M.EMPTY[0]),        V.constants.empty_0);
            assert.strictEqual(M.toHex(M.EMPTY[1]),        V.constants.empty_1);
            assert.strictEqual(M.toHex(M.EMPTY[255]),      V.constants.empty_255);
            assert.strictEqual(M.toHex(M.EMPTY_SMT_ROOT),  V.constants.empty_smt_root);
            // recurrence: EMPTY[1] == nodeHash(EMPTY[0], EMPTY[0])
            assert.strictEqual(M.toHex(M.nodeHash(M.EMPTY[0], M.EMPTY[0])), V.constants.empty_1);
        });
        it('leaf/node domain separation is frozen', function(){
            assert.strictEqual(M.toHex(M.leafHash('hello')), V.primitives.leaf_hello);
            // leaf and node hashing of the same bytes must differ (domain separation)
            assert.notStrictEqual(M.toHex(M.leafHash(Buffer.concat([M.EMPTY[0], M.EMPTY[0]]))),
                                  M.toHex(M.nodeHash(M.EMPTY[0], M.EMPTY[0])));
        });
    });

    describe('canonical amount (§3.2)', function(){
        it('normalizes to fixed 18 dp', function(){
            for(const c of V.canonical_amount) assert.strictEqual(M.canonicalAmount(c.in), c.out);
        });
        it('rejects negative, exponent, and overlong fraction', function(){
            assert.throws(() => M.canonicalAmount('-1'));
            assert.throws(() => M.canonicalAmount('1e3'));
            assert.throws(() => M.canonicalAmount('1.0000000000000000001')); // 19 dp
            assert.throws(() => M.canonicalAmount(42));                        // non-string
        });
    });
});

describe('merkle: SPV light-client primitives @regression', function(){

    describe('key derivation (§3.2, §4.2)', function(){
        it('balance + stake keys match the golden', function(){
            assert.strictEqual(M.toHex(balKey(V.smt.membership.target)), V.keys.member_balance_key);
            assert.strictEqual(M.toHex(M.stakeKey(V.smt.stake_entries[0].pubkey, V.smt.stake_entries[0].capability)),
                               V.keys.stake_key_example);
        });
        it('rejects a 0x00-bearing field (injectivity guard)', function(){
            const nulField = 'a\u0000b';
            assert.strictEqual(nulField.charCodeAt(1), 0);
            assert.throws(() => M.joinFields(['XCHAIN_BAL', nulField]));
        });
        it('field boundaries are unambiguous: shifting a split point changes the encoding', function(){
            // SPV conformance vector: ['AB','C'] and ['A','BC'] concatenate to the
            // same bytes; the 0x00 separator must land differently, so the joined
            // encodings (and any keys derived from them) must not collide.
            assert.ok(!M.joinFields(['AB', 'C']).equals(M.joinFields(['A', 'BC'])));
            assert.notStrictEqual(M.toHex(M.smtKey('AB', ['C'])), M.toHex(M.smtKey('A', ['BC'])));
            // same shift inside balanceKey identity fields (address/tick split)
            assert.notStrictEqual(M.toHex(M.balanceKey('BTC', 'regtest', 'addrX', 'YTK')),
                                  M.toHex(M.balanceKey('BTC', 'regtest', 'addrXY', 'TK')));
        });
        it('different identities derive different keys', function(){
            assert.notStrictEqual(M.toHex(M.balanceKey('BTC', 'regtest', 'A', 'X')),
                                  M.toHex(M.balanceKey('BTC', 'regtest', 'A', 'Y')));
        });
    });
});

describe('merkle: SPV light-client primitives @regression', function(){

    describe('SMT roots + proofs (§4)', function(){
        it('balances_root + stakes_root match the golden', function(){
            assert.strictEqual(buildBalTree(V.smt.bal_entries).rootHex(), V.smt.balances_root);
            const stk = new M.SparseMerkleTree();
            for(const e of V.smt.stake_entries) stk.set(M.stakeKey(e.pubkey, e.capability), M.amountLeaf(e.amount));
            assert.strictEqual(stk.rootHex(), V.smt.stakes_root);
        });
        it('membership proof verifies (compressed + decompressed)', function(){
            const t = buildBalTree(V.smt.bal_entries);
            const k = balKey(V.smt.membership.target);
            const c = V.smt.membership.compressed;
            assert.strictEqual(V.smt.membership.leaf_value, M.toHex(M.amountLeaf(V.smt.membership.target.amount)));
            assert.ok(M.verifyCompressedSmtProof(V.smt.balances_root, k, V.smt.membership.leaf_value, c));
            // a wrong value leaf must fail
            assert.ok(!M.verifyCompressedSmtProof(V.smt.balances_root, k, M.toHex(M.amountLeaf('999')), c));
            assert.ok(t.has(k));
        });
        it('non-membership proof verifies (leaf_value null)', function(){
            const k = M.balanceKey(CHAIN, NETWORK, V.smt.nonmembership.target.address, V.smt.nonmembership.target.tick);
            assert.ok(M.verifyCompressedSmtProof(V.smt.balances_root, k, null, V.smt.nonmembership.compressed));
            // claiming membership for an absent key must fail
            assert.ok(!M.verifyCompressedSmtProof(V.smt.balances_root, k, M.toHex(M.amountLeaf('1')),
                       V.smt.nonmembership.compressed));
        });
        it('compression actually omits empty siblings', function(){
            // a 4-entry tree has at most a handful of real siblings, not 256
            assert.ok(V.smt.membership.compressed.siblings.length < 32,
                'compressed proof should omit EMPTY siblings: got ' + V.smt.membership.compressed.siblings.length);
        });
        it('delete-on-zero returns the committed post-delete root, and re-insert restores it', function(){
            const t = buildBalTree(V.smt.bal_entries);
            t.delete(balKey(V.smt.delete_target));
            assert.strictEqual(t.rootHex(), V.smt.balances_root_after_delete);
            assert.notStrictEqual(t.rootHex(), V.smt.balances_root, 'delete must change the root');
            // re-insert the same identity/value restores the original root
            t.set(balKey(V.smt.delete_target), M.amountLeaf(V.smt.delete_target.amount));
            assert.strictEqual(t.rootHex(), V.smt.balances_root);
            assert.strictEqual(V.smt.balances_root_reinserted, V.smt.balances_root);
        });
        it('an empty SMT has the empty-SMT root', function(){
            assert.strictEqual(new M.SparseMerkleTree().rootHex(), V.constants.empty_smt_root);
        });
    });
});

describe('merkle: SPV light-client primitives @regression', function(){

    describe('top-level state root (§4.1)', function(){
        it('state_root matches the golden and the sub_root_path verifies', function(){
            const root = M.toHex(M.stateRoot({ balances_root: V.smt.balances_root, stakes_root: V.smt.stakes_root }));
            assert.strictEqual(root, V.state_root.root);
            const p = V.state_root.sub_root_path_balances;
            assert.ok(M.verifyFixedMerkleProof(root, M.toBuf(V.smt.balances_root), p.index, p.siblings));
        });
        it('absent named sub-trees commit the empty-SMT root, not EMPTY[0]', function(){
            // balances+stakes only; ownership/tokens/contract_state default to EMPTY_SMT_ROOT
            const explicit = M.toHex(M.stateRoot({
                balances_root: V.smt.balances_root, stakes_root: V.smt.stakes_root,
                ownership_root: V.constants.empty_smt_root, tokens_root: V.constants.empty_smt_root,
                contract_state_root: V.constants.empty_smt_root
            }));
            assert.strictEqual(explicit, V.state_root.root);
        });
    });

    describe('block-content Merkle root (§5)', function(){
        it('block_merkle_root matches the golden over the frozen cross-kind order', function(){
            const leaves = [
                ...V.block_merkle.ledger_rows.map(M.ledgerLeaf),
                ...V.block_merkle.actions_rows.map(M.actionsLeaf)
            ];
            assert.strictEqual(M.toHex(M.blockMerkleRoot(leaves)), V.block_merkle.root);
        });
        it('inclusion proof verifies for the committed leaf', function(){
            const leaves = [
                ...V.block_merkle.ledger_rows.map(M.ledgerLeaf),
                ...V.block_merkle.actions_rows.map(M.actionsLeaf)
            ];
            const idx = V.block_merkle.include_index;
            const p = V.block_merkle.include_proof;
            assert.ok(M.verifyFixedMerkleProof(V.block_merkle.root, leaves[idx], p.index, p.siblings));
            // tampering the leaf must fail
            assert.ok(!M.verifyFixedMerkleProof(V.block_merkle.root, M.leafHash('tampered'), p.index, p.siblings));
        });
        it('tx_index NULL encodes distinctly from tx_index 0', function(){
            const a = M.toHex(M.actionsLeaf({ action_index: 11, tx_index: null, action: 'ORDER' }));
            const b = M.toHex(M.actionsLeaf({ action_index: 11, tx_index: 0,    action: 'ORDER' }));
            assert.notStrictEqual(a, b);
        });
    });
});
