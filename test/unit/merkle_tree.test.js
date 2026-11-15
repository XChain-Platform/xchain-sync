// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const crypto = require('crypto');
const MerkleTree = require('../../src/server/merkle_tree');

const sha = (s) => crypto.createHash('sha256').update(s).digest('hex');
const leaves = (n) => Array.from({ length: n }, (_, i) => sha('leaf' + i));

describe('MerkleTree @consensus @regression', function () {

    describe('computeLeaf() / hashPair()', function () {
        it('computeLeaf is the sha256 of the three concatenated hashes', function () {
            assert.strictEqual(MerkleTree.computeLeaf('aa', 'bb', 'cc'), sha('aabbcc'));
        });
        it('computeLeaf treats null/undefined components as empty string', function () {
            assert.strictEqual(MerkleTree.computeLeaf('aa', null, undefined), sha('aa'));
        });
        it('hashPair is the sha256 of left||right and is order-sensitive', function () {
            assert.strictEqual(MerkleTree.hashPair('aa', 'bb'), sha('aabb'));
            assert.notStrictEqual(MerkleTree.hashPair('aa', 'bb'), MerkleTree.hashPair('bb', 'aa'));
        });
    });

    describe('buildTree()', function () {
        it('returns {root:null, layers:[]} for empty input', function () {
            assert.deepStrictEqual(MerkleTree.buildTree([]), { root: null, layers: [] });
            assert.deepStrictEqual(MerkleTree.buildTree(null), { root: null, layers: [] });
        });
        it('a single leaf is its own root', function () {
            const l = leaves(1);
            const { root, layers } = MerkleTree.buildTree(l);
            assert.strictEqual(root, l[0]);
            assert.strictEqual(layers.length, 1);
        });
        it('two leaves: root = hashPair(l0,l1)', function () {
            const l = leaves(2);
            const { root, layers } = MerkleTree.buildTree(l);
            assert.strictEqual(root, MerkleTree.hashPair(l[0], l[1]));
            assert.strictEqual(layers.length, 2);
        });
        it('odd count duplicates the last node', function () {
            const l = leaves(3);
            const { root } = MerkleTree.buildTree(l);
            const expected = MerkleTree.hashPair(
                MerkleTree.hashPair(l[0], l[1]),
                MerkleTree.hashPair(l[2], l[2]) // last duplicated
            );
            assert.strictEqual(root, expected);
        });
        it('does not mutate the caller\'s leaves array', function () {
            const l = leaves(4);
            const copy = l.slice();
            MerkleTree.buildTree(l);
            assert.deepStrictEqual(l, copy);
        });
    });
});

describe('MerkleTree published root vectors @consensus @regression', function () {
    const roots = {
        1: '4d5a9584d985e8fb44015a8affa9b76f1ff16f65e61df7156d8e8159e1448978',
        2: '038708619304e7a3a5ba4d7de5b41c3c300b90138611fce14c9c33ac66629f43',
        3: '2486eb950a0dac62332dc453316fd6aceb8ee50ca0515f3536bb297ceb401fc2',
        5: '25c7fb8afe4aee93add8e1761152ba52b2017d85218c340ac7198c3629caae39',
        8: '96b59903bb3eb1a3b597e493a39725c93a82efaa0298f773ebaee6ffa8552a96'
    };

    it('matches independently computed roots for fixed leaf counts', function () {
        for (const n of [1, 2, 3, 5, 8]) {
            assert.strictEqual(MerkleTree.buildTree(leaves(n)).root, roots[n], `${n} leaves`);
        }
    });

    it('matches the independently computed proof for leaf 4 of 5', function () {
        const l = leaves(5);
        const { layers } = MerkleTree.buildTree(l);
        const proof = [
            { hash: '565fb0e0cefe32cf4000e4a67ddec8820111a733aa8ba010d242a5fe477e04c4', position: 'right' },
            { hash: 'ff376073e2083f6caa4d1b51eb44d346745cda5a2db1f2fa91ec3bcf3482f791', position: 'right' },
            { hash: '2587098af43ebfff5e7e8345d5fdfdafb87d4a4eff4712eb1fd03c21fb231b2f', position: 'left' }
        ];
        assert.deepStrictEqual(MerkleTree.generateProof(layers, 4), proof);
        assert.strictEqual(MerkleTree.verifyProof(l[4], proof, roots[5]), true);
    });

    it('documents the legacy odd-leaf duplication collision', function () {
        const three = leaves(3);
        const four = [...three, three[2]];
        const tree3 = MerkleTree.buildTree(three);
        const tree4 = MerkleTree.buildTree(four);
        assert.strictEqual(tree3.root, tree4.root);
        assert.strictEqual(MerkleTree.verifyProof(
            three[2], MerkleTree.generateProof(tree3.layers, 2), tree4.root
        ), true);
        assert.strictEqual(MerkleTree.verifyProof(
            four[3], MerkleTree.generateProof(tree4.layers, 3), tree3.root
        ), true);
    });
});

describe('MerkleTree @consensus @regression', function () {

    describe('generateProof() / verifyProof() round-trip', function () {
        // The consensus-critical property: for a tree of N leaves, EVERY leaf
        // must produce a proof that reconstructs the root, for both even and
        // odd N (odd exercises the self-duplication path).
        for (const n of [1, 2, 3, 4, 5, 8, 13]) {
            it(`every one of ${n} leaves proves inclusion against the root`, function () {
                const l = leaves(n);
                const { root, layers } = MerkleTree.buildTree(l);
                for (let i = 0; i < n; i++) {
                    const proof = MerkleTree.generateProof(layers, i);
                    assert.ok(proof, `proof for index ${i}`);
                    assert.strictEqual(MerkleTree.verifyProof(l[i], proof, root), true, `index ${i}`);
                }
            });
        }

        it('rejects a proof for the wrong leaf', function () {
            const l = leaves(8);
            const { root, layers } = MerkleTree.buildTree(l);
            const proof = MerkleTree.generateProof(layers, 3);
            assert.strictEqual(MerkleTree.verifyProof(l[4], proof, root), false);
        });

        it('rejects a proof with a tampered sibling hash', function () {
            const l = leaves(8);
            const { root, layers } = MerkleTree.buildTree(l);
            const proof = MerkleTree.generateProof(layers, 3);
            proof[0] = { hash: sha('evil'), position: proof[0].position };
            assert.strictEqual(MerkleTree.verifyProof(l[3], proof, root), false);
        });

        it('rejects a proof against the wrong root', function () {
            const l = leaves(8);
            const { layers } = MerkleTree.buildTree(l);
            const proof = MerkleTree.generateProof(layers, 3);
            assert.strictEqual(MerkleTree.verifyProof(l[3], proof, sha('not-the-root')), false);
        });

        it('generateProof returns null for out-of-range / empty', function () {
            const { layers } = MerkleTree.buildTree(leaves(4));
            assert.strictEqual(MerkleTree.generateProof(layers, -1), null);
            assert.strictEqual(MerkleTree.generateProof(layers, 4), null);
            assert.strictEqual(MerkleTree.generateProof([], 0), null);
        });

        it('verifyProof returns false on missing arguments', function () {
            assert.strictEqual(MerkleTree.verifyProof(null, [], 'root'), false);
            assert.strictEqual(MerkleTree.verifyProof('leaf', null, 'root'), false);
            assert.strictEqual(MerkleTree.verifyProof('leaf', [], null), false);
        });
    });
});
