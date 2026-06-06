/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available —
 * contact legal@dankest.llc.
 *
 **********************************************************************
 *
 * XChain Indexer Sync - Merkle Tree
 *
 * Pure binary SHA-256 Merkle tree for transparency log proofs.
 * No external dependencies — uses Node.js built-in crypto.
 *
 ********************************************************************/

const crypto = require('crypto');

class MerkleTree {

    // Compute a leaf hash from the three per-block hashes
    static computeLeaf(ledgerHash, actionsHash, contractHash) {
        let data = (ledgerHash || '') + (actionsHash || '') + (contractHash || '');
        return crypto.createHash('sha256').update(data).digest('hex');
    }

    // Hash two nodes together (internal node)
    static hashPair(left, right) {
        let data = left + right;
        return crypto.createHash('sha256').update(data).digest('hex');
    }

    // Build a Merkle tree from an array of leaf hashes
    // Returns { root: string, layers: string[][] } where layers[0] = leaves, layers[last] = [root]
    static buildTree(leaves) {
        if (!leaves || leaves.length === 0) {
            return { root: null, layers: [] };
        }

        let layers = [leaves.slice()]; // Copy leaves as first layer

        let currentLayer = leaves.slice();
        while (currentLayer.length > 1) {
            let nextLayer = [];
            for (let i = 0; i < currentLayer.length; i += 2) {
                let left  = currentLayer[i];
                let right = (i + 1 < currentLayer.length) ? currentLayer[i + 1] : left; // Duplicate last if odd
                nextLayer.push(MerkleTree.hashPair(left, right));
            }
            layers.push(nextLayer);
            currentLayer = nextLayer;
        }

        return {
            root:   currentLayer[0],
            layers: layers
        };
    }

    // Generate an inclusion proof for a leaf at a given index
    // Returns array of { hash, position } where position is 'left' or 'right'
    static generateProof(layers, leafIndex) {
        if (!layers || layers.length === 0 || leafIndex < 0 || leafIndex >= layers[0].length) {
            return null;
        }

        let proof = [];
        let index = leafIndex;

        for (let i = 0; i < layers.length - 1; i++) {
            let layer = layers[i];
            let isRight = index % 2 === 1;
            let siblingIndex = isRight ? index - 1 : index + 1;

            if (siblingIndex < layer.length) {
                proof.push({
                    hash:     layer[siblingIndex],
                    position: isRight ? 'left' : 'right'
                });
            } else {
                // Odd node — sibling is self (duplicated)
                proof.push({
                    hash:     layer[index],
                    position: 'right'
                });
            }

            index = Math.floor(index / 2);
        }

        return proof;
    }

    // Verify an inclusion proof
    // Returns true if the proof reconstructs the expected root
    static verifyProof(leaf, proof, expectedRoot) {
        if (!leaf || !proof || !expectedRoot) return false;

        let current = leaf;
        for (let step of proof) {
            if (step.position === 'left') {
                current = MerkleTree.hashPair(step.hash, current);
            } else {
                current = MerkleTree.hashPair(current, step.hash);
            }
        }

        return current === expectedRoot;
    }
}

module.exports = MerkleTree;
