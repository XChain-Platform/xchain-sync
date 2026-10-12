/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 *********************************************************************/

'use strict';

const assert = require('assert');
const {
    NODE_PUT_CHUNK,
    selectNodeRow,
    insertNodeRow,
    insertNodeRows,
} = require('../../../../src/db/subtree/node_store_rows');

function recordingDb(marker){
    const calls = [];
    return {
        calls,
        doQueryStrict(sql, args){
            calls.push({ sql, args });
            return marker;
        },
    };
}

describe('node store rows', function(){
    it('exports the node write chunk size', function(){
        assert.strictEqual(NODE_PUT_CHUNK, 128);
    });

    it('selects child hashes by node hash and returns the query result', function(){
        const marker = {};
        const db = recordingDb(marker);

        const result = selectNodeRow(db, 'node-hash');

        assert.strictEqual(result, marker);
        assert.deepStrictEqual(db.calls, [{
            sql: 'SELECT left_hash, right_hash FROM state_tree_nodes WHERE node_hash=? LIMIT 1',
            args: ['node-hash'],
        }]);
    });

    it('inserts one node in hash, left, right order and returns the query result', function(){
        const marker = {};
        const db = recordingDb(marker);

        const result = insertNodeRow(db, 'hash', 'left', 'right');

        assert.strictEqual(result, marker);
        assert.deepStrictEqual(db.calls, [{
            sql: 'INSERT IGNORE INTO state_tree_nodes (node_hash, left_hash, right_hash) VALUES (?, ?, ?)',
            args: ['hash', 'left', 'right'],
        }]);
    });
});

describe('node store row chunks', function(){
    it('inserts a one-node chunk and returns the query result', function(){
        const marker = {};
        const db = recordingDb(marker);

        const result = insertNodeRows(db, [{ hash: 'h1', left: 'l1', right: 'r1' }]);

        assert.strictEqual(result, marker);
        assert.deepStrictEqual(db.calls, [{
            sql: 'INSERT IGNORE INTO state_tree_nodes (node_hash, left_hash, right_hash) VALUES (?, ?, ?)',
            args: ['h1', 'l1', 'r1'],
        }]);
    });

    it('inserts a three-node chunk with one tuple per node and flattened arguments', function(){
        const marker = {};
        const db = recordingDb(marker);
        const nodes = [
            { hash: 'h1', left: 'l1', right: 'r1' },
            { hash: 'h2', left: 'l2', right: 'r2' },
            { hash: 'h3', left: 'l3', right: 'r3' },
        ];

        const result = insertNodeRows(db, nodes);

        assert.strictEqual(result, marker);
        assert.deepStrictEqual(db.calls, [{
            sql: 'INSERT IGNORE INTO state_tree_nodes (node_hash, left_hash, right_hash) VALUES ' +
                '(?, ?, ?), (?, ?, ?), (?, ?, ?)',
            args: ['h1', 'l1', 'r1', 'h2', 'l2', 'r2', 'h3', 'l3', 'r3'],
        }]);
    });
});
