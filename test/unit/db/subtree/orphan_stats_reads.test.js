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
    countStateTreeNodes,
    selectRetainedRootUnion,
    selectNodeRowsByHash,
} = require('../../../../src/db/subtree/orphan_stats_reads');

function recordingQuery(result){
    const calls = [];
    return {
        calls,
        query(sql, params){
            calls.push({ sql, params });
            return result;
        },
    };
}

function assertNodeRowsQuery(hashes, placeholders){
    const expected = [];
    const recorder = recordingQuery(expected);

    assert.strictEqual(selectNodeRowsByHash(recorder.query, hashes), expected);
    assert.strictEqual(recorder.calls.length, 1);
    assert.strictEqual(recorder.calls[0].sql,
        'SELECT node_hash, left_hash, right_hash FROM state_tree_nodes WHERE node_hash IN (' +
        placeholders + ')');
    assert.strictEqual(recorder.calls[0].params, hashes);
}

describe('orphan stats reads', function(){
    it('counts state tree nodes without query parameters', function(){
        const expected = { count: 17 };
        const recorder = recordingQuery(expected);

        const result = countStateTreeNodes(recorder.query);

        assert.strictEqual(result, expected);
        assert.deepStrictEqual(recorder.calls, [{
            sql: 'SELECT COUNT(*) AS c FROM state_tree_nodes',
            params: [],
        }]);
    });

    it('selects the union of retained roots and excludes null contract roots', function(){
        const recorder = recordingQuery([]);

        selectRetainedRootUnion(recorder.query, 'chain-a', 'network-b');

        assert.deepStrictEqual(recorder.calls, [{
            sql: 'SELECT DISTINCT balances_root AS r FROM state_tree_roots WHERE chain=? AND network=? ' +
                'UNION SELECT DISTINCT stakes_root AS r FROM state_tree_roots WHERE chain=? AND network=? ' +
                'UNION SELECT DISTINCT contract_state_root AS r FROM state_tree_roots WHERE chain=? AND network=? AND contract_state_root IS NOT NULL',
            params: ['chain-a', 'network-b', 'chain-a', 'network-b', 'chain-a', 'network-b'],
        }]);
    });

    it('selects node rows with one placeholder for one hash', function(){
        assertNodeRowsQuery(['hash-a'], '?');
    });

    it('selects node rows with one placeholder for each of several hashes', function(){
        assertNodeRowsQuery(['hash-a', 'hash-b', 'hash-c'], '?,?,?');
    });
});
