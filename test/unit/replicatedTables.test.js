// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available —
// contact legal@dankest.llc.

const assert = require('assert');
const { getTopology, getReplicatedTables } = require('../../src/replicatedTables');

describe('replicatedTables', function(){

    describe('getReplicatedTables (indexer)', function(){
        let tables;
        before(function(){ tables = getReplicatedTables('indexer'); });

        it('covers the core block/action tables a follower must hold', function(){
            for(let t of ['blocks', 'transactions', 'actions', 'contract_balances',
                          'contract_stakes', 'attests']){
                assert.ok(tables.includes(t), 'expected replicated set to include ' + t);
            }
        });

        it('includes the transparency log (sync_meta) in the completeness check', function(){
            // sync_meta is streamed inline by ServerPoller (not via the blockScoped
            // extraction loop) and carried by snapshots, so it belongs in the
            // /status row-count check even though it is not in the per-scope lists.
            assert.ok(tables.includes('sync_meta'),
                      'expected replicated set to include sync_meta for completeness counting');
            // ...but it must NOT leak into the per-scope lists ServerPoller iterates,
            // or _buildBlockPayload would read the not-yet-recorded row (ordering trap).
            let topo = getTopology('indexer');
            for(let scope of ['blockScoped', 'txScoped', 'actionScoped', 'index']){
                assert.ok(!topo[scope].includes('sync_meta'),
                          'sync_meta must not be in the extracted ' + scope + ' list');
            }
        });

        it('excludes snapshot-only / operator-local / non-deterministic tables', function(){
            // These converge via other channels (full snapshot, hub mirror) and
            // legitimately diverge between nodes — comparing their counts would
            // raise false incompleteness alarms.
            for(let t of ['markets', 'icons', 'price_snapshots',
                          'attestation_validator_stats', 'mempool_transactions']){
                assert.ok(!tables.includes(t), 'expected replicated set to exclude ' + t);
            }
        });

        it('returns a de-duplicated union of the per-scope lists', function(){
            assert.strictEqual(tables.length, new Set(tables).size, 'no duplicates expected');
        });

        it('is the union of every per-scope list in the topology', function(){
            let topo = getTopology('indexer');
            let union = new Set([].concat(topo.blockScoped, topo.txScoped, topo.actionScoped, topo.index, topo.special));
            assert.strictEqual(tables.length, union.size);
            for(let t of union) assert.ok(tables.includes(t));
        });
    });

    describe('getReplicatedTables (decoder)', function(){
        it('covers the decoder per-block set', function(){
            let tables = getReplicatedTables('decoder');
            for(let t of ['blocks', 'transactions', 'transaction_outputs',
                          'index_transactions']){
                assert.ok(tables.includes(t), 'expected decoder set to include ' + t);
            }
        });

        it('excludes dispensers (live-pruned each block; full-snapshot only)', function(){
            // The decoder prunes expired dispensers every block with a
            // count-reducing DELETE that the insert-only per-block stream cannot
            // represent, so a per-block-replicated count would diverge upward
            // forever. dispensers converges via the full snapshot instead and is
            // therefore deliberately absent from the per-block replicated set.
            assert.ok(!getReplicatedTables('decoder').includes('dispensers'),
                      'expected decoder set to exclude dispensers');
        });

        it('has no action-scoped tables', function(){
            assert.strictEqual(getTopology('decoder').actionScoped.length, 0);
        });
    });

    describe('getTopology', function(){
        it('defaults unknown dbTypes to the indexer topology', function(){
            assert.strictEqual(getTopology(undefined), getTopology('indexer'));
            assert.strictEqual(getTopology('weird'), getTopology('indexer'));
        });
    });
});
