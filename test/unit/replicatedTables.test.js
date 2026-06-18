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
const { getTopology, getReplicatedTables } = require('../../src/replicatedTables');

describe('replicatedTables', function(){

    describe('getReplicatedTables (indexer)', function(){
        let tables;
        before(function(){ tables = getReplicatedTables('indexer'); });

        it('covers the core block/action tables a follower must hold', function(){
            // balances is intentionally NOT replicated per-block; it is a
            // derived aggregate the follower recomputes locally (see replicatedTables.js
            // and rollback-coverage.test.js RECOMPUTED). So it must NOT appear here.
            for(let t of ['blocks', 'transactions', 'actions',
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
            // legitimately diverge between nodes, so comparing their counts would
            // raise false incompleteness alarms.
            for(let t of ['markets', 'icons', 'price_snapshots',
                          'attest_validator_stats', 'mempool_transactions']){
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

        it('includes dispensers in the completeness count set but NOT per-block-streamed', function(){
            // #3835: the decoder soft-expires dispensers (UPDATE) and hard-purges
            // them (DELETE) off the block stream, so it is NOT per-block-streamed
            // (absent from blockScoped/txScoped/actionScoped/index). It IS listed in
            // `special` so it enters the /status completeness count: the drift the
            // bootstrap full dump cannot replay now surfaces as a TABLE_COUNT_MISMATCH
            // instead of drifting silently.
            let topo = getTopology('decoder');
            for(let bucket of ['blockScoped', 'txScoped', 'actionScoped', 'index']){
                assert.ok(!topo[bucket].includes('dispensers'),
                          'dispensers must not be per-block-streamed (bucket ' + bucket + ')');
            }
            assert.ok((topo.special || []).includes('dispensers'),
                      'dispensers must be in the decoder special bucket');
            assert.ok(getReplicatedTables('decoder').includes('dispensers'),
                      'dispensers must join the decoder completeness count set');
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
