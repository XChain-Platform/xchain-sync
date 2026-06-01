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
            let union = new Set([].concat(topo.blockScoped, topo.txScoped, topo.actionScoped, topo.index));
            assert.strictEqual(tables.length, union.size);
            for(let t of union) assert.ok(tables.includes(t));
        });
    });

    describe('getReplicatedTables (decoder)', function(){
        it('covers the decoder per-block set', function(){
            let tables = getReplicatedTables('decoder');
            for(let t of ['blocks', 'transactions', 'transaction_outputs',
                          'dispensers', 'index_transactions']){
                assert.ok(tables.includes(t), 'expected decoder set to include ' + t);
            }
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
