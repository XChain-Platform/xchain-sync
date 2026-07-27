// Copyright © 2025–2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

// : connection pools are sized per dbType, and the sizing is
// load-tested against the per-block query fan-out it exists to absorb.

const assert     = require('assert');
const sinon      = require('sinon');
const proxyquire = require('proxyquire').noCallThru();
const poolSizing = require('../../src/poolSizing');

// ServerPoller assembles one indexer block payload from roughly this many
// sequential queries (block-scoped rows over 8+ tables, action-scoped rows
// over 80+ tables, index-id pooling). The figure is the one the scale review
// measured and the reason the indexer pool is sized above the decoder pool.
const QUERIES_PER_BLOCK = 113;

const POOL_ENV_KEYS = [
    'DB_POOL_SIZE', 'DB_POOL_SIZE_INDEXER', 'DB_POOL_SIZE_DECODER',
    'DB_CONNECT_TIMEOUT', 'DB_CONNECT_TIMEOUT_INDEXER', 'DB_CONNECT_TIMEOUT_DECODER',
    'DB_ACQUIRE_TIMEOUT', 'DB_ACQUIRE_TIMEOUT_INDEXER', 'DB_ACQUIRE_TIMEOUT_DECODER',
    'DB_QUERY_TIMEOUT', 'DB_QUERY_TIMEOUT_INDEXER', 'DB_QUERY_TIMEOUT_DECODER'
];

function clearPoolEnv(){
    for(let k of POOL_ENV_KEYS) delete process.env[k];
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. resolver semantics
// ═══════════════════════════════════════════════════════════════════════════
describe('poolSizing.resolvePoolSize()', function(){
    let saved;

    beforeEach(function(){
        saved = {};
        for(let k of POOL_ENV_KEYS) saved[k] = process.env[k];
        clearPoolEnv();
    });

    afterEach(function(){
        for(let k of POOL_ENV_KEYS){
            if(saved[k] === undefined) delete process.env[k];
            else process.env[k] = saved[k];
        }
    });

    it('gives the indexer a bigger default pool than the decoder', function(){
        let indexer = poolSizing.resolvePoolSize('indexer');
        let decoder = poolSizing.resolvePoolSize('decoder');
        assert.strictEqual(indexer, poolSizing.DEFAULT_POOL_SIZE.indexer);
        assert.strictEqual(decoder, poolSizing.DEFAULT_POOL_SIZE.decoder);
        assert.ok(indexer > decoder, 'indexer fan-out needs more connections than decoder replication');
    });

    it('sizes the indexer default to absorb the per-block fan-out at under 12x serialization', function(){
        // The old flat default of 5 serialized ~113 queries/block 23 deep.
        let depth = Math.ceil(QUERIES_PER_BLOCK / poolSizing.resolvePoolSize('indexer'));
        assert.ok(depth <= 12, 'indexer default should at least halve the old 23-deep serialization, got ' + depth);
    });

    it('honours the per-dbType override ahead of the global one', function(){
        process.env.DB_POOL_SIZE          = '7';
        process.env.DB_POOL_SIZE_INDEXER  = '30';
        assert.strictEqual(poolSizing.resolvePoolSize('indexer'), 30);
        // decoder has no scoped override, so it inherits the global
        assert.strictEqual(poolSizing.resolvePoolSize('decoder'), 7);
    });

    it('falls back to the legacy global DB_POOL_SIZE for every dbType', function(){
        process.env.DB_POOL_SIZE = '9';
        assert.strictEqual(poolSizing.resolvePoolSize('indexer'), 9);
        assert.strictEqual(poolSizing.resolvePoolSize('decoder'), 9);
    });

    it('ignores empty and malformed overrides instead of producing NaN', function(){
        process.env.DB_POOL_SIZE_INDEXER = '';
        assert.strictEqual(poolSizing.resolvePoolSize('indexer'), poolSizing.DEFAULT_POOL_SIZE.indexer);
        process.env.DB_POOL_SIZE_INDEXER = 'lots';
        assert.strictEqual(poolSizing.resolvePoolSize('indexer'), poolSizing.DEFAULT_POOL_SIZE.indexer);
    });

    it('clamps to [MIN, MAX] so no setting can deadlock the poller or exhaust MariaDB', function(){
        process.env.DB_POOL_SIZE_INDEXER = '1';
        assert.strictEqual(poolSizing.resolvePoolSize('indexer'), poolSizing.MIN_POOL_SIZE);
        process.env.DB_POOL_SIZE_INDEXER = '5000';
        assert.strictEqual(poolSizing.resolvePoolSize('indexer'), poolSizing.MAX_POOL_SIZE);
    });

    it('is case- and whitespace-tolerant on dbType, defaulting unknown types to the indexer profile', function(){
        assert.strictEqual(poolSizing.resolvePoolSize(' Indexer '), poolSizing.DEFAULT_POOL_SIZE.indexer);
        assert.strictEqual(poolSizing.resolvePoolSize('DECODER'), poolSizing.DEFAULT_POOL_SIZE.decoder);
        assert.strictEqual(poolSizing.resolvePoolSize(undefined), poolSizing.DEFAULT_POOL_SIZE.indexer);
        assert.strictEqual(poolSizing.resolvePoolSize('something-new'), poolSizing.DEFAULT_POOL_SIZE.indexer);
    });

    it('reads from an injected env map when given one (no process.env coupling)', function(){
        let env = { DB_POOL_SIZE_DECODER: '21' };
        assert.strictEqual(poolSizing.resolvePoolSize('decoder', env), 21);
        assert.strictEqual(poolSizing.resolvePoolSize('indexer', env), poolSizing.DEFAULT_POOL_SIZE.indexer);
    });

    it('resolves every pool knob per dbType, not just the connection limit', function(){
        let env = {
            DB_ACQUIRE_TIMEOUT:          '1000',
            DB_ACQUIRE_TIMEOUT_INDEXER:  '45000',
            DB_QUERY_TIMEOUT_DECODER:    '5000'
        };
        let indexer = poolSizing.resolvePoolParams('indexer', env);
        let decoder = poolSizing.resolvePoolParams('decoder', env);
        assert.strictEqual(indexer.acquireTimeout, 45000);
        assert.strictEqual(decoder.acquireTimeout, 1000);
        assert.strictEqual(decoder.queryTimeout, 5000);
        assert.strictEqual(indexer.queryTimeout, 30000);
        assert.strictEqual(indexer.connectTimeout, 10000);
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 2. Database wiring: the pool a chain actually opens
// ═══════════════════════════════════════════════════════════════════════════
describe('Database pool sizing per dbType', function(){
    let saved, createdPools;

    function fakeMariadb(){
        return {
            createPool: (cfg) => { createdPools.push(cfg); return { end: sinon.stub().resolves() }; },
            createConnection: sinon.stub().resolves({ query: sinon.stub().resolves([]), end: sinon.stub().resolves() }),
            '@noCallThru': true
        };
    }

    function makeDb(dbType){
        let FakeDatabase = proxyquire('../../src/db', { mariadb: fakeMariadb() });
        return new FakeDatabase('localhost', 3306, 'db_' + dbType, 'u', 'p', {
            isNull: (v) => v === null || v === undefined,
            throwError: (m) => { throw new Error(m); },
            sleep: sinon.stub().resolves(),
            logError: sinon.stub()
        }, dbType);
    }

    beforeEach(function(){
        createdPools = [];
        saved = {};
        for(let k of POOL_ENV_KEYS) saved[k] = process.env[k];
        clearPoolEnv();
    });

    afterEach(function(){
        for(let k of POOL_ENV_KEYS){
            if(saved[k] === undefined) delete process.env[k];
            else process.env[k] = saved[k];
        }
    });

    it('opens differently sized pools for the indexer and decoder DBs of the same chain', function(){
        let indexer = makeDb('indexer');
        let decoder = makeDb('decoder');
        assert.strictEqual(indexer.connectionPoolParams.connectionLimit, poolSizing.DEFAULT_POOL_SIZE.indexer);
        assert.strictEqual(decoder.connectionPoolParams.connectionLimit, poolSizing.DEFAULT_POOL_SIZE.decoder);
        assert.notStrictEqual(indexer.connectionPoolParams.connectionLimit,
                              decoder.connectionPoolParams.connectionLimit);
    });

    it('passes the resolved limit to mariadb.createPool', function(){
        process.env.DB_POOL_SIZE_DECODER = '4';
        makeDb('decoder');
        assert.strictEqual(createdPools.length, 1);
        assert.strictEqual(createdPools[0].connectionLimit, 4);
    });

    it('still honours the legacy flat DB_POOL_SIZE for operators who set it', function(){
        process.env.DB_POOL_SIZE = '8';
        assert.strictEqual(makeDb('indexer').connectionPoolParams.connectionLimit, 8);
        assert.strictEqual(makeDb('decoder').connectionPoolParams.connectionLimit, 8);
    });

    it('applies per-dbType timeouts to the pool config', function(){
        process.env.DB_QUERY_TIMEOUT_INDEXER = '60000';
        let db = makeDb('indexer');
        assert.strictEqual(db.connectionPoolParams.queryTimeout, 60000);
        assert.strictEqual(db.connectionPoolParams.acquireTimeout, 10000);
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 3. Fan-out load test
// ═══════════════════════════════════════════════════════════════════════════
//
// Drives the real per-block query fan-out (QUERIES_PER_BLOCK queries, issued
// as fast as the pool hands out connections) through a pool harness that
// enforces a connection limit exactly like mariadb's does, and measures how
// deep the fan-out serializes. This is the load the pool sizing exists for:
// at limit=5 the block's queries stack ~23 deep behind 5 connections, and
// nothing is left over for ServerPoller's every-3s getLastBlock.

// Minimal stand-in for a mariadb pool: `limit` connections, FIFO waiters,
// each query costing one macrotask tick. Records peak concurrency and how
// many queries each connection slot had to serve (the serialization depth).
function makeLoadPool(limit){
    let free     = [];
    let waiters  = [];
    let inFlight = 0;
    let stats    = { limit, maxInFlight: 0, served: 0, perSlot: [], acquireWaits: 0 };

    for(let i = 0; i < limit; i++){
        free.push(i);
        stats.perSlot.push(0);
    }

    function release(slot){
        let next = waiters.shift();
        if(next) next(slot);
        else free.push(slot);
    }

    async function getConnection(){
        let slot;
        if(free.length > 0) slot = free.shift();
        else {
            stats.acquireWaits++;
            slot = await new Promise(resolve => waiters.push(resolve));
        }
        inFlight++;
        if(inFlight > stats.maxInFlight) stats.maxInFlight = inFlight;
        let released = false;
        return {
            slot,
            query: async () => {
                stats.perSlot[slot]++;
                stats.served++;
                await new Promise(r => setTimeout(r, 1));
                return [];
            },
            release: () => {
                if(released) return;
                released = true;
                inFlight--;
                release(slot);
            }
        };
    }

    return { getConnection, stats };
}

// One block's worth of queries, each taking its own pooled connection the way
// db.doQuery does (acquire, query, release).
async function runBlockFanOut(pool, queries){
    let work = [];
    for(let i = 0; i < queries; i++){
        work.push((async () => {
            let conn = await pool.getConnection();
            try { await conn.query(); }
            finally { conn.release(); }
        })());
    }
    await Promise.all(work);
}

describe('pool sizing under per-block fan-out load', function(){
    this.timeout(20000);

    it('serializes the fan-out to exactly the pool limit (harness models mariadb)', async function(){
        let pool = makeLoadPool(5);
        await runBlockFanOut(pool, QUERIES_PER_BLOCK);
        assert.strictEqual(pool.stats.served, QUERIES_PER_BLOCK);
        assert.strictEqual(pool.stats.maxInFlight, 5, 'never more concurrent queries than connections');
        assert.ok(pool.stats.acquireWaits > 0, 'the fan-out must actually queue at limit=5');
    });

    it('the old flat default of 5 stacks the block ~23 deep on one connection', async function(){
        let pool = makeLoadPool(5);
        await runBlockFanOut(pool, QUERIES_PER_BLOCK);
        let depth = Math.max(...pool.stats.perSlot);
        assert.ok(depth >= Math.ceil(QUERIES_PER_BLOCK / 5),
            'expected at least ' + Math.ceil(QUERIES_PER_BLOCK / 5) + '-deep serialization, got ' + depth);
    });

    it('the per-dbType indexer default cuts that serialization depth roughly in half or better', async function(){
        let small = makeLoadPool(5);
        let sized = makeLoadPool(poolSizing.DEFAULT_POOL_SIZE.indexer);
        await runBlockFanOut(small, QUERIES_PER_BLOCK);
        await runBlockFanOut(sized, QUERIES_PER_BLOCK);
        let smallDepth = Math.max(...small.stats.perSlot);
        let sizedDepth = Math.max(...sized.stats.perSlot);
        assert.ok(sizedDepth * 2 <= smallDepth,
            'indexer default should at least halve fan-out depth (' + smallDepth + ' -> ' + sizedDepth + ')');
        assert.strictEqual(sized.stats.served, QUERIES_PER_BLOCK);
    });

    it('leaves the poller a free connection while a snapshot-cohort pins the rest', async function(){
        // Model the D5 starvation shape: SnapshotBuilder's cap (poolSize - 2)
        // streams pinning connections while ServerPoller still needs one every
        // ~3s. With per-dbType sizing the reserve is real, not a rounding
        // artifact of a pool of 5.
        let limit = poolSizing.DEFAULT_POOL_SIZE.indexer;
        let pool  = makeLoadPool(limit);
        let cap   = limit - 2;

        let pinned = [];
        for(let i = 0; i < cap; i++) pinned.push(await pool.getConnection());

        // Poller acquire must not block behind the pinned snapshot streams.
        let pollerConn = await Promise.race([
            pool.getConnection(),
            new Promise((_, reject) => setTimeout(() => reject(new Error('poller starved')), 250))
        ]);
        assert.ok(pollerConn, 'poller acquired a connection while ' + cap + ' snapshots streamed');
        await pollerConn.query();
        pollerConn.release();
        for(let c of pinned) c.release();
        assert.strictEqual(pool.stats.maxInFlight <= limit, true);
    });
});
