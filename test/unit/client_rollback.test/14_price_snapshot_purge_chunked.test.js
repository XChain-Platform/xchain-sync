// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// test/unit/client_rollback.test/14_price_snapshot_purge_chunked.test.js
//
// The BTC replica's reorg purge of price_snapshots runs in bounded chunks and survives a
// statement timeout, as the indexer's purge does. One unbounded DELETE over a large mirror
// outlasted the per-statement timeout, and inside the rollback transaction that failed the
// whole reorg reset. The stub db plays the table: it honours the statement's predicate and
// LIMIT, so the assertions are on the statements the shipped code issues and the rows left.

'use strict';

const assert = require('assert');
const sinon  = require('sinon');
const ClientRollback = require('../../../src/client/rollback');
const Utility = require('../../../src/util');

const purge   = ClientRollback.purgeOrphanedPriceSnapshots;
const CHUNK   = ClientRollback.PRICE_SNAPSHOT_PURGE_CHUNK;
const RETRIES = ClientRollback.PRICE_SNAPSHOT_PURGE_TIMEOUT_RETRIES;
const PURGE_SQL = "DELETE FROM price_snapshots WHERE reference_chain = 'BTC' AND reference_block >= ? LIMIT ?";

const norm = s => s.replace(/\s+/g, ' ').trim();

function timeoutError(){
    return Object.assign(new Error('Query execution was interrupted (max_statement_time exceeded)'),
        { errno: 1969, code: 'ER_STATEMENT_TIMEOUT' });
}

// `orphaned` BTC rows at or above `block`, plus two rows the purge must keep: a BTC row
// below the block and an LTC-published row above it.
function makeTable(block, orphaned){
    const rows = [
        { reference_chain: 'BTC', reference_block: block - 1, keep: true },
        { reference_chain: 'LTC', reference_block: block + 5, keep: true },
    ];
    for(let i = 0; i < orphaned; i++)
        rows.push({ reference_chain: 'BTC', reference_block: block + (i % 7), keep: false });
    return rows;
}

// Run one price_snapshots purge statement against `table`, honouring its predicate and LIMIT.
function applyPurge(table, args){
    const [block, limit] = args;
    const hits = [];
    for(let at = 0; at < table.length && hits.length < limit; at++){
        const row = table[at];
        if(row.reference_chain === 'BTC' && row.reference_block >= block) hits.push(at);
    }
    for(let i = hits.length - 1; i >= 0; i--) table.splice(hits[i], 1);
    return { affectedRows: hits.length };
}

// A stub db over `table`. `failOn(n)` returns an error to throw on the nth purge statement
// (1-based) instead of running it.
function makeDb(table, failOn){
    const statements = [];
    const db = {
        async doQuery(sql, args){
            const n = statements.length + 1;
            statements.push({ sql: norm(sql), args: args.slice() });
            const failure = failOn ? failOn(n) : null;
            if(failure) throw failure;
            return applyPurge(table, args);
        },
    };
    return { db, statements };
}

function registerChunkTests(){
    describe('bounded chunks', function(){
        it('purges a table larger than one chunk in statements of at most the chunk size', async function(){
            const table = makeTable(500, 2 * CHUNK + 37);
            const { db, statements } = makeDb(table);

            await purge(db, 500);

            assert.strictEqual(CHUNK, 1000);
            assert.deepStrictEqual(statements.map(s => s.args), [[500, CHUNK], [500, CHUNK], [500, CHUNK]]);
            for(const { sql } of statements) assert.strictEqual(sql, PURGE_SQL);
            assert.ok(table.every(row => row.keep), 'only the rows the purge must keep remain');
            assert.strictEqual(table.length, 2);
        });

        it('stops after one statement when the first chunk comes back short', async function(){
            const table = makeTable(500, 3);
            const { db, statements } = makeDb(table);
            await purge(db, 500);
            assert.strictEqual(statements.length, 1);
            assert.strictEqual(table.length, 2);
        });

        it('stops when the driver reports no affectedRows', async function(){
            const db = { doQuery: sinon.stub().resolves([]) };
            await purge(db, 500);
            assert.strictEqual(db.doQuery.callCount, 1);
        });
    });
}

function registerTimeoutTests(){
    describe('statement timeouts', function(){
        it('retries a timed-out chunk at half size and completes the purge', async function(){
            const table = makeTable(500, 2 * CHUNK + 501);
            const { db, statements } = makeDb(table, n => (n === 2 ? timeoutError() : null));

            await purge(db, 500);

            const limits = statements.map(s => s.args[1]);
            assert.deepStrictEqual(limits.slice(0, 3), [CHUNK, CHUNK, CHUNK / 2],
                'the timed-out chunk is asked for again at half the size');
            assert.ok(limits.every(limit => limit <= CHUNK), 'no statement is unbounded');
            assert.ok(table.every(row => row.keep), 'the purge still removes every orphaned row');
            assert.strictEqual(table.length, 2);
        });

        it('treats MySQL ER_QUERY_TIMEOUT as a statement timeout too', async function(){
            const table = makeTable(500, 10);
            const mysqlTimeout = Object.assign(new Error('max_execution_time exceeded'), { errno: 3024, code: 'ER_QUERY_TIMEOUT' });
            const { db, statements } = makeDb(table, n => (n === 1 ? mysqlTimeout : null));
            await purge(db, 500);
            assert.strictEqual(statements.length, 2);
            assert.strictEqual(table.length, 2);
        });

        it('resets the retry budget after a chunk succeeds', async function(){
            const table = makeTable(500, 4 * CHUNK);
            // A timeout before every success: more timeouts in total than the budget, never two in a row.
            const { db } = makeDb(table, n => (n % 2 === 1 && n <= 2 * (RETRIES + 2) ? timeoutError() : null));
            await purge(db, 500);
            assert.strictEqual(table.length, 2);
        });

        it('gives up once consecutive timeouts exceed the retry budget', async function(){
            const table = makeTable(500, 10);
            const { db, statements } = makeDb(table, () => timeoutError());
            await assert.rejects(purge(db, 500), { code: 'ER_STATEMENT_TIMEOUT' });
            assert.strictEqual(statements.length, RETRIES + 1);
            assert.strictEqual(table.length, 12, 'nothing was deleted');
        });

        it('does not retry an error that is not a statement timeout', async function(){
            const table = makeTable(500, 10);
            const deadlock = Object.assign(new Error('Deadlock found'), { errno: 1213, code: 'ER_LOCK_DEADLOCK' });
            const { db, statements } = makeDb(table, () => deadlock);
            await assert.rejects(purge(db, 500), { code: 'ER_LOCK_DEADLOCK' });
            assert.strictEqual(statements.length, 1);
        });
    });
}

function registerRollbackTests(){
    describe('inside a BTC replica rollback', function(){
        beforeEach(function(){
            sinon.stub(console, 'log');
            sinon.stub(console, 'error');
        });
        afterEach(function(){ sinon.restore(); });

        // A rollback db whose price_snapshots purges run against `table`; every other statement
        // succeeds empty. The first purge statement times out.
        function makeRollbackDb(table){
            const purges = [];
            const doQuery = sinon.spy(async (sql, args) => {
                if(typeof sql === 'string' && sql.includes('DELETE FROM price_snapshots')){
                    purges.push(args.slice());
                    if(purges.length === 1) throw timeoutError();
                    return applyPurge(table, args);
                }
                return [];
            });
            return {
                db: {
                    doQuery,
                    getFirstActionIndex: sinon.stub().resolves(500),
                    getStatusId: sinon.stub().resolves(null),
                    beginTransaction: sinon.stub().resolves(),
                    commitTransaction: sinon.stub().resolves(),
                    rollbackTransaction: sinon.stub().resolves(),
                },
                purges,
            };
        }

        it('completes the reorg reset through a statement timeout, purging in bounded chunks', async function(){
            const table = makeTable(100, CHUNK + 250);
            const { db, purges } = makeRollbackDb(table);

            await new ClientRollback(db, new Utility(), 'bitcoin', 'regtest').rollback(100);

            assert.ok(purges.length >= 3, 'the timed-out chunk was retried and the purge carried on');
            assert.deepStrictEqual(purges[0], [100, CHUNK]);
            assert.deepStrictEqual(purges[1], [100, CHUNK / 2]);
            assert.ok(purges.every(([, limit]) => limit <= CHUNK), 'no purge statement is unbounded');
            assert.ok(table.every(row => row.keep), 'every orphaned BTC snapshot was purged');
            assert.strictEqual(db.rollbackTransaction.callCount, 0, 'the timeout did not abort the rollback');
        });
    });
}

describe('ClientRollback price_snapshots purge @regression', function(){
    registerChunkTests();
    registerTimeoutTests();
    registerRollbackTests();
});
