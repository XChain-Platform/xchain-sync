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
const sinon  = require('sinon');
const { PassThrough } = require('stream');
const zlib = require('zlib');
const SnapshotBuilder = require('../../src/SnapshotBuilder');
const Utility = require('../../src/utility');

function createMockDb(dbName){
    return {
        dbName: dbName || 'test_db',
        doQuery: sinon.stub().resolves([]),
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getFirstActionIndex: sinon.stub().resolves(null),
        getTablePage: sinon.stub().resolves([]),
        getTableCount: sinon.stub().resolves(0),
        beginReadSnapshot: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(true),
        rollbackTransaction: sinon.stub().resolves()
    };
}

function createMockRes(){
    let chunks = [];
    let headers = {};
    let passthrough = new PassThrough();
    passthrough.on('data', (chunk) => chunks.push(chunk));

    return {
        _chunks: chunks,
        _headers: headers,
        _statusCode: 200,
        setHeader: sinon.stub().callsFake((k, v) => { headers[k] = v; }),
        status: sinon.stub().returnsThis(),
        json: sinon.stub(),
        write: passthrough.write.bind(passthrough),
        end: passthrough.end.bind(passthrough),
        pipe: passthrough.pipe.bind(passthrough),
        on: passthrough.on.bind(passthrough),
        once: passthrough.once.bind(passthrough),
        emit: passthrough.emit.bind(passthrough),
        // Make it a writable stream for gzip.pipe(res)
        _write: passthrough._write.bind(passthrough),
        _final: passthrough._final ? passthrough._final.bind(passthrough) : undefined,
        _passthrough: passthrough,
        getCollectedData: function(){
            return Buffer.concat(this._chunks);
        }
    };
}

describe('SnapshotBuilder', function(){

    let builder, util;

    beforeEach(function(){
        util = new Utility();
        builder = new SnapshotBuilder(util);
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    describe('_getOrderedTables', function(){
        it('orders priority tables first, trailing tables last, middle alphabetically', async function(){
            let db = createMockDb();
            db.doQuery.resolves([
                { table_name: 'balances' },      // trailing
                { table_name: 'zebra' },          // middle
                { table_name: 'actions' },        // priority
                { table_name: 'blocks' },         // priority
                { table_name: 'apple' },          // middle
                { table_name: 'sync_meta' },      // trailing
                { table_name: 'index_actions' }   // priority (first)
            ]);

            let ordered = await builder._getOrderedTables(db);

            // Priority tables come first in defined order
            assert.strictEqual(ordered[0], 'index_actions');
            assert.ok(ordered.indexOf('blocks') < ordered.indexOf('actions'));
            assert.ok(ordered.indexOf('actions') < ordered.indexOf('apple'));

            // Middle tables alphabetically
            assert.ok(ordered.indexOf('apple') < ordered.indexOf('zebra'));

            // Trailing tables last
            assert.ok(ordered.indexOf('zebra') < ordered.indexOf('balances'));
            assert.strictEqual(ordered[ordered.length - 1], 'sync_meta');
        });

        it('skips priority/trailing tables not in DB', async function(){
            let db = createMockDb();
            db.doQuery.resolves([
                { table_name: 'blocks' },
                { table_name: 'custom_table' }
            ]);
            let ordered = await builder._getOrderedTables(db);
            assert.strictEqual(ordered.length, 2);
            assert.strictEqual(ordered[0], 'blocks');
            assert.strictEqual(ordered[1], 'custom_table');
        });
    });

    describe('streamFullSnapshot', function(){
        it('returns 404 when no blocks in database', async function(){
            let db = createMockDb();
            db.getLastBlock.resolves(null);
            let res = createMockRes();
            await builder.streamFullSnapshot(db, res);
            assert.strictEqual(res.status.calledWith(404), true);
            assert.strictEqual(res.json.calledOnce, true);
        });

        it('sets correct response headers', async function(){
            let db = createMockDb();
            db.getLastBlock.resolves(100);
            db.getBlockHashRow.resolves({ ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch' });
            db.doQuery.resolves([]); // no tables

            let res = new PassThrough();
            let setHeaderStub = sinon.stub();
            res.setHeader = setHeaderStub;

            await new Promise((resolve) => {
                res.on('finish', resolve);
                builder.streamFullSnapshot(db, res);
            });

            assert.ok(setHeaderStub.calledWith('Content-Type', 'application/json'));
            assert.ok(setHeaderStub.calledWith('Content-Encoding', 'gzip'));
            assert.ok(setHeaderStub.calledWith('X-Block-Height', 100));
            assert.ok(setHeaderStub.calledWith('X-Ledger-Hash', 'lh'));
            assert.ok(setHeaderStub.calledWith('X-Actions-Hash', 'ah'));
            assert.ok(setHeaderStub.calledWith('X-Contract-Hash', 'ch'));
        });

        it('streams valid gzip JSON for tables with data', async function(){
            let db = createMockDb();
            db.getLastBlock.resolves(50);
            db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
            db.doQuery.resolves([{ table_name: 'blocks' }]);
            db.getTableCount.resolves(1);
            db.getTablePage.resolves([{ block_index: 50, block_time: 100 }]);

            // Collect the gzipped output
            let res = new PassThrough();
            let chunks = [];
            res.on('data', c => chunks.push(c));
            res.setHeader = sinon.stub();

            await new Promise((resolve) => {
                res.on('finish', resolve);
                builder.streamFullSnapshot(db, res);
            });

            let compressed = Buffer.concat(chunks);
            let json = zlib.gunzipSync(compressed).toString();
            let parsed = JSON.parse(json);

            assert.strictEqual(parsed.block_height, 50);
            assert.ok(parsed.tables);
            assert.ok(parsed.tables.blocks);
            assert.strictEqual(parsed.tables.blocks.length, 1);
            assert.strictEqual(parsed.tables.blocks[0].block_index, 50);
        });

        it('skips tables with 0 rows', async function(){
            let db = createMockDb();
            db.getLastBlock.resolves(10);
            db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
            db.doQuery.resolves([{ table_name: 'blocks' }, { table_name: 'empty_table' }]);
            db.getTableCount.withArgs('blocks').resolves(1);
            db.getTableCount.withArgs('empty_table').resolves(0);
            db.getTablePage.resolves([{ block_index: 10 }]);

            let res = new PassThrough();
            let chunks = [];
            res.on('data', c => chunks.push(c));
            res.setHeader = sinon.stub();

            await new Promise((resolve) => {
                res.on('finish', resolve);
                builder.streamFullSnapshot(db, res);
            });

            let json = zlib.gunzipSync(Buffer.concat(chunks)).toString();
            let parsed = JSON.parse(json);
            assert.ok(parsed.tables.blocks);
            assert.strictEqual(parsed.tables.empty_table, undefined);
        });
    });

    describe('streamIncrementalSnapshot', function(){
        it('returns 404 when no blocks after sinceBlock', async function(){
            let db = createMockDb();
            db.getLastBlock.resolves(50);
            let res = createMockRes();
            await builder.streamIncrementalSnapshot(db, 100, res);
            assert.strictEqual(res.status.calledWith(404), true);
        });

        it('returns 404 when no blocks at all', async function(){
            let db = createMockDb();
            db.getLastBlock.resolves(null);
            let res = createMockRes();
            await builder.streamIncrementalSnapshot(db, 10, res);
            assert.strictEqual(res.status.calledWith(404), true);
        });

        it('streams incremental data with since_block field', async function(){
            let db = createMockDb();
            db.getLastBlock.resolves(100);
            db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
            db.getFirstActionIndex.resolves(500);
            db.doQuery.callsFake(async (query, args) => {
                if(query.includes('information_schema'))
                    return [{ table_name: 'blocks' }];
                if(query.includes('SELECT * FROM'))
                    return [{ block_index: 90 }];
                return [];
            });

            let res = new PassThrough();
            let chunks = [];
            res.on('data', c => chunks.push(c));
            res.setHeader = sinon.stub();

            await new Promise((resolve) => {
                res.on('finish', resolve);
                builder.streamIncrementalSnapshot(db, 80, res);
            });

            let json = zlib.gunzipSync(Buffer.concat(chunks)).toString();
            let parsed = JSON.parse(json);
            assert.strictEqual(parsed.block_height, 100);
            assert.strictEqual(parsed.since_block, 80);
        });

        // Regression: events has no block_index/tx_index cursor, so a decoder
        // incremental snapshot must re-dump it in full. Previously events sat in
        // decoderSkip and was never emitted incrementally, leaving an
        // incrementally-caught-up follower silently drifted behind the source.
        it('decoder: re-dumps the events table in full on incremental', async function(){
            let db = createMockDb('decoder_db');
            db.dbType = 'decoder';
            db.getLastBlock.resolves(100);
            db.getBlockHashRow.resolves({ block_hash: 'h' });
            db.doQuery.callsFake(async (query) => {
                if(query.includes('information_schema'))
                    return [{ table_name: 'events' }];
                // events full-dump query has no WHERE clause (whole table)
                if(/SELECT \* FROM `events`\s*$/.test(query))
                    return [{ id: 1, code: 'x', data: 'y' }, { id: 2, code: 'z', data: 'w' }];
                return [];
            });

            let res = new PassThrough();
            let chunks = [];
            res.on('data', c => chunks.push(c));
            res.setHeader = sinon.stub();

            await new Promise((resolve) => {
                res.on('finish', resolve);
                builder.streamIncrementalSnapshot(db, 80, res);
            });

            let parsed = JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
            assert.ok(parsed.tables.events, 'events table present in incremental snapshot');
            assert.strictEqual(parsed.tables.events.length, 2, 'all events rows re-dumped');
        });

        // Regression: indexer index_* lookup tables have no block_index/action_index
        // cursor, so they previously fell through to the action_index branch, threw on
        // the missing column, and were skipped — an incremental gap-heal left the
        // follower short on index rows (row-count + ledger-hash mismatch). They must be
        // re-dumped in full (the client applies them with INSERT IGNORE).
        it('indexer: re-dumps index_* lookup tables in full on incremental', async function(){
            let db = createMockDb();   // dbType defaults to indexer
            db.getLastBlock.resolves(100);
            db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
            db.getFirstActionIndex.resolves(500);
            db.doQuery.callsFake(async (query) => {
                if(query.includes('information_schema'))
                    return [{ table_name: 'index_addresses' }];
                // Full-dump query has no WHERE clause (whole table). If the code instead
                // tried the action_index branch the query would carry a WHERE and this
                // would not match, so the table would come back empty and the assert below
                // would fail — exactly the bug being guarded against.
                if(/SELECT \* FROM `index_addresses`\s*$/.test(query))
                    return [{ id: 1, address: 'a1' }, { id: 2, address: 'a2' }, { id: 3, address: 'a3' }];
                return [];
            });

            let res = new PassThrough();
            let chunks = [];
            res.on('data', c => chunks.push(c));
            res.setHeader = sinon.stub();

            await new Promise((resolve) => {
                res.on('finish', resolve);
                builder.streamIncrementalSnapshot(db, 80, res);
            });

            let parsed = JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
            assert.ok(parsed.tables.index_addresses, 'index_addresses present in incremental snapshot');
            assert.strictEqual(parsed.tables.index_addresses.length, 3, 'all index_addresses rows re-dumped');
        });
    });

    // The snapshot must be read inside a single REPEATABLE READ transaction so
    // the block-height anchor, the hash headers, and every table read observe
    // one consistent point in time. These tests pin that boundary: the snapshot
    // opens before the anchor read, and the connection is always released
    // (commit on success/empty, rollback on error) so it can't leak.
    describe('transactional boundary', function(){
        it('full: opens read snapshot before reading the block anchor', async function(){
            let db = createMockDb();
            db.getLastBlock.resolves(50);
            db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
            db.doQuery.resolves([]); // no tables

            let res = new PassThrough();
            res.setHeader = sinon.stub();
            await new Promise((resolve) => {
                res.on('finish', resolve);
                builder.streamFullSnapshot(db, res);
            });

            assert.ok(db.beginReadSnapshot.calledOnce, 'beginReadSnapshot called once');
            assert.ok(db.beginReadSnapshot.calledBefore(db.getLastBlock), 'snapshot opens before anchor read');
            assert.ok(db.commitTransaction.calledOnce, 'commit releases the read view');
            assert.ok(db.rollbackTransaction.notCalled, 'no rollback on success');
        });

        it('full: commits (releases) the snapshot even on the 404 empty-db path', async function(){
            let db = createMockDb();
            db.getLastBlock.resolves(null);
            let res = createMockRes();
            await builder.streamFullSnapshot(db, res);
            assert.ok(db.beginReadSnapshot.calledOnce);
            assert.ok(db.commitTransaction.calledOnce, 'snapshot released on 404 so the connection is not leaked');
        });

        it('full: rolls back the snapshot if a read throws before streaming', async function(){
            let db = createMockDb();
            db.getLastBlock.resolves(50);
            db.getBlockHashRow.rejects(new Error('boom'));
            let res = createMockRes();
            await assert.rejects(builder.streamFullSnapshot(db, res), /boom/);
            assert.ok(db.rollbackTransaction.calledOnce, 'snapshot rolled back on error');
            assert.ok(db.commitTransaction.notCalled, 'no commit on error');
        });

        it('incremental: opens read snapshot before reading the block anchor', async function(){
            let db = createMockDb();
            db.getLastBlock.resolves(100);
            db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
            db.getFirstActionIndex.resolves(500);
            db.doQuery.callsFake(async (query) => {
                if(query.includes('information_schema')) return [{ table_name: 'blocks' }];
                return [];
            });

            let res = new PassThrough();
            res.setHeader = sinon.stub();
            await new Promise((resolve) => {
                res.on('finish', resolve);
                builder.streamIncrementalSnapshot(db, 80, res);
            });

            assert.ok(db.beginReadSnapshot.calledOnce);
            assert.ok(db.beginReadSnapshot.calledBefore(db.getLastBlock));
            assert.ok(db.commitTransaction.calledOnce);
            assert.ok(db.rollbackTransaction.notCalled);
        });

        it('incremental: commits (releases) the snapshot on the 404 path', async function(){
            let db = createMockDb();
            db.getLastBlock.resolves(50);
            let res = createMockRes();
            await builder.streamIncrementalSnapshot(db, 100, res);
            assert.ok(db.beginReadSnapshot.calledOnce);
            assert.ok(db.commitTransaction.calledOnce, 'snapshot released on 404');
        });
    });

    describe('branch coverage', function(){
        // gzip.pipe(res) needs a REAL writable stream (removeListener etc.), so use a
        // genuine PassThrough with a setHeader stub + chunk collection — the plain
        // createMockRes() object only works on the early-return (404) paths.
        function streamRes(){
            let res = new PassThrough();
            let headers = {};
            res.setHeader = (k, v) => { headers[k] = v; };
            res.status = sinon.stub().returnsThis();
            res.json = sinon.stub();
            res._headers = headers;
            let chunks = [];
            res.on('data', c => chunks.push(c));
            res.getCollectedData = () => Buffer.concat(chunks);
            return res;
        }
        // Attach the finish listener BEFORE starting the stream, then resolve once
        // gzip.end() flushes through the PassThrough (mirrors the pattern above).
        function run(start, res){ return new Promise(r => { res.on('finish', r); start(); }); }

        describe('_getOrderedTables', function(){
            it('drops operator-local tables and tolerates the uppercase TABLE_NAME variant', async function(){
                let db = createMockDb();
                db.doQuery.resolves([
                    { TABLE_NAME: 'icons' },          // operator-local → dropped
                    { table_name: 'price_snapshots' },// operator-local → dropped
                    { TABLE_NAME: 'middle_upper' },   // uppercase fallback, middle
                    { table_name: 'blocks' }          // priority
                ]);
                let ordered = await builder._getOrderedTables(db);
                assert.ok(!ordered.includes('icons'));
                assert.ok(!ordered.includes('price_snapshots'));
                assert.ok(ordered.includes('middle_upper'));
                assert.strictEqual(ordered[0], 'blocks');
            });
        });

        describe('streamFullSnapshot', function(){
            it('writes empty hash headers when the hashRow lacks fields, comma-joins tables/rows, and serializes BigInt', async function(){
                let db = createMockDb();
                db.getLastBlock.resolves(100);
                db.getBlockHashRow.resolves({}); // present but no ledger/actions/contract fields → '' fallbacks
                db.doQuery.resolves([{ table_name: 'blocks' }, { table_name: 'actions' }]);
                db.getTableCount.resolves(2);
                // Two rows (exercise the inter-row comma) incl. a BigInt (exercise bigIntReplacer).
                db.getTablePage.resolves([{ id: 1n, n: 'a' }, { id: 2n, n: 'b' }]);
                let res = streamRes();
                await run(() => builder.streamFullSnapshot(db, res), res);
                assert.strictEqual(res._headers['X-Ledger-Hash'], '');
                assert.strictEqual(res._headers['X-Actions-Hash'], '');
                assert.strictEqual(res._headers['X-Contract-Hash'], '');
                assert.ok(db.commitTransaction.calledOnce);
                let out = JSON.parse(zlib.gunzipSync(res.getCollectedData()).toString());
                assert.deepStrictEqual(Object.keys(out.tables), ['blocks', 'actions']);
                assert.strictEqual(out.tables.blocks[0].id, '1'); // BigInt → string
            });

            it('skips zero-count tables and swallows a per-table read error', async function(){
                let db = createMockDb();
                db.getLastBlock.resolves(10);
                db.getBlockHashRow.resolves(null);
                db.doQuery.resolves([{ table_name: 'blocks' }, { table_name: 'actions' }]);
                db.getTableCount.withArgs('blocks').resolves(0);          // skipped (count 0)
                db.getTableCount.withArgs('actions').rejects(new Error('boom')); // caught
                let res = streamRes();
                await run(() => builder.streamFullSnapshot(db, res), res);
                assert.ok(console.error.getCalls().some(c => /Error reading table actions/.test(c.args[0])));
                assert.ok(db.commitTransaction.calledOnce);
            });

            it('rolls back and rethrows when the snapshot read throws', async function(){
                let db = createMockDb();
                db.getLastBlock.rejects(new Error('read fail'));
                let res = createMockRes();
                await assert.rejects(() => builder.streamFullSnapshot(db, res), { message: 'read fail' });
                assert.ok(db.rollbackTransaction.calledOnce);
            });
        });

        describe('streamIncrementalSnapshot', function(){
            it('decoder: emits X-Block-Hash and scopes skip/block/tx/full-dump tables correctly', async function(){
                let db = createMockDb();
                db.dbType = 'decoder';
                db.getLastBlock.resolves(100);
                db.getBlockHashRow.resolves({ block_hash: 'BH' });
                db.doQuery.callsFake(async (sql) => {
                    if(/information_schema/.test(sql))
                        return [
                            { table_name: 'mempool_transactions' }, // decoderSkip
                            { table_name: 'blocks' },               // decoderBlockScoped
                            { table_name: 'transaction_outputs' },  // decoderTxScoped
                            { table_name: 'pubkeys' },              // decoderFullDump
                            { table_name: 'random_other' }          // else → continue
                        ];
                    if(/`blocks`/.test(sql)) return [{ block_index: 5 }];
                    if(/`transaction_outputs`/.test(sql)) return [{ tx_index: 1 }];
                    if(/`pubkeys`/.test(sql)) return [{ address_id: 1 }];
                    return [];
                });
                let res = streamRes();
                await run(() => builder.streamIncrementalSnapshot(db, 3, res), res);
                assert.strictEqual(res._headers['X-Block-Hash'], 'BH');
                let out = JSON.parse(zlib.gunzipSync(res.getCollectedData()).toString());
                assert.deepStrictEqual(Object.keys(out.tables).sort(), ['blocks', 'pubkeys', 'transaction_outputs']);
                assert.ok(!('mempool_transactions' in out.tables));
                assert.ok(!('random_other' in out.tables));
            });

            it('indexer: emits empty hash headers, dumps full + action-scoped tables, and comma-joins them', async function(){
                let db = createMockDb();
                db.dbType = 'indexer';
                db.getLastBlock.resolves(100);
                db.getBlockHashRow.resolves({}); // missing fields → '' fallbacks
                db.getFirstActionIndex.resolves(500);
                db.doQuery.callsFake(async (sql) => {
                    if(/information_schema/.test(sql))
                        return [{ table_name: 'blocks' }, { table_name: 'index_actions' },
                                { table_name: 'sends' }, { table_name: 'no_action_col' }];
                    if(/`blocks`/.test(sql)) return [{ block_index: 7 }];          // block-scoped
                    if(/`index_actions`/.test(sql)) return [{ id: 1 }];            // full dump
                    if(/`sends`/.test(sql) && /action_index/.test(sql)) return [{ action_index: 501 }]; // action-scoped
                    // A table reached via the action_index branch that has no such column:
                    // the inner try/catch swallows it and skips the table.
                    if(/`no_action_col`/.test(sql)) throw new Error('Unknown column action_index');
                    return [];
                });
                let res = streamRes();
                await run(() => builder.streamIncrementalSnapshot(db, 3, res), res);
                assert.strictEqual(res._headers['X-Ledger-Hash'], '');
                assert.strictEqual(res._headers['X-Actions-Hash'], '');
                assert.strictEqual(res._headers['X-Contract-Hash'], '');
                let out = JSON.parse(zlib.gunzipSync(res.getCollectedData()).toString());
                assert.ok('blocks' in out.tables && 'index_actions' in out.tables && 'sends' in out.tables);
                assert.ok(!('no_action_col' in out.tables), 'action_index-less table is skipped');
            });

            it('indexer: skips middle tables when there is no firstActionIndex (no actions since the cursor)', async function(){
                let db = createMockDb();
                db.dbType = 'indexer';
                db.getLastBlock.resolves(100);
                db.getBlockHashRow.resolves(null);
                db.getFirstActionIndex.resolves(null); // no actions at/after sinceBlock → else-continue
                db.doQuery.callsFake(async (sql) => {
                    if(/information_schema/.test(sql))
                        return [{ table_name: 'blocks' }, { table_name: 'sends' }];
                    if(/`blocks`/.test(sql)) return [{ block_index: 7 }]; // block-scoped still emitted
                    return [];
                });
                let res = streamRes();
                await run(() => builder.streamIncrementalSnapshot(db, 3, res), res);
                let out = JSON.parse(zlib.gunzipSync(res.getCollectedData()).toString());
                assert.ok('blocks' in out.tables);
                assert.ok(!('sends' in out.tables), 'action-scoped table skipped when firstActionIndex is null');
            });

            it('swallows a per-table read error during incremental', async function(){
                let db = createMockDb();
                db.dbType = 'indexer';
                db.getLastBlock.resolves(100);
                db.getBlockHashRow.resolves(null);
                db.getFirstActionIndex.resolves(500);
                db.doQuery.callsFake(async (sql) => {
                    if(/information_schema/.test(sql)) return [{ table_name: 'blocks' }];
                    throw new Error('table boom');
                });
                let res = streamRes();
                await run(() => builder.streamIncrementalSnapshot(db, 3, res), res);
                assert.ok(console.error.getCalls().some(c => /Error reading table blocks for incremental/.test(c.args[0])));
                assert.ok(db.commitTransaction.calledOnce);
            });

            it('rolls back and rethrows when the incremental read throws before streaming', async function(){
                let db = createMockDb();
                db.getLastBlock.rejects(new Error('inc read fail'));
                let res = createMockRes();
                await assert.rejects(() => builder.streamIncrementalSnapshot(db, 3, res), { message: 'inc read fail' });
                assert.ok(db.rollbackTransaction.calledOnce);
            });
        });
    });
});
