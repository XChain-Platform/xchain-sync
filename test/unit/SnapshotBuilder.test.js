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
const sinon  = require('sinon');
const { PassThrough, Readable } = require('stream');
const { EventEmitter } = require('events');
const zlib = require('zlib');
const SnapshotBuilder = require('../../src/SnapshotBuilder');
const { SnapshotStreamWriter } = require('../../src/SnapshotBuilder');
const Utility = require('../../src/utility');

function createMockDb(dbName){
    return {
        dbName: dbName || 'test_db',
        doQuery: sinon.stub().resolves([]),
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getFirstActionIndex: sinon.stub().resolves(null),
        // Single-pass row stream per table (replaced ORDER BY 1 LIMIT/OFFSET
        // paging, which had no total order on keyless tables). Tests override
        // with Readable.from(rows) for populated tables.
        streamTableRows: sinon.stub().callsFake(() => Readable.from([])),
        getTableCount: sinon.stub().resolves(0),
        // beginReadSnapshot returns a dedicated connection handle the builder
        // threads into every read and ends via commit/rollbackReadSnapshot.
        beginReadSnapshot: sinon.stub().resolves({ _snapshotConn: true }),
        commitReadSnapshot: sinon.stub().resolves(true),
        rollbackReadSnapshot: sinon.stub().resolves()
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

        it('excludes mempool_transactions (node-local, non-deterministic) like every other channel', async function(){
            // The per-block stream, the incremental decoderSkip, and the /status
            // completeness count all exclude mempool_transactions; the FULL snapshot
            // must too, or full-bootstrap replicas freeze the source's mempool forever.
            let db = createMockDb();
            db.doQuery.resolves([
                { table_name: 'blocks' },
                { table_name: 'mempool_transactions' },
                { table_name: 'oracle_prices' } // existing operator-local exclusion, sanity check
            ]);
            let ordered = await builder._getOrderedTables(db);
            assert.deepStrictEqual(ordered, ['blocks']);
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
            db.streamTableRows.callsFake(() => Readable.from([{ block_index: 50, block_time: 100 }]));

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
            db.streamTableRows.callsFake(() => Readable.from([{ block_index: 10 }]));

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

        it('emits every row of a keyless table exactly once in a single ordered pass (no offset re-paging)', async function(){
            // Regression for the `ORDER BY 1 LIMIT ? OFFSET ?` pagination: keyless
            // ledger tables (credits/debits/...) have a non-unique first column, so
            // offset re-paging had no total order and a page-boundary tie could be
            // emitted twice or skipped. The table here far exceeds pageSize; the
            // builder must read it in ONE streaming pass.
            let db = createMockDb();
            builder.pageSize = 2; // the old scheme would have re-queried 3 offset pages
            db.getLastBlock.resolves(50);
            db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
            db.doQuery.resolves([{ table_name: 'credits' }]);
            // Five rows sharing one action_index (a match credits buyer+seller+fee...).
            let rows = [0, 1, 2, 3, 4].map(i => ({ action_index: 7, address_id: i, amount: String(i) }));
            db.getTableCount.resolves(rows.length);
            db.streamTableRows.callsFake(() => Readable.from(rows));

            let res = new PassThrough();
            let chunks = [];
            res.on('data', c => chunks.push(c));
            res.setHeader = sinon.stub();

            await new Promise((resolve) => {
                res.on('finish', resolve);
                builder.streamFullSnapshot(db, res);
            });

            let parsed = JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
            assert.deepStrictEqual(parsed.tables.credits.map(r => r.address_id), [0, 1, 2, 3, 4],
                'every row exactly once, in stream order (no duplicate, no skip)');
            assert.strictEqual(db.streamTableRows.callCount, 1,
                'a table is read in one single-pass stream, not per-offset pages');
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
                // events is full-dumped, now streamed by id cursor in pages (a single
                // unbounded SELECT * would OOM on a large table). 2 rows < pageSize, so
                // one page then the loop stops.
                if(/SELECT \* FROM `events` WHERE `id` > \? ORDER BY `id` ASC LIMIT \?/.test(query))
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
        // the missing column, and were skipped; an incremental gap-heal left the
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
                // index_* lookup tables are full-dumped, now streamed by id cursor in
                // pages. If the code instead tried the action_index branch the query
                // would carry a `WHERE action_index >=` and this would not match, so the
                // table would come back empty and the assert below would fail, exactly
                // the bug being guarded against.
                if(/SELECT \* FROM `index_addresses` WHERE `id` > \? ORDER BY `id` ASC LIMIT \?/.test(query))
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

        // Regression: the indexer `events` audit log has no block_index/action_index
        // cursor, so it previously fell through to the action_index branch, threw errno
        // 1054 on the missing column, and was swallowed; an incrementally-caught-up
        // follower froze its events table at bootstrap height (silent, since events is
        // replication:'snapshot' and outside the /status count). It must be re-dumped in
        // full (the client applies it with INSERT IGNORE). Mirrors the decoder events
        // fix above. The re-dump is PAGED by id cursor: events is append-only on an
        // AUTO_INCREMENT id PK, and the bundled `SELECT * FROM events` it used to take
        // materialized the whole audit log on every catch-up.
        it('indexer: re-dumps the events audit log in full on incremental, paged by id', async function(){
            let db = createMockDb();   // dbType defaults to indexer
            db.getLastBlock.resolves(100);
            db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
            db.getFirstActionIndex.resolves(500);
            let unbounded = 0;
            db.doQuery.callsFake(async (query) => {
                if(query.includes('information_schema'))
                    return [{ table_name: 'events' }];
                if(/SELECT \* FROM `events`$/.test(query)){ unbounded++; return []; }
                // events takes the id-cursor pager, NOT the bundled full-dump and NOT the
                // action_index branch (which would carry `WHERE action_index >=`, match
                // nothing here, and leave the table empty: the frozen-at-bootstrap bug
                // this case has always guarded). 2 rows < pageSize, so one page and stop.
                if(/SELECT \* FROM `events` WHERE `id` > \? ORDER BY `id` ASC LIMIT \?/.test(query))
                    return [{ id: 1, event: 'REORG', data: 'x' }, { id: 2, event: 'REORG', data: 'y' }];
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
            assert.ok(parsed.tables.events, 'events audit log present in incremental snapshot');
            assert.strictEqual(parsed.tables.events.length, 2, 'all events rows re-dumped');
            assert.strictEqual(unbounded, 0, 'the unbounded full-table events SELECT is never issued');
        });

        // skipLookups: a truncated/fast-chain replica syncs the append-only `.index`
        // lookup tables out of band (paged), so this response must OMIT them while
        // still streaming the block-scoped data the window needs.
        it('skipLookups omits the .index lookup tables but keeps block-scoped data', async function(){
            let db = createMockDb();
            db.getLastBlock.resolves(100);
            db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
            db.getFirstActionIndex.resolves(500);
            db.doQuery.callsFake(async (query) => {
                if(query.includes('information_schema'))
                    return [{ table_name: 'index_addresses' }, { table_name: 'blocks' }];
                if(/SELECT \* FROM `index_addresses`/.test(query))
                    return [{ id: 1, address: 'a1' }];
                if(/SELECT \* FROM `blocks`/.test(query))
                    return [{ block_index: 90 }];
                return [];
            });

            let res = new PassThrough();
            let chunks = [];
            res.on('data', c => chunks.push(c));
            res.setHeader = sinon.stub();

            await new Promise((resolve) => {
                res.on('finish', resolve);
                builder.streamIncrementalSnapshot(db, 80, res, 'BTC', { skipLookups: true });
            });

            let parsed = JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
            assert.strictEqual(parsed.tables.index_addresses, undefined, 'lookup table omitted under skipLookups');
            assert.ok(parsed.tables.blocks, 'block-scoped table still streamed');
        });
    });

    describe('streamTableRowsById', function(){
        it('rejects a non-pageable table with 400', async function(){
            let db = createMockDb();
            let res = createMockRes();
            await builder.streamTableRowsById(db, 'balances', 0, 100, res);
            assert.ok(res.status.calledWith(400), 'only allowlisted .index tables are pageable');
        });

        it('streams an id-ordered page with max_id and has_more=false when short', async function(){
            let db = createMockDb();
            db.doQuery.resolves([{ id: 11, x: 'a' }, { id: 12, x: 'b' }]);
            let res = new PassThrough();
            let chunks = [];
            res.on('data', c => chunks.push(c));
            res.setHeader = sinon.stub();
            await new Promise((resolve) => {
                res.on('finish', resolve);
                builder.streamTableRowsById(db, 'index_transactions', 10, 50000, res);
            });
            let parsed = JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
            assert.strictEqual(parsed.table, 'index_transactions');
            assert.strictEqual(parsed.rows.length, 2);
            assert.strictEqual(parsed.max_id, 12, 'max_id is the last returned id');
            assert.strictEqual(parsed.has_more, false, 'short page -> no more');
        });

        it('queries id > after_id ORDER BY id LIMIT, and sets has_more when a full page returns', async function(){
            let db = createMockDb();
            db.doQuery.resolves([{ id: 1 }, { id: 2 }, { id: 3 }]);
            let res = new PassThrough();
            let chunks = [];
            res.on('data', c => chunks.push(c));
            res.setHeader = sinon.stub();
            await new Promise((resolve) => {
                res.on('finish', resolve);
                builder.streamTableRowsById(db, 'index_transactions', 0, 3, res);
            });
            let parsed = JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
            assert.strictEqual(parsed.has_more, true, 'rows.length === limit -> more pages remain');
            let q = db.doQuery.firstCall.args[0];
            assert.ok(/WHERE `id` > \? ORDER BY `id` ASC LIMIT \?/.test(q), 'paged id-cursor query');
            assert.deepStrictEqual(db.doQuery.firstCall.args[1], [0, 3], 'after_id and limit bound as params');
        });

        it('clamps an oversized limit to the ceiling', async function(){
            let db = createMockDb();
            db.doQuery.resolves([]);
            let res = new PassThrough();
            res.on('data', () => {});
            res.setHeader = sinon.stub();
            await new Promise((resolve) => {
                res.on('finish', resolve);
                builder.streamTableRowsById(db, 'index_transactions', 0, 9999999, res);
            });
            assert.strictEqual(db.doQuery.firstCall.args[1][1], SnapshotBuilder.ROWS_PAGE_MAX, 'limit clamped to ceiling');
        });

        // The decoder pubkeys table's PK (address_id) is NOT monotonic w.r.t. insert
        // order (inserted at first-SPEND, address_id assigned at first-SEEN), so an
        // address_id cursor would permanently skip late-inserted rows. pubkeys
        // now carries a surrogate monotonic AUTO_INCREMENT `id` and must page by it.
        it('decoder: pages the pubkeys table by its surrogate monotonic id cursor', async function(){
            let db = createMockDb('decoder_db');
            db.dbType = 'decoder';
            db.doQuery.resolves([
                { id: 5, address_id: 90, pubkey: 'p1' },
                { id: 9, address_id: 12, pubkey: 'p2' }
            ]);
            let res = new PassThrough();
            let chunks = [];
            res.on('data', c => chunks.push(c));
            res.setHeader = sinon.stub();
            await new Promise((resolve) => {
                res.on('finish', resolve);
                builder.streamTableRowsById(db, 'pubkeys', 0, 50000, res);
            });
            let parsed = JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
            assert.strictEqual(parsed.table, 'pubkeys');
            assert.strictEqual(parsed.max_id, 9, 'cursor high-water is the max surrogate id, not address_id');
            let q = db.doQuery.firstCall.args[0];
            assert.ok(/WHERE `id` > \? ORDER BY `id` ASC LIMIT \?/.test(q), 'pages by surrogate id, not address_id');
        });
    });

    // The snapshot must be read inside a single REPEATABLE READ transaction so
    // the block-height anchor, the hash headers, and every table read observe
    // one consistent point in time. These tests pin that boundary: the snapshot
    // opens before the anchor read, and the connection is always released
    // (commit on success/empty, rollback on error) so it can't leak.
    describe('streamDispensers', function(){
        it('rejects a non-decoder dbType with 400', async function(){
            let db = createMockDb(); // dbType defaults to indexer-shaped (undefined)
            let res = createMockRes();
            await builder.streamDispensers(db, NaN, NaN, 50000, res);
            assert.ok(res.status.calledWith(400), 'dispensers reconcile is decoder-only');
        });

        it('first page (no cursor) selects the full table ordered by the composite PK', async function(){
            let db = createMockDb();
            db.dbType = 'decoder';
            db.doQuery.resolves([{ tx_index: 5, address_id: 9 }, { tx_index: 7, address_id: 2 }]);
            let res = new PassThrough();
            let chunks = [];
            res.on('data', c => chunks.push(c));
            res.setHeader = sinon.stub();
            await new Promise((resolve) => {
                res.on('finish', resolve);
                builder.streamDispensers(db, NaN, NaN, 50000, res);
            });
            let parsed = JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
            assert.strictEqual(parsed.rows.length, 2);
            assert.strictEqual(parsed.max_tx, 7, 'max_tx is the last row tx_index');
            assert.strictEqual(parsed.max_addr, 2, 'max_addr is the last row address_id');
            assert.strictEqual(parsed.has_more, false, 'single-response contract: never more');
            let q = db.doQuery.firstCall.args[0];
            assert.ok(/ORDER BY tx_index ASC, address_id ASC/.test(q));
            assert.ok(!/LIMIT/.test(q),
                'no LIMIT: the full table must ship in ONE statement-consistent response ' +
                '(cross-request pages tear under in-place soft-expires)');
            assert.ok(!/WHERE/.test(q), 'no cursor -> no predicate');
        });

        it('honours a legacy cursor within the same single response and never reports more', async function(){
            let db = createMockDb();
            db.dbType = 'decoder';
            db.doQuery.resolves([{ tx_index: 8, address_id: 1 }, { tx_index: 8, address_id: 4 }, { tx_index: 9, address_id: 0 }]);
            let res = new PassThrough();
            let chunks = [];
            res.on('data', c => chunks.push(c));
            res.setHeader = sinon.stub();
            await new Promise((resolve) => {
                res.on('finish', resolve);
                builder.streamDispensers(db, 8, 1, 3, res);
            });
            let parsed = JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
            assert.strictEqual(parsed.has_more, false,
                'has_more is always false so an old paging client completes in one round trip');
            let q = db.doQuery.firstCall.args[0];
            assert.ok(/WHERE \(tx_index > \? OR \(tx_index = \? AND address_id > \?\)\)/.test(q), 'composite keyset predicate');
            assert.ok(!/LIMIT/.test(q), 'legacy limit arg is ignored: no paging');
            assert.deepStrictEqual(db.doQuery.firstCall.args[1], [8, 8, 1]);
        });
    });

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
            assert.ok(db.commitReadSnapshot.calledOnce, 'commit releases the read view');
            assert.ok(db.rollbackReadSnapshot.notCalled, 'no rollback on success');
        });

        it('full: commits (releases) the snapshot even on the 404 empty-db path', async function(){
            let db = createMockDb();
            db.getLastBlock.resolves(null);
            let res = createMockRes();
            await builder.streamFullSnapshot(db, res);
            assert.ok(db.beginReadSnapshot.calledOnce);
            assert.ok(db.commitReadSnapshot.calledOnce, 'snapshot released on 404 so the connection is not leaked');
        });

        it('full: rolls back the snapshot if a read throws before streaming', async function(){
            let db = createMockDb();
            db.getLastBlock.resolves(50);
            db.getBlockHashRow.rejects(new Error('boom'));
            let res = createMockRes();
            await assert.rejects(builder.streamFullSnapshot(db, res), /boom/);
            assert.ok(db.rollbackReadSnapshot.calledOnce, 'snapshot rolled back on error');
            assert.ok(db.commitReadSnapshot.notCalled, 'no commit on error');
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
            assert.ok(db.commitReadSnapshot.calledOnce);
            assert.ok(db.rollbackReadSnapshot.notCalled);
        });

        it('incremental: commits (releases) the snapshot on the 404 path', async function(){
            let db = createMockDb();
            db.getLastBlock.resolves(50);
            let res = createMockRes();
            await builder.streamIncrementalSnapshot(db, 100, res);
            assert.ok(db.beginReadSnapshot.calledOnce);
            assert.ok(db.commitReadSnapshot.calledOnce, 'snapshot released on 404');
        });
    });

    describe('branch coverage', function(){
        // gzip.pipe(res) needs a REAL writable stream (removeListener etc.), so use a
        // genuine PassThrough with a setHeader stub + chunk collection; the plain
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
                    { TABLE_NAME: 'icons' },             // operator-local → dropped
                    { table_name: 'price_snapshots' },   // operator-local → dropped
                    { table_name: 'pending_hub_pushes' },// operator-local → dropped
                    { TABLE_NAME: 'middle_upper' },      // uppercase fallback, middle
                    { table_name: 'blocks' }             // priority
                ]);
                let ordered = await builder._getOrderedTables(db);
                assert.ok(!ordered.includes('icons'));
                assert.ok(!ordered.includes('price_snapshots'));
                assert.ok(!ordered.includes('pending_hub_pushes'));
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
                db.streamTableRows.callsFake(() => Readable.from([{ id: 1n, n: 'a' }, { id: 2n, n: 'b' }]));
                let res = streamRes();
                await run(() => builder.streamFullSnapshot(db, res), res);
                assert.strictEqual(res._headers['X-Ledger-Hash'], '');
                assert.strictEqual(res._headers['X-Actions-Hash'], '');
                assert.strictEqual(res._headers['X-Contract-Hash'], '');
                assert.ok(db.commitReadSnapshot.calledOnce);
                let out = JSON.parse(zlib.gunzipSync(res.getCollectedData()).toString());
                assert.deepStrictEqual(Object.keys(out.tables), ['blocks', 'actions']);
                assert.strictEqual(out.tables.blocks[0].id, '1'); // BigInt → string
            });

            it('skips zero-count tables without failing the snapshot', async function(){
                let db = createMockDb();
                db.getLastBlock.resolves(10);
                db.getBlockHashRow.resolves(null);
                db.doQuery.resolves([{ table_name: 'blocks' }, { table_name: 'actions' }]);
                db.getTableCount.withArgs('blocks').resolves(0);   // legitimately empty → omitted
                db.getTableCount.withArgs('actions').resolves(1);
                db.streamTableRows.callsFake(() => Readable.from([{ id: 1 }]));
                let res = streamRes();
                await run(() => builder.streamFullSnapshot(db, res), res);
                let out = JSON.parse(zlib.gunzipSync(res.getCollectedData()).toString());
                assert.deepStrictEqual(Object.keys(out.tables), ['actions'],
                    'a zero-row table is still omitted; only real read errors abort');
                assert.ok(db.commitReadSnapshot.calledOnce);
            });

            // This per-table catch used to log 'Error reading table ...' and
            // continue, so a COUNT(*) lock-wait/timeout published syntactically valid JSON
            // with that table simply absent while still advertising block_height at the tip.
            // ClientApplier.applyFullSnapshot DELETEs every snapshot-eligible local table and
            // re-inserts only what the payload carries, so a populated table reached the
            // replica EMPTY and the replica advanced to the tip; a single-source deployment
            // runs no post-apply content check to catch it. A partial snapshot must never be
            // published, so the whole read now fails.
            it('aborts the whole snapshot on a per-table read error rather than omitting the table', async function(){
                let db = createMockDb();
                db.getLastBlock.resolves(10);
                db.getBlockHashRow.resolves(null);
                db.doQuery.resolves([{ table_name: 'blocks' }, { table_name: 'actions' }]);
                db.getTableCount.withArgs('blocks').resolves(1);
                db.getTableCount.withArgs('actions').rejects(new Error('Lock wait timeout exceeded'));
                db.streamTableRows.callsFake(() => Readable.from([{ id: 1 }]));
                let res = streamRes();
                await assert.rejects(() => builder.streamFullSnapshot(db, res),
                    { message: 'Lock wait timeout exceeded' });
                assert.strictEqual(db.commitReadSnapshot.called, false,
                    'a partial snapshot must never be committed/closed');
                assert.ok(db.rollbackReadSnapshot.calledOnce, 'the read view is released by rollback');
                // The closing '}}' is never emitted, so the client's JSON.parse of the
                // truncated download throws and the bootstrap retries instead of committing.
                assert.ok(!/\}\}\s*$/.test(res.getCollectedData().toString('binary')));
            });

            it('still returns quietly on a client-disconnect abort (not treated as a read error)', async function(){
                let db = createMockDb();
                db.getLastBlock.resolves(10);
                db.getBlockHashRow.resolves(null);
                db.doQuery.resolves([{ table_name: 'blocks' }]);
                db.getTableCount.withArgs('blocks').rejects(Object.assign(new Error('gone'), { aborted: true }));
                let res = streamRes();
                await builder.streamFullSnapshot(db, res); // returns, does not throw
                assert.strictEqual(db.commitReadSnapshot.called, false);
                assert.ok(db.rollbackReadSnapshot.calledOnce);
            });

            it('rolls back and rethrows when the snapshot read throws', async function(){
                let db = createMockDb();
                db.getLastBlock.rejects(new Error('read fail'));
                let res = createMockRes();
                await assert.rejects(() => builder.streamFullSnapshot(db, res), { message: 'read fail' });
                assert.ok(db.rollbackReadSnapshot.calledOnce);
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
                    // the inner try/catch swallows the genuine schema gap (errno 1054) and
                    // skips the table (a transient error would instead re-throw / fail closed).
                    if(/`no_action_col`/.test(sql)){ let e = new Error('Unknown column action_index'); e.errno = 1054; throw e; }
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

            // contract_emissions.action_index is NULL for INTERNAL emissions (SLASH), so
            // the generic `action_index >= ?` cursor drops them from a catch-up window
            // while the consensus contract_hash counts them via the execution_index
            // chain. The follower then carries a short table and only an advisory parity
            // log says so. Reach them the way the live stream does, by block.
            it('indexer: scopes contract_emissions by block through the execution_index chain, not the action_index cursor @regression', async function(){
                let db = createMockDb();
                db.dbType = 'indexer';
                db.getLastBlock.resolves(100);
                db.getBlockHashRow.resolves(null);
                db.getFirstActionIndex.resolves(500);
                let emissionSql = null, emissionArgs = null;
                db.doQuery.callsFake(async (sql, args) => {
                    if(/information_schema/.test(sql)) return [{ table_name: 'contract_emissions' }];
                    if(/contract_emissions/.test(sql)){
                        emissionSql = sql; emissionArgs = args;
                        // The internal emission the action_index cursor would have dropped.
                        return [{ execution_index: 900, emitted_action: 'SLASH', action_index: null, position: 0 }];
                    }
                    return [];
                });
                let res = streamRes();
                await run(() => builder.streamIncrementalSnapshot(db, 3, res), res);
                let out = JSON.parse(zlib.gunzipSync(res.getCollectedData()).toString());
                assert.deepStrictEqual(out.tables.contract_emissions,
                    [{ execution_index: 900, emitted_action: 'SLASH', action_index: null, position: 0 }],
                    'the NULL-action internal emission must ride the catch-up payload');
                assert.ok(/contract_executions ce ON \(ce\.action_index = em\.execution_index\)/.test(emissionSql),
                    'must reach the rows through the execution_index chain');
                assert.ok(/a\.block_index >= \?/.test(emissionSql), 'must be block-scoped');
                assert.ok(!/WHERE action_index >= /.test(emissionSql),
                    'must not use the generic action_index cursor, which drops NULL-action rows');
                assert.deepStrictEqual(emissionArgs, [3], 'bound to sinceBlock, not firstActionIndex');
                // The AUTO_INCREMENT id is local to each node (the live stream never
                // carries it), so shipping the source's would collide on a plain INSERT.
                assert.ok(!/em\.\*/.test(emissionSql) && !/em\.id/.test(emissionSql),
                    'must name the four protocol columns, never em.* (which carries the local id)');
            });

            // Same branch with a quiet window: block scoping makes it independent of
            // firstActionIndex, which the generic cursor branch is not.
            it('indexer: still ships internal emissions when firstActionIndex is null @regression', async function(){
                let db = createMockDb();
                db.dbType = 'indexer';
                db.getLastBlock.resolves(100);
                db.getBlockHashRow.resolves(null);
                db.getFirstActionIndex.resolves(null);
                db.doQuery.callsFake(async (sql) => {
                    if(/information_schema/.test(sql)) return [{ table_name: 'contract_emissions' }];
                    if(/contract_emissions/.test(sql))
                        return [{ execution_index: 900, emitted_action: 'SLASH', action_index: null, position: 0 }];
                    return [];
                });
                let res = streamRes();
                await run(() => builder.streamIncrementalSnapshot(db, 3, res), res);
                let out = JSON.parse(zlib.gunzipSync(res.getCollectedData()).toString());
                assert.ok(out.tables.contract_emissions && out.tables.contract_emissions.length === 1,
                    'a quiet action window must not silence the emission branch');
            });

            it('swallows a per-table SCHEMA-GAP read error (errno 1146) during incremental', async function(){
                let db = createMockDb();
                db.dbType = 'indexer';
                db.getLastBlock.resolves(100);
                db.getBlockHashRow.resolves(null);
                db.getFirstActionIndex.resolves(500);
                db.doQuery.callsFake(async (sql) => {
                    if(/information_schema/.test(sql)) return [{ table_name: 'blocks' }];
                    let e = new Error('table missing'); e.errno = 1146; throw e;
                });
                let res = streamRes();
                await run(() => builder.streamIncrementalSnapshot(db, 3, res), res);
                assert.ok(console.error.getCalls().some(c => /Error reading table blocks for incremental/.test(c.args[0])));
                assert.ok(db.commitReadSnapshot.calledOnce);
            });

            // A transient/operational error (deadlock 1213, lock-wait 1205,
            // connection drop) must NOT be swallowed. Rows are fully fetched before any byte
            // is written, so swallowing it would ship a structurally-valid but silently
            // INCOMPLETE catch-up (the table's window vanishes yet the payload still parses).
            // Only genuine schema gaps (1146/1054) are tolerated; everything else fails closed.
            it('fails closed (rejects + rolls back) on a transient per-table read error during incremental @regression', async function(){
                let db = createMockDb();
                db.dbType = 'indexer';
                db.getLastBlock.resolves(100);
                db.getBlockHashRow.resolves(null);
                db.getFirstActionIndex.resolves(500);
                db.doQuery.callsFake(async (sql) => {
                    if(/information_schema/.test(sql)) return [{ table_name: 'blocks' }];
                    let e = new Error('Deadlock found'); e.errno = 1213; throw e;
                });
                let res = streamRes();
                await assert.rejects(() => builder.streamIncrementalSnapshot(db, 3, res), /Deadlock found/);
                assert.ok(db.rollbackReadSnapshot.calledOnce, 'read snapshot rolled back on fail-closed abort');
            });

            it('fails closed on a connection-drop (no errno) per-table read error during incremental @regression', async function(){
                let db = createMockDb();
                db.dbType = 'indexer';
                db.getLastBlock.resolves(100);
                db.getBlockHashRow.resolves(null);
                db.getFirstActionIndex.resolves(500);
                db.doQuery.callsFake(async (sql) => {
                    if(/information_schema/.test(sql)) return [{ table_name: 'blocks' }];
                    throw new Error('Connection lost: The server closed the connection');
                });
                let res = streamRes();
                await assert.rejects(() => builder.streamIncrementalSnapshot(db, 3, res), /Connection lost/);
                assert.ok(db.rollbackReadSnapshot.calledOnce);
            });

            // A catch-up window with zero actions (getFirstActionIndex null)
            // that contains only a legacy-era cooldown maturity mints NO actions row, so the
            // credits action-scoped base query is empty. The matured-cooldown merge keys off
            // the maturity block, not action_index, and must still run so the backdated refund
            // credit ships; otherwise the follower gets the updated_rows status flip but not
            // the credit and its balances silently diverge.
            it('ships matured cooldown refund credits when firstActionIndex is null (quiet window) @regression', async function(){
                let db = createMockDb();
                db.dbType = 'indexer';
                db.getLastBlock.resolves(100);
                db.getBlockHashRow.resolves(null);
                db.getFirstActionIndex.resolves(null); // quiet window: zero actions since cursor
                db.getStatusId = sinon.stub().resolves(3); // 'completed'
                db.doQuery.callsFake(async (sql) => {
                    if(/information_schema/.test(sql)) return [{ table_name: 'credits' }];
                    // Capability maturity refund join → one backdated credit maturing in-window.
                    if(/JOIN unstakes u/.test(sql))
                        return [{ action_index: 42, address_id: 7, tick_id: 1, amount: '100' }];
                    // Contract maturity refund join → none.
                    if(/JOIN contract_unstakes cu/.test(sql)) return [];
                    return [];
                });
                let res = streamRes();
                await run(() => builder.streamIncrementalSnapshot(db, 3, res), res);
                let out = JSON.parse(zlib.gunzipSync(res.getCollectedData()).toString());
                assert.ok('credits' in out.tables, 'credits table present even with null firstActionIndex');
                assert.strictEqual(out.tables.credits.length, 1, 'the backdated refund credit is shipped');
                assert.strictEqual(out.tables.credits[0].action_index, 42);
            });

            it('rolls back and rethrows when the incremental read throws before streaming', async function(){
                let db = createMockDb();
                db.getLastBlock.rejects(new Error('inc read fail'));
                let res = createMockRes();
                await assert.rejects(() => builder.streamIncrementalSnapshot(db, 3, res), { message: 'inc read fail' });
                assert.ok(db.rollbackReadSnapshot.calledOnce);
            });
        });
    });

    // Backpressure + client-abort now also cover the two bounded paging streams
    // (streamTableRowsById / streamDispensers), which hold no read view but could still
    // buffer their output in RAM / write to a dead socket on a slow or vanished reader.
    describe('paging-stream client-abort', function(){
        // Force gzip backpressure by stubbing createGzip with a fake whose write() always
        // returns false, so the first writer.write parks on 'drain'; then disconnect.
        function fakeBackpressuredGzip(){
            let ee = new (require('events').EventEmitter)();
            ee.destroyed = false;
            ee.write   = sinon.stub().returns(false);
            ee.end     = sinon.stub();
            ee.destroy = sinon.stub().callsFake(() => { ee.destroyed = true; });
            ee.pipe    = sinon.stub();
            return ee;
        }

        it('streamTableRowsById: swallows the abort and destroys gzip on client disconnect', async function(){
            let db = createMockDb();
            db.doQuery.resolves([{ id: 1 }, { id: 2 }]);
            let fake = fakeBackpressuredGzip();
            sinon.stub(zlib, 'createGzip').returns(fake);
            let res = new PassThrough(); res.setHeader = sinon.stub(); res.on('data', () => {});

            let p = builder.streamTableRowsById(db, 'index_transactions', 0, 50000, res);
            await new Promise(r => setImmediate(r));   // reach the parked first write
            res.emit('close');                          // client vanished
            await p;                                    // resolves (abort swallowed, not a throw)
            assert.ok(fake.destroy.called, 'gzip destroyed to free buffered output on abort');
        });

        it('streamDispensers: swallows the abort and destroys gzip on client disconnect', async function(){
            let db = createMockDb(); db.dbType = 'decoder';
            db.doQuery.resolves([{ tx_index: 1, address_id: 2 }]);
            let fake = fakeBackpressuredGzip();
            sinon.stub(zlib, 'createGzip').returns(fake);
            let res = new PassThrough(); res.setHeader = sinon.stub(); res.on('data', () => {});

            let p = builder.streamDispensers(db, NaN, NaN, 50000, res);
            await new Promise(r => setImmediate(r));
            res.emit('close');
            await p;
            assert.ok(fake.destroy.called, 'gzip destroyed on abort');
        });
    });

    // Fix #1 (HIGH): the gzip write loop ignored backpressure and had no client-abort
    // handler, so a slow/half-open reader pinned the REPEATABLE READ connection and
    // buffered the whole snapshot in RAM (a pool-exhaustion / OOM path on the
    // unauthenticated snapshot routes). SnapshotStreamWriter applies backpressure and
    // tears the stream down the moment the client disconnects.
    describe('SnapshotStreamWriter (backpressure + client-abort)', function(){
        // Minimal gzip stand-in: an EventEmitter whose write() returns a preset value
        // so we can drive the drain/backpressure paths deterministically.
        function fakeGzip(writeReturns){
            let ee = new EventEmitter();
            ee.destroyed = false;
            ee.write = sinon.stub().returns(writeReturns);
            ee.end = sinon.stub();
            ee.destroy = sinon.stub().callsFake(() => { ee.destroyed = true; });
            return ee;
        }
        const tick = () => new Promise(r => setImmediate(r));

        it('resolves immediately when the buffer has room (write() returns true)', async function(){
            let gzip = fakeGzip(true);
            let w = new SnapshotStreamWriter(gzip, new EventEmitter());
            await w.write('x');
            assert.ok(gzip.write.calledOnceWith('x'));
        });

        it('blocks until drain when the buffer is full (write() returns false)', async function(){
            let gzip = fakeGzip(false);
            let w = new SnapshotStreamWriter(gzip, new EventEmitter());
            let settled = false;
            let p = w.write('x').then(() => { settled = true; });
            await tick();
            assert.strictEqual(settled, false, 'write awaits drain when backpressured');
            gzip.emit('drain');
            await p;
            assert.strictEqual(settled, true, 'resolves once drain fires');
        });

        it('rejects (aborted) and destroys gzip when the client disconnects mid-write', async function(){
            let gzip = fakeGzip(false);
            let res = new EventEmitter();
            let w = new SnapshotStreamWriter(gzip, res);
            let p = w.write('x');            // parks on drain
            res.emit('close');               // client vanished
            await assert.rejects(p, e => e.aborted === true);
            assert.ok(gzip.destroy.called, 'gzip destroyed to free buffered chunks');
            // Every subsequent write also fails fast rather than writing to a dead stream.
            await assert.rejects(w.write('y'), e => e.aborted === true);
        });

        it('finish() detaches the disconnect handler and ends the stream', async function(){
            let gzip = fakeGzip(true);
            let res = new EventEmitter();
            let w = new SnapshotStreamWriter(gzip, res);
            w.finish();
            assert.ok(gzip.end.called, 'stream flushed on normal completion');
            // A late close (normal streams emit close after finish) must NOT re-abort.
            res.emit('close');
            assert.ok(gzip.destroy.notCalled, 'close after finish is ignored');
        });
    });

    describe('streamFullSnapshot client-abort', function(){
        it('releases the read view and stops when the client disconnects mid-stream', async function(){
            let db = createMockDb();
            db.getLastBlock.resolves(50);
            db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
            db.doQuery.resolves([{ table_name: 'blocks' }]);
            db.getTableCount.resolves(1);
            let res = new PassThrough();
            res.setHeader = sinon.stub();
            res.on('data', () => {});
            // Simulate the client vanishing exactly when the table read starts.
            db.streamTableRows.callsFake(() => { res.emit('close'); return Readable.from([{ block_index: 50 }]); });

            // Resolves (does not reject): a client abort is swallowed, not a 500.
            await builder.streamFullSnapshot(db, res);
            assert.ok(db.rollbackReadSnapshot.calledOnce, 'read view rolled back on abort');
            assert.ok(db.commitReadSnapshot.notCalled, 'never commits after an abort');
        });
    });

    // Fix #2 (MED): the incremental full-dump of append-only lookup tables ran an
    // unbounded `SELECT *`, materializing a whole multi-million-row table into the
    // driver array. They are now streamed by id cursor in pageSize batches.
    describe('streamIncrementalSnapshot lookup paging', function(){
        it('pages a full-dump lookup table by id cursor instead of one unbounded SELECT *', async function(){
            let db = createMockDb();
            db.dbType = 'indexer';
            builder.pageSize = 2;              // force multiple pages over a small set
            db.getLastBlock.resolves(100);
            db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
            db.getFirstActionIndex.resolves(null);
            let pageCalls = [];
            db.doQuery.callsFake(async (query, args) => {
                if(query.includes('information_schema')) return [{ table_name: 'index_addresses' }];
                let m = /SELECT \* FROM `index_addresses` WHERE `id` > \? ORDER BY `id` ASC LIMIT \?/.exec(query);
                if(m){
                    pageCalls.push(args);      // [after, limit]
                    let after = args[0];
                    // 3 rows total (ids 1,2,3), served 2 per page by id cursor.
                    let all = [{ id: 1, address: 'a1' }, { id: 2, address: 'a2' }, { id: 3, address: 'a3' }];
                    return all.filter(r => r.id > after).slice(0, args[1]);
                }
                return [];
            });

            let res = new PassThrough();
            let chunks = [];
            res.on('data', c => chunks.push(c));
            res.setHeader = sinon.stub();
            await new Promise((resolve) => { res.on('finish', resolve); builder.streamIncrementalSnapshot(db, 80, res); });

            let parsed = JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
            assert.deepStrictEqual(parsed.tables.index_addresses.map(r => r.id), [1, 2, 3], 'all rows streamed across pages');
            // Never an unbounded SELECT *: every read of the table carried the id-cursor + LIMIT.
            assert.ok(pageCalls.length >= 2, 'read in more than one bounded page');
            assert.ok(pageCalls.every(a => a[1] === 2), 'each page bounded by pageSize');
            assert.deepStrictEqual(pageCalls[0], [0, 2], 'first page starts at cursor 0');
        });
    });

    // The indexer `pubkeys` table is replication:'snapshot', so streamTopology()
    // puts it in no per-block bucket. It used to fall through to the action_index
    // branch, where the missing column raised errno 1054 and was swallowed as a
    // schema gap: pubkeys then rode NO incremental snapshot and froze at bootstrap
    // height on every incrementally-caught-up follower, invisibly (it is excluded
    // from the /status count check and is not consensus-hashed).
    describe('streamIncrementalSnapshot snapshot-replication tables', function(){
        it('full-dumps indexer pubkeys instead of action-scoping it into errno 1054', async function(){
            let db = createMockDb();
            db.dbType = 'indexer';
            db.getLastBlock.resolves(100);
            db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
            // Non-null: the pre-fix code took the action_index branch precisely here.
            db.getFirstActionIndex.resolves(500);
            let queries = [];
            db.doQuery.callsFake(async (query) => {
                if(query.includes('information_schema')) return [{ table_name: 'pubkeys' }];
                queries.push(query);
                if(/SELECT \* FROM `pubkeys`\s*$/.test(query.trim())){
                    return [{ address_id: 7, pubkey: 'pk7' }, { address_id: 9, pubkey: 'pk9' }];
                }
                if(query.includes('`pubkeys`') && query.includes('action_index')){
                    let e = new Error("Unknown column 'action_index' in 'where clause'");
                    e.errno = 1054;
                    throw e;
                }
                return [];
            });

            let res = new PassThrough();
            let chunks = [];
            res.on('data', c => chunks.push(c));
            res.setHeader = sinon.stub();
            await new Promise((resolve) => { res.on('finish', resolve); builder.streamIncrementalSnapshot(db, 80, res); });

            let parsed = JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
            assert.deepStrictEqual(parsed.tables.pubkeys.map(r => r.address_id), [7, 9], 'pubkeys rides the incremental payload');
            assert.ok(!queries.some(q => q.includes('`pubkeys`') && q.includes('action_index')),
                'pubkeys is never action-scoped (that query 1054s and is swallowed)');
        });
    });

    // Per-Database concurrency cap on the long-lived read-snapshot
    // streams, so a bootstrap stampede can never pin every pool connection and
    // starve ServerPoller's live-broadcast reads.
    describe('snapshot concurrency cap', function(){
        let savedEnv;

        beforeEach(function(){
            savedEnv = {
                DB_POOL_SIZE: process.env.DB_POOL_SIZE,
                MAX_CONCURRENT_SNAPSHOTS: process.env.MAX_CONCURRENT_SNAPSHOTS
            };
            delete process.env.DB_POOL_SIZE;
            delete process.env.MAX_CONCURRENT_SNAPSHOTS;
        });

        afterEach(function(){
            for(let k of Object.keys(savedEnv)){
                if(savedEnv[k] === undefined) delete process.env[k];
                else process.env[k] = savedEnv[k];
            }
        });

        it('defaults the cap to poolSize - 2 (reserves poller + one short-read conn)', function(){
            let db = createMockDb();
            db.connectionPoolParams = { connectionLimit: 5 };
            assert.strictEqual(builder._snapshotCap(db), 3);
        });

        // With no connectionPoolParams to read, the cap falls back to the
        // same per-dbType sizing db.js uses (DB_POOL_SIZE_<DBTYPE> > DB_POOL_SIZE >
        // per-dbType default), so a decoder Database never inherits the indexer's cap.
        it('falls back to per-dbType pool sizing, then DB_POOL_SIZE env', function(){
            let poolSizing = require('../../src/poolSizing');
            let indexerDb  = createMockDb();
            let decoderDb  = createMockDb();
            decoderDb.dbType = 'decoder';
            assert.strictEqual(builder._snapshotCap(indexerDb), poolSizing.DEFAULT_POOL_SIZE.indexer - 2);
            assert.strictEqual(builder._snapshotCap(decoderDb), poolSizing.DEFAULT_POOL_SIZE.decoder - 2);
            process.env.DB_POOL_SIZE = '10';
            assert.strictEqual(builder._snapshotCap(createMockDb()), 8);
            process.env.DB_POOL_SIZE_DECODER = '6';
            assert.strictEqual(builder._snapshotCap(decoderDb), 4);
            delete process.env.DB_POOL_SIZE_DECODER;
        });

        it('honours MAX_CONCURRENT_SNAPSHOTS but clamps to [1, poolSize - 1]', function(){
            let db = createMockDb();
            db.connectionPoolParams = { connectionLimit: 5 };
            process.env.MAX_CONCURRENT_SNAPSHOTS = '2';
            assert.strictEqual(builder._snapshotCap(db), 2);
            // Can never hand the poller's last connection to snapshots.
            process.env.MAX_CONCURRENT_SNAPSHOTS = '99';
            assert.strictEqual(builder._snapshotCap(db), 4);
            // Never below 1 (a 0/negative override would deadlock bootstraps).
            process.env.MAX_CONCURRENT_SNAPSHOTS = '0';
            assert.strictEqual(builder._snapshotCap(db), 1);
        });

        it('cap is per Database instance, floored at 1 for tiny pools', function(){
            let db = createMockDb();
            db.connectionPoolParams = { connectionLimit: 2 };
            assert.strictEqual(builder._snapshotCap(db), 1);
        });

        it('rejects a full snapshot with 503 SNAPSHOT_BUSY once the cap is reached, without opening a read view', async function(){
            process.env.MAX_CONCURRENT_SNAPSHOTS = '1';
            let db = createMockDb();

            // First stream: hold beginReadSnapshot open so the slot stays taken.
            let releaseFirst;
            db.beginReadSnapshot = sinon.stub().callsFake(() =>
                new Promise(resolve => { releaseFirst = () => resolve({ _snapshotConn: true }); }));
            let res1 = createMockRes();
            let firstStream = builder.streamFullSnapshot(db, res1);
            await new Promise(setImmediate);

            // Second stream: must be refused up front.
            let res2 = createMockRes();
            await builder.streamFullSnapshot(db, res2);
            assert.ok(res2.status.calledWith(503), 'second request got 503');
            assert.strictEqual(res2.json.firstCall.args[0].code, 'SNAPSHOT_BUSY');
            assert.strictEqual(res2._headers['Retry-After'], '30');
            assert.strictEqual(db.beginReadSnapshot.callCount, 1, 'rejected request never touched the pool');
            assert.strictEqual(builder.snapshotsRejected, 1);

            // Let the first finish (getLastBlock null -> 404 path) and verify the
            // slot is released for the next bootstrap.
            releaseFirst();
            await firstStream;
            assert.strictEqual(builder._inflightSnapshots.size, 0, 'slot released');
            db.beginReadSnapshot = sinon.stub().resolves({ _snapshotConn: true });
            let res3 = createMockRes();
            await builder.streamFullSnapshot(db, res3);
            assert.ok(res3.status.calledWith(404), 'post-release request admitted');
        });

        it('caps incremental snapshots on the same per-Database semaphore', async function(){
            process.env.MAX_CONCURRENT_SNAPSHOTS = '1';
            let db = createMockDb();
            let releaseFirst;
            db.beginReadSnapshot = sinon.stub().callsFake(() =>
                new Promise(resolve => { releaseFirst = () => resolve({ _snapshotConn: true }); }));
            let inflightFull = builder.streamFullSnapshot(db, createMockRes());
            await new Promise(setImmediate);

            let res = createMockRes();
            await builder.streamIncrementalSnapshot(db, 100, res);
            assert.ok(res.status.calledWith(503), 'incremental refused while full stream holds the slot');
            assert.strictEqual(res.json.firstCall.args[0].code, 'SNAPSHOT_BUSY');

            releaseFirst();
            await inflightFull;
        });

        it('tracks slots independently per Database (one chain cannot starve another)', async function(){
            process.env.MAX_CONCURRENT_SNAPSHOTS = '1';
            let dbA = createMockDb('db_a');
            let releaseA;
            dbA.beginReadSnapshot = sinon.stub().callsFake(() =>
                new Promise(resolve => { releaseA = () => resolve({ _snapshotConn: true }); }));
            let inflightA = builder.streamFullSnapshot(dbA, createMockRes());
            await new Promise(setImmediate);

            // A different Database has its own pool, so its slot is free.
            let dbB = createMockDb('db_b');
            let resB = createMockRes();
            await builder.streamFullSnapshot(dbB, resB);
            assert.ok(resB.status.calledWith(404), 'other Database admitted (hit its normal empty-db 404)');

            releaseA();
            await inflightA;
        });

        it('releases the slot when the stream throws', async function(){
            process.env.MAX_CONCURRENT_SNAPSHOTS = '1';
            let db = createMockDb();
            db.beginReadSnapshot = sinon.stub().rejects(new Error('pool acquire failed'));
            await assert.rejects(() => builder.streamFullSnapshot(db, createMockRes()), /pool acquire failed/);
            assert.strictEqual(builder._inflightSnapshots.size, 0, 'slot released on error');
        });
    });
});
