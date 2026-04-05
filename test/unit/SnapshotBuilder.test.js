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
        getTableCount: sinon.stub().resolves(0)
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
    });
});
