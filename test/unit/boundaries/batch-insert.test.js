const assert = require('assert');
const sinon  = require('sinon');
const ClientApplier = require('../../../src/ClientApplier');
const Utility = require('../../../src/utility');

function createMockDb(){
    return {
        doQuery: sinon.stub().resolves([]),
        getBlockHashRow: sinon.stub().resolves(null),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves(),
        truncateTable: sinon.stub().resolves()
    };
}

function makeRows(count){
    let rows = [];
    for(let i = 0; i < count; i++) rows.push({ id: i, value: 'v' + i });
    return rows;
}

describe('Boundary: INSERT Batch Size (100 rows)', function(){

    let applier, db;

    beforeEach(function(){
        db = createMockDb();
        applier = new ClientApplier(db, new Utility());
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){ sinon.restore(); });

    it('0 rows — no INSERT executed', async function(){
        await applier._insertRows('blocks', []);
        assert.strictEqual(db.doQuery.called, false);
    });

    it('null rows — no INSERT executed', async function(){
        await applier._insertRows('blocks', null);
        assert.strictEqual(db.doQuery.called, false);
    });

    it('1 row — single INSERT with 1 value clause', async function(){
        await applier._insertRows('blocks', makeRows(1));
        assert.strictEqual(db.doQuery.callCount, 1);
        assert.strictEqual(db.doQuery.firstCall.args[1].length, 2); // 2 columns * 1 row
    });

    it('99 rows — single INSERT', async function(){
        await applier._insertRows('blocks', makeRows(99));
        assert.strictEqual(db.doQuery.callCount, 1);
        assert.strictEqual(db.doQuery.firstCall.args[1].length, 198); // 2 cols * 99 rows
    });

    it('100 rows — single INSERT (at boundary)', async function(){
        await applier._insertRows('blocks', makeRows(100));
        assert.strictEqual(db.doQuery.callCount, 1);
        assert.strictEqual(db.doQuery.firstCall.args[1].length, 200); // 2 cols * 100 rows
    });

    it('101 rows — two INSERTs: 100 + 1', async function(){
        await applier._insertRows('blocks', makeRows(101));
        assert.strictEqual(db.doQuery.callCount, 2);
        assert.strictEqual(db.doQuery.firstCall.args[1].length, 200);  // 100 rows
        assert.strictEqual(db.doQuery.secondCall.args[1].length, 2);   // 1 row
    });

    it('200 rows — two INSERTs of 100 each', async function(){
        await applier._insertRows('blocks', makeRows(200));
        assert.strictEqual(db.doQuery.callCount, 2);
        assert.strictEqual(db.doQuery.firstCall.args[1].length, 200);
        assert.strictEqual(db.doQuery.secondCall.args[1].length, 200);
    });

    it('250 rows — three INSERTs: 100 + 100 + 50', async function(){
        await applier._insertRows('blocks', makeRows(250));
        assert.strictEqual(db.doQuery.callCount, 3);
        assert.strictEqual(db.doQuery.getCall(2).args[1].length, 100); // 50 rows * 2 cols
    });

    it('uses INSERT IGNORE for index tables at any batch size', async function(){
        await applier._insertRows('index_addresses', makeRows(150));
        assert.strictEqual(db.doQuery.callCount, 2);
        assert.ok(db.doQuery.firstCall.args[0].startsWith('INSERT IGNORE'));
        assert.ok(db.doQuery.secondCall.args[0].startsWith('INSERT IGNORE'));
    });

    it('uses INSERT for non-index tables at any batch size', async function(){
        await applier._insertRows('credits', makeRows(150));
        assert.strictEqual(db.doQuery.callCount, 2);
        assert.ok(db.doQuery.firstCall.args[0].startsWith('INSERT INTO'));
        assert.ok(!db.doQuery.firstCall.args[0].includes('IGNORE'));
    });
});
