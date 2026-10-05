const assert = require('assert');
const sinon = require('sinon');
const indexLookups = require('../../../src/db/index_lookups.js');

describe('db/index_lookups', function(){

    it('finds index transactions by id on the supplied connection', async function(){
        const rows = [{ id: 1 }, { id: 2 }, { id: 3 }];
        const conn = {};
        const context = { doQuery: sinon.stub().resolves(rows) };
        const ids = [1, 2, 3];

        const result = await indexLookups.findIndexTransactionsByIds.call(context, ids, conn);

        assert.strictEqual(result, rows);
        assert.deepStrictEqual(context.doQuery.firstCall.args, [
            'SELECT * FROM index_transactions WHERE id IN (?,?,?)', ids, conn
        ]);
        assert.strictEqual(context.doQuery.callCount, 1);
    });

    it('finds index addresses by id on the supplied connection', async function(){
        const rows = [{ id: 1 }, { id: 2 }, { id: 3 }];
        const conn = {};
        const context = { doQuery: sinon.stub().resolves(rows) };
        const ids = [1, 2, 3];

        const result = await indexLookups.findIndexAddressesByIds.call(context, ids, conn);

        assert.strictEqual(result, rows);
        assert.deepStrictEqual(context.doQuery.firstCall.args, [
            'SELECT * FROM index_addresses WHERE id IN (?,?,?)', ids, conn
        ]);
        assert.strictEqual(context.doQuery.callCount, 1);
    });

    it('finds public keys for one address id on the supplied connection', async function(){
        const rows = [{ address_id: 6, pubkey: 'key' }];
        const conn = {};
        const context = { doQuery: sinon.stub().resolves(rows) };
        const ids = [6];

        const result = await indexLookups.findPubkeysByAddressIds.call(context, ids, conn);

        assert.strictEqual(result, rows);
        assert.deepStrictEqual(context.doQuery.firstCall.args, [
            'SELECT * FROM pubkeys WHERE address_id IN (?)', ids, conn
        ]);
        assert.strictEqual(context.doQuery.callCount, 1);
    });
});

describe('db/index_lookups text and failure paths', function(){

    it('finds address text with one placeholder and no connection', async function(){
        const rows = [{ id: 7, address: 'address' }];
        const context = { doQuery: sinon.stub().resolves(rows) };
        const ids = [7];

        const result = await indexLookups.findIndexAddressTextByIds.call(context, ids);

        assert.strictEqual(result, rows);
        assert.strictEqual(context.doQuery.callCount, 1);
        assert.strictEqual(context.doQuery.firstCall.args[0],
            'SELECT id, address FROM index_addresses WHERE id IN (?)');
        assert.strictEqual(context.doQuery.firstCall.args[1], ids);
        assert.strictEqual(context.doQuery.firstCall.args[2], undefined);
    });

    it('finds tick text with one placeholder per id and no connection', async function(){
        const rows = [{ id: 8, tick: 'ONE' }, { id: 9, tick: 'TWO' }];
        const context = { doQuery: sinon.stub().resolves(rows) };
        const ids = [8, 9];

        const result = await indexLookups.findIndexTickTextByIds.call(context, ids);

        assert.strictEqual(result, rows);
        assert.strictEqual(context.doQuery.callCount, 1);
        assert.strictEqual(context.doQuery.firstCall.args[0],
            'SELECT id, tick FROM index_tickers WHERE id IN (?,?)');
        assert.strictEqual(context.doQuery.firstCall.args[1], ids);
        assert.strictEqual(context.doQuery.firstCall.args[2], undefined);
    });

    it('propagates a query rejection unchanged', async function(){
        const rejection = new Error('query failed');
        const context = { doQuery: sinon.stub().rejects(rejection) };

        await assert.rejects(
            indexLookups.findIndexTransactionsByIds.call(context, [1], {}),
            (error) => error === rejection
        );
    });
});
