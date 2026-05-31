const assert = require('assert');
const sinon  = require('sinon');
const TransparencyLog = require('../../src/TransparencyLog');

describe('TransparencyLog', function(){

    let log, db;

    beforeEach(function(){
        db = { doQuery: sinon.stub().resolves([]) };
        log = new TransparencyLog(db);
    });

    describe('recordBlock', function(){
        it('calls doQuery with INSERT IGNORE and correct params', async function(){
            // Use a non-epoch-boundary block (epochSize=100) so recordBlock makes exactly
            // one doQuery; block 100 would also trigger a commitEpoch insert.
            await log.recordBlock(101, 1700000000, 'lhash', 'ahash', 'chash');
            assert.strictEqual(db.doQuery.calledOnce, true);
            let query = db.doQuery.firstCall.args[0];
            assert.ok(query.includes('INSERT IGNORE'));
            assert.ok(query.includes('sync_meta'));
            let args = db.doQuery.firstCall.args[1];
            assert.deepStrictEqual(args, [101, 1700000000, 'lhash', 'ahash', 'chash']);
        });
    });

    describe('getPage', function(){
        it('returns paginated results', async function(){
            let rows = [{ block_index: 100, block_time: 123, ledger_hash: 'a', actions_hash: 'b', contract_hash: 'c', logged_at: '2025-01-01' }];
            db.doQuery.onFirstCall().resolves([{ total: 50n }]);
            db.doQuery.onSecondCall().resolves(rows);

            let result = await log.getPage(0, 10);
            assert.strictEqual(result.page, 0);
            assert.strictEqual(result.limit, 10);
            assert.strictEqual(result.total, 50);
            assert.deepStrictEqual(result.results, rows);
        });

        it('clamps negative page to 0', async function(){
            db.doQuery.onFirstCall().resolves([{ total: 0n }]);
            db.doQuery.onSecondCall().resolves([]);
            let result = await log.getPage(-5, 10);
            assert.strictEqual(result.page, 0);
        });

        it('falls back to default 100 for limit of 0 (parseInt(0) || 100)', async function(){
            db.doQuery.onFirstCall().resolves([{ total: 0n }]);
            db.doQuery.onSecondCall().resolves([]);
            let result = await log.getPage(0, 0);
            assert.strictEqual(result.limit, 100);
        });

        it('clamps limit below 1 to 1 for negative values', async function(){
            db.doQuery.onFirstCall().resolves([{ total: 0n }]);
            db.doQuery.onSecondCall().resolves([]);
            let result = await log.getPage(0, -5);
            assert.strictEqual(result.limit, 1);
        });

        it('clamps limit above 1000 to 1000', async function(){
            db.doQuery.onFirstCall().resolves([{ total: 0n }]);
            db.doQuery.onSecondCall().resolves([]);
            let result = await log.getPage(0, 5000);
            assert.strictEqual(result.limit, 1000);
        });

        it('calculates correct offset', async function(){
            db.doQuery.onFirstCall().resolves([{ total: 100n }]);
            db.doQuery.onSecondCall().resolves([]);
            await log.getPage(3, 25);
            let args = db.doQuery.secondCall.args[1];
            assert.strictEqual(args[1], 75); // page 3 * limit 25
        });

        it('handles non-numeric page gracefully', async function(){
            db.doQuery.onFirstCall().resolves([{ total: 0n }]);
            db.doQuery.onSecondCall().resolves([]);
            let result = await log.getPage('abc', 10);
            assert.strictEqual(result.page, 0);
        });
    });
});
