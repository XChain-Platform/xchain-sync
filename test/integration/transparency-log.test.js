const assert = require('assert');
const setup  = require('./helpers/setup');
const testDb = require('./helpers/testDb');
const TransparencyLog = require('../../src/TransparencyLog');

describe('Integration: TransparencyLog', function() {

    let sourceDb, log;

    before(async function() {
        await setup.globalSetup();
        sourceDb = setup.getSourceDb();
        log = new TransparencyLog(sourceDb);
    });

    after(async function() {
        await setup.globalTeardown();
    });

    beforeEach(async function() {
        await testDb.truncateAll(sourceDb);
    });

    describe('recordBlock', function() {
        it('inserts a row into sync_meta', async function() {
            await log.recordBlock(1, 1700000001, 'ledger1', 'actions1', 'contract1');
            let count = await testDb.getRowCount(sourceDb, 'sync_meta');
            assert.strictEqual(count, 1);

            let rows = await sourceDb.doQuery("SELECT * FROM sync_meta WHERE block_index = 1");
            assert.strictEqual(rows.length, 1);
            assert.strictEqual(Number(rows[0].block_index), 1);
            assert.strictEqual(Number(rows[0].block_time), 1700000001);
            assert.strictEqual(rows[0].ledger_hash, 'ledger1');
            assert.strictEqual(rows[0].actions_hash, 'actions1');
            assert.strictEqual(rows[0].contract_hash, 'contract1');
        });

        it('records multiple blocks', async function() {
            for (let i = 1; i <= 10; i++) {
                await log.recordBlock(i, 1700000000 + i, 'lh' + i, 'ah' + i, 'ch' + i);
            }
            let count = await testDb.getRowCount(sourceDb, 'sync_meta');
            assert.strictEqual(count, 10);
        });

        it('ignores duplicate block_index (INSERT IGNORE)', async function() {
            await log.recordBlock(5, 100, 'a', 'b', 'c');
            await log.recordBlock(5, 200, 'x', 'y', 'z');

            let count = await testDb.getRowCount(sourceDb, 'sync_meta');
            assert.strictEqual(count, 1);

            // Original values preserved
            let rows = await sourceDb.doQuery("SELECT * FROM sync_meta WHERE block_index = 5");
            assert.strictEqual(rows[0].ledger_hash, 'a');
        });
    });

    describe('getPage', function() {
        beforeEach(async function() {
            for (let i = 1; i <= 50; i++) {
                await log.recordBlock(i, 1700000000 + i, 'lh' + i, 'ah' + i, 'ch' + i);
            }
        });

        it('returns paginated results with correct total', async function() {
            let result = await log.getPage(0, 10);
            assert.strictEqual(result.page, 0);
            assert.strictEqual(result.limit, 10);
            assert.strictEqual(result.total, 50);
            assert.strictEqual(result.results.length, 10);
        });

        it('returns results ordered by block_index DESC', async function() {
            let result = await log.getPage(0, 5);
            assert.strictEqual(Number(result.results[0].block_index), 50);
            assert.strictEqual(Number(result.results[4].block_index), 46);
        });

        it('paginates correctly with offset', async function() {
            let result = await log.getPage(2, 10);
            // Page 2, limit 10 → offset 20 → blocks 30 down to 21
            assert.strictEqual(Number(result.results[0].block_index), 30);
            assert.strictEqual(result.results.length, 10);
        });

        it('returns empty results for page beyond data', async function() {
            let result = await log.getPage(10, 10);
            assert.strictEqual(result.results.length, 0);
            assert.strictEqual(result.total, 50);
        });

        it('includes logged_at timestamp', async function() {
            let result = await log.getPage(0, 1);
            assert.ok(result.results[0].logged_at);
        });
    });

    describe('rollback cleanup', function() {
        it('preserves entries before rollback point after manual delete', async function() {
            for (let i = 1; i <= 10; i++) {
                await log.recordBlock(i, 1700000000 + i, 'lh' + i, 'ah' + i, 'ch' + i);
            }

            // Simulate rollback cleanup (as ClientRollback does)
            await sourceDb.doQuery("DELETE FROM sync_meta WHERE block_index >= ?", [8]);

            let count = await testDb.getRowCount(sourceDb, 'sync_meta');
            assert.strictEqual(count, 7);

            // Entries 1-7 still exist
            let result = await log.getPage(0, 100);
            assert.strictEqual(result.total, 7);
            assert.strictEqual(Number(result.results[0].block_index), 7);
        });
    });
});
