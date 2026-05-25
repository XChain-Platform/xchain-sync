const assert        = require('assert');
const sinon         = require('sinon');
const axios         = require('axios');
const setup         = require('./helpers/setup');
const testDb        = require('./helpers/testDb');
const fixtures      = require('./helpers/fixtures');
const ServerProcess = require('./helpers/serverProcess');
const ClientProcess = require('./helpers/clientProcess');
const { waitFor, waitForReplicaBlock } = require('./helpers/waitFor');

const SERVER_PORT = 29800;

describe('E2E: Transparency Log', function() {

    let sourceDb, replicaDb, server, client;

    before(async function() {
        await setup.globalSetup();
        sourceDb  = setup.getSourceDb();
        replicaDb = setup.getReplicaDb();

        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    after(async function() {
        sinon.restore();
        if (client) client.stop();
        if (server) await server.stop();
        await setup.globalTeardown();
    });

    beforeEach(async function() {
        if (client) { client.stop(); client = null; }
        if (server) { await server.stop(); server = null; }
        await setup.resetDatabases();
    });

    describe('8.1 Transparency log populated during sync', function() {
        it('records hashes for all synced blocks', async function() {
            this.timeout(30000);

            await fixtures.seedBlocks(sourceDb, 1, 20);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();
            // The poller's default init skips history (production semantic);
            // for transparency we need recordBlock to fire for the pre-seeded
            // blocks, so reset the cursor and force a poll cycle.
            server.poller.lastPolledBlock = 0;
            await server.poll();

            // Wait on the transparency endpoint itself — /status reads sourceDb
            // directly, so it reports block_height=20 the instant the seed is
            // done, before the poller has had a chance to call recordBlock.
            await waitFor(async () => {
                let res = await axios.get(
                    server.getUrl() + '/transparency/indexer/bitcoin/mainnet/roots?page=0&limit=1',
                    { timeout: 3000 }
                );
                return res.data.total >= 20;
            }, 15000);

            // Query transparency log
            let res = await axios.get(
                server.getUrl() + '/transparency/indexer/bitcoin/mainnet/roots?page=0&limit=100',
                { timeout: 5000 }
            );

            assert.strictEqual(res.data.total, 20);
            assert.strictEqual(res.data.results.length, 20);

            // Results are ordered DESC — first result is block 20
            assert.strictEqual(Number(res.data.results[0].block_index), 20);
            assert.strictEqual(Number(res.data.results[19].block_index), 1);

            // Each entry should have all three hashes
            for (let entry of res.data.results) {
                assert.ok(entry.ledger_hash, 'Entry should have ledger_hash');
                assert.ok(entry.actions_hash, 'Entry should have actions_hash');
                assert.ok(entry.contract_hash, 'Entry should have contract_hash');
                assert.ok(entry.logged_at, 'Entry should have logged_at');
            }

            // Verify hashes match source DB
            let block10Entry = res.data.results.find(e => Number(e.block_index) === 10);
            let sourceHash = await sourceDb.getBlockHashRow(10);
            assert.strictEqual(block10Entry.ledger_hash, sourceHash.ledger_hash);
            assert.strictEqual(block10Entry.actions_hash, sourceHash.actions_hash);
            assert.strictEqual(block10Entry.contract_hash, sourceHash.contract_hash);
        });
    });

    describe('8.2 Transparency log after reorg', function() {
        it('removes reorged entries and adds new ones', async function() {
            this.timeout(30000);

            await fixtures.seedBlocks(sourceDb, 1, 20);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();
            // The poller's default init skips history (production semantic);
            // for transparency we need recordBlock to fire for the pre-seeded
            // blocks, so reset the cursor and force a poll cycle.
            server.poller.lastPolledBlock = 0;
            await server.poll();

            // Wait for all blocks to be logged
            await waitFor(async () => {
                let res = await axios.get(
                    server.getUrl() + '/transparency/indexer/bitcoin/mainnet/roots?page=0&limit=1',
                    { timeout: 3000 }
                );
                return res.data.total >= 20;
            }, 15000);

            // Simulate reorg at block 18
            await fixtures.deleteBlocksFrom(sourceDb, 18);
            await server.poll();

            // Add new blocks 18-22
            await fixtures.seedBlocks(sourceDb, 18, 22, { creditAmount: '7777' });

            // Poll to process new blocks
            for (let i = 0; i < 10; i++) {
                await server.poll();
                await new Promise(r => setTimeout(r, 200));
            }

            await waitFor(async () => {
                let res = await axios.get(
                    server.getUrl() + '/transparency/indexer/bitcoin/mainnet/roots?page=0&limit=1',
                    { timeout: 3000 }
                );
                return res.data.total >= 22;
            }, 10000);

            let res = await axios.get(
                server.getUrl() + '/transparency/indexer/bitcoin/mainnet/roots?page=0&limit=100',
                { timeout: 5000 }
            );

            // Should have 22 entries (17 original + 5 new)
            assert.strictEqual(res.data.total, 22);

            // Entries 1-17 should still exist
            let block5Entry = res.data.results.find(e => Number(e.block_index) === 5);
            assert.ok(block5Entry, 'Block 5 entry should still exist');
        });
    });

    describe('8.3 Transparency log pagination', function() {
        it('paginates correctly with page and limit params', async function() {
            this.timeout(30000);

            await fixtures.seedBlocks(sourceDb, 1, 50);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();
            // The poller's default init skips history (production semantic);
            // for transparency we need recordBlock to fire for the pre-seeded
            // blocks, so reset the cursor and force a poll cycle.
            server.poller.lastPolledBlock = 0;
            await server.poll();

            // Wait for all blocks to be logged
            await waitFor(async () => {
                let res = await axios.get(
                    server.getUrl() + '/transparency/indexer/bitcoin/mainnet/roots?page=0&limit=1',
                    { timeout: 3000 }
                );
                return res.data.total >= 50;
            }, 20000);

            // First page
            let page0 = await axios.get(
                server.getUrl() + '/transparency/indexer/bitcoin/mainnet/roots?page=0&limit=10',
                { timeout: 5000 }
            );
            assert.strictEqual(page0.data.results.length, 10);
            assert.strictEqual(page0.data.total, 50);
            assert.strictEqual(Number(page0.data.results[0].block_index), 50); // DESC order

            // Last page
            let page4 = await axios.get(
                server.getUrl() + '/transparency/indexer/bitcoin/mainnet/roots?page=4&limit=10',
                { timeout: 5000 }
            );
            assert.strictEqual(page4.data.results.length, 10);
            assert.strictEqual(Number(page4.data.results[9].block_index), 1); // Last entry

            // Middle page
            let page2 = await axios.get(
                server.getUrl() + '/transparency/indexer/bitcoin/mainnet/roots?page=2&limit=10',
                { timeout: 5000 }
            );
            assert.strictEqual(page2.data.results.length, 10);
            assert.strictEqual(Number(page2.data.results[0].block_index), 30);
        });
    });
});
