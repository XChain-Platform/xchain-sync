const assert        = require('assert');
const sinon         = require('sinon');
const setup         = require('./helpers/setup');
const testDb        = require('./helpers/testDb');
const fixtures      = require('./helpers/fixtures');
const ServerProcess = require('./helpers/serverProcess');
const ClientProcess = require('./helpers/clientProcess');
const { waitFor, waitForReplicaBlock } = require('./helpers/waitFor');
const { assertBlockExists } = require('./helpers/assertions');

const SERVER_PORT_1 = 29500;
const SERVER_PORT_2 = 29501;

describe('E2E: Cross-Source Hash Verification', function() {

    let sourceDb, replicaDb, server1, server2, client;

    before(async function() {
        await setup.globalSetup();
        sourceDb  = setup.getSourceDb();
        replicaDb = setup.getReplicaDb();

        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    after(async function() {
        sinon.restore();
        if (client)  client.stop();
        if (server1) await server1.stop();
        if (server2) await server2.stop();
        await setup.globalTeardown();
    });

    beforeEach(async function() {
        if (client)  { client.stop(); client = null; }
        if (server1) { await server1.stop(); server1 = null; }
        if (server2) { await server2.stop(); server2 = null; }
        await setup.resetDatabases();
    });

    describe('5.1 Two matching sources — normal operation', function() {
        it('syncs when both sources agree on hashes', async function() {
            this.timeout(30000);

            await fixtures.seedBlocks(sourceDb, 1, 10);

            // Both servers read from the same source DB
            server1 = new ServerProcess(sourceDb, SERVER_PORT_1);
            await server1.start();

            server2 = new ServerProcess(sourceDb, SERVER_PORT_2);
            await server2.start();

            // Client configured with both sources and hash verification enabled
            client = new ClientProcess(replicaDb, server1.getUrl(), 'bitcoin', 'mainnet', {
                syncSources: server1.getUrl() + ',' + server2.getUrl(),
                verifyHashes: true,
                hashConfirmTimeout: 2000
            });
            await client.start();
            await waitForReplicaBlock(replicaDb, 10, 20000);

            assert.strictEqual(await replicaDb.getLastBlock(), 10);

            // Add more blocks and verify cross-source sync continues
            await fixtures.seedBlocks(sourceDb, 11, 15);
            await waitForReplicaBlock(replicaDb, 15, 20000);

            assert.strictEqual(await replicaDb.getLastBlock(), 15);
            await assertBlockExists(replicaDb, 15);
        });
    });

    describe('5.3 Secondary source unavailable — timeout fallback', function() {
        it('applies from primary after timeout when secondary is unavailable', async function() {
            this.timeout(30000);

            await fixtures.seedBlocks(sourceDb, 1, 5);

            // Only start one server
            server1 = new ServerProcess(sourceDb, SERVER_PORT_1);
            await server1.start();

            // Client configured with two sources, but second doesn't exist
            client = new ClientProcess(replicaDb, server1.getUrl(), 'bitcoin', 'mainnet', {
                syncSources: server1.getUrl() + ',http://127.0.0.1:' + SERVER_PORT_2,
                verifyHashes: true,
                hashConfirmTimeout: 2000
            });
            await client.start();

            // Should eventually sync after timeouts
            await waitForReplicaBlock(replicaDb, 5, 20000);
            assert.strictEqual(await replicaDb.getLastBlock(), 5);
        });
    });

    describe('5.4 Verification disabled — immediate apply', function() {
        it('syncs without waiting for second source when verification is off', async function() {
            this.timeout(20000);

            await fixtures.seedBlocks(sourceDb, 1, 10);

            server1 = new ServerProcess(sourceDb, SERVER_PORT_1);
            await server1.start();

            // Two sources but verification disabled
            client = new ClientProcess(replicaDb, server1.getUrl(), 'bitcoin', 'mainnet', {
                syncSources: server1.getUrl() + ',http://127.0.0.1:' + SERVER_PORT_2,
                verifyHashes: false
            });
            await client.start();

            await waitForReplicaBlock(replicaDb, 10, 15000);
            assert.strictEqual(await replicaDb.getLastBlock(), 10);
        });
    });
});
