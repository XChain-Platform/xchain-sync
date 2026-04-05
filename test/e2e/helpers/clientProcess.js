const ClientSync     = require('../../../src/ClientSync');
const ClientApplier  = require('../../../src/ClientApplier');
const ClientRollback = require('../../../src/ClientRollback');
const HashVerifier   = require('../../../src/HashVerifier');
const testDb         = require('./testDb');

class ClientProcess {

    constructor(replicaDb, serverUrl, chain, network, opts = {}) {
        this.replicaDb = replicaDb;
        this.serverUrl = serverUrl;
        this.chain     = chain || 'bitcoin';
        this.network   = network || 'mainnet';

        this.config = {
            SYNC_SOURCES: opts.syncSources || serverUrl,
            VERIFY_HASHES: opts.verifyHashes !== undefined ? opts.verifyHashes : false,
            CLIENT_RECONNECT_DELAY: opts.reconnectDelay || 500,
            HASH_CONFIRM_TIMEOUT: opts.hashConfirmTimeout || 2000
        };

        this.applier    = new ClientApplier(replicaDb, testDb.util);
        this.rollbacker = new ClientRollback(replicaDb, testDb.util);
        this.verifier   = new HashVerifier();
        this.sync       = new ClientSync(
            this.chain, this.network, replicaDb,
            this.applier, this.rollbacker, this.verifier,
            this.config, testDb.util
        );
    }

    // Bootstrap from server snapshot (blocking)
    async bootstrap() {
        await this.sync._bootstrapFromSnapshot();
        this.sync.lastAppliedBlock = await this.replicaDb.getLastBlock();
        if (this.sync.lastAppliedBlock !== null) {
            this.sync.lastHashes = await this.replicaDb.getBlockHashRow(this.sync.lastAppliedBlock);
        }
    }

    // Start live WebSocket sync (non-blocking)
    connectLive() {
        this.sync._connectWebSockets();
    }

    // Bootstrap and start live sync
    async start() {
        await this.bootstrap();
        this.connectLive();
    }

    // Stop all WebSocket connections
    stop() {
        this.sync.stop();
    }

    // Get the last applied block index
    getLastAppliedBlock() {
        return this.sync.lastAppliedBlock;
    }

    // Trigger incremental catch-up manually
    async incrementalCatchUp(sinceBlock) {
        await this.sync._incrementalCatchUp(sinceBlock);
        this.sync.lastAppliedBlock = await this.replicaDb.getLastBlock();
        if (this.sync.lastAppliedBlock !== null) {
            this.sync.lastHashes = await this.replicaDb.getBlockHashRow(this.sync.lastAppliedBlock);
        }
    }
}

module.exports = ClientProcess;
