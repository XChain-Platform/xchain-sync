const http    = require('http');
const express = require('express');
const cors    = require('cors');
const WebSocket = require('ws');
const ServerPoller     = require('../../../src/ServerPoller');
const BlockBroadcaster = require('../../../src/BlockBroadcaster');
const TransparencyLog  = require('../../../src/TransparencyLog');
const SnapshotBuilder  = require('../../../src/SnapshotBuilder');
const testDb           = require('./testDb');

class ServerProcess {

    constructor(sourceDb, port, chain, network) {
        this.sourceDb = sourceDb;
        this.port     = port;
        this.chain    = chain || 'bitcoin';
        this.network  = network || 'mainnet';

        this.config = {
            SYNC_MODE: 'server',
            WS_MAX_PER_IP: 20,
            WS_BACKPRESSURE_LIMIT: 50,
            BLOCK_POLL_INTERVAL: 200,
            WS_STATUS_INTERVAL: 500,
            WS_PING_INTERVAL: 30000,
            SNAPSHOT_RATE_FULL: 100,
            SNAPSHOT_RATE_INCR: 100
        };

        this.server      = null;
        this.wss         = null;
        this.broadcaster = null;
        this.poller      = null;
        this.log         = null;
        this.snapshotBuilder = null;
        this.pollInterval = null;
        this.statusInterval = null;
    }

    async start() {
        this.broadcaster     = new BlockBroadcaster(this.config);
        this.log             = new TransparencyLog(this.sourceDb);
        this.snapshotBuilder = new SnapshotBuilder(testDb.util);
        this.poller          = new ServerPoller(
            this.chain, this.network, this.sourceDb,
            this.broadcaster, this.log, this.config, testDb.util
        );

        let app = express();
        app.use(cors({ origin: '*', methods: ['GET'] }));

        // Status endpoints
        app.get('/status', async (req, res) => {
            try {
                let lastBlock = await this.sourceDb.getLastBlock();
                let hashRow = lastBlock !== null ? await this.sourceDb.getBlockHashRow(lastBlock) : null;
                let result = {};
                result[this.chain] = {};
                result[this.chain][this.network] = {
                    block_height: hashRow ? Number(hashRow.block_index) : null,
                    block_time: hashRow ? Number(hashRow.block_time) : null,
                    ledger_hash: hashRow ? hashRow.ledger_hash : null,
                    actions_hash: hashRow ? hashRow.actions_hash : null,
                    contract_hash: hashRow ? hashRow.contract_hash : null
                };
                result.last_updated = new Date().toISOString();
                res.json(result);
            } catch (e) {
                res.status(500).json({ error: e.message });
            }
        });

        app.get('/status/:chain/:network', async (req, res) => {
            let { chain, network } = req.params;
            if (chain !== this.chain || network !== this.network)
                return res.status(404).json({ error: 'Chain/network not found' });

            try {
                let lastBlock = await this.sourceDb.getLastBlock();
                let hashRow = lastBlock !== null ? await this.sourceDb.getBlockHashRow(lastBlock) : null;
                res.json({
                    chain, network,
                    block_height: hashRow ? Number(hashRow.block_index) : null,
                    block_time: hashRow ? Number(hashRow.block_time) : null,
                    ledger_hash: hashRow ? hashRow.ledger_hash : null,
                    actions_hash: hashRow ? hashRow.actions_hash : null,
                    contract_hash: hashRow ? hashRow.contract_hash : null,
                    last_updated: new Date().toISOString()
                });
            } catch (e) {
                res.status(500).json({ error: e.message });
            }
        });

        app.get('/schema/:chain/:network', async (req, res) => {
            try {
                let tables = await this.sourceDb.doQuery(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name",
                    [this.sourceDb.dbName]
                );
                let schema = {};
                for (let row of tables) {
                    let tn = row.table_name || row.TABLE_NAME;
                    let ddl = await this.sourceDb.doQuery("SHOW CREATE TABLE `" + tn + "`");
                    if (ddl.length > 0) schema[tn] = ddl[0]['Create Table'];
                }
                res.json({ chain: this.chain, network: this.network, tables: schema });
            } catch (e) {
                res.status(500).json({ error: e.message });
            }
        });

        app.get('/snapshot/:chain/:network', async (req, res) => {
            try {
                await this.snapshotBuilder.streamFullSnapshot(this.sourceDb, res);
            } catch (e) {
                if (!res.headersSent) res.status(500).json({ error: e.message });
            }
        });

        app.get('/snapshot/:chain/:network/since/:blockHeight', async (req, res) => {
            let sinceBlock = parseInt(req.params.blockHeight);
            if (isNaN(sinceBlock) || sinceBlock < 0)
                return res.status(400).json({ error: 'Invalid blockHeight' });
            try {
                await this.snapshotBuilder.streamIncrementalSnapshot(this.sourceDb, sinceBlock, res);
            } catch (e) {
                if (!res.headersSent) res.status(500).json({ error: e.message });
            }
        });

        app.get('/transparency/:chain/:network/roots', async (req, res) => {
            let page  = parseInt(req.query.page) || 0;
            let limit = parseInt(req.query.limit) || 100;
            try {
                let result = await this.log.getPage(page, limit);
                res.json(result);
            } catch (e) {
                res.status(500).json({ error: e.message });
            }
        });

        this.server = http.createServer(app);
        this.wss = new WebSocket.Server({ noServer: true });

        this.server.on('upgrade', (request, socket, head) => {
            let match = request.url.match(/^\/subscribe\/([^\/]+)\/([^\/\?]+)/);
            if (!match) { socket.destroy(); return; }
            this.wss.handleUpgrade(request, socket, head, (ws) => {
                this.broadcaster.addSubscription(ws, request, match[1], match[2]);
            });
        });

        await new Promise(resolve => this.server.listen(this.port, resolve));

        // Initialize poller state
        this.poller.lastPolledBlock = await this.sourceDb.getLastBlock();
        await this.poller._updateStatus();

        // Start polling loop
        this.pollInterval = setInterval(async () => {
            try { await this.poller._poll(); } catch (e) {}
        }, this.config.BLOCK_POLL_INTERVAL);

        // Start status broadcast loop
        this.statusInterval = setInterval(() => {
            this.broadcaster.broadcastStatus(this.chain, this.network);
        }, this.config.WS_STATUS_INTERVAL);
    }

    async stop() {
        if (this.pollInterval)   clearInterval(this.pollInterval);
        if (this.statusInterval) clearInterval(this.statusInterval);
        if (this.poller)         this.poller.stop();

        // Close all WebSocket connections
        if (this.wss) {
            for (let client of this.wss.clients) {
                try { client.close(); } catch (e) {}
            }
        }

        if (this.server) {
            await new Promise(resolve => this.server.close(resolve));
        }
    }

    // Force a poll cycle (useful for tests that need immediate response)
    async poll() {
        await this.poller._poll();
    }

    getUrl() {
        return 'http://127.0.0.1:' + this.port;
    }

    getWsUrl() {
        return 'ws://127.0.0.1:' + this.port;
    }
}

module.exports = ServerProcess;
