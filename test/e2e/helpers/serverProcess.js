// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const http    = require('http');
const express = require('express');
const cors    = require('cors');
const { parseCorsOrigin } = require('../../../src/corsOrigin');
const WebSocket = require('ws');
const ServerPoller     = require('../../../src/ServerPoller');
const BlockBroadcaster = require('../../../src/BlockBroadcaster');
const TransparencyLog  = require('../../../src/TransparencyLog');
const SnapshotBuilder  = require('../../../src/SnapshotBuilder');
const testDb           = require('./testDb');
// Trust-proxy and rate-limiter wiring is imported from the real api.js rather
// than re-declared here. api.js now guards its startup env check and listen()
// behind require.main === module (see module.exports at the bottom), so
// requiring it for these two seams no longer opens a port or starts polling.
const { trustProxyHops, createRateLimiters } = require('../../../src/api');

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
            SNAPSHOT_RATE_INCR: 100,
            // Matches the production default (no reverse proxy trusted); a test
            // that needs to exercise the TRUST_PROXY=true path can flip this on
            // server.config before calling start().
            TRUST_PROXY: false,
            // Production defaults to 10/min (see createRateLimiters in api.js).
            // waitFor() (helpers/waitFor.js) polls /transparency/.../roots every
            // 100ms, so the production limit would 429 a single test's own
            // wait-loop within ~1s. The LIMITER WIRING (which route it guards,
            // the trust-proxy-derived key) is still the real one from api.js;
            // only this threshold is widened to fit e2e's poll cadence.
            TRANSPARENCY_RATE_LIMIT: 100000
        };

        this.server      = null;
        this.wss         = null;
        this.broadcaster = null;
        this.poller      = null;
        this.log         = null;
        this.snapshotBuilder = null;
        this.pollInterval = null;
        this.statusInterval = null;

        // Poll-cycle census. The background loop swallows poll errors (a real
        // server keeps polling through a source-DB outage), which leaves a chaos
        // test no way to know the outage was actually felt except by guessing a
        // duration. Counting settled cycles and failures turns "wait 5s and hope
        // several cycles ran" into a condition a test can wait ON.
        this.pollCycles   = 0;
        this.pollFailures = 0;
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
        // Must precede the limiters below (they read req.ip): same ordering
        // requirement as api.js's startApi(). Deriving from trustProxyHops
        // rather than a re-declared literal is the whole point of this seam:
        // a hand-rolled 'false'/unset here previously let a proxy-trust bug
        // through that this real wiring catches.
        app.set('trust proxy', trustProxyHops(this.config.TRUST_PROXY));
        app.use(cors({ origin: parseCorsOrigin(process.env.CORS_ORIGIN), methods: ['GET'] }));

        // Same limiter instances startApi() builds and mounts, not a
        // re-declaration of their windows/limits/keying. Only the config
        // values differ (see TRANSPARENCY_RATE_LIMIT above), never the code
        // that turns them into middleware or the route->limiter assignment.
        let limiters = createRateLimiters(this.config);
        app.use(limiters.backstopLimiter);

        // Routes mirror src/api.js (all namespaced by :dbType). This helper
        // backs the e2e suite, so its surface needs to match the real API
        // after the Phase 3 path migration; otherwise the suite would
        // either 404 on status checks or pass-by-accident on snapshots.
        // :dbType is one of 'indexer' or 'decoder'; this helper only seeds
        // indexer-shaped data, so 'decoder' requests respond as a stub.

        let validateDbType = (dt) => (dt === 'indexer' || dt === 'decoder') ? dt : null;

        // Status endpoints
        app.get('/status', async (req, res) => {
            try {
                let lastBlock = await this.sourceDb.getLastBlock();
                let hashRow = lastBlock !== null ? await this.sourceDb.getBlockHashRow(lastBlock) : null;
                let result = {};
                result[this.chain] = {};
                result[this.chain][this.network] = {
                    indexer: {
                        block_height: hashRow ? Number(hashRow.block_index) : null,
                        block_time:   hashRow ? Number(hashRow.block_time)  : null,
                        ledger_hash:  hashRow ? hashRow.ledger_hash         : null,
                        actions_hash: hashRow ? hashRow.actions_hash        : null,
                        contract_hash:hashRow ? hashRow.contract_hash       : null
                    }
                };
                result.last_updated = new Date().toISOString();
                res.json(result);
            } catch (e) {
                res.status(500).json({ error: e.message });
            }
        });

        app.get('/status/:dbType/:chain/:network', async (req, res) => {
            let dbType = validateDbType(req.params.dbType);
            if (!dbType) return res.status(400).json({ error: 'Invalid dbType' });
            let { chain, network } = req.params;
            if (chain !== this.chain || network !== this.network)
                return res.status(404).json({ error: 'Chain/network not found' });

            try {
                let lastBlock = await this.sourceDb.getLastBlock();
                let hashRow = lastBlock !== null ? await this.sourceDb.getBlockHashRow(lastBlock) : null;
                let body = {
                    chain, network, dbType,
                    block_height: hashRow ? Number(hashRow.block_index) : null,
                    block_time:   hashRow ? Number(hashRow.block_time)  : null,
                    last_updated: new Date().toISOString()
                };
                if (dbType === 'decoder') {
                    body.block_hash = hashRow ? hashRow.ledger_hash : null;  // stub: helper seeds indexer rows
                } else {
                    body.ledger_hash   = hashRow ? hashRow.ledger_hash   : null;
                    body.actions_hash  = hashRow ? hashRow.actions_hash  : null;
                    body.contract_hash = hashRow ? hashRow.contract_hash : null;
                }
                res.json(body);
            } catch (e) {
                res.status(500).json({ error: e.message });
            }
        });

        app.get('/schema/:dbType/:chain/:network', async (req, res) => {
            if (!validateDbType(req.params.dbType))
                return res.status(400).json({ error: 'Invalid dbType' });
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
                res.json({ chain: this.chain, network: this.network, dbType: req.params.dbType, tables: schema });
            } catch (e) {
                res.status(500).json({ error: e.message });
            }
        });

        app.get('/snapshot/:dbType/:chain/:network', limiters.fullSnapshotLimiter, async (req, res) => {
            if (!validateDbType(req.params.dbType))
                return res.status(400).json({ error: 'Invalid dbType' });
            try {
                await this.snapshotBuilder.streamFullSnapshot(this.sourceDb, res);
            } catch (e) {
                if (!res.headersSent) res.status(500).json({ error: e.message });
            }
        });

        app.get('/snapshot/:dbType/:chain/:network/since/:blockHeight', limiters.incrSnapshotLimiter, async (req, res) => {
            if (!validateDbType(req.params.dbType))
                return res.status(400).json({ error: 'Invalid dbType' });
            let sinceBlock = parseInt(req.params.blockHeight);
            if (isNaN(sinceBlock) || sinceBlock < 0)
                return res.status(400).json({ error: 'Invalid blockHeight' });
            try {
                await this.snapshotBuilder.streamIncrementalSnapshot(this.sourceDb, sinceBlock, res);
            } catch (e) {
                if (!res.headersSent) res.status(500).json({ error: e.message });
            }
        });

        app.get('/transparency/:dbType/:chain/:network/roots', limiters.transparencyLimiter, async (req, res) => {
            if (req.params.dbType !== 'indexer')
                return res.status(400).json({ error: 'Transparency log is indexer-only' });
            let page  = parseInt(req.query.page) || 0;
            let limit = parseInt(req.query.limit) || 100;
            try {
                let result = await this.log.getPage(page, limit);
                res.json(result);
            } catch (e) {
                res.status(500).json({ error: e.message });
            }
        });

        app.get('/transparency/:dbType/:chain/:network/proof/:block_index', limiters.transparencyLimiter, async (req, res) => {
            if (req.params.dbType !== 'indexer')
                return res.status(400).json({ error: 'Transparency log is indexer-only' });
            let { chain, network, block_index } = req.params;
            if (chain !== this.chain || network !== this.network)
                return res.status(404).json({ error: 'Chain/network not found' });
            try {
                let result = await this.log.getProof(block_index);
                if (!result) return res.status(404).json({ error: 'Block not found' });
                res.json(result);
            } catch (e) {
                res.status(500).json({ error: e.message });
            }
        });

        app.get('/transparency/:dbType/:chain/:network/root/latest', limiters.transparencyLimiter, async (req, res) => {
            if (req.params.dbType !== 'indexer')
                return res.status(400).json({ error: 'Transparency log is indexer-only' });
            let { chain, network } = req.params;
            if (chain !== this.chain || network !== this.network)
                return res.status(404).json({ error: 'Chain/network not found' });
            try {
                let result = await this.log.getLatestRoot();
                res.json(result || { epoch: null, merkle_root: null });
            } catch (e) {
                res.status(500).json({ error: e.message });
            }
        });

        this.server = http.createServer(app);
        this.wss = new WebSocket.Server({ noServer: true });

        this.server.on('upgrade', (request, socket, head) => {
            let match = request.url.match(/^\/subscribe\/([^\/]+)\/([^\/]+)\/([^\/\?]+)/);
            if (!match) { socket.destroy(); return; }
            let [, dbType, chain, network] = match;
            if (!validateDbType(dbType)) { socket.destroy(); return; }
            this.wss.handleUpgrade(request, socket, head, (ws) => {
                this.broadcaster.addSubscription(ws, request, chain, network, 'full', dbType);
            });
        });

        await new Promise((resolve, reject) => {
            // Without an error handler a failed bind (EADDRINUSE when a prior
            // server on the same port hasn't fully released it) would never
            // resolve, hanging start() until the test times out. Reject instead.
            let onError = (err) => reject(err);
            this.server.once('error', onError);
            this.server.listen(this.port, () => {
                this.server.removeListener('error', onError);
                resolve();
            });
        });

        // Initialize poller state to match production semantics: lastPolledBlock
        // starts at the current chain tip, so the poll loop only streams *new*
        // blocks. Tests that need the poller to also process pre-seeded blocks
        // (e.g. transparency tests asserting on sync_meta) should set
        // `server.poller.lastPolledBlock = 0` and then `await server.poll()`
        // before asserting.
        this.poller.lastPolledBlock = await this.sourceDb.getLastBlock();
        // Also seed lastPolledBlockHash, exactly as production ServerPoller.start()
        // does. Without it the hash stays null and the net-forward reorg guard
        // (which is gated on lastPolledBlockHash !== null) never arms, so a reorg
        // that rolls back then readvances to an equal-or-higher tip is missed
        // entirely and the replica is left with stale orphaned blocks.
        this.poller.lastPolledBlockHash = (this.poller.lastPolledBlock !== null)
            ? await this.poller._sourceBlockHash(this.poller.lastPolledBlock) : null;
        // Pre-populate recentBroadcastHashes for the blocks already present at start,
        // mirroring a production poller that has been running and streamed them. The
        // net-forward reorg walk-back reads these PRE-reorg hashes to descend to the
        // true fork point; with the map empty (these blocks reached the replica via the
        // bootstrap snapshot, not the poller) the walk-back can't go below the start
        // tip, so a reorg into a pre-seeded block is only partially rolled back and the
        // replica keeps a stale orphan -> recompute halt. Bounded to the same window the
        // live poller retains (RECENT_HASH_CAP = 256 in ServerPoller).
        if(this.poller.lastPolledBlock !== null){
            let floor = Math.max(1, this.poller.lastPolledBlock - 255);
            for(let bi = floor; bi <= this.poller.lastPolledBlock; bi++){
                let h = await this.poller._sourceBlockHash(bi);
                if(h !== null) this.poller.recentBroadcastHashes.set(bi, h);
            }
        }
        await this.poller._updateStatus();

        // Start polling loop
        // Serialize poll cycles. Production's ServerPoller.start() is a
        // sequential while-loop; one _poll() can never overlap the next. A
        // bare setInterval breaks that invariant whenever a cycle runs longer
        // than the interval (easy against a remote/loaded MariaDB): two
        // concurrent _poll()s race the cursor and broadcast blocks out of
        // order. Skip the tick if the previous cycle is still in flight, and
        // remember the in-flight promise so stop() can drain it. An
        // un-awaited zombie poll outliving stop() kept writing to the shared
        // test DB across test boundaries.
        this.pollInterval = setInterval(() => {
            if (this._pollInFlight) return;
            this._pollInFlight = (async () => {
                try { await this.poller._poll(); } catch (e) { this.pollFailures++; }
                finally { this.pollCycles++; this._pollInFlight = null; }
            })();
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
        // Drain an in-flight poll cycle; clearInterval stops future ticks but
        // not one already running against the (shared) test DB.
        if (this._pollInFlight)  await this._pollInFlight;

        // Close all WebSocket connections
        if (this.wss) {
            for (let client of this.wss.clients) {
                try { client.close(); } catch (e) {}
            }
        }

        if (this.server) {
            // close() only stops accepting new connections and waits for existing
            // ones to drain. A live-follow client reconnecting (CLIENT_RECONNECT_DELAY)
            // keeps opening sockets, so close() can hang and the port is never
            // released; the next start() on the same port then hits EADDRINUSE.
            // Force-close lingering sockets so close() completes promptly.
            if (typeof this.server.closeAllConnections === 'function') {
                this.server.closeAllConnections();
            }
            await new Promise(resolve => this.server.close(resolve));
        }
    }

    // Force a poll cycle (useful for tests that need immediate response).
    // Serialized with the background loop; a manual poll overlapping a
    // background cycle is the same cursor race the loop guard prevents.
    async poll() {
        while (this._pollInFlight) await this._pollInFlight;
        this._pollInFlight = (async () => {
            // Counted like a background cycle, but the error still propagates: a
            // manual poll is a test's own step and must fail it, not be swallowed.
            try { await this.poller._poll(); } catch (e) { this.pollFailures++; throw e; }
            finally { this.pollCycles++; this._pollInFlight = null; }
        })();
        await this._pollInFlight;
    }

    // Drain poll cycles until the poller has processed (hashed + recorded)
    // through `height`. Seeding the source DB does not make blocks servable:
    // /snapshot serves only what the poller has recorded into sync_meta, and
    // each _poll() cycle is capped at 100 blocks, so a test that seeds and
    // immediately snapshots/catches-up races the 200ms background loop.
    async pollUntil(height, maxCycles = 50) {
        for (let i = 0; i < maxCycles; i++) {
            if (this.poller.lastPolledBlock !== null && this.poller.lastPolledBlock >= height) return;
            await this.poll();
        }
        throw new Error('pollUntil: poller stuck at ' + this.poller.lastPolledBlock + ', wanted ' + height);
    }

    getUrl() {
        return 'http://127.0.0.1:' + this.port;
    }

    getWsUrl() {
        return 'ws://127.0.0.1:' + this.port;
    }
}

module.exports = ServerProcess;
