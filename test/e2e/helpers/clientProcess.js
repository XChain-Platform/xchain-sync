// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available —
// contact legal@dankest.llc.

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

    // Start live WebSocket sync. Mirror ClientSync.start():
    //  - `running` must be true or _scheduleReconnect() bails out (if(!this.running)
    //    return), which would silently disable reconnect-after-disconnect — the very
    //    behavior the recovery tests exercise.
    //  - lastAppliedBlock must be initialized from the replica DB when resuming
    //    without a fresh bootstrap. Gap detection in _handleEvent/_handleBlock guards
    //    on `lastAppliedBlock !== null`; a resumed client whose replica already holds
    //    blocks but whose cursor is still null never detects the gap and can't catch
    //    up (ClientSync.start() does this init before connecting — connectLive must
    //    too). Skip when a preceding bootstrap() already set it.
    async connectLive() {
        if (this.sync.lastAppliedBlock === null) {
            this.sync.lastAppliedBlock = await this.replicaDb.getLastBlock();
            if (this.sync.lastAppliedBlock !== null) {
                this.sync.lastHashes = await this.replicaDb.getBlockHashRow(this.sync.lastAppliedBlock);
            }
        }
        this.sync.running = true;
        this.sync._connectWebSockets();
    }

    // Bootstrap and start live sync
    async start() {
        await this.bootstrap();
        await this.connectLive();
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
