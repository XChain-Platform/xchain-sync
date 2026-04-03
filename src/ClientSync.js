/*********************************************************************
 *
 * Copyright © 2025 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * Licensed under the Dankest Community License (Apache License 2.0 + Additional Terms).
 * You may not use this file except in compliance with that License.
 *
 * A copy of the License is available at:
 *     https://dankest.llc/license
 *
 * This software is provided "AS IS", without warranties or conditions of any kind.
 *
 **********************************************************************
 *
 * XChain Indexer Sync - Client Sync
 *
 * Client-mode orchestrator for one chain/network. Handles bootstrap
 * (full snapshot), catch-up (incremental snapshot), and live sync
 * (WebSocket subscription). Manages reconnection and gap detection.
 *
 ********************************************************************/

const WebSocket = require('ws');
const axios     = require('axios');
const zlib      = require('zlib');

class ClientSync {

    constructor(chain, network, db, applier, rollback, hashVerifier, config, util) {
        this.chain        = chain;
        this.network      = network;
        this.db           = db;
        this.applier      = applier;
        this.rollback     = rollback;
        this.hashVerifier = hashVerifier;
        this.config       = config;
        this.util         = util;

        this.sources    = this.config['SYNC_SOURCES'].split(',').map(s => s.trim()).filter(s => s);
        this.running    = false;
        this.wsConns    = [];
        this.lastAppliedBlock = null;
        this.lastHashes = null;

        // Pending blocks from secondary sources for cross-verification
        this.pendingHashes = new Map(); // blockHeight -> { sourceIndex: hashes }
    }

    // Start the client sync loop
    async start(){
        this.running = true;
        console.log('ClientSync starting for ' + this.chain + '/' + this.network);

        // Check local replica state
        this.lastAppliedBlock = await this.db.getLastBlock();

        if(this.lastAppliedBlock === null){
            // Empty database — bootstrap from full snapshot
            console.log('No local data found, bootstrapping from full snapshot...');
            await this._bootstrapFromSnapshot();
        } else {
            // Partial data — incremental catch-up
            console.log('Resuming from block ' + this.lastAppliedBlock);
            await this._incrementalCatchUp(this.lastAppliedBlock);
        }

        // Load last block hashes for continuity checking
        if(this.lastAppliedBlock !== null)
            this.lastHashes = await this.db.getBlockHashRow(this.lastAppliedBlock);

        // Open WebSocket connections to all sources
        this._connectWebSockets();

        // Keep alive
        while(this.running){
            await this.util.sleep(5000);
        }
    }

    // Stop the client sync
    stop(){
        this.running = false;
        for(let ws of this.wsConns){
            try { ws.close(); } catch(e){}
        }
        this.wsConns = [];
    }

    // Fetch and apply schema from a remote sync server
    async _fetchAndApplySchema(source){
        console.log('Fetching schema from ' + source + '...');
        try {
            let url = source + '/schema/' + this.chain + '/' + this.network;
            let response = await axios.get(url, { timeout: 30000 });
            let schema = response.data;
            if(schema && schema.tables){
                for(let tableName in schema.tables){
                    let createSql = schema.tables[tableName];
                    if(!createSql) continue;
                    try {
                        // Check if table already exists
                        let exists = await this.db.doQuery(
                            "SELECT * FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
                            [this.db.dbName, tableName]
                        );
                        if(exists.length === 0){
                            await this.db.doQuery(createSql);
                            console.log('  Created table: ' + tableName);
                        }
                    } catch(e){
                        // May fail on ordering — retry will catch it
                    }
                }
                console.log('Schema applied from ' + source);
            }
        } catch(e){
            console.error('Failed to fetch schema from ' + source + ':', e.message);
        }
    }

    // Bootstrap from a full snapshot
    async _bootstrapFromSnapshot(){
        let source = this.sources[0];
        if(!source){
            console.error('No sync sources configured');
            return;
        }

        // Fetch and apply schema before downloading data
        await this._fetchAndApplySchema(source);

        console.log('Downloading full snapshot from ' + source + '...');
        try {
            let url = source + '/snapshot/' + this.chain + '/' + this.network;
            let response = await axios.get(url, {
                responseType: 'arraybuffer',
                timeout: 600000, // 10 minute timeout for large snapshots
                decompress: true
            });

            let jsonStr = response.data;
            if(Buffer.isBuffer(jsonStr)){
                // Try to decompress if gzipped
                try {
                    jsonStr = zlib.gunzipSync(jsonStr);
                } catch(e){
                    // May already be decompressed by axios
                }
            }

            let snapshotData = JSON.parse(jsonStr.toString());
            await this.applier.applyFullSnapshot(snapshotData);
            this.lastAppliedBlock = snapshotData.block_height;

            // Verify against second source if available
            if(this.sources.length > 1 && this.config['VERIFY_HASHES']){
                await this._verifyAgainstSource(this.sources[1], this.lastAppliedBlock);
            }

            console.log('Bootstrap complete at block ' + this.lastAppliedBlock);
        } catch(e){
            console.error('Bootstrap failed:', e.message);
            // Try next source
            if(this.sources.length > 1){
                console.log('Trying secondary source...');
                this.sources.push(this.sources.shift());
                await this._bootstrapFromSnapshot();
            }
        }
    }

    // Incremental catch-up from a given block
    async _incrementalCatchUp(sinceBlock){
        let source = this.sources[0];
        if(!source) return;

        console.log('Incremental catch-up from block ' + sinceBlock + '...');
        try {
            let url = source + '/snapshot/' + this.chain + '/' + this.network + '/since/' + sinceBlock;
            let response = await axios.get(url, {
                responseType: 'arraybuffer',
                timeout: 300000,
                decompress: true
            });

            let jsonStr = response.data;
            if(Buffer.isBuffer(jsonStr)){
                try { jsonStr = zlib.gunzipSync(jsonStr); } catch(e){}
            }

            let snapshotData = JSON.parse(jsonStr.toString());
            await this.applier.applyIncrementalSnapshot(snapshotData);
            this.lastAppliedBlock = snapshotData.block_height;

            console.log('Incremental catch-up complete at block ' + this.lastAppliedBlock);
        } catch(e){
            console.error('Incremental catch-up failed:', e.message);
        }
    }

    // Verify local block hashes against a remote source
    async _verifyAgainstSource(source, blockHeight){
        try {
            let url = source + '/status/' + this.chain + '/' + this.network;
            let response = await axios.get(url, { timeout: 10000 });
            let remoteStatus = response.data;

            let localHashes = await this.db.getBlockHashRow(blockHeight);
            if(!localHashes) return;

            let result = this.hashVerifier.compareBlockHashes(blockHeight, {
                ledger_hash: localHashes.ledger_hash,
                actions_hash: localHashes.actions_hash,
                contract_hash: localHashes.contract_hash
            }, {
                ledger_hash: remoteStatus.ledger_hash,
                actions_hash: remoteStatus.actions_hash,
                contract_hash: remoteStatus.contract_hash
            });

            if(!result.match){
                console.error('HASH MISMATCH at block ' + blockHeight + ' against ' + source);
                console.error('Mismatches:', JSON.stringify(result.mismatches));
            } else {
                console.log('Hash verification passed against ' + source);
            }
        } catch(e){
            console.error('Hash verification failed against ' + source + ':', e.message);
        }
    }

    // Connect WebSocket to all sources for live sync
    _connectWebSockets(){
        for(let i = 0; i < this.sources.length; i++){
            this._connectWebSocket(this.sources[i], i);
        }
    }

    // Connect a single WebSocket
    _connectWebSocket(source, sourceIndex){
        let wsUrl = source.replace(/^http/, 'ws') + '/subscribe/' + this.chain + '/' + this.network;
        console.log('Connecting WebSocket to ' + wsUrl);

        let ws;
        try {
            ws = new WebSocket(wsUrl);
        } catch(e){
            console.error('WebSocket connection error:', e.message);
            this._scheduleReconnect(source, sourceIndex);
            return;
        }

        ws.on('open', () => {
            console.log('WebSocket connected to ' + source + ' for ' + this.chain + '/' + this.network);
        });

        ws.on('message', async (data) => {
            try {
                let event = JSON.parse(data.toString());
                await this._handleEvent(event, sourceIndex);
            } catch(e){
                console.error('Error handling WebSocket message:', e.message);
            }
        });

        ws.on('close', () => {
            console.log('WebSocket disconnected from ' + source);
            this._scheduleReconnect(source, sourceIndex);
        });

        ws.on('error', (err) => {
            console.error('WebSocket error from ' + source + ':', err.message);
        });

        this.wsConns[sourceIndex] = ws;
    }

    // Schedule a WebSocket reconnection
    _scheduleReconnect(source, sourceIndex){
        if(!this.running) return;
        setTimeout(() => {
            if(this.running)
                this._connectWebSocket(source, sourceIndex);
        }, this.config['CLIENT_RECONNECT_DELAY']);
    }

    // Handle an incoming WebSocket event
    async _handleEvent(event, sourceIndex){
        if(event.type === 'block'){
            await this._handleBlock(event, sourceIndex);
        } else if(event.type === 'reorg'){
            await this._handleReorg(event);
        } else if(event.type === 'status'){
            // Check for gaps on status update
            if(this.lastAppliedBlock !== null && event.block_height > this.lastAppliedBlock + 1){
                console.log('Block gap detected: local=' + this.lastAppliedBlock + ' remote=' + event.block_height);
                await this._incrementalCatchUp(this.lastAppliedBlock + 1);
            }
        }
    }

    // Handle a block event
    async _handleBlock(event, sourceIndex){
        let blockIndex = event.block_index;

        // Skip if we already have this block
        if(this.lastAppliedBlock !== null && blockIndex <= this.lastAppliedBlock) return;

        // Verify chain continuity
        if(this.lastAppliedBlock !== null){
            let continuity = this.hashVerifier.verifyChainContinuity(
                this.lastAppliedBlock, this.lastHashes, event
            );
            if(!continuity.valid){
                console.error('Chain continuity error: ' + continuity.reason);
                await this._incrementalCatchUp(this.lastAppliedBlock + 1);
                return;
            }
        }

        // Cross-source verification
        if(this.config['VERIFY_HASHES'] && this.sources.length > 1){
            // Store hashes from this source
            if(!this.pendingHashes.has(blockIndex))
                this.pendingHashes.set(blockIndex, {});
            this.pendingHashes.get(blockIndex)[sourceIndex] = {
                ledger_hash: event.ledger_hash,
                actions_hash: event.actions_hash,
                contract_hash: event.contract_hash
            };

            // Check if we have hashes from at least 2 sources
            let pending = this.pendingHashes.get(blockIndex);
            let sourceIndices = Object.keys(pending);
            if(sourceIndices.length < 2){
                // Wait for second source (with timeout)
                if(sourceIndex === 0){
                    setTimeout(() => {
                        // If still waiting after timeout, apply from primary source anyway
                        if(this.pendingHashes.has(blockIndex) && this.lastAppliedBlock < blockIndex){
                            console.log('Cross-source timeout for block ' + blockIndex + ', applying from primary');
                            this._applyBlockEvent(event);
                            this.pendingHashes.delete(blockIndex);
                        }
                    }, this.config['HASH_CONFIRM_TIMEOUT']);
                }
                return;
            }

            // Compare hashes
            let hashA = pending[sourceIndices[0]];
            let hashB = pending[sourceIndices[1]];
            let result = this.hashVerifier.compareBlockHashes(blockIndex, hashA, hashB);
            if(!result.match){
                console.error('DISCREPANCY ALERT: Hash mismatch at block ' + blockIndex);
                console.error('Mismatches:', JSON.stringify(result.mismatches));
                this.pendingHashes.delete(blockIndex);
                return; // Don't apply contested blocks
            }

            this.pendingHashes.delete(blockIndex);
        }

        // Apply the block
        await this._applyBlockEvent(event);
    }

    // Apply a verified block event
    async _applyBlockEvent(event){
        try {
            await this.applier.applyBlock(event);
            this.lastAppliedBlock = event.block_index;
            this.lastHashes = {
                ledger_hash: event.ledger_hash,
                actions_hash: event.actions_hash,
                contract_hash: event.contract_hash
            };

            // Clean up old pending hashes
            for(let [key] of this.pendingHashes){
                if(key <= this.lastAppliedBlock)
                    this.pendingHashes.delete(key);
            }
        } catch(e){
            console.error('Error applying block ' + event.block_index + ':', e.message);
        }
    }

    // Handle a reorg event
    async _handleReorg(event){
        console.log('Reorg event received for ' + this.chain + '/' + this.network + ' at block ' + event.block_index);
        try {
            await this.rollback.rollback(event.block_index);
            this.lastAppliedBlock = event.block_index - 1;
            if(this.lastAppliedBlock > 0)
                this.lastHashes = await this.db.getBlockHashRow(this.lastAppliedBlock);
            else
                this.lastHashes = null;
        } catch(e){
            console.error('Reorg rollback failed:', e.message);
        }
    }
}

module.exports = ClientSync;
