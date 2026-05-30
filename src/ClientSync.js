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
 * XChain Sync - Client Sync
 *
 * Client-mode orchestrator for one chain/network/dbType triple.
 * Handles bootstrap (full snapshot), catch-up (incremental snapshot),
 * and live sync (WebSocket subscription). Manages reconnection and
 * gap detection.
 *
 * dbType is read from db.dbType. Decoder DB instances skip the
 * three-hash cross-source verification (decoder has no synthetic
 * ledger/actions/contract hashes — content is deterministic from
 * the coin node).
 *
 ********************************************************************/

const WebSocket  = require('ws');
const axios      = require('axios');
const zlib       = require('zlib');
const validation = require('./validation');

class ClientSync {

    constructor(chain, network, db, applier, rollback, hashVerifier, config, util) {
        this.chain        = chain;
        this.network      = network;
        this.db           = db;
        this.dbType       = (db && db.dbType) ? db.dbType : 'indexer';
        this.applier      = applier;
        this.rollback     = rollback;
        this.hashVerifier = hashVerifier;
        this.config       = config;
        this.util         = util;

        this.sources    = this.config['SYNC_SOURCES'].split(',').map(s => s.trim()).filter(s => s);
        this.running    = false;
        this.wsConns    = [];
        this.lastAppliedBlock     = null;
        this.lastHashes           = null;
        this.lastKnownServerBlock = null;

        // Pending blocks from secondary sources for cross-verification
        this.pendingHashes = new Map(); // blockHeight -> { sourceIndex: hashes }

        // Applied-block heartbeat state. After committing each live block we report
        // our applied height back to the source servers so operators can observe
        // this validator's lag via the server's /status endpoint. Debounced to avoid
        // a round-trip per block under fast sync: flush every 10 blocks, or after 5s,
        // whichever comes first.
        this._hbLastSentBlock = null;
        this._hbTimer         = null;
    }

    // Start the client sync loop
    async start(){
        this.running = true;
        console.log('ClientSync starting for ' + this.chain + '/' + this.network + '/' + this.dbType);

        // Check local replica state
        this.lastAppliedBlock = await this.db.getLastBlock();

        if(this.lastAppliedBlock === null){
            // Empty database — bootstrap from full snapshot
            console.log('No local data found, bootstrapping from full snapshot...');
            await this._bootstrapFromSnapshot();
        } else {
            // Partial data — incremental catch-up.
            // Pass the next needed block (lastAppliedBlock + 1): the server uses
            // inclusive >= bounds, so passing lastAppliedBlock re-delivers already
            // applied rows and the non-ignore INSERT throws a duplicate-key error.
            console.log('Resuming from block ' + this.lastAppliedBlock);
            await this._incrementalCatchUp(this.lastAppliedBlock + 1);
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
        if(this._hbTimer){
            clearTimeout(this._hbTimer);
            this._hbTimer = null;
        }
        for(let ws of this.wsConns){
            try { ws.close(); } catch(e){}
        }
        this.wsConns = [];
    }

    // Schedule an applied-block heartbeat (debounced). Flushes immediately once at
    // least 10 blocks have been applied since the last report; otherwise arms a 5s
    // timer so a trickle of blocks still gets reported without a per-block send.
    _scheduleHeartbeat(){
        if(this._hbLastSentBlock === null ||
           (this.lastAppliedBlock - this._hbLastSentBlock) >= 10){
            this._flushHeartbeat();
        } else if(!this._hbTimer){
            this._hbTimer = setTimeout(() => {
                this._hbTimer = null;
                this._flushHeartbeat();
            }, 5000);
        }
    }

    // Send the current applied height to every open source connection. Best-effort:
    // a server running an older build simply ignores the message, and a failed send
    // is swallowed (the next heartbeat will carry the latest height anyway).
    _flushHeartbeat(){
        if(this.lastAppliedBlock === null) return;
        if(this._hbTimer){
            clearTimeout(this._hbTimer);
            this._hbTimer = null;
        }
        let msg = JSON.stringify({ type: 'heartbeat', appliedBlock: this.lastAppliedBlock });
        for(let ws of this.wsConns){
            try {
                if(ws && ws.readyState === WebSocket.OPEN)
                    ws.send(msg);
            } catch(e){ /* best-effort */ }
        }
        this._hbLastSentBlock = this.lastAppliedBlock;
    }

    // Fetch and apply schema from a remote sync server
    async _fetchAndApplySchema(source){
        console.log('Fetching schema from ' + source + '...');
        try {
            let url = source + '/schema/' + this.dbType + '/' + this.chain + '/' + this.network;
            let response = await axios.get(url, { timeout: 30000 });
            let schema = response.data;
            if(schema && schema.tables){
                for(let tableName in schema.tables){
                    let createSql = schema.tables[tableName];
                    if(!createSql) continue;

                    // Validate table name and DDL before executing
                    let idCheck = validation.validateIdentifier(tableName);
                    if(!idCheck.valid){
                        console.error('Rejected table name from schema: ' + tableName + ' (' + idCheck.reason + ')');
                        continue;
                    }
                    let ddlCheck = validation.validateDdl(createSql);
                    if(!ddlCheck.valid){
                        console.error('Rejected DDL for table ' + tableName + ': ' + ddlCheck.reason);
                        continue;
                    }

                    try {
                        // Check if table already exists
                        let exists = await this.db.doQuery(
                            "SELECT * FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
                            [this.db.dbName, tableName]
                        );
                        if(exists.length === 0){
                            await this.db.doQuery(createSql);
                            console.log('  Created table: ' + tableName);
                        } else {
                            // Table already exists — propagate any columns the
                            // master has added since this replica was bootstrapped.
                            // Without this the path is CREATE-only and a replica
                            // that pre-dates a column addition stalls on the first
                            // snapshot carrying it ("Unknown column ... in field
                            // list"). Runs before the snapshot apply, so the ALTERs
                            // are outside any snapshot transaction.
                            await this.db.addMissingColumns(tableName, createSql);
                        }
                    } catch(e){
                        // May fail on ordering — retry will catch it
                    }
                }
                console.log('Schema applied from ' + source);
            }
        } catch(e){
            console.error('Failed to fetch schema from ' + source + ':', e);
        }
    }

    // Bootstrap from a full snapshot
    // attempt tracks recursion depth to prevent infinite retries when all sources fail
    async _bootstrapFromSnapshot(attempt){
        attempt = attempt || 0;
        let source = this.sources[0];
        if(!source){
            console.error('No sync sources configured');
            return;
        }

        // Fetch and apply schema before downloading data
        await this._fetchAndApplySchema(source);

        console.log('Downloading full snapshot from ' + source + '...');
        try {
            let url = source + '/snapshot/' + this.dbType + '/' + this.chain + '/' + this.network;
            let response = await axios.get(url, {
                responseType: 'arraybuffer',
                timeout: 600000, // 10 minute timeout for large snapshots
                decompress: true,
                maxContentLength: this.config['SNAPSHOT_MAX_CONTENT']
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
            console.error('Bootstrap failed:', e);
            // Try next source, but only if we haven't exhausted all sources
            if(this.sources.length > 1 && attempt < this.sources.length - 1){
                console.log('Trying secondary source...');
                this.sources.push(this.sources.shift());
                await this._bootstrapFromSnapshot(attempt + 1);
            } else {
                console.error('All sync sources exhausted after ' + (attempt + 1) + ' attempt(s)');
            }
        }
    }

    // Incremental catch-up from a given block
    async _incrementalCatchUp(sinceBlock){
        let source = this.sources[0];
        if(!source) return;

        console.log('Incremental catch-up from block ' + sinceBlock + '...');
        try {
            let url = source + '/snapshot/' + this.dbType + '/' + this.chain + '/' + this.network + '/since/' + sinceBlock;
            let response = await axios.get(url, {
                responseType: 'arraybuffer',
                timeout: 300000,
                decompress: true,
                maxContentLength: this.config['SNAPSHOT_MAX_CONTENT']
            });

            let jsonStr = response.data;
            if(Buffer.isBuffer(jsonStr)){
                try { jsonStr = zlib.gunzipSync(jsonStr); } catch(e){}
            }

            let snapshotData = JSON.parse(jsonStr.toString());
            await this.applier.applyIncrementalSnapshot(snapshotData);
            this.lastAppliedBlock = snapshotData.block_height;
        } catch(e){
            console.error('Incremental catch-up failed:', e);
        }
    }

    // Verify local block hashes against a remote source.
    // Indexer-only — decoder DB has no synthetic chain-of-state hashes to compare.
    async _verifyAgainstSource(source, blockHeight){
        if(this.dbType !== 'indexer') return;
        try {
            let url = source + '/status/' + this.dbType + '/' + this.chain + '/' + this.network;
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

            // Replica-completeness check (additive — never overrides the hash result).
            //
            // The committed ledger/actions/contract hashes are computed on the
            // source during block processing and replicated verbatim, so a follower
            // missing entire tables still agrees on every hash — the hashes describe
            // the source's blockchain computation, not what actually landed
            // downstream. The source now publishes per-table row counts on /status
            // (api.buildStatusRow); compare them against our own to surface any table
            // the source has rows in that we do not. A shortfall is logged as a
            // health signal for operators — it does NOT reject the block, since a
            // passing hash check is still a valid consensus result.
            let countMismatches = await this._verifyTableCounts(remoteStatus.table_counts);
            if(countMismatches.length){
                console.error('TABLE_COUNT_MISMATCH at block ' + blockHeight + ' against ' + source +
                    ' — follower may be missing replicated rows:');
                console.error(JSON.stringify(countMismatches));
            } else if(remoteStatus.table_counts){
                console.log('Table-count verification passed against ' + source);
            }
        } catch(e){
            console.error('Hash verification failed against ' + source + ':', e);
        }
    }

    // Compare the source's published per-table row counts against this replica's
    // own counts. Returns an array of { table, sourceCount, localCount, delta } for
    // every table the source has MORE rows in than the follower — a shortfall that
    // indicates missing replicated data. Followers legitimately holding extra local
    // rows are ignored: only source-ahead deltas signal incomplete replication.
    // Best-effort — a table that can't be counted locally (absent in this replica's
    // schema) is reported as a full shortfall rather than silently skipped.
    async _verifyTableCounts(remoteCounts){
        let mismatches = [];
        if(!remoteCounts || typeof remoteCounts !== 'object') return mismatches;
        for(let table of Object.keys(remoteCounts)){
            let remote = Number(remoteCounts[table]);
            if(!Number.isFinite(remote)) continue;
            let local;
            try {
                local = Number(await this.db.getTableCount(table));
            } catch(e){
                local = 0;
            }
            if(!Number.isFinite(local)) local = 0;
            if(remote > local)
                mismatches.push({ table: table, sourceCount: remote, localCount: local, delta: remote - local });
        }
        return mismatches;
    }

    // Connect WebSocket to all sources for live sync
    _connectWebSockets(){
        for(let i = 0; i < this.sources.length; i++){
            this._connectWebSocket(this.sources[i], i);
        }
    }

    // Connect a single WebSocket
    _connectWebSocket(source, sourceIndex){
        // Per-chain sync mode preference: 'full' (default) or 'infra-only'
        // Set via env: SYNC_MODE_BTC, SYNC_MODE_LTC, SYNC_MODE_DOGE (e.g., SYNC_MODE_DOGE=infra-only)
        let envKey   = 'SYNC_MODE_' + String(this.chain).toUpperCase();
        let syncMode = process.env[envKey] || this.config[envKey] || 'full';
        let modeQs   = (syncMode === 'infra-only') ? '?sync_mode=infra-only' : '';
        let wsUrl    = source.replace(/^http/, 'ws') + '/subscribe/' + this.dbType + '/' + this.chain + '/' + this.network + modeQs;
        console.log('Connecting WebSocket to ' + wsUrl + ' (sync_mode=' + syncMode + ')');

        let ws;
        try {
            ws = new WebSocket(wsUrl, { maxPayload: this.config['WS_MAX_PAYLOAD'] });
        } catch(e){
            console.error('WebSocket connection error:', e);
            this._scheduleReconnect(source, sourceIndex);
            return;
        }

        ws.on('open', () => {
            console.log('WebSocket connected to ' + source + ' for ' + this.chain + '/' + this.network);
        });

        ws.on('message', async (data) => {
            try {
                let event = JSON.parse(data.toString());
                let check = validation.validateWsEvent(event);
                if(!check.valid){
                    console.error('Invalid WS event from ' + source + ': ' + check.reason);
                    return;
                }
                await this._handleEvent(event, sourceIndex);
            } catch(e){
                console.error('Error handling WebSocket message:', e);
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
            // Track the server's advancing block height
            if(typeof event.block_index === 'number' &&
               (this.lastKnownServerBlock === null || event.block_index > this.lastKnownServerBlock)){
                this.lastKnownServerBlock = event.block_index;
            }
            await this._handleBlock(event, sourceIndex);
        } else if(event.type === 'reorg'){
            await this._handleReorg(event);
        } else if(event.type === 'status'){
            // Track the server's current block height
            if(typeof event.block_height === 'number' &&
               (this.lastKnownServerBlock === null || event.block_height > this.lastKnownServerBlock)){
                this.lastKnownServerBlock = event.block_height;
            }
            // Check for gaps on status update. Use a strict '>' (not '>='): a
            // server exactly one block ahead is the normal steady state — that
            // next block arrives over the live WS stream — so only a shortfall of
            // two or more blocks is a real gap worth an out-of-band catch-up. This
            // mirrors the decoder gap check in _handleBlock and avoids firing a
            // redundant incremental fetch on every status tick during live sync;
            // a genuinely dropped block is still picked up on the next status tick.
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

        // Both dbTypes require block-height continuity: indexer uses chain hashes to detect
        // gaps and forks; decoder has no synthetic hashes but still needs gap detection so
        // blocks silently dropped between bootstrap and the first WS event are caught up.
        if(this.lastAppliedBlock !== null){
            if(this.dbType === 'indexer'){
                let continuity = this.hashVerifier.verifyChainContinuity(
                    this.lastAppliedBlock, this.lastHashes, event
                );
                if(!continuity.valid){
                    console.error('Chain continuity error: ' + continuity.reason);
                    await this._incrementalCatchUp(this.lastAppliedBlock + 1);
                    return;
                }
            } else if(blockIndex > this.lastAppliedBlock + 1){
                console.log('Block gap detected (decoder): local=' + this.lastAppliedBlock + ' incoming=' + blockIndex);
                await this._incrementalCatchUp(this.lastAppliedBlock + 1);
                return;
            }
        }

        // Cross-source verification — indexer only (decoder has no synthetic chain hashes)
        if(this.dbType === 'indexer' && this.config['VERIFY_HASHES'] && this.sources.length > 1){
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
                        // If still waiting after timeout, handle based on strict mode
                        if(this.pendingHashes.has(blockIndex) && this.lastAppliedBlock < blockIndex){
                            if(this.config['HASH_CONFIRM_STRICT']){
                                console.error('STRICT: Cross-source timeout for block ' + blockIndex + ', rejecting (HASH_CONFIRM_STRICT=true)');
                                this.pendingHashes.delete(blockIndex);
                            } else {
                                console.log('Cross-source timeout for block ' + blockIndex + ', applying from primary');
                                this._applyBlockEvent(event);
                                this.pendingHashes.delete(blockIndex);
                            }
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
            // Report our applied height back to the source(s), debounced.
            this._scheduleHeartbeat();
            if(this.dbType === 'decoder'){
                this.lastHashes = { block_hash: event.block_hash };
            } else {
                this.lastHashes = {
                    ledger_hash: event.ledger_hash,
                    actions_hash: event.actions_hash,
                    contract_hash: event.contract_hash
                };
            }

            // Clean up old pending hashes
            for(let [key] of this.pendingHashes){
                if(key <= this.lastAppliedBlock)
                    this.pendingHashes.delete(key);
            }
        } catch(e){
            console.error('Error applying block ' + event.block_index + ':', e);
        }
    }

    // Handle a reorg event
    async _handleReorg(event){
        console.log('Reorg event received for ' + this.chain + '/' + this.network + ' at block ' + event.block_index);

        // Enforce max rollback depth
        if(this.lastAppliedBlock !== null){
            let depth = this.lastAppliedBlock - event.block_index + 1;
            if(depth > this.config['MAX_ROLLBACK_DEPTH']){
                console.error('Reorg depth ' + depth + ' exceeds MAX_ROLLBACK_DEPTH ' + this.config['MAX_ROLLBACK_DEPTH'] + ' — rejecting');
                return;
            }
        }

        try {
            await this.rollback.rollback(event.block_index);
            this.lastAppliedBlock = event.block_index - 1;
            if(this.lastAppliedBlock > 0)
                this.lastHashes = await this.db.getBlockHashRow(this.lastAppliedBlock);
            else
                this.lastHashes = null;
        } catch(e){
            console.error('Reorg rollback failed:', e);
        }
    }
}

module.exports = ClientSync;
