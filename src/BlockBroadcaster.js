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
 * XChain Sync - Block Broadcaster
 *
 * Manages WebSocket subscriptions per chain/network/dbType and broadcasts
 * block, reorg, and status events to all subscribers. Subscribers
 * watching indexer DB do not receive decoder DB events and vice versa
 * — the dbType discriminator is part of the subscription key.
 *
 ********************************************************************/

const WebSocket = require('ws');

// JSON replacer that converts BigInt to string (mariadb driver returns BigInt for BIGINT columns)
const bigIntReplacer = (k, v) => typeof v === 'bigint' ? v.toString() : v;

class BlockBroadcaster {

    constructor(config) {
        this.config = config;

        // Subscribers per chain/network/dbType: Map<"chain:network:dbType", Set<ws>>
        this.subscribers = new Map();

        // Track connections per IP for rate limiting: Map<ip, Set<ws>>
        this.ipConnections = new Map();

        // Status data per chain/network/dbType for periodic broadcasts
        this.statusData = new Map();
    }

    // Get the key for a chain/network/dbType triple
    _key(chain, network, dbType){
        return chain + ':' + network + ':' + (dbType || 'indexer');
    }

    // Get client IP from WebSocket request
    _getIp(req){
        if(this.config['TRUST_PROXY']){
            let forwarded = req.headers['x-forwarded-for'];
            if(forwarded)
                return forwarded.split(',')[0].trim();
        }
        return req.socket.remoteAddress || 'unknown';
    }

    // Add a subscriber for a chain/network/dbType
    // syncMode: 'full' (default) or 'infra-only' — controls which tables are forwarded
    // dbType:   'indexer' (default) or 'decoder' — controls which DB's events are received
    addSubscription(ws, req, chain, network, syncMode, dbType){
        let ip = this._getIp(req);
        let type = dbType || 'indexer';
        let key = this._key(chain, network, type);

        // Check per-IP connection limit
        if(!this.ipConnections.has(ip))
            this.ipConnections.set(ip, new Set());
        let ipSet = this.ipConnections.get(ip);
        if(ipSet.size >= this.config['WS_MAX_PER_IP']){
            ws.close(1008, 'Too many connections from this IP');
            return false;
        }

        // Register the subscription
        if(!this.subscribers.has(key))
            this.subscribers.set(key, new Set());
        this.subscribers.get(key).add(ws);
        ipSet.add(ws);

        // Store metadata on the ws object
        ws._syncChain   = chain;
        ws._syncNetwork = network;
        ws._syncDbType  = type;
        ws._syncIp      = ip;
        ws._syncBuffered = 0;
        ws._syncMode    = (syncMode === 'infra-only' && type === 'indexer') ? 'infra-only' : 'full';

        // Per-subscriber applied-block tracking. _syncLastSentBlock is the highest
        // block this server has pushed to the subscriber; _syncAppliedBlock is the
        // highest block the subscriber reports having committed to its replica DB
        // (via the heartbeat message handler below). The difference is the
        // subscriber's lag, surfaced through getSubscribers()/the /status endpoint.
        // Both stay null for legacy clients that never send heartbeats.
        ws._syncLastSentBlock = null;
        ws._syncAppliedBlock  = null;

        // Setup cleanup on close
        ws.on('close', () => this.removeSubscription(ws));
        ws.on('error', () => this.removeSubscription(ws));

        // Inbound messages from the subscriber. The only message type understood
        // is a heartbeat carrying the subscriber's last applied block height;
        // anything else is ignored silently (the channel is otherwise push-only).
        ws.on('message', (data) => this._handleClientMessage(ws, data));

        // Send initial status if available
        let status = this.statusData.get(key);
        if(status){
            this._send(ws, { type: 'status', chain, network, dbType: type, ...status });
        }

        console.log('WebSocket subscriber added for ' + key + ' from ' + ip + ' (' + this.subscribers.get(key).size + ' total)');
        return true;
    }

    // Remove a subscriber
    removeSubscription(ws){
        let chain   = ws._syncChain;
        let network = ws._syncNetwork;
        let dbType  = ws._syncDbType;
        let ip      = ws._syncIp;
        if(!chain || !network) return;

        let key = this._key(chain, network, dbType);
        let subs = this.subscribers.get(key);
        if(subs){
            subs.delete(ws);
            if(subs.size === 0)
                this.subscribers.delete(key);
        }

        let ipSet = this.ipConnections.get(ip);
        if(ipSet){
            ipSet.delete(ws);
            if(ipSet.size === 0)
                this.ipConnections.delete(ip);
        }

        // Clear metadata
        ws._syncChain   = null;
        ws._syncNetwork = null;
        ws._syncDbType  = null;
    }

    // Handle an inbound message from a subscriber. Currently the only supported
    // message is { type: 'heartbeat', appliedBlock: <number> }, which records how
    // far the subscriber has applied blocks to its replica DB. Malformed JSON or
    // unrecognised message types are ignored silently — this channel is otherwise
    // server→client push only.
    _handleClientMessage(ws, data){
        let msg;
        try {
            msg = JSON.parse(typeof data === 'string' ? data : data.toString());
        } catch(e){
            return;
        }
        if(msg && msg.type === 'heartbeat' && typeof msg.appliedBlock === 'number'){
            ws._syncAppliedBlock = msg.appliedBlock;
        }
    }

    // Update status data for a chain/network/dbType
    updateStatus(chain, network, statusObj){
        // dbType is part of the statusObj per ServerPoller._updateStatus.
        // Fall back to 'indexer' for backward compat with code that doesn't set it.
        let dbType = (statusObj && statusObj.dbType) || 'indexer';
        this.statusData.set(this._key(chain, network, dbType), statusObj);
    }

    // Broadcast an event to all subscribers for a chain/network/dbType.
    // dbType is read from event.dbType (set by ServerPoller) — falls back to 'indexer'.
    // For block events with a `tables` payload, infra-only subscribers receive a filtered
    // version containing only infrastructure tables (passed in as `infraTables`).
    broadcast(chain, network, event, infraTables){
        let dbType = (event && event.dbType) || 'indexer';
        let key  = this._key(chain, network, dbType);
        let subs = this.subscribers.get(key);
        if(!subs || subs.size === 0) return;

        let fullMessage = JSON.stringify(event, bigIntReplacer);
        let infraMessage = null;

        // Pre-build the infra-only filtered message if any subscriber needs it.
        // infra-only is indexer-only (decoder has no infra tables concept), but we
        // still check the subscriber's mode here for symmetry.
        let hasInfraOnly = false;
        for(let ws of subs){
            if(ws._syncMode === 'infra-only'){ hasInfraOnly = true; break; }
        }
        if(hasInfraOnly && event && event.tables && infraTables){
            let filteredTables = {};
            for(let tbl of Object.keys(event.tables)){
                if(infraTables.has(tbl)){
                    filteredTables[tbl] = event.tables[tbl];
                }
            }
            let infraEvent = Object.assign({}, event, { tables: filteredTables, sync_mode: 'infra-only' });
            infraMessage = JSON.stringify(infraEvent, bigIntReplacer);
        }

        // Track the highest block height pushed to each subscriber, so /status can
        // report per-subscriber lag against the applied height each one reports back.
        // Only 'block' events advance this cursor (reorgs/status carry no applied
        // progression). _send may evict a backpressured subscriber, but writing the
        // field on an already-removed ws is harmless.
        let sentBlock = (event && event.type === 'block' && typeof event.block_index === 'number')
            ? event.block_index : null;

        for(let ws of subs){
            if(ws._syncMode === 'infra-only' && infraMessage){
                this._send(ws, infraMessage, true);
            } else {
                this._send(ws, fullMessage, true);
            }
            if(sentBlock !== null)
                ws._syncLastSentBlock = sentBlock;
        }
    }

    // Return per-subscriber lag info for a chain/network/dbType, used by /status.
    // Each entry: { ip, lastSentBlock, appliedBlock, lag }. lastSentBlock is null
    // until the first block is broadcast; appliedBlock is null until the subscriber
    // sends its first heartbeat; lag (lastSentBlock - appliedBlock) is null whenever
    // either side is unavailable.
    getSubscribers(chain, network, dbType){
        let subs = this.subscribers.get(this._key(chain, network, dbType));
        if(!subs) return [];
        let out = [];
        for(let ws of subs){
            let lastSent = (typeof ws._syncLastSentBlock === 'number') ? ws._syncLastSentBlock : null;
            let applied  = (typeof ws._syncAppliedBlock === 'number')  ? ws._syncAppliedBlock  : null;
            let lag = (lastSent !== null && applied !== null) ? lastSent - applied : null;
            out.push({ ip: ws._syncIp || null, lastSentBlock: lastSent, appliedBlock: applied, lag });
        }
        return out;
    }

    // Broadcast status to all subscribers for a chain/network/dbType
    broadcastStatus(chain, network, dbType){
        let type = dbType || 'indexer';
        let key = this._key(chain, network, type);
        let status = this.statusData.get(key);
        if(!status) return;

        let subs = this.subscribers.get(key);
        if(!subs || subs.size === 0) return;

        let event = { type: 'status', chain, network, dbType: type, ...status };
        let message = JSON.stringify(event, bigIntReplacer);
        for(let ws of subs){
            this._send(ws, message, true);
        }
    }

    // Send a message to a single WebSocket with backpressure handling
    _send(ws, message, isPreSerialized){
        if(ws.readyState !== WebSocket.OPEN) return;

        let data = isPreSerialized ? message : JSON.stringify(message, bigIntReplacer);

        // Backpressure: check buffered amount
        if(ws.bufferedAmount > 0)
            ws._syncBuffered = (ws._syncBuffered || 0) + 1;
        else
            ws._syncBuffered = 0;

        if(ws._syncBuffered > this.config['WS_BACKPRESSURE_LIMIT']){
            console.log('WebSocket backpressure limit exceeded for ' + ws._syncIp + ', dropping connection');
            ws.close(1008, 'Backpressure limit exceeded');
            this.removeSubscription(ws);
            return;
        }

        try {
            ws.send(data);
        } catch(e){
            console.log('WebSocket send error:', e.message);
            this.removeSubscription(ws);
        }
    }

    // Get subscriber count for a chain/network/dbType (or all)
    getSubscriberCount(chain, network, dbType){
        if(chain && network){
            let subs = this.subscribers.get(this._key(chain, network, dbType));
            return subs ? subs.size : 0;
        }
        let total = 0;
        for(let subs of this.subscribers.values())
            total += subs.size;
        return total;
    }
}

module.exports = BlockBroadcaster;
