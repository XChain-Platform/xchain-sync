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
 * XChain Indexer Sync - Block Broadcaster
 *
 * Manages WebSocket subscriptions per chain/network and broadcasts
 * block, reorg, and status events to all subscribers.
 *
 ********************************************************************/

const WebSocket = require('ws');

// JSON replacer that converts BigInt to string (mariadb driver returns BigInt for BIGINT columns)
const bigIntReplacer = (k, v) => typeof v === 'bigint' ? v.toString() : v;

class BlockBroadcaster {

    constructor(config) {
        this.config = config;

        // Subscribers per chain/network: Map<"chain:network", Set<ws>>
        this.subscribers = new Map();

        // Track connections per IP for rate limiting: Map<ip, Set<ws>>
        this.ipConnections = new Map();

        // Status data per chain/network for periodic broadcasts
        this.statusData = new Map();
    }

    // Get the key for a chain/network pair
    _key(chain, network){
        return chain + ':' + network;
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

    // Add a subscriber for a chain/network
    // syncMode: 'full' (default) or 'infra-only' — controls which tables are forwarded
    addSubscription(ws, req, chain, network, syncMode){
        let ip = this._getIp(req);
        let key = this._key(chain, network);

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
        ws._syncIp      = ip;
        ws._syncBuffered = 0;
        ws._syncMode    = (syncMode === 'infra-only') ? 'infra-only' : 'full';

        // Setup cleanup on close
        ws.on('close', () => this.removeSubscription(ws));
        ws.on('error', () => this.removeSubscription(ws));

        // Send initial status if available
        let status = this.statusData.get(key);
        if(status){
            this._send(ws, { type: 'status', chain, network, ...status });
        }

        console.log('WebSocket subscriber added for ' + key + ' from ' + ip + ' (' + this.subscribers.get(key).size + ' total)');
        return true;
    }

    // Remove a subscriber
    removeSubscription(ws){
        let chain   = ws._syncChain;
        let network = ws._syncNetwork;
        let ip      = ws._syncIp;
        if(!chain || !network) return;

        let key = this._key(chain, network);
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
    }

    // Update status data for a chain/network
    updateStatus(chain, network, statusObj){
        this.statusData.set(this._key(chain, network), statusObj);
    }

    // Broadcast an event to all subscribers for a chain/network
    // For block events with a `tables` payload, infra-only subscribers receive a filtered
    // version containing only infrastructure tables (passed in as `infraTables`).
    broadcast(chain, network, event, infraTables){
        let key  = this._key(chain, network);
        let subs = this.subscribers.get(key);
        if(!subs || subs.size === 0) return;

        let fullMessage = JSON.stringify(event, bigIntReplacer);
        let infraMessage = null;

        // Pre-build the infra-only filtered message if any subscriber needs it
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

        for(let ws of subs){
            if(ws._syncMode === 'infra-only' && infraMessage){
                this._send(ws, infraMessage, true);
            } else {
                this._send(ws, fullMessage, true);
            }
        }
    }

    // Broadcast status to all subscribers for a chain/network
    broadcastStatus(chain, network){
        let key = this._key(chain, network);
        let status = this.statusData.get(key);
        if(!status) return;

        let subs = this.subscribers.get(key);
        if(!subs || subs.size === 0) return;

        let event = { type: 'status', chain, network, ...status };
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

    // Get subscriber count for a chain/network (or all)
    getSubscriberCount(chain, network){
        if(chain && network){
            let subs = this.subscribers.get(this._key(chain, network));
            return subs ? subs.size : 0;
        }
        let total = 0;
        for(let subs of this.subscribers.values())
            total += subs.size;
        return total;
    }
}

module.exports = BlockBroadcaster;
