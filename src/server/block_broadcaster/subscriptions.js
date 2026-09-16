/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************/

const { getLogger } = require('../../observability');
const logger = getLogger();

module.exports = {

    // Resolve the client address used as the WS_MAX_PER_IP bucket key.
    //
    // TRUST_PROXY means exactly ONE trusted hop, the co-located Apache (api.js sets
    // Express `trust proxy` to 1 off the same flag). mod_proxy APPENDS the peer it
    // actually saw to the right of X-Forwarded-For, so the RIGHTMOST entry is the only
    // one our trusted hop wrote; everything left of it arrived from the client and is
    // forgeable. Keying on the leftmost entry therefore let any client rotate a fake
    // leading value to escape the per-IP cap, or claim another validator's address and
    // poison its connection accounting.
    getIp(req){
        if(this.config['TRUST_PROXY']){
            let forwarded = req.headers['x-forwarded-for'];
            if(forwarded){
                let entries = String(forwarded).split(',');
                let client  = entries[entries.length - 1].trim();
                // A malformed header (trailing comma, whitespace-only tail) would key
                // every such connection under '' and merge unrelated clients into one
                // bucket, which is the very accounting bug this method exists to avoid.
                // Fall through to the socket address instead.
                if(client) return client;
            }
        }
        return req.socket.remoteAddress || 'unknown';
    },

    // Add a subscriber for a chain/network/dbType.
    // syncMode: 'full' (default) or 'infra-only' (controls which tables are forwarded).
    // dbType:   'indexer' (default) or 'decoder' (controls which DB's events are received).
    addSubscription(ws, req, chain, network, syncMode, dbType){
        let ip = this.getIp(req);
        let type = dbType || 'indexer';
        let key = this.key(chain, network, type);

        if(!this.ipConnections.has(ip))
            this.ipConnections.set(ip, new Set());
        let ipSet = this.ipConnections.get(ip);
        if(ipSet.size >= this.config['WS_MAX_PER_IP']){
            ws.close(1008, 'Too many connections from this IP');
            return false;
        }

        if(!this.subscribers.has(key))
            this.subscribers.set(key, new Set());
        this.subscribers.get(key).add(ws);
        ipSet.add(ws);

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

        ws.on('close', () => this.removeSubscription(ws));
        ws.on('error', () => this.removeSubscription(ws));

        // Inbound messages from the subscriber. The only message type understood
        // is a heartbeat carrying the subscriber's last applied block height;
        // anything else is ignored silently (the channel is otherwise push-only).
        ws.on('message', (data) => this.handleClientMessage(ws, data));

        let status = this.getStatus(chain, network, type);
        if(status){
            this.send(ws, { type: 'status', chain, network, dbType: type, ...status });
        }

        logger.info('WebSocket subscriber added for ' + key + ' from ' + ip + ' (' + this.subscribers.get(key).size + ' total)');
        return true;
    },

    removeSubscription(ws){
        let chain   = ws._syncChain;
        let network = ws._syncNetwork;
        let dbType  = ws._syncDbType;
        let ip      = ws._syncIp;
        if(!chain || !network) return;

        let key = this.key(chain, network, dbType);
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

        ws._syncChain   = null;
        ws._syncNetwork = null;
        ws._syncDbType  = null;
    },

    // Handle an inbound message from a subscriber. Currently the only supported
    // message is { type: 'heartbeat', appliedBlock: <number> }, which records how
    // far the subscriber has applied blocks to its replica DB. Malformed JSON or
    // unrecognised message types are ignored silently (the channel is otherwise
    // server-to-client push only).
    handleClientMessage(ws, data){
        let msg;
        try {
            msg = JSON.parse(typeof data === 'string' ? data : data.toString());
        } catch(e){
            return;
        }
        // Require a non-negative integer, matching the REST validator-heartbeat guard
        // (api.js). A bare `typeof === 'number'` accepts NaN/Infinity/negatives, letting
        // a subscriber forge its reported lag on /status (negative lag reads as
        // "always caught up"; NaN serializes to null and hides the peer from lag alerts).
        if(msg && msg.type === 'heartbeat' && Number.isInteger(msg.appliedBlock) && msg.appliedBlock >= 0){
            ws._syncAppliedBlock = msg.appliedBlock;
        }
    }
};
