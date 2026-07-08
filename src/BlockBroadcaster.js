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
 **********************************************************************
 *
 * XChain Sync - Block Broadcaster
 *
 * Manages WebSocket subscriptions per chain/network/dbType and broadcasts
 * block, reorg, and status events to all subscribers. Subscribers
 * watching indexer DB do not receive decoder DB events and vice versa;
 * the dbType discriminator is part of the subscription key.
 *
 ********************************************************************/

const WebSocket = require('ws');
const { encodeTables } = require('./wireCodec');

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

        // Named-validator REST heartbeat state.
        // Map<"chain:network:dbType", Map<validatorId, { applied_height, applied_block_time, last_seen, status?, evicted_at? }>>
        // Populated by POST /validator-heartbeat; entries past the TTL are transitioned
        // to status 'stale' (kept visible) by evictStaleValidators() rather than deleted.
        this.validatorHeartbeats = new Map();
    }

    _key(chain, network, dbType){
        return chain + ':' + network + ':' + (dbType || 'indexer');
    }

    _getIp(req){
        if(this.config['TRUST_PROXY']){
            let forwarded = req.headers['x-forwarded-for'];
            if(forwarded)
                return forwarded.split(',')[0].trim();
        }
        return req.socket.remoteAddress || 'unknown';
    }

    // Add a subscriber for a chain/network/dbType.
    // syncMode: 'full' (default) or 'infra-only' (controls which tables are forwarded).
    // dbType:   'indexer' (default) or 'decoder' (controls which DB's events are received).
    addSubscription(ws, req, chain, network, syncMode, dbType){
        let ip = this._getIp(req);
        let type = dbType || 'indexer';
        let key = this._key(chain, network, type);

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
        ws.on('message', (data) => this._handleClientMessage(ws, data));

        let status = this.statusData.get(key);
        if(status){
            this._send(ws, { type: 'status', chain, network, dbType: type, ...status });
        }

        console.log('WebSocket subscriber added for ' + key + ' from ' + ip + ' (' + this.subscribers.get(key).size + ' total)');
        return true;
    }

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

        ws._syncChain   = null;
        ws._syncNetwork = null;
        ws._syncDbType  = null;
    }

    // Handle an inbound message from a subscriber. Currently the only supported
    // message is { type: 'heartbeat', appliedBlock: <number> }, which records how
    // far the subscriber has applied blocks to its replica DB. Malformed JSON or
    // unrecognised message types are ignored silently (the channel is otherwise
    // server-to-client push only).
    _handleClientMessage(ws, data){
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

    // Update status data for a chain/network/dbType
    updateStatus(chain, network, statusObj){
        // dbType is part of the statusObj per ServerPoller._updateStatus.
        // Fall back to 'indexer' for backward compat with code that doesn't set it.
        let dbType = (statusObj && statusObj.dbType) || 'indexer';
        this.statusData.set(this._key(chain, network, dbType), statusObj);
    }

    // Broadcast an event to all subscribers for a chain/network/dbType.
    // dbType is read from event.dbType (set by ServerPoller), falling back to 'indexer'.
    // For block events with a `tables` payload, infra-only subscribers receive a filtered
    // version containing only infrastructure tables (passed in as `infraTables`).
    broadcast(chain, network, event, infraTables){
        let dbType = (event && event.dbType) || 'indexer';
        let key  = this._key(chain, network, dbType);
        let subs = this.subscribers.get(key);
        if(!subs || subs.size === 0) return;

        // Encode binary (Buffer) columns to the base64 wire sentinel before
        // serializing. Block payloads carry rows under `data`; without this,
        // JSON.stringify mangles Buffers and the replica's blob columns corrupt
        // (see src/wireCodec.js).
        let wireEvent = event;
        if(event && (event.data || event.updated_rows)){
            wireEvent = Object.assign({}, event);
            if(event.data)         wireEvent.data         = encodeTables(event.data);
            // updated_rows carries the same { table: [rows] } shape as data and may
            // hold binary columns (e.g. attests/xcalls payload blobs), so it must ride
            // the same base64 wire encoding or those columns corrupt on the replica.
            if(event.updated_rows) wireEvent.updated_rows = encodeTables(event.updated_rows);
        }
        let fullMessage = JSON.stringify(wireEvent, bigIntReplacer);
        let infraMessage = null;

        // Pre-build the infra-only filtered message if any subscriber needs it.
        // infra-only is indexer-only (decoder has no infra tables concept), but we
        // still check the subscriber's mode here for symmetry.
        let hasInfraOnly = false;
        for(let ws of subs){
            if(ws._syncMode === 'infra-only'){ hasInfraOnly = true; break; }
        }
        if(hasInfraOnly && event && event.data && infraTables){
            // Block payloads carry rows under `data` (same key the full-message
            // wire encoding reads above and ServerPoller writes); filtering on
            // `event.tables` left infraMessage null, so infra-only subscribers
            // silently received the full block. Filter `data` and re-emit it
            // under `data` so the consumer decodes the infra subset identically.
            let filteredTables = {};
            for(let tbl of Object.keys(event.data)){
                if(infraTables.has(tbl)){
                    filteredTables[tbl] = event.data[tbl];
                }
            }
            let infraEvent = Object.assign({}, event, { data: encodeTables(filteredTables), sync_mode: 'infra-only' });
            // Filter updated_rows to the infra subset too (e.g. stakes/delegations are
            // infra tables), so infra-only subscribers still receive in-place mutations
            // to the consensus-relevant tables they do track.
            if(event.updated_rows){
                let filteredUpdated = {};
                for(let tbl of Object.keys(event.updated_rows)){
                    if(infraTables.has(tbl))
                        filteredUpdated[tbl] = event.updated_rows[tbl];
                }
                infraEvent.updated_rows = encodeTables(filteredUpdated);
            }
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
    // Each entry: { ip, lastSentBlock, appliedBlock, lag, heartbeatReceived, lagStatus }.
    // lastSentBlock is null until the first block is broadcast; appliedBlock is null
    // until the subscriber sends its first heartbeat; lag (lastSentBlock - appliedBlock)
    // is null whenever either side is unavailable. heartbeatReceived is true once the
    // subscriber has reported an applied block at least once; it lets callers tell a
    // caught-up subscriber (lag 0) apart from one that has never reported (lag null
    // because it is unknown, not because it is in sync). Clients that never send a
    // heartbeat (legacy builds, third-party validators) stay heartbeatReceived:false.
    getSubscribers(chain, network, dbType){
        let subs = this.subscribers.get(this._key(chain, network, dbType));
        if(!subs) return [];
        let out = [];
        for(let ws of subs){
            let lastSent = (typeof ws._syncLastSentBlock === 'number') ? ws._syncLastSentBlock : null;
            let applied  = (typeof ws._syncAppliedBlock === 'number')  ? ws._syncAppliedBlock  : null;
            let lag = (lastSent !== null && applied !== null) ? lastSent - applied : null;
            let heartbeatReceived = ws._syncAppliedBlock !== null;
            out.push({
                ip: ws._syncIp || null,
                lastSentBlock: lastSent,
                appliedBlock: applied,
                lag,
                heartbeatReceived,
                // lagStatus is an explicit machine-readable signal so an operator or
                // alerting script does not have to interpret what a null `lag` means.
                // 'known':   a heartbeat established a baseline, so `lag` is a real
                //            number (including a genuine 0 = caught up).
                // 'unknown': no heartbeat yet, so `lag` is null because it is
                //            undetermined, NOT because the subscriber is in sync.
                //            Scanning only for non-zero `lag` would silently skip these.
                lagStatus: heartbeatReceived ? 'known' : 'unknown'
            });
        }
        return out;
    }

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

        // Backpressure (item 5410): drop a peer only when it is genuinely stuck, not merely
        // slow. Two independent signals on the OS send buffer:
        //   1) a hard byte ceiling - the peer is accumulating unboundedly (server-memory risk);
        //   2) a non-draining stall timeout - the buffer has not made any downward progress for
        //      WS_BACKPRESSURE_STALL_MS. The stall timer resets on ANY drop in bufferedAmount,
        //      so a slow-but-draining replica keeps resetting and stays connected instead of
        //      being force-dropped into a re-bootstrap thrash loop (the old count-based check
        //      dropped it because the buffer rarely returned to exactly zero under load).
        let buffered = ws.bufferedAmount;
        let drop     = null;
        if(buffered > this.config['WS_BACKPRESSURE_MAX_BYTES']){
            drop = 'buffer ceiling exceeded (' + buffered + ' bytes)';
        } else if(buffered > 0 && buffered >= (ws._syncLastBuffered || 0)){
            // Flat or growing since the last send: start or continue the stall window.
            if(!ws._syncBackpressureSince) ws._syncBackpressureSince = Date.now();
            else if(Date.now() - ws._syncBackpressureSince > this.config['WS_BACKPRESSURE_STALL_MS'])
                drop = 'send buffer not draining for ' + this.config['WS_BACKPRESSURE_STALL_MS'] + 'ms';
        } else {
            // Drained to zero or made downward progress since the last send: healthy.
            ws._syncBackpressureSince = null;
        }
        ws._syncLastBuffered = buffered;

        if(drop){
            console.log('WebSocket backpressure: dropping ' + ws._syncIp + ' (' + drop + ')');
            ws.close(1008, 'Backpressure: ' + drop);
            this.removeSubscription(ws);
            return;
        }

        try {
            ws.send(data);
        } catch(e){
            console.log('WebSocket send error:', e);
            this.removeSubscription(ws);
        }
    }

    // Record a named-validator REST heartbeat for a chain/network/dbType.
    // Called by POST /validator-heartbeat in api.js.
    recordValidatorHeartbeat(chain, network, dbType, validatorId, appliedHeight, appliedBlockTime){
        let key = this._key(chain, network, dbType);
        if(!this.validatorHeartbeats.has(key))
            this.validatorHeartbeats.set(key, new Map());
        this.validatorHeartbeats.get(key).set(validatorId, {
            applied_height:     appliedHeight,
            applied_block_time: appliedBlockTime != null ? appliedBlockTime : null,
            last_seen:          Date.now()
        });
    }

    // Return per-validator heartbeat state for a chain/network/dbType.
    //
    // Shape: { validators: { <id>: {...} }, total, expected_total, unknown_count }.
    //   validators:     map keyed by validator_id; each entry carries
    //                   lag_blocks = source block_height - applied_height (null when
    //                   undeterminable) and a `status` of 'known' | 'unknown' |
    //                   'stale' | 'absent'.
    //   total:          number of live (non-stale) validators with a heartbeat on record.
    //                   Stale entries remain visible in the validators map but are
    //                   excluded from total so a going-stale validator decrements the
    //                   count (the only meaningful erosion signal for an operator).
    //   expected_total: size of the configured EXPECTED_VALIDATORS roster, or null when
    //                   no roster is configured. Gives operators a denominator: when
    //                   total < expected_total a federation member is missing.
    //   unknown_count:  how many on-record entries have lag_blocks === null (source
    //                   height not yet known), i.e. their lag is genuinely unknown
    //                   rather than 0.
    //
    // Two statuses close the silent-loss gaps a heartbeat-only view would otherwise have:
    //   'stale':  a previously-active validator whose last_seen lapsed past the TTL.
    //             evictStaleValidators() transitions it in place instead of deleting it,
    //             so a validator that restarted or briefly partitioned stays visible
    //             (with its last known applied_height) rather than silently vanishing.
    //   'absent': a member of the EXPECTED_VALIDATORS roster that has never POSTed a
    //             heartbeat for this chain/network/dbType. Without the roster such a
    //             validator (misconfigured sync URL, partitioned before its first POST)
    //             would be completely invisible. Surfaced here with null lag/last_seen.
    //
    // When no roster is configured the method behaves exactly as before for observed
    // validators; 'absent' entries simply never appear and expected_total is null.
    getValidatorHeartbeats(chain, network, dbType){
        let key = this._key(chain, network, dbType);
        let map = this.validatorHeartbeats.get(key);

        let expected      = this.config['EXPECTED_VALIDATORS'] || [];
        let expectedTotal = expected.length > 0 ? expected.length : null;

        let statusData   = this.statusData.get(key);
        let sourceHeight = (statusData && typeof statusData.block_height === 'number')
            ? statusData.block_height : null;

        let validators = {};
        let unknownCount = 0;
        let total = 0;
        if(map){
            for(let [id, entry] of map){
                // Only count live (non-stale) entries in `total` so that a validator
                // transitioning from healthy to stale is reflected as a drop in total
                // rather than being invisible. The 'stale' entry remains visible in
                // the validators map but no longer props up the denominator.
                if(entry.status !== 'stale') total++;
                let lagBlocks = (sourceHeight !== null && entry.applied_height !== null)
                    ? Math.max(0, sourceHeight - entry.applied_height)
                    : null;
                if(lagBlocks === null) unknownCount++;
                validators[id] = {
                    applied_height:     entry.applied_height,
                    applied_block_time: entry.applied_block_time,
                    last_seen:          new Date(entry.last_seen).toISOString(),
                    lag_blocks:         lagBlocks,
                    // 'stale' (lapsed past the TTL) takes precedence over the lag-based
                    // status; otherwise 'unknown' when lag_blocks could not be computed
                    // (source height not yet known) so the validator's distance from the
                    // tip is undetermined, not 0.
                    status:             entry.status === 'stale' ? 'stale'
                                          : (lagBlocks === null ? 'unknown' : 'known')
                };
                if(entry.status === 'stale' && entry.evicted_at != null)
                    validators[id].evicted_at = new Date(entry.evicted_at).toISOString();
            }
        }

        // Fill in roster members that have never reported for this leaf. A stale roster
        // member is already in the map above, so this only adds the never-seen ones.
        for(let id of expected){
            if(validators[id]) continue;
            validators[id] = {
                applied_height:     null,
                applied_block_time: null,
                last_seen:          null,
                lag_blocks:         null,
                status:             'absent'
            };
        }

        return { validators, total, expected_total: expectedTotal, unknown_count: unknownCount };
    }

    // Age out named-validator entries whose last_seen is older than thresholdMs.
    // Called on a periodic interval from api.js (server mode only).
    //
    // An active entry past the TTL is transitioned to a 'stale' status IN PLACE rather
    // than deleted, so a validator that restarted or briefly partitioned stays visible
    // in /validator-status (with its last known applied_height) instead of silently
    // disappearing. recordValidatorHeartbeat() overwrites the entry with a fresh,
    // status-less record on the next POST, so a recovered validator returns to
    // 'known'/'unknown' automatically.
    //
    // To keep memory bounded (validator_id is caller-supplied, so soft-delete alone
    // would let arbitrary ids accumulate forever), a stale entry that is NOT in the
    // expected roster is hard-removed once it has been stale for a further thresholdMs
    // window. Roster members are kept indefinitely (bounded by the roster size) so a
    // genuinely-expected validator stays surfaced as 'stale' until it reports again.
    evictStaleValidators(thresholdMs){
        let now = Date.now();
        let expected = new Set(this.config['EXPECTED_VALIDATORS'] || []);
        for(let [key, map] of this.validatorHeartbeats){
            for(let [id, entry] of map){
                if(entry.status === 'stale'){
                    if(!expected.has(id) && entry.evicted_at != null && now - entry.evicted_at > thresholdMs)
                        map.delete(id);
                } else if(now - entry.last_seen > thresholdMs){
                    map.set(id, { ...entry, status: 'stale', evicted_at: now });
                }
            }
            if(map.size === 0)
                this.validatorHeartbeats.delete(key);
        }
    }

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
