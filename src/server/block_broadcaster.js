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

const { getLogger } = require('../observability');
const logger = getLogger();

const MIXIN_FILES = [
    './block_broadcaster/subscriptions.js',
    './block_broadcaster/broadcast_and_heartbeats.js'
];

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

    key(chain, network, dbType){
        return chain + ':' + network + ':' + (dbType || 'indexer');
    }
}

for(const file of MIXIN_FILES){
    const mixin = require(file);
    for(const name of Object.keys(mixin))
        Object.defineProperty(BlockBroadcaster.prototype, name, {
            value: mixin[name], enumerable: false, writable: true, configurable: true
        });
}

module.exports = BlockBroadcaster;
