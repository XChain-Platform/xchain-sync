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
 * XChain Sync - Hub Client
 *
 * JSON-RPC client for calling xchain-hub instances to discover
 * installed chains and their indexer + decoder database connections.
 * Supports multi-endpoint fallback for high availability.
 *
 ********************************************************************/

const axios = require('axios');

// Fold a getallconfigs delta (only the rows that changed since our cursor) into
// the cached nested config map, mutating and returning `base`. The hub's configs
// table is upsert-only (rows are never deleted), so applying successive deltas
// reconstructs exactly the tree a full fetch would have produced.
function mergeConfigDelta(base, delta){
    for(let coin in delta){
        if(!base[coin]) base[coin] = {};
        for(let network in delta[coin]){
            if(!base[coin][network]) base[coin][network] = {};
            for(let module in delta[coin][network]){
                if(!base[coin][network][module]) base[coin][network][module] = {};
                let params = delta[coin][network][module];
                for(let param in params){
                    base[coin][network][module][param] = params[param];
                }
            }
        }
    }
    return base;
}

class HubClient {

    // Accept an array of endpoint URLs, or legacy (host, port, protocol) for backward compat
    constructor(endpoints, port, protocol) {
        if(Array.isArray(endpoints)){
            this.urls = endpoints;
        } else {
            let proto = (port === 'https' || protocol === 'https') ? 'https' : 'http';
            let host = endpoints;
            let p = (typeof port === 'number' || /^\d+$/.test(port)) ? port : '10000';
            this.urls = [proto + "://" + host + ":" + p];
        }
        // Sticky-last-good endpoint: start each call at the last endpoint that
        // answered, so a degraded first endpoint isn't retried first every call
        // (which would cost the full timeout per call before falling back).
        this._lastGoodIdx = 0;
        // Per-endpoint failure detail from the most recent _call(). Populated with
        // "url => code|message" strings for each unreachable endpoint so callers
        // can report exactly what was tried and why, instead of a bare null.
        this.lastFailures = [];
        // Cached full config tree + its high-water mark (epoch seconds). The mark
        // is sent back as `since_updated_at` so the hub returns only rows changed
        // since the previous poll; the delta is merged into this cache and the
        // full map returned, so _extractDbConfigs is unaffected. 0 (initial /
        // post-restart / old hub) requests the full tree. The client is long-lived
        // (one per SyncService), so the cursor persists across poll cycles, and
        // getIndexerConfigs + getDecoderConfigs within one cycle share the cache.
        this.configs       = null;
        this.lastWatermark = 0;
        // The endpoint index `lastWatermark` was obtained from. A wall-clock
        // since_updated_at cursor is only valid against the hub that produced it (each
        // hub stamps updated_at = NOW() at its own apply time of a PBFT-committed
        // config), so on failover to a different endpoint the cursor must be reset.
        this._watermarkEndpointIdx = null;
        // Epoch ms of the last successful getallconfigs() fetch (null until the first
        // success). sync rediscovers chains from this config on a timer; if the hub goes
        // dark this timestamp stops advancing while sync keeps replicating against the last
        // chain set it saw, so exposing its age in /health lets an operator notice the hub
        // view has gone stale.
        this.lastSuccessfulFetchAt = null;
    }

    // Tries each endpoint starting from the last one that succeeded, wrapping around
    // through the rest. x-api-key is attached whenever HUB_API_KEY is configured:
    // getallconfigs sits in the hub's sensitive-read tier (its response carries DB
    // credentials) and 401s without it once the hub sets a key, while methods that do
    // not need it ignore it, so sending unconditionally is safe.
    async _call(data, timeout = 5000){
        this.lastFailures = [];
        let headers = {};
        if(process.env.HUB_API_KEY) headers['x-api-key'] = process.env.HUB_API_KEY;
        for(let i = 0; i < this.urls.length; i++){
            let idx = (this._lastGoodIdx + i) % this.urls.length;
            let url = this.urls[idx];
            try {
                let response = await axios.post(url, data, { timeout, headers });
                if(response.data && response.data.result !== undefined){
                    this._lastGoodIdx = idx;
                    return response.data.result;
                }
            } catch(err){
                this.lastFailures.push(url + ' → ' + (err.code || err.message));
                console.warn('Hub endpoint ' + url + ' failed: ', err);
            }
        }
        return null;
    }

    async ping(){
        let result = await this._call({ jsonrpc: '2.0', method: 'ping', id: 1 });
        return result !== null;
    }

    // Returns the full nested tree { coin: { network: { module: { param: value } } } }
    // whatever shape the hub answered in; see _applyConfigResult for the version
    // handling. Sync discovers DBs at startup, so this.lastSeq is tracked for
    // completeness rather than used for invalidation here.
    async getallconfigs(){
        let cursorEndpoint = this._watermarkEndpointIdx;
        let sentCursor     = this.lastWatermark;
        let result = await this._call({
            jsonrpc: '2.0',
            method:  'getallconfigs',
            // Echo the high-water mark so the hub returns only rows changed since
            // our last poll; 0 requests the full tree (initial fetch / old hub).
            params:  { since_updated_at: this.lastWatermark },
            id:      1
        }, 10000);
        // _call returns null when every endpoint failed; preserve that signal so
        // _extractDbConfigs (which treats null as "no configs") stays unchanged.
        if(result === null) return null;

        // If _call failed over to a different endpoint than the one our cursor came
        // from, the delta we just received was filtered against a stale cross-hub
        // cursor and may have skipped rows (each hub's updated_at for the same config
        // differs). Discard it and re-fetch the full tree from the new endpoint with a
        // reset cursor; the configs table is small and the merge is idempotent.
        if(sentCursor > 0 && this._lastGoodIdx !== cursorEndpoint){
            this.lastWatermark = 0;
            this.configs       = null;
            result = await this._call({
                jsonrpc: '2.0',
                method:  'getallconfigs',
                params:  { since_updated_at: 0 },
                id:      1
            }, 10000);
            if(result === null) return null;
        }

        this.configs = this._applyConfigResult(result);
        // Bind the (possibly advanced) cursor to the endpoint that answered.
        this._watermarkEndpointIdx = this._lastGoodIdx;
        // A non-null result means at least one endpoint answered; record the fetch time
        // even on a delta poll that changed nothing, so the age reflects last hub contact.
        this.lastSuccessfulFetchAt = Date.now();
        return this.configs;
    }

    // Folds a getallconfigs result into this.configs and returns the full nested map,
    // absorbing the hub-version differences so _extractDbConfigs sees one shape. Newer
    // hubs wrap the payload as { configs, seq, watermark }, and a watermark means the
    // payload is a delta against the cursor we sent, so it is MERGED and the cursor
    // advances. Older hubs return the bare map, or a { configs, seq } wrapper with no
    // watermark; those are the full tree and REPLACE the cache, with seq 0.
    _applyConfigResult(result){
        let payload, seq, watermark;
        if(result && typeof result === 'object' && result.configs && typeof result.configs === 'object' && ('seq' in result)){
            payload   = result.configs;
            seq       = Number(result.seq) || 0;
            watermark = ('watermark' in result) ? result.watermark : undefined;
        } else {
            payload   = result;
            seq       = 0;
            watermark = undefined;
        }
        this.lastSeq = seq;

        if(watermark === undefined || watermark === null){
            // Hub doesn't report a watermark; payload is the full tree. Reset the
            // cursor so the next poll also requests in full.
            this.lastWatermark = 0;
            return payload;
        }

        let sentCursor = this.lastWatermark > 0;
        this.lastWatermark = Number(watermark) || 0;

        if(sentCursor && this.configs){
            // Delta against the cursor we sent: merge changed rows into the cache.
            return mergeConfigDelta(this.configs, payload || {});
        }
        // First fetch (or post-restart): payload is the full tree.
        return payload;
    }

    async getIndexerConfigs(){
        return this._extractDbConfigs(await this.getallconfigs(), 'xchain-indexer', 'indexer');
    }

    async getDecoderConfigs(){
        return this._extractDbConfigs(await this.getallconfigs(), 'xchain-decoder', 'decoder');
    }

    // Walks the hub config tree for one module type, returning
    // [{ coin, network, dbType, db_host, db_port, db_name, db_user, db_pass }].
    _extractDbConfigs(allConfigs, moduleName, dbType){
        if(!allConfigs) return [];

        let configs = [];
        for(let coin in allConfigs){
            if(coin === '') continue;
            let coinObj = allConfigs[coin];
            if(!coinObj || typeof coinObj !== 'object') continue;
            for(let network in coinObj){
                let modules = coinObj[network];
                if(!modules || typeof modules !== 'object') continue;
                if(modules[moduleName]){
                    let mod = modules[moduleName];
                    configs.push({
                        coin:    coin,
                        network: network,
                        dbType:  dbType,
                        db_host: mod.db_host || mod.host || '127.0.0.1',
                        db_port: HubClient._parsePort(mod.db_port, mod.port),
                        db_name: mod.name,
                        db_user: mod.user,
                        db_pass: mod.pass
                    });
                }
            }
        }
        return configs;
    }

    // Falls back to 3306 when the value is absent, empty or non-numeric, and handles
    // a literal 0 correctly, unlike the `parseInt(x) || default` shorthand.
    static _parsePort(primary, fallback){
        let val = primary !== undefined && primary !== null && primary !== '' ? primary : fallback;
        if(val === undefined || val === null || val === '') return 3306;
        let parsed = parseInt(val, 10);
        return isNaN(parsed) || parsed < 0 ? 3306 : parsed;
    }
}

// HUB_VALIDATORS entries may be bare host:port or full URLs; bare ones inherit
// HUB_PROTOCOL. Returns an array of URL strings.
HubClient.parseEndpoints = function(config){
    if(config.HUB_VALIDATORS){
        return config.HUB_VALIDATORS.split(',')
            .map(e => e.trim())
            .filter(e => e)
            .map(e => {
                if(e.startsWith('http')) return e;
                let proto = (config.HUB_PROTOCOL === 'https') ? 'https' : 'http';
                return proto + '://' + e;
            });
    }
    let host = config.HUB_API_HOST || 'localhost';
    let port = config.HUB_PORT || 10000;
    let proto = (config.HUB_PROTOCOL === 'https') ? 'https' : 'http';
    return [proto + '://' + host + ':' + port];
};

module.exports = HubClient;
