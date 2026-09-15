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
const util = require('node:util');
const { getLogger } = require('../observability');
const envConfig = require('../config');
const logger = getLogger();

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
        // Per-endpoint failure detail from the most recent call(). Populated with
        // "url => code|message" strings for each unreachable endpoint so callers
        // can report exactly what was tried and why, instead of a bare null.
        this.lastFailures = [];
        // Cached full config tree + its high-water mark (epoch seconds). The mark
        // is sent back as `since_updated_at` so the hub returns only rows changed
        // since the previous poll; the delta is merged into this cache and the
        // full map returned, so extractDbConfigs is unaffected. 0 (initial /
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

    // Internal: call a JSON-RPC method, trying each endpoint starting from the
    // last one that succeeded and wrapping around through the rest.
    // Attaches x-api-key when HUB_API_KEY is configured: getallconfigs is in
    // the hub's sensitive-read tier (its response carries DB credentials) and
    // 401s without it once the hub sets a key. Methods that don't need it
    // ignore it, so sending unconditionally is safe.
    // HUB_CONFIG_SECRETS_API_KEY wins when set: the hub can split the credential
    // tier (getallconfigs with include_secrets, the only way this client gets the
    // replication sources' DB passwords) onto a key of its own, and one request
    // carries one x-api-key header. Unset, the bulk key covers both tiers.
    async call(data, timeout = 5000){
        this.lastFailures = [];
        let headers = {};
        let hubKey = envConfig.hubApiKeyFromEnv();
        if(hubKey) headers['x-api-key'] = hubKey;
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
                logger.warn(util.format('Hub endpoint ' + url + ' failed: ', err));
            }
        }
        return null;
    }

    async ping(){
        let result = await this.call({ jsonrpc: '2.0', method: 'ping', id: 1 });
        return result !== null;
    }

    // Params for every getallconfigs call this client makes.
    //
    // include_secrets is NOT optional for sync: extractDbConfigs turns this tree
    // into the replication sources' connection details (db_user/db_pass per
    // coin/network), so a redacted response hands every source the literal
    // "[redacted]" as its password. The hub redacts secret-bearing params by
    // default and serves them only to a caller that asks and is authorized
    // (HUB_CONFIG_SECRETS_API_KEY when the hub sets one, the bulk HUB_API_KEY
    // otherwise). Older hubs ignore an unknown param and return the full tree, so
    // this is safe to deploy ahead of the hub change (and must be: a sync without
    // the flag against a redacting hub loses its DB passwords).
    configParams(cursor){
        return { since_updated_at: cursor, include_secrets: true };
    }

    // One warning, not one per poll: a redacted response means this service asked
    // for credentials and is not authorized for them, so every DB pool built from
    // the result will fail to authenticate several layers away from the cause.
    warnIfRedacted(result){
        if(!result || typeof result !== 'object' || result.secrets_redacted !== true) return;
        if(this._warnedRedacted) return;
        this._warnedRedacted = true;
        logger.error('Hub served a CREDENTIAL-REDACTED config tree (' + (result.redacted_params || 0) +
            ' params withheld): this service asked for secrets but is not authorized for them. Set ' +
            'HUB_API_KEY (or the hub\'s HUB_CONFIG_SECRETS_API_KEY) to the value the hub expects; until ' +
            'then every replication source built from this config will fail to authenticate.');
    }

    async getIndexerConfigs(){
        return this.extractDbConfigs(await this.getallconfigs(), 'xchain-indexer', 'indexer');
    }

    async getDecoderConfigs(){
        return this.extractDbConfigs(await this.getallconfigs(), 'xchain-decoder', 'decoder');
    }

    // Extract indexer database configs from the hub response.
    // Returns array of: [{ coin, network, dbType, db_host, db_port, db_name, db_user, db_pass }]
    extractDbConfigs(allConfigs, moduleName, dbType){
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
                        db_port: HubClient.parsePort(mod.db_port, mod.port),
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
    static parsePort(primary, fallback){
        let val = primary !== undefined && primary !== null && primary !== '' ? primary : fallback;
        if(val === undefined || val === null || val === '') return 3306;
        let parsed = parseInt(val, 10);
        return isNaN(parsed) || parsed < 0 ? 3306 : parsed;
    }
}

for(const mixin of [require('./client/config_fetch.js')]){
    for(const name of Object.keys(mixin))
        Object.defineProperty(HubClient.prototype, name, {
            value: mixin[name], enumerable: false, writable: true, configurable: true
        });
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
