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
 * XChain Sync - Hub Client
 *
 * JSON-RPC client for calling xchain-hub instances to discover
 * installed chains and their indexer + decoder database connections.
 * Supports multi-endpoint fallback for high availability.
 *
 ********************************************************************/

const axios = require('axios');

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
    }

    // Internal: call a JSON-RPC method, trying each endpoint in order
    async _call(data, timeout = 5000){
        for(let url of this.urls){
            try {
                let response = await axios.post(url, data, { timeout });
                if(response.data && response.data.result !== undefined)
                    return response.data.result;
            } catch(err){
                console.warn('Hub endpoint ' + url + ' failed: ', err);
            }
        }
        return null;
    }

    // Ping the hub to check if it's alive
    async ping(){
        let result = await this._call({ jsonrpc: '2.0', method: 'ping', id: 1 });
        return result !== null;
    }

    // Get all configs from the hub
    // Returns nested object: { coin: { network: { module: { param: value } } } }
    async getallconfigs(){
        return await this._call({ jsonrpc: '2.0', method: 'getallconfigs', id: 1 }, 10000);
    }

    // Extract indexer database configs from the hub response.
    // Returns array of: [{ coin, network, dbType, db_host, db_port, db_name, db_user, db_pass }]
    async getIndexerConfigs(){
        return this._extractDbConfigs(await this.getallconfigs(), 'xchain-indexer', 'indexer');
    }

    // Extract decoder database configs from the hub response.
    // Returns array of: [{ coin, network, dbType, db_host, db_port, db_name, db_user, db_pass }]
    async getDecoderConfigs(){
        return this._extractDbConfigs(await this.getallconfigs(), 'xchain-decoder', 'decoder');
    }

    // Walk the hub config tree and extract DB connection info for a specific
    // module type. Used by both getIndexerConfigs and getDecoderConfigs to
    // avoid duplicating the traversal logic.
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
                        db_host: mod.db_host || mod.host || 'localhost',
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

    // Parse a port value safely — returns defaultPort when the value is
    // absent, empty, or non-numeric.  Handles 0 correctly (unlike parseInt(x) || default).
    static _parsePort(primary, fallback){
        let val = primary !== undefined && primary !== null && primary !== '' ? primary : fallback;
        if(val === undefined || val === null || val === '') return 3306;
        let parsed = parseInt(val, 10);
        return isNaN(parsed) || parsed < 0 ? 3306 : parsed;
    }
}

// Parse hub endpoints from environment/config
// Returns an array of URL strings
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
