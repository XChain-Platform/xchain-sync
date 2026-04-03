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
 * XChain Indexer Sync - Hub Client
 *
 * JSON-RPC client for calling the local xchain-hub to discover
 * installed chains and their indexer database connections.
 *
 ********************************************************************/

const axios = require('axios');

class HubClient {

    constructor(host, port) {
        this.url = "http://" + host + ":" + port;
    }

    // Ping the hub to check if it's alive
    async ping(){
        const data = {
            jsonrpc: '2.0',
            method: 'ping',
            id: 1
        };
        try {
            let response = await axios.post(this.url, data, { timeout: 5000 });
            return (response.data && response.data.result);
        } catch (err) {
            return false;
        }
    }

    // Get all configs from the hub
    // Returns nested object: { coin: { network: { module: { param: value } } } }
    async getallconfigs(){
        const data = {
            jsonrpc: '2.0',
            method: 'getallconfigs',
            id: 1
        };
        try {
            let response = await axios.post(this.url, data, { timeout: 10000 });
            if(response.data && response.data.result)
                return response.data.result;
            return null;
        } catch (err) {
            console.error('Error calling hub getallconfigs:', err.message);
            return null;
        }
    }

    // Extract indexer database configs from the hub response
    // Returns array of: [{ coin, network, db_host, db_port, db_name, db_user, db_pass }]
    async getIndexerConfigs(){
        let allConfigs = await this.getallconfigs();
        if(!allConfigs) return [];

        let indexerConfigs = [];
        for(let coin in allConfigs){
            if(coin === '') continue;
            for(let network in allConfigs[coin]){
                let modules = allConfigs[coin][network];
                if(modules['xchain-indexer']){
                    let idx = modules['xchain-indexer'];
                    indexerConfigs.push({
                        coin:    coin,
                        network: network,
                        db_host: idx.db_host || idx.host || 'localhost',
                        db_port: parseInt(idx.db_port || idx.port) || 3306,
                        db_name: idx.name,
                        db_user: idx.user,
                        db_pass: idx.pass
                    });
                }
            }
        }
        return indexerConfigs;
    }
}

module.exports = HubClient;
