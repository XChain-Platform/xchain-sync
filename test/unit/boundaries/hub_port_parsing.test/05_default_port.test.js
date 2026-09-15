// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers default database ports. One part of hub_port_parsing.test.js.
const assert = require('assert');
const sinon  = require('sinon');
const HubClient = require('../../../../src/hub/client');
const axios = require('axios');
const { registerHooks } = require('./helpers/hub_port_parsing_suite');

describe('Boundary: HubClient Port Parsing', function(){
    registerHooks();
    describe('getIndexerConfigs integration', function(){
        it('defaults to 3306 when neither port field present', async function(){
            let hub = new HubClient('localhost', 10000);
            sinon.stub(axios, 'post').resolves({
                data: {
                    jsonrpc: '2.0',
                    result: {
                        bitcoin: {
                            mainnet: {
                                'xchain-indexer': {
                                    host: 'db.local',
                                    name: 'xchain_btc',
                                    user: 'root',
                                    pass: 'pass'
                                }
                            }
                        }
                    }
                }
            });

            let configs = await hub.getIndexerConfigs();
            assert.strictEqual(configs[0].db_port, 3306);
            axios.post.restore();
        });
    });
});
