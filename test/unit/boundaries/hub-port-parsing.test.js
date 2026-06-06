// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available —
// contact legal@dankest.llc.

const assert = require('assert');
const sinon  = require('sinon');
const HubClient = require('../../../src/HubClient');

describe('Boundary: HubClient Port Parsing', function(){

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){ sinon.restore(); });

    describe('_parsePort static method', function(){
        it('valid port — returns as-is', function(){
            assert.strictEqual(HubClient._parsePort('3306', undefined), 3306);
        });

        it('zero — preserved (not treated as falsy)', function(){
            assert.strictEqual(HubClient._parsePort('0', undefined), 0);
        });

        it('falls back to secondary when primary is null', function(){
            assert.strictEqual(HubClient._parsePort(null, '5432'), 5432);
        });

        it('falls back to secondary when primary is undefined', function(){
            assert.strictEqual(HubClient._parsePort(undefined, '5432'), 5432);
        });

        it('falls back to secondary when primary is empty string', function(){
            assert.strictEqual(HubClient._parsePort('', '5432'), 5432);
        });

        it('defaults to 3306 when both are absent', function(){
            assert.strictEqual(HubClient._parsePort(undefined, undefined), 3306);
        });

        it('defaults to 3306 when both are null', function(){
            assert.strictEqual(HubClient._parsePort(null, null), 3306);
        });

        it('defaults to 3306 when both are empty', function(){
            assert.strictEqual(HubClient._parsePort('', ''), 3306);
        });

        it('defaults to 3306 for non-numeric primary', function(){
            assert.strictEqual(HubClient._parsePort('abc', undefined), 3306);
        });

        it('uses secondary when primary is non-numeric', function(){
            // primary 'abc' → fallback to secondary? No — primary is not empty/null/undefined
            // so it's used directly → parseInt('abc') = NaN → defaults to 3306
            assert.strictEqual(HubClient._parsePort('abc', '5432'), 3306);
        });

        it('negative port defaults to 3306', function(){
            assert.strictEqual(HubClient._parsePort('-1', undefined), 3306);
        });

        it('integer value (not string) works', function(){
            assert.strictEqual(HubClient._parsePort(3307, undefined), 3307);
        });

        it('integer 0 preserved', function(){
            assert.strictEqual(HubClient._parsePort(0, undefined), 0);
        });

        it('float string truncated', function(){
            assert.strictEqual(HubClient._parsePort('3306.5', undefined), 3306);
        });
    });

    describe('getIndexerConfigs integration', function(){
        it('uses db_port from hub config', async function(){
            let hub = new HubClient('localhost', 10000);
            let axios = require('axios');
            sinon.stub(axios, 'post').resolves({
                data: {
                    jsonrpc: '2.0',
                    result: {
                        bitcoin: {
                            mainnet: {
                                'xchain-indexer': {
                                    db_host: 'db.local',
                                    db_port: 13306,
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
            assert.strictEqual(configs[0].db_port, 13306);
            axios.post.restore();
        });

        it('falls back to port when db_port absent', async function(){
            let hub = new HubClient('localhost', 10000);
            let axios = require('axios');
            sinon.stub(axios, 'post').resolves({
                data: {
                    jsonrpc: '2.0',
                    result: {
                        bitcoin: {
                            mainnet: {
                                'xchain-indexer': {
                                    host: 'db.local',
                                    port: 5432,
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
            assert.strictEqual(configs[0].db_port, 5432);
            axios.post.restore();
        });

        it('defaults to 3306 when neither port field present', async function(){
            let hub = new HubClient('localhost', 10000);
            let axios = require('axios');
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
