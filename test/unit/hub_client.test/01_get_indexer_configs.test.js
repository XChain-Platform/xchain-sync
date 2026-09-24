// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const sinon  = require('sinon');
const axios  = require('axios');
const HubClient = require('../../../src/hub/client');

describe('HubClient', function(){

    let hub;

    beforeEach(function(){
        hub = new HubClient('localhost', 10000);
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    describe('getIndexerConfigs', function(){
        it('extracts xchain-indexer entries', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: {
                bitcoin: {
                    mainnet: {
                        'xchain-indexer': {
                            db_host: 'db1', db_port: '3307', name: 'btc_main', user: 'fixture-user', pass: 'fixture-pass'
                        }
                    }
                }
            }}});
            let configs = await hub.getIndexerConfigs();
            assert.strictEqual(configs.length, 1);
            assert.strictEqual(configs[0].coin, 'bitcoin');
            assert.strictEqual(configs[0].network, 'mainnet');
            assert.strictEqual(configs[0].db_host, 'db1');
            assert.strictEqual(configs[0].db_port, 3307);
            assert.strictEqual(configs[0].db_name, 'btc_main');
        });

        it('falls back db_host to host', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: {
                litecoin: {
                    testnet: {
                        'xchain-indexer': { host: 'fallback_host', port: '3308', name: 'ltc', user: 'fixture-user', pass: 'fixture-pass' }
                    }
                }
            }}});
            let configs = await hub.getIndexerConfigs();
            assert.strictEqual(configs[0].db_host, 'fallback_host');
            assert.strictEqual(configs[0].db_port, 3308);
        });

        it('defaults db_host to 127.0.0.1 when neither present', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: {
                doge: { regtest: { 'xchain-indexer': { name: 'd', user: 'fixture-user', pass: 'fixture-pass' } } }
            }}});
            let configs = await hub.getIndexerConfigs();
            assert.strictEqual(configs[0].db_host, '127.0.0.1');
            assert.strictEqual(configs[0].db_port, 3306);
        });
    });
});

describe('HubClient', function(){

    let hub;

    beforeEach(function(){
        hub = new HubClient('localhost', 10000);
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    describe('getIndexerConfigs', function(){
        it('returns empty array when hub returns null', async function(){
            sinon.stub(axios, 'post').resolves({ data: {} });
            let configs = await hub.getIndexerConfigs();
            assert.deepStrictEqual(configs, []);
        });

        it('skips networks without xchain-indexer module', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: {
                bitcoin: {
                    mainnet: { 'xchain-decoder': { host: 'x' } }
                }
            }}});
            let configs = await hub.getIndexerConfigs();
            assert.strictEqual(configs.length, 0);
        });

        it('skips empty coin keys', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: {
                '': { mainnet: { 'xchain-indexer': { name: 'x', user: 'fixture-user', pass: 'fixture-pass' } } }
            }}});
            let configs = await hub.getIndexerConfigs();
            assert.strictEqual(configs.length, 0);
        });

        it('handles multiple chains', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: {
                bitcoin: { mainnet: { 'xchain-indexer': { name: 'b', user: 'fixture-user', pass: 'fixture-pass' } } },
                litecoin: { mainnet: { 'xchain-indexer': { name: 'l', user: 'fixture-user', pass: 'fixture-pass' } } }
            }}});
            let configs = await hub.getIndexerConfigs();
            assert.strictEqual(configs.length, 2);
        });
    });
});
