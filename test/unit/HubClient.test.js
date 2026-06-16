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
const HubClient = require('../../src/HubClient');

describe('HubClient', function(){

    let hub;

    beforeEach(function(){
        hub = new HubClient('localhost', 10000);
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    describe('constructor', function(){
        it('builds the correct URL', function(){
            assert.strictEqual(hub.urls[0], 'http://localhost:10000');
        });
    });

    describe('ping', function(){
        it('returns true on successful response', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: true } });
            let result = await hub.ping();
            assert.strictEqual(result, true);
        });

        it('returns false on error', async function(){
            sinon.stub(axios, 'post').rejects(new Error('timeout'));
            let result = await hub.ping();
            assert.strictEqual(result, false);
        });

        it('returns falsy when result is null', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: null } });
            let result = await hub.ping();
            assert.ok(!result);
        });

        it('sends correct JSON-RPC payload', async function(){
            let stub = sinon.stub(axios, 'post').resolves({ data: { result: true } });
            await hub.ping();
            let payload = stub.firstCall.args[1];
            assert.strictEqual(payload.method, 'ping');
            assert.strictEqual(payload.jsonrpc, '2.0');
        });
    });

    describe('getallconfigs', function(){
        it('returns parsed result on success', async function(){
            let mockResult = { bitcoin: { mainnet: {} } };
            sinon.stub(axios, 'post').resolves({ data: { result: mockResult } });
            let result = await hub.getallconfigs();
            assert.deepStrictEqual(result, mockResult);
        });

        it('returns null on error', async function(){
            sinon.stub(axios, 'post').rejects(new Error('fail'));
            let result = await hub.getallconfigs();
            assert.strictEqual(result, null);
        });

        it('returns null when no result in response', async function(){
            sinon.stub(axios, 'post').resolves({ data: {} });
            let result = await hub.getallconfigs();
            assert.strictEqual(result, null);
        });
    });

    describe('getIndexerConfigs', function(){
        it('extracts xchain-indexer entries', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: {
                bitcoin: {
                    mainnet: {
                        'xchain-indexer': {
                            db_host: 'db1', db_port: '3307', name: 'btc_main', user: 'u', pass: 'p'
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
                        'xchain-indexer': { host: 'fallback_host', port: '3308', name: 'ltc', user: 'u', pass: 'p' }
                    }
                }
            }}});
            let configs = await hub.getIndexerConfigs();
            assert.strictEqual(configs[0].db_host, 'fallback_host');
            assert.strictEqual(configs[0].db_port, 3308);
        });

        it('defaults db_host to localhost when neither present', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: {
                doge: { regtest: { 'xchain-indexer': { name: 'd', user: 'u', pass: 'p' } } }
            }}});
            let configs = await hub.getIndexerConfigs();
            assert.strictEqual(configs[0].db_host, 'localhost');
            assert.strictEqual(configs[0].db_port, 3306);
        });

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
                '': { mainnet: { 'xchain-indexer': { name: 'x', user: 'u', pass: 'p' } } }
            }}});
            let configs = await hub.getIndexerConfigs();
            assert.strictEqual(configs.length, 0);
        });

        it('handles multiple chains', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: {
                bitcoin: { mainnet: { 'xchain-indexer': { name: 'b', user: 'u', pass: 'p' } } },
                litecoin: { mainnet: { 'xchain-indexer': { name: 'l', user: 'u', pass: 'p' } } }
            }}});
            let configs = await hub.getIndexerConfigs();
            assert.strictEqual(configs.length, 2);
        });
    });

    describe('getDecoderConfigs', function(){
        it('extracts xchain-decoder entries', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: {
                bitcoin: { mainnet: { 'xchain-decoder': { db_host: 'dh', db_port: '3309', name: 'dec', user: 'u', pass: 'p' } } }
            }}});
            let configs = await hub.getDecoderConfigs();
            assert.strictEqual(configs.length, 1);
            assert.strictEqual(configs[0].dbType, 'decoder');
            assert.strictEqual(configs[0].db_port, 3309);
        });

        it('skips non-object coin/network values defensively', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: {
                bitcoin: 'not-an-object',
                litecoin: { mainnet: 'also-not-an-object' }
            }}});
            let configs = await hub.getDecoderConfigs();
            assert.deepStrictEqual(configs, []);
        });
    });

    describe('constructor — array form', function(){
        it('uses an explicit endpoint array verbatim', function(){
            let h = new HubClient(['https://a:1', 'http://b:2']);
            assert.deepStrictEqual(h.urls, ['https://a:1', 'http://b:2']);
        });

        it('honors https via the legacy port arg', function(){
            let h = new HubClient('host', 'https');
            assert.strictEqual(h.urls[0], 'https://host:10000');
        });

        it('defaults a non-numeric port to 10000', function(){
            let h = new HubClient('host', 'notaport');
            assert.strictEqual(h.urls[0], 'http://host:10000');
        });
    });

    describe('_call multi-endpoint fallback', function(){
        it('falls back to the next endpoint and stickies the good one', async function(){
            let h = new HubClient(['http://bad:1', 'http://good:2']);
            let stub = sinon.stub(axios, 'post');
            stub.withArgs('http://bad:1').rejects({ code: 'ECONNREFUSED' });
            stub.withArgs('http://good:2').resolves({ data: { result: 'ok' } });

            let r = await h._call({});
            assert.strictEqual(r, 'ok');
            assert.strictEqual(h._lastGoodIdx, 1);
            assert.deepStrictEqual(h.lastFailures, ['http://bad:1 → ECONNREFUSED']);

            // Next call starts at the sticky good endpoint.
            stub.resetHistory();
            await h._call({});
            assert.strictEqual(stub.firstCall.args[0], 'http://good:2');
        });

        it('returns null and records failures when every endpoint fails', async function(){
            let h = new HubClient(['http://a:1', 'http://b:2']);
            let stub = sinon.stub(axios, 'post');
            stub.rejects(new Error('down'));
            let r = await h._call({});
            assert.strictEqual(r, null);
            assert.strictEqual(h.lastFailures.length, 2);
        });
    });

    describe('getallconfigs cursor + watermark handling', function(){
        it('records lastSuccessfulFetchAt on a successful bare-map fetch and resets the cursor', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: { bitcoin: { mainnet: {} } } } });
            await hub.getallconfigs();
            assert.strictEqual(typeof hub.lastSuccessfulFetchAt, 'number');
            assert.strictEqual(hub.lastWatermark, 0);
            assert.strictEqual(hub.lastSeq, 0);
        });

        it('unwraps a { configs, seq } payload (no watermark) and resets the cursor', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: {
                configs: { bitcoin: { mainnet: {} } }, seq: 5
            } } });
            let r = await hub.getallconfigs();
            assert.deepStrictEqual(r, { bitcoin: { mainnet: {} } });
            assert.strictEqual(hub.lastSeq, 5);
            assert.strictEqual(hub.lastWatermark, 0);
        });

        it('merges a delta against the cursor it previously sent', async function(){
            let stub = sinon.stub(axios, 'post');
            // First fetch: full tree + watermark advances the cursor.
            stub.onCall(0).resolves({ data: { result: {
                configs: { btc: { main: { 'xchain-indexer': { name: 'a' } } } },
                seq: 1, watermark: 1000
            } } });
            let first = await hub.getallconfigs();
            assert.strictEqual(hub.lastWatermark, 1000);
            assert.ok(first.btc.main['xchain-indexer']);

            // Second fetch: client now sends since_updated_at=1000; hub replies with a delta
            // that adds a module to an existing branch AND introduces a brand-new coin/network.
            stub.onCall(1).resolves({ data: { result: {
                configs: {
                    btc: { main: { 'xchain-decoder': { name: 'b' } } },
                    ltc: { test: { 'xchain-indexer': { name: 'c' } } }
                },
                seq: 2, watermark: 2000
            } } });
            let second = await hub.getallconfigs();
            // Delta merged into the cache: both modules present.
            assert.ok(second.btc.main['xchain-indexer'], 'kept the prior module');
            assert.ok(second.btc.main['xchain-decoder'], 'merged the delta module');
            assert.ok(second.ltc.test['xchain-indexer'], 'created the brand-new coin branch');
            assert.strictEqual(hub.lastWatermark, 2000);
            // The second request echoed the cursor from the first.
            assert.strictEqual(stub.secondCall.args[1].params.since_updated_at, 1000);
        });

        it('treats a watermarked payload as a full tree on the first fetch (no cursor sent yet)', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: {
                configs: { btc: { main: {} } }, seq: 1, watermark: 500
            } } });
            let r = await hub.getallconfigs();
            assert.deepStrictEqual(r, { btc: { main: {} } });
            assert.strictEqual(hub.lastWatermark, 500);
        });
    });

    describe('_parsePort', function(){
        it('parses a numeric primary', function(){
            assert.strictEqual(HubClient._parsePort('3307', undefined), 3307);
        });
        it('uses the fallback when primary is absent/empty', function(){
            assert.strictEqual(HubClient._parsePort('', '3308'), 3308);
            assert.strictEqual(HubClient._parsePort(undefined, 3309), 3309);
        });
        it('preserves a literal 0 (does not fall through to default)', function(){
            assert.strictEqual(HubClient._parsePort(0, 9999), 0);
        });
        it('defaults to 3306 when both are absent', function(){
            assert.strictEqual(HubClient._parsePort(undefined, undefined), 3306);
        });
        it('defaults to 3306 for a non-numeric or negative value', function(){
            assert.strictEqual(HubClient._parsePort('abc', undefined), 3306);
            assert.strictEqual(HubClient._parsePort('-1', undefined), 3306);
        });
    });

    describe('parseEndpoints', function(){
        it('parses a comma-separated HUB_VALIDATORS list, prefixing bare hosts', function(){
            let urls = HubClient.parseEndpoints({ HUB_VALIDATORS: 'http://a:1, b:2 ,' });
            assert.deepStrictEqual(urls, ['http://a:1', 'http://b:2']);
        });
        it('prefixes bare HUB_VALIDATORS hosts with https when configured', function(){
            let urls = HubClient.parseEndpoints({ HUB_VALIDATORS: 'b:2', HUB_PROTOCOL: 'https' });
            assert.deepStrictEqual(urls, ['https://b:2']);
        });
        it('falls back to HUB_API_HOST/HUB_PORT when no validators are set', function(){
            assert.deepStrictEqual(
                HubClient.parseEndpoints({ HUB_API_HOST: 'h', HUB_PORT: 9000 }),
                ['http://h:9000']);
        });
        it('uses https in the fallback path when HUB_PROTOCOL is https', function(){
            assert.deepStrictEqual(
                HubClient.parseEndpoints({ HUB_API_HOST: 'h', HUB_PORT: 9000, HUB_PROTOCOL: 'https' }),
                ['https://h:9000']);
        });
        it('defaults host/port/proto when nothing is configured', function(){
            assert.deepStrictEqual(HubClient.parseEndpoints({}), ['http://localhost:10000']);
        });
    });
});
