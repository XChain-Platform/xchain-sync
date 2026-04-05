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
            assert.strictEqual(hub.url, 'http://localhost:10000');
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
});
