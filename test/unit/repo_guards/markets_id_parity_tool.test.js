// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// bin/markets-id-parity.js compares markets.id by pair NAME across sync sources.

const assert = require('assert');
const sinon  = require('sinon');
const zlib   = require('zlib');
const axios  = require('axios');
const tool = require('../../../bin/markets-id-parity.js');

function source(tickers, markets){
    return tool.idsByPair(markets, new Map(Object.entries(tickers)));
}

function gz(obj){ return { data: zlib.gzipSync(JSON.stringify(obj)) }; }

describe('bin/markets-id-parity fetch', function(){
    afterEach(function(){ sinon.restore(); });
    const opts = { chain: 'BTC', network: 'mainnet', headers: {} };

    it('reads markets at the tip and pages every ticker', async function(){
        let get = sinon.stub(axios, 'get');
        get.withArgs('http://a/status/indexer/BTC/mainnet').resolves({ data: Buffer.from('{"block_height":90}') });
        get.withArgs('http://a/snapshot/indexer/BTC/mainnet/since/90?skip_lookups=1')
            .resolves(gz({ tables: { markets: [{ id: 1, tick1_id: 1, tick2_id: 2 }] } }));
        get.withArgs('http://a/snapshot-rows/indexer/BTC/mainnet/index_tickers?after_id=0&limit=50000')
            .resolves(gz({ has_more: true, max_id: 1, rows: [{ id: 1, tick: 'XCP' }] }));
        get.withArgs('http://a/snapshot-rows/indexer/BTC/mainnet/index_tickers?after_id=1&limit=50000')
            .resolves(gz({ has_more: false, max_id: 2, rows: [{ id: 2, tick: 'PEPE' }] }));
        let got = await tool.fetchSource('http://a', opts);
        assert.strictEqual(got.tip, 90);
        assert.deepStrictEqual([...tool.idsByPair(got.markets, got.tickers)], [['XCP/PEPE', '1']]);
    });

    it('refuses a source with no polled height instead of pulling since/0', async function(){
        let get = sinon.stub(axios, 'get').resolves({ data: { block_height: null } });
        await assert.rejects(() => tool.fetchSource('http://a', opts), /no block_height/);
        assert.strictEqual(get.callCount, 1, 'no snapshot is requested');
    });
});

describe('bin/markets-id-parity', function(){
    it('keys pairs by ticker name so differing ticker ids still match', function(){
        let a = source({ 1: 'XCP', 2: 'PEPE' }, [{ id: 5, tick1_id: 1, tick2_id: 2 }]);
        let b = source({ 7: 'XCP', 9: 'PEPE' }, [{ id: 5, tick1_id: '7', tick2_id: '9' }]);
        let r = tool.compareSources({ a, b });
        assert.strictEqual(r.pairs, 1);
        assert.deepStrictEqual(r.differing, []);
        assert.deepStrictEqual(r.missing, []);
        assert.deepStrictEqual(r.collisions, []);
    });

    it('names a native-coin side by its coin id', function(){
        let a = source({ 1: 'XCP' }, [{ id: 3, tick1_id: 1, tick2_id: 0, coin2_id: 4 }]);
        assert.deepStrictEqual([...a.keys()], ['XCP/coin#4']);
    });

    it('reports differing ids and the id that names two pairs', function(){
        let t = { 1: 'XCP', 2: 'PEPE', 3: 'RARE' };
        let a = source(t, [{ id: 1, tick1_id: 1, tick2_id: 2 }, { id: 2, tick1_id: 1, tick2_id: 3 }]);
        let b = source(t, [{ id: 2, tick1_id: 1, tick2_id: 2 }, { id: 1, tick1_id: 1, tick2_id: 3 }]);
        let r = tool.compareSources({ a, b });
        assert.strictEqual(r.differing.length, 2);
        assert.deepStrictEqual(r.collisions.map(c => c.id), ['1', '2']);
        let report = tool.renderReport({ chain: 'BTC', network: 'mainnet', sources: ['a', 'b'] },
            { a: { tip: 9, markets: [1, 2] }, b: { tip: 9, markets: [1, 2] } }, r);
        assert.strictEqual(report.agree, false);
        assert.match(report.text, /NOT reproducible/);
    });

    it('reports a pair only one source holds', function(){
        let a = source({ 1: 'XCP', 2: 'PEPE' }, [{ id: 1, tick1_id: 1, tick2_id: 2 }]);
        let b = source({ 1: 'XCP', 2: 'PEPE' }, []);
        let r = tool.compareSources({ a, b });
        assert.deepStrictEqual(r.missing, [{ pair: 'XCP/PEPE', ids: { a: '1', b: null } }]);
    });

    it('refuses fewer than two sources', function(){
        assert.throws(() => tool.parseArgs(['--chain', 'BTC', '--network', 'mainnet'], { SYNC_SOURCES: 'http://a' }),
            /two sources/);
        let o = tool.parseArgs(['--chain', 'BTC', '--network', 'mainnet'], { SYNC_SOURCES: 'http://a/, http://b' });
        assert.deepStrictEqual(o.sources, ['http://a', 'http://b']);
    });
});
