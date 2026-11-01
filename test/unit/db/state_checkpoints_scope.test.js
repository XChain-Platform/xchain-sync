// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// The /checkpoint routes read one chain's rows out of a table that holds every chain's.
//
// The hub mirrors BTC, LTC and DOGE checkpoints into every indexer DB, so an unscoped
// "newest checkpoint" on the litecoin/testnet source was the DOGE row at ~68M, and the
// replica halted on checkpoint-binding-mismatch. These cases pin both halves of the
// scope: each read binds (ticker, network) ahead of its own params, and each route
// folds its :chain segment to the ticker the rows carry before calling the read. The
// real-MariaDB proof over a mixed table is test/integration/state_checkpoints/scope_real.test.js.

const assert = require('assert');
const http   = require('http');
const sinon  = require('sinon');
const { createApp }   = require('../../../src/api');
const checkpointReads = require('../../../src/db/state_checkpoints');

const KEY = 'unit-test-key';

// A served LTC testnet row: what the scoped reads should hand back to the litecoin route.
const LTC_ROW = {
    chain: 'LTC', network: 'testnet', block_index: 4950000, block_hash: 'aa', ledger_hash: 'bb',
    actions_hash: 'cc', contract_hash: 'dd', checkpoint_seq: 77, snapshot_block: 150000,
    state_root: null, state_root_version: null, block_merkle_root: null, block_merkle_version: null,
    validator_signatures: '[]'
};

// A Database stand-in carrying the REAL mixin methods, so the route reaches the actual
// SQL and its bound params; only doQuery, the driver edge, is stubbed.
function fakeDb(rows){
    return Object.assign({ doQuery: sinon.stub().resolves(rows) }, checkpointReads);
}

function provider(db){
    return {
        isReady:                () => true,
        getHubConfigAgeSeconds: () => 0,
        getChains:              () => [],
        getDatabase:            () => db,
        getBroadcaster:         () => null,
        getSnapshotBuilder:     () => null,
        getPoller:              () => null,
        getTransparencyLog:     () => null,
        getClientSync:          () => null
    };
}

const cfg = () => ({
    SYNC_MODE: 'server',
    SYNC_API_KEY: KEY,
    TRUST_PROXY: false,
    SNAPSHOT_RATE_FULL: 1000,
    SNAPSHOT_RATE_INCR: 1000,
    TRANSPARENCY_RATE_LIMIT: 1000
});

describe('state_checkpoints reads are scoped to one chain and network: the SQL each read binds', function(){

    it('getLatestCheckpoint filters on chain and network before taking the newest row', async function(){
        let db = fakeDb([]);
        await db.getLatestCheckpoint('LTC', 'testnet');
        let [sql, params] = db.doQuery.firstCall.args;
        assert.ok(/WHERE chain=\? AND network=\? ORDER BY block_index DESC, checkpoint_seq DESC LIMIT 1$/.test(sql), sql);
        assert.deepStrictEqual(params, ['LTC', 'testnet']);
    });

    it('findCheckpointsInRange filters the outer rows and correlates the MAX(checkpoint_seq) subquery on chain and network', async function(){
        let db = fakeDb([]);
        await db.findCheckpointsInRange('LTC', 'testnet', 4870000, 4950000, 2000);
        let [sql, params] = db.doQuery.firstCall.args;
        assert.ok(/WHERE sc\.chain=\? AND sc\.network=\? AND sc\.block_index >= \? AND sc\.block_index <= \?/.test(sql), sql);
        assert.ok(/WHERE s2\.chain = sc\.chain AND s2\.network = sc\.network AND s2\.block_index = sc\.block_index\)/.test(sql), sql);
        assert.deepStrictEqual(params, ['LTC', 'testnet', 4870000, 4950000, 2000]);
    });

    it('getCheckpointAtHeight filters on chain, network and the exact height', async function(){
        let db = fakeDb([]);
        await db.getCheckpointAtHeight('LTC', 'testnet', 4950000);
        let [sql, params] = db.doQuery.firstCall.args;
        assert.ok(/WHERE chain=\? AND network=\? AND block_index=\? ORDER BY checkpoint_seq DESC LIMIT 1$/.test(sql), sql);
        assert.deepStrictEqual(params, ['LTC', 'testnet', 4950000]);
    });
});

describe('state_checkpoints reads are scoped to one chain and network: the /checkpoint routes pass the route chain as its ticker', function(){
    let server, base, db;

    before(async function(){
        db = fakeDb([LTC_ROW]);
        server = http.createServer(createApp(provider(db), cfg()));
        await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
        base = 'http://127.0.0.1:' + server.address().port;
    });

    after(function(done){
        server.closeAllConnections();
        server.close(done);
    });

    beforeEach(function(){ db.doQuery.resetHistory(); });

    async function get(path){
        let res = await fetch(base + path, { headers: { authorization: 'Bearer ' + KEY } });
        return { status: res.status, body: await res.json() };
    }

    it('/latest on litecoin/testnet reads the LTC testnet rows and serves that row', async function(){
        let { status, body } = await get('/checkpoint/indexer/litecoin/testnet/latest');
        assert.strictEqual(status, 200);
        assert.deepStrictEqual(db.doQuery.firstCall.args[1], ['LTC', 'testnet']);
        assert.strictEqual(body.chain, 'LTC');
        assert.strictEqual(body.block_index, 4950000);
    });

    it('/range on litecoin/testnet binds LTC and testnet ahead of the bounds and the cap', async function(){
        let { status, body } = await get('/checkpoint/indexer/litecoin/testnet/range?from=4870000&to=4950000');
        assert.strictEqual(status, 200);
        assert.deepStrictEqual(db.doQuery.firstCall.args[1], ['LTC', 'testnet', 4870000, 4950000, 2000]);
        assert.deepStrictEqual(body.checkpoints.map((c) => c.chain), ['LTC']);
    });

    it('/:height on litecoin/testnet binds LTC and testnet ahead of the height', async function(){
        let { status } = await get('/checkpoint/indexer/litecoin/testnet/4950000');
        assert.strictEqual(status, 200);
        assert.deepStrictEqual(db.doQuery.firstCall.args[1], ['LTC', 'testnet', 4950000]);
    });

    it('folds every route form of a chain (full name, ticker, any case) onto one ticker', async function(){
        for(let [chain, ticker] of [['litecoin', 'LTC'], ['LTC', 'LTC'], ['ltc', 'LTC'],
                                    ['dogecoin', 'DOGE'], ['bitcoin', 'BTC'], ['Bitcoin', 'BTC']]){
            db.doQuery.resetHistory();
            await get('/checkpoint/indexer/' + chain + '/TESTNET/latest');
            assert.deepStrictEqual(db.doQuery.firstCall.args[1], [ticker, 'testnet'], chain);
        }
    });
});
