// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// ClientSync.verifyAgainstSource compares state_hash across sources when both carry one.

const assert     = require('assert');
const sinon      = require('sinon');
const axios      = require('axios');
const ClientSync = require('../../../../src/client/sync');
const Utility    = require('../../../../src/util');
const HashVerifier = require('../../../../src/client/hash_verifier');
const { withDbMixins } = require('../../../helpers/db_mixins.js');

const BASE = { ledger_hash: 'lh1', actions_hash: 'ah1', contract_hash: 'ch1' };

function makeSync(haltOnDivergence, localState){
    let db = {
        dbName: 'test_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(Object.assign({ block_height: 100 }, BASE, { state_hash: localState })),
        doQuery: sinon.stub().resolves([]),
        getActiveHalt: sinon.stub().resolves(null),
        getTableCount: sinon.stub().resolves(0),
        recordHalt: sinon.stub().resolves({ block_index: 0 })
    };
    let config = {
        SYNC_SOURCES: 'http://src1:3006', VERIFY_HASHES: true, HALT_ON_DIVERGENCE: haltOnDivergence,
        CLIENT_RECONNECT_DELAY: 5000, HASH_CONFIRM_TIMEOUT: 5000, MAX_ROLLBACK_DEPTH: 10
    };
    let applier = { applyBlock: sinon.stub().resolves(), applyFullSnapshot: sinon.stub().resolves() };
    return new ClientSync('bitcoin', 'mainnet', withDbMixins(db), applier,
        { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
}

function remoteStatus(state){
    let data = Object.assign({ block_height: 100, table_counts: null }, BASE);
    if(state !== undefined) data.state_hash = state;
    sinon.stub(axios, 'get').resolves({ data });
}

describe('ClientSync: verifyAgainstSource compares state_hash', function(){
    beforeEach(function(){ sinon.stub(console, 'log'); sinon.stub(console, 'warn'); sinon.stub(console, 'error'); });
    afterEach(function(){ sinon.restore(); });

    it('reports diverge when only state_hash differs at the same height', async function(){
        let sync = makeSync(false, 'S-local');
        remoteStatus('S-remote');
        assert.strictEqual(await sync.verifyAgainstSource('http://src1:3006', 100), 'diverge');
    });

    it('halts on a state_hash-only divergence under HALT_ON_DIVERGENCE', async function(){
        let sync = makeSync(true, 'S-local');
        let halt = sinon.stub(sync, 'haltOnDivergence').resolves();
        remoteStatus('S-remote');
        assert.strictEqual(await sync.verifyAgainstSource('http://src1:3006', 100), 'halted');
        assert.deepStrictEqual(halt.firstCall.args[1], [{ field: 'state_hash', a: 'S-local', b: 'S-remote' }]);
    });

    it('agrees and logs a skip when the source publishes no state_hash', async function(){
        let sync = makeSync(true, 'S-local');
        remoteStatus(undefined);
        assert.strictEqual(await sync.verifyAgainstSource('http://src1:3006', 100), 'agree');
        let warned = console.warn.getCalls().map(c => String(c.args[0]));
        assert.ok(warned.some(m => m.indexOf('state_hash not compared') !== -1 && m.indexOf('absent-remote') !== -1));
    });

    it('agrees when both state hashes match', async function(){
        let sync = makeSync(true, 'S1');
        remoteStatus('S1');
        assert.strictEqual(await sync.verifyAgainstSource('http://src1:3006', 100), 'agree');
    });
});
