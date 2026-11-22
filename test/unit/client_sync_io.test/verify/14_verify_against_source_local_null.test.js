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
const sinon = require('sinon');
const axios = require('axios');
const ClientSync = require('../../../../src/client/sync');
const HashVerifier = require('../../../../src/client/hash_verifier');
const Utility = require('../../../../src/util');
const { withDbMixins } = require('../../../helpers/db_mixins.js');

function makeSync(localHashes){
    let db = withDbMixins({
        dbType: 'indexer',
        getBlockHashRow: sinon.stub().resolves(localHashes),
        getTableCount: sinon.stub().resolves(0),
        doQuery: sinon.stub().resolves([]),
        getActiveHalt: sinon.stub().resolves(null)
    });
    let config = {
        SYNC_SOURCES: 'http://source:3006', VERIFY_RECOMPUTE: true,
        HALT_ON_DIVERGENCE: true, MAX_ROLLBACK_DEPTH: 10,
        SNAPSHOT_MAX_CONTENT: 1024 * 1024, WS_MAX_PAYLOAD: 1024 * 1024
    };
    return new ClientSync('bitcoin', 'mainnet', db, {}, {}, new HashVerifier(), config, new Utility());
}

describe('ClientSync verifyAgainstSource unresolved local hashes', function(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(axios, 'get').resolves({ data: {
            block_height: 100, ledger_hash: 'lh', actions_hash: 'ah',
            contract_hash: 'ch', table_counts: null
        }});
    });
    afterEach(function(){ sinon.restore(); });

    it('returns unresolved for each null consensus field without recompute or halt', async function(){
        for(const field of ['ledger_hash', 'actions_hash', 'contract_hash']){
            let local = { ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch' };
            local[field] = null;
            let sync = makeSync(local);
            sinon.stub(sync, 'verifyRecompute').resolves([]);
            sinon.stub(sync, 'haltOnDivergence').resolves();

            assert.strictEqual(await sync.verifyAgainstSource(sync.sources[0], 100), 'unresolved');
            assert.strictEqual(sync.verifyRecompute.called, false, field + ' must skip recompute');
            assert.strictEqual(sync.haltOnDivergence.called, false, field + ' must not halt');
        }
    });

    it('still runs the table-count and content parity checks on an unresolved local hash', async function(){
        axios.get.resolves({ data: {
            block_height: 100, ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch',
            table_counts: { blocks: 1 }
        }});
        let sync = makeSync({ ledger_hash: null, actions_hash: 'ah', contract_hash: 'ch' });
        sinon.stub(sync, 'verifyRecompute').resolves([]);
        sinon.stub(sync, 'haltOnDivergence').resolves();
        sinon.stub(sync, 'remoteCountsAtHeight').returns(true);
        sinon.stub(sync, 'verifyTableCounts').resolves([]);
        sinon.stub(sync, 'verifyTableContentParity').resolves();
        sinon.stub(sync, 'verifyTokenFoldParity').resolves();

        assert.strictEqual(await sync.verifyAgainstSource(sync.sources[0], 100), 'unresolved');
        assert.strictEqual(sync.verifyTableCounts.calledOnce, true, 'count check must run');
        assert.strictEqual(sync.verifyTableContentParity.calledOnce, true, 'content parity must run');
        assert.strictEqual(sync.verifyTokenFoldParity.calledOnce, true, 'token fold parity must run');
        assert.strictEqual(sync.verifyRecompute.called, false);
        assert.strictEqual(sync.haltOnDivergence.called, false);
    });
});
