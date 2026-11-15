// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert     = require('assert');
const sinon      = require('sinon');
const ClientSync = require('../../../../src/client/sync');
const { SCHEMA_VERSION } = require('../../../../src/schema/version');
const Utility    = require('../../../../src/util');
const HashVerifier = require('../../../../src/client/hash_verifier');
const { withDbMixins } = require('../../../helpers/db_mixins.js');

function makeSync(){
    let db = {
        dbName:           'test_db',
        dbType:           'indexer',
        getLastBlock:     sinon.stub().resolves(null),
        getBlockHashRow:  sinon.stub().resolves(null),
        doQuery:          sinon.stub().resolves([]),
        getActiveHalt:    sinon.stub().resolves(null),
        getTableCount:    sinon.stub().resolves(0),
        addMissingColumns: sinon.stub().resolves(),
        recordHalt:       sinon.stub().resolves({ block_index: 0 }),
        clearHalt:        sinon.stub().resolves(1)
    };
    let applier = {
        applyBlock:               sinon.stub().resolves(),
        applyFullSnapshot:        sinon.stub().resolves(),
        applyIncrementalSnapshot: sinon.stub().resolves()
    };
    let config = {
        SYNC_SOURCES:          'http://src1:3006',
        VERIFY_HASHES:         false,
        CLIENT_RECONNECT_DELAY: 5000,
        HASH_CONFIRM_TIMEOUT:  5000,
        SNAPSHOT_MAX_CONTENT:  200 * 1024 * 1024,
        WS_MAX_PAYLOAD:        50 * 1024 * 1024,
        MAX_ROLLBACK_DEPTH:    10,
        GAP_LOG_INTERVAL_MS:   30000
    };
    let sync = new ClientSync('bitcoin', 'mainnet', withDbMixins(db), applier,
        { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
    return { sync, db, applier };
}

describe('ClientSync syncLookupTablesPaged', function(){
    let sync, db, rt;
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
        rt = require('../../../../src/schema/replicated_tables');
    });
    afterEach(function(){ sinon.restore(); });

    function page(rows, has_more, max_id){
        return { data: Buffer.from(JSON.stringify({ schema_version: SCHEMA_VERSION.indexer, table: 'index_transactions', max_id, has_more, rows })) };
    }

    it('uses an explicit window tip only for reissuing lookup tables', async function(){
        ({ sync, db } = makeSync());
        sinon.stub(rt, 'getTopology').returns({
            index: ['index_addresses', 'index_tickers', 'index_transactions']
        });
        db.doQuery.resolves([{ m: 0 }]);
        sync.lastAppliedBlock = 100;
        let fetch = sinon.stub(sync, 'fetchLookupPage')
            .resolves(page([], false, 0));

        await sync.syncLookupTablesPaged('http://src:3006', { tip: 105 });

        assert.deepStrictEqual(fetch.getCalls().map(call => [call.args[1], call.args[4]]), [
            ['index_addresses', 105],
            ['index_tickers', 105],
            ['index_transactions', null]
        ]);

        fetch.resetHistory();
        await sync.syncLookupTablesPaged('http://src:3006');
        assert.deepStrictEqual(fetch.getCalls().map(call => call.args[4]), [100, 100, null],
            'ordinary paging still caps reissuing tables at the applied tip');
    });
});
