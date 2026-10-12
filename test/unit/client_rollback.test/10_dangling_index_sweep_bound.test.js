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
const ClientRollback = require('../../../src/client/rollback');
const Utility = require('../../../src/util');

const PUBKEYS_SWEEP = /^DELETE FROM pubkeys WHERE address_id > \? AND address_id NOT IN/;
const MARKETS_SWEEP = /^DELETE FROM markets WHERE \(tick1_id <> \? AND tick1_id > \?/;

function createMockDb(maxIds){
    return {
        doQuery: sinon.stub().callsFake(async (query) => {
            if(query === 'SELECT MAX(id) AS max_id FROM index_addresses') return [{ max_id: maxIds.addresses }];
            if(query === 'SELECT MAX(id) AS max_id FROM index_tickers') return [{ max_id: maxIds.tickers }];
            return [];
        }),
        getFirstActionIndex: sinon.stub().resolves(500),
        getStatusId: sinon.stub().resolves(null),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves()
    };
}

function callIndex(db, re){
    return db.doQuery.getCalls().findIndex(c => re.test(c.args[0]));
}

function findCall(db, re){
    return db.doQuery.getCalls().find(c => re.test(c.args[0]));
}

describe('ClientRollback dangling index-reference sweep bound', function(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    it('bounds the pubkeys and markets sweeps by the surviving lookup MAX(id) @regression', async function(){
        const db = createMockDb({ addresses: 500, tickers: 40 });
        await new ClientRollback(db, new Utility(), undefined, 'regtest').rollback(100);

        const pubkeys = findCall(db, PUBKEYS_SWEEP);
        const markets = findCall(db, MARKETS_SWEEP);
        assert.ok(pubkeys, 'the pubkeys sweep must carry the address_id > ? bound');
        assert.ok(markets, 'the markets sweep must carry the tick_id > ? bound');
        assert.deepStrictEqual(pubkeys.args[1], [500]);
        assert.deepStrictEqual(markets.args[1], [0, 40, 0, 40]);
        assert.strictEqual(db.commitTransaction.calledOnce, true);
    });

    it('reads each MAX(id) after the index lookup delete it bounds', async function(){
        const db = createMockDb({ addresses: 500, tickers: 40 });
        await new ClientRollback(db, new Utility(), undefined, 'regtest').rollback(100);

        const addrDelete = callIndex(db, /^DELETE FROM `index_addresses` WHERE block_index >= \?/);
        const tickDelete = callIndex(db, /^DELETE FROM `index_tickers` WHERE block_index >= \?/);
        const addrMax = callIndex(db, /^SELECT MAX\(id\) AS max_id FROM index_addresses$/);
        const tickMax = callIndex(db, /^SELECT MAX\(id\) AS max_id FROM index_tickers$/);
        assert.ok(addrDelete >= 0 && tickDelete >= 0 && addrMax >= 0 && tickMax >= 0);
        assert.ok(addrMax > addrDelete, 'a MAX(id) read before the delete keeps rolled-back ids under the bound');
        assert.ok(tickMax > tickDelete, 'a MAX(id) read before the delete keeps rolled-back ids under the bound');
        assert.ok(addrMax < callIndex(db, PUBKEYS_SWEEP));
        assert.ok(tickMax < callIndex(db, MARKETS_SWEEP));
    });

    it('treats an empty lookup table as a 0 bound', async function(){
        const db = createMockDb({ addresses: null, tickers: null });
        await new ClientRollback(db, new Utility(), undefined, 'regtest').rollback(100);

        assert.deepStrictEqual(findCall(db, PUBKEYS_SWEEP).args[1], [0]);
        assert.deepStrictEqual(findCall(db, MARKETS_SWEEP).args[1], [0, 0, 0, 0]);
    });
});
