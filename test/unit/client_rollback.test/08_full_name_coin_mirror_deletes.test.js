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

function createMockDb(){
    return {
        doQuery: sinon.stub().resolves([]),
        getFirstActionIndex: sinon.stub().resolves(500),
        getStatusId: sinon.stub().resolves(null),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves()
    };
}

// Return every reorg DELETE issued against one table.
function deletesOf(db, table){
    return db.doQuery.getCalls().filter(c =>
        typeof c.args[0] === 'string' && c.args[0].includes('DELETE FROM ' + table + ' WHERE'));
}

// Production builds ClientRollback from the hub's cfg.coin, which is the FULL NAME
// ('bitcoin'), while the source indexer writes source_chain / a_chain / b_chain /
// src_chain and gates the price_snapshots prune in TICKER form ('BTC'). The mirror
// deletes must bind the ticker or they match no row on any production replica.
describe('ClientRollback full-name coin @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    for(const name of ['bitcoin', 'Bitcoin']){
        it('binds the BTC ticker in every hub-mirror delete when built with ' + name, async function(){
            let db = createMockDb();
            await new ClientRollback(db, new Utility(), name, 'regtest').rollback(100);

            let snaps = deletesOf(db, 'price_snapshots');
            assert.strictEqual(snaps.length, 1, 'the BTC replica must prune orphaned price_snapshots');
            assert.ok(snaps[0].args[0].includes("reference_chain = 'BTC' AND reference_block >= ?"));
            assert.deepStrictEqual(snaps[0].args[1], [100]);

            assert.deepStrictEqual(deletesOf(db, 'oracle_prices')[0].args[1], ['BTC', 500]);
            assert.deepStrictEqual(deletesOf(db, 'cross_chain_calls')[0].args[1], ['BTC', 500]);
            assert.deepStrictEqual(deletesOf(db, 'cross_chain_matches')[0].args[1], ['BTC', 500, 'BTC', 500]);
            assert.deepStrictEqual(deletesOf(db, 'bridge_transfers')[0].args[1], ['BTC', 500]);
        });
    }

    it('binds the LTC ticker and issues no price_snapshots prune when built with litecoin', async function(){
        let db = createMockDb();
        await new ClientRollback(db, new Utility(), 'litecoin', 'regtest').rollback(100);

        assert.strictEqual(deletesOf(db, 'price_snapshots').length, 0,
            'reference_block is a BTC anchor height, so only the BTC replica may prune by it');
        assert.deepStrictEqual(deletesOf(db, 'oracle_prices')[0].args[1], ['LTC', 500]);
        assert.deepStrictEqual(deletesOf(db, 'cross_chain_calls')[0].args[1], ['LTC', 500]);
        assert.deepStrictEqual(deletesOf(db, 'cross_chain_matches')[0].args[1], ['LTC', 500, 'LTC', 500]);
        assert.deepStrictEqual(deletesOf(db, 'bridge_transfers')[0].args[1], ['LTC', 500]);
    });

    it('keeps the legacy no-coin path and the unknown-coin refusal unchanged', function(){
        assert.strictEqual(new ClientRollback(createMockDb(), new Utility(), undefined, 'regtest').coin, undefined);
        assert.throws(() => new ClientRollback(createMockDb(), new Utility(), 'XRP', 'regtest'), /unrecognized coin "XRP"/);
    });
});
