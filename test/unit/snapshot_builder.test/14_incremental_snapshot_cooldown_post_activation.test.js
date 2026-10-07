// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.
//
// Once UNSTAKE_COOLDOWN_COMPLETION_ACTION is active, a maturity writes its refund
// credit and escrow release under a synthetic UNSTAKE action minted AT the maturity
// block, so the incremental snapshot's action_index cursor carries both rows. The
// unstake-keyed matured join then matches nothing, and each row must ship once.

const { assert, sinon, SnapshotBuilder, Utility, createMockDb } = require('./helpers/support');

const REFUND  = { action_index: 120, address_id: 7, tick_id: 1, amount: '100' };
const RELEASE = { action_index: 120, address_id: 7, tick_id: 1, amount: '-100' };

// An indexer source whose matured-cooldown joins return `joined` per table.
function sourceDb(joined){
    let db = createMockDb();
    db.dbType = 'indexer';
    db.getStatusId = sinon.stub().resolves(3);
    db.doQuery.callsFake(async (sql) => {
        if(/FROM escrows e JOIN unstakes u/.test(sql)) return joined.escrows;
        if(/FROM credits c JOIN unstakes u/.test(sql)) return joined.credits;
        return [];
    });
    return db;
}

describe('SnapshotBuilder: post-activation cooldown maturity in an incremental window', function(){
    let builder;
    beforeEach(function(){ builder = new SnapshotBuilder(new Utility()); });
    afterEach(function(){ sinon.restore(); });

    // Select one table the way streamIncrementalSnapshot does, with the
    // action_index cursor read stubbed to the synthetic action's row.
    async function select(db, table, cursorRow){
        sinon.stub(builder, 'selectIndexerRows').resolves([Object.assign({}, cursorRow)]);
        return builder.selectIncrementalRows(db, table, { dbType: 'indexer', sinceBlock: 40, lastBlock: 60, conn: null });
    }

    it('ships the synthetic-action credit and release once when the matured join is empty @regression', async function(){
        let db = sourceDb({ credits: [], escrows: [] });
        assert.deepStrictEqual(await select(db, 'credits', REFUND), [REFUND]);
        builder.selectIndexerRows.restore();
        assert.deepStrictEqual(await select(db, 'escrows', RELEASE), [RELEASE]);
        assert.ok(db.doQuery.getCalls().some(c => /FROM escrows e JOIN unstakes u/.test(c.args[0])),
            'the matured join must still run (pre-activation history needs it)');
    });

    it('ships each row once when both channels return the same synthetic-action row @regression', async function(){
        let db = sourceDb({ credits: [Object.assign({}, REFUND)], escrows: [Object.assign({}, RELEASE)] });
        assert.deepStrictEqual(await select(db, 'credits', REFUND), [REFUND]);
        builder.selectIndexerRows.restore();
        assert.deepStrictEqual(await select(db, 'escrows', RELEASE), [RELEASE]);
    });
});
