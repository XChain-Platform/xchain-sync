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
const ClientApplier = require('../../../src/client/applier');
const ClientRollback = require('../../../src/client/rollback');
const Utility = require('../../../src/util');
const { withDbMixins } = require('../../helpers/db_mixins.js');

// A decoder-shaped replica whose commit finds no open transaction (Database.commitTransaction returns false).
function decoderDbWithLostTransaction(){
    return withDbMixins({
        dbType: 'decoder',
        doQuery: sinon.stub().resolves([]),
        getBlockHashRow: sinon.stub().resolves(null),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(false),
        rollbackTransaction: sinon.stub().resolves()
    });
}

describe('Replica units refuse a commit that found no open transaction', function(){
    let db;

    beforeEach(function(){
        db = decoderDbWithLostTransaction();
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    it('a live block apply throws instead of reporting the block applied', async function(){
        const applier = new ClientApplier(db, new Utility());
        await assert.rejects(() => applier.applyBlockTransaction({ block_index: 77, data: {} }),
            /block 77: no open transaction at commit; nothing was committed/);
        assert.ok(db.rollbackTransaction.calledOnce);
    });

    it('a dispensers swap throws instead of reporting the table replaced', async function(){
        const applier = new ClientApplier(db, new Utility());
        await assert.rejects(() => applier.applyDispensersReplace([]),
            /dispensers reconcile: no open transaction at commit/);
    });

    it('a decoder reorg rollback throws instead of reporting the rollback completed', async function(){
        const rollback = new ClientRollback(db, new Utility(), undefined, 'regtest');
        await assert.rejects(() => rollback.rollbackDecoder(50),
            /decoder rollback to block 50: no open transaction at commit/);
    });

    it('a commit that returns nothing (a double without the flag) still counts as committed', async function(){
        db.commitTransaction = sinon.stub().resolves();
        const applier = new ClientApplier(db, new Utility());
        await applier.applyBlockTransaction({ block_index: 78, data: {} });
        assert.ok(db.rollbackTransaction.notCalled);
    });
});
