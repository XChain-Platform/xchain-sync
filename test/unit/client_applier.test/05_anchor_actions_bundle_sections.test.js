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
const ClientApplier = require('../../../src/client/applier');
const Utility = require('../../../src/util');
const { SCHEMA_VERSION } = require('../../../src/schema/version');
const balanceHelpers = require('../../../src/db/balance_helpers');
const { withDbMixins } = require('../../helpers/db_mixins.js');

function createMockDb(){
    return withDbMixins({
        doQuery: sinon.stub().resolves([]),
        getBlockHashRow: sinon.stub().resolves(null),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves(),
        truncateTable: sinon.stub().resolves()
    });
}

// ANCHOR v7 puts N rows per action_index in anchor_actions (one per bundle
// section). Two properties of the apply path have to hold for that, and both are
// easy to break by "tidying" the table sets in the constructor.
describe('ClientApplier: anchor_actions bundle sections', function(){
    let applier, db, util;

    beforeEach(function(){
        db = createMockDb();
        util = new Utility();
        applier = new ClientApplier(db, util, 'DOGE', 'regtest');
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('inserts every section of a bundle, naming section_index in the write', async function(){
        let sections = [0, 1, 2].map(i => ({
            action_index: 41, section_index: i, version: 7,
            chain: ['BTC', 'DOGE', 'LTC'][i], network: 'regtest', block_index: 900 + i
        }));
        await applier.insertRows('anchor_actions', sections);
        assert.ok(db.doQuery.calledOnce, 'one batched INSERT for the three rows');
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(/`section_index`/.test(sql),
            'section_index must be named in the INSERT; without it every section lands on the DEFAULT 0 ' +
            'and sections 1..N-1 collide with section 0 on the composite primary key');
        assert.strictEqual((sql.match(/\(\?/g) || []).length, 3, 'all three sections are written');
    });

    it('leaves anchor_actions on a plain INSERT, so a section collision cannot pass silently', function(){
        // INSERT IGNORE here would turn a replica still carrying the OLD
        // single-column PRIMARY KEY (action_index) into one that silently keeps
        // only section 0 and drops the rest: a permanently short, permanently
        // wrong consensus table with no divergence signal. The loud ER_DUP_ENTRY
        // is the wanted behavior; Database.ensureReplicaSecondaryIndexes widens
        // the key so it never fires in the first place.
        assert.ok(!applier.ignoreTables.has('anchor_actions'),
            'anchor_actions must not use INSERT IGNORE');
        assert.ok(!applier.upsertFullDumpTables.has('anchor_actions'),
            'anchor_actions is action-scoped, not a re-dumped mutable aggregate');
    });
});
