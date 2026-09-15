// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers reorg fixtures and hooks. One part of reorg_detection.test.js.
const sinon  = require('sinon');
const ServerPoller = require('../../../../../src/server/poller');
const Utility = require('../../../../../src/util');
const { withDbMixins } = require('../../../../helpers/db_mixins.js');

function createMockDb(){
    // Queries read through named Database methods. The real ones are installed for
    // any this fake does not stub, so they still reach doQuery below and every
    // doQuery call count these suites assert keeps counting them.
    return withDbMixins({
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getBlockScopedRows: sinon.stub().resolves([]),
        getTxScopedRows: sinon.stub().resolves([]),
        getActionScopedRows: sinon.stub().resolves([]),
        getEmissionRowsForBlock: sinon.stub().resolves([]),
        getTransactions: sinon.stub().resolves([]),
        getActions: sinon.stub().resolves([]),
        getStatusId: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        beginReadSnapshot: sinon.stub().resolves({ mockSnapshotConn: true }),
        commitReadSnapshot: sinon.stub().resolves(),
        rollbackReadSnapshot: sinon.stub().resolves()
    });
}

function registerHooks(setContext){
    beforeEach(function(){
        let db = createMockDb();
        let broadcaster = { broadcast: sinon.stub(), updateStatus: sinon.stub(), getSubscribers: sinon.stub().returns([]), getSubscriberCount: sinon.stub().returns(0) };
        let log = { recordBlock: sinon.stub().resolves(), pruneFrom: sinon.stub().resolves() };
        let poller = new ServerPoller('bitcoin', 'mainnet', db, broadcaster, log, { BLOCK_POLL_INTERVAL: 100 }, new Utility());
        db.getBlockHashRow.callsFake(async (idx) => ({
            block_index: idx, block_time: idx * 10,
            ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
        }));
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        setContext({ poller, db, broadcaster });
    });
    afterEach(function(){ sinon.restore(); });
}

module.exports = { registerHooks };
