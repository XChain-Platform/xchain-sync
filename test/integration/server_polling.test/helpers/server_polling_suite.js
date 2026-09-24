// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Provides the polling fixtures and hooks. One part of server_polling.test.js.
const assert   = require('assert');
const sinon    = require('sinon');
const setup    = require('../../helpers/setup');
const testDb   = require('../../helpers/testDb');
const fixtures = require('../../helpers/fixtures');
const ServerPoller = require('../../../../src/server/poller');

let sourceDb, poller, broadcaster, transparencyLog, config;

function installHooks() {
    before(async function() {
        await setup.globalSetup();
    });
    after(async function() {
        await setup.globalTeardown();
    });
    beforeEach(async function() {
        sourceDb = setup.getSourceDb();
        await testDb.truncateAll(sourceDb);
        broadcaster = {
            broadcast: sinon.stub(),
            updateStatus: sinon.stub(),
            getSubscriberCount: sinon.stub().returns(0)
        };
        transparencyLog = {
            recordBlock: sinon.stub().resolves(),
            pruneFrom:   sinon.stub().resolves()
        };
        config = { BLOCK_POLL_INTERVAL: 100 };
        poller = new ServerPoller('bitcoin', 'mainnet', sourceDb, broadcaster, transparencyLog, config, testDb.util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function() {
        sinon.restore();
    });
}

// Same build with the probe removed from the db object, which is the pre-fix
// query-every-table path verbatim.
// getNonEmptyActionScopedTables lives on TestDatabase's PROTOTYPE, so it is
// shadowed with an own `undefined` rather than deleted: a delete removes nothing
// and the comparison would silently be probe-against-probe, which passes while
// proving nothing.
async function buildUnprobed(blockIndex) {
    sourceDb.getNonEmptyActionScopedTables = undefined;
    try {
        assert.strictEqual(typeof sourceDb.getNonEmptyActionScopedTables, 'undefined');
        return await poller.buildBlockPayload(blockIndex);
    } finally {
        delete sourceDb.getNonEmptyActionScopedTables;
    }
}

module.exports = {
    assert,
    sinon,
    testDb,
    fixtures,
    get sourceDb() { return sourceDb; },
    get poller() { return poller; },
    get broadcaster() { return broadcaster; },
    get transparencyLog() { return transparencyLog; },
    installHooks,
    buildUnprobed
};
