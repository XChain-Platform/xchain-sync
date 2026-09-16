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
const axios  = require('axios');
const ClientSync = require('../../../../src/client/sync');
const Utility = require('../../../../src/util');
const HashVerifier = require('../../../../src/client/hash_verifier');

function createMockDb(){
    return {
        dbName: 'test_db',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        // start() reads the durable halt table first; a clean (no-halt) read lets the
        // normal bootstrap/catch-up flow proceed. The halt check fails CLOSED, so
        // this must resolve rather than be absent (an absent/erroring read holds idle).
        getActiveHalt: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([])
    };
}

function createMockApplier(){
    return {
        applyBlock: sinon.stub().resolves(),
        applyFullSnapshot: sinon.stub().resolves(),
        applyIncrementalSnapshot: sinon.stub().resolves()
    };
}

function createMockRollback(){
    return {
        rollback: sinon.stub().resolves()
    };
}

function registerClientSyncHooks(assignState){
    beforeEach(function(){
        let db = createMockDb();
        let applier = createMockApplier();
        let rollback = createMockRollback();
        let hashVerifier = new HashVerifier();
        let config = {
            SYNC_SOURCES: 'http://source1:3006,http://source2:3006',
            VERIFY_HASHES: true,
            CLIENT_RECONNECT_DELAY: 5000,
            HASH_CONFIRM_TIMEOUT: 5000
        };
        let util = new Utility();
        let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
        assignState({ sync, db, applier, rollback, hashVerifier, config, util });
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });
}

module.exports = { assert, sinon, axios, ClientSync, createMockDb, registerClientSyncHooks };
