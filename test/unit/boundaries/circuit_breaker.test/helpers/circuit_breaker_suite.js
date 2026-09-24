// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers shared fixtures and hooks. One part of circuit_breaker.test.js.
const sinon = require('sinon');
const Utility = require('../../../../../src/util');
const proxyquire = require('proxyquire');

// We test the circuit breaker logic by constructing a Database instance
// with a stubbed pool. Since the mariadb import is at module level, we
// use proxyquire to inject a mock pool.
function createDatabase(poolStub){
    let mockMariadb = {
        createPool: sinon.stub().returns(poolStub)
    };
    let Database = proxyquire('../../../../../src/db', { 'mariadb': mockMariadb });
    let util = new Utility();
    return new Database('unit-test-fixture', 0, 'testdb', 'user', 'pass', util);
}

function registerHooks(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });
}

module.exports = { createDatabase, registerHooks };
