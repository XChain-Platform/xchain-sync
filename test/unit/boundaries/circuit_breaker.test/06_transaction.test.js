// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers transaction connection bypass. One part of circuit_breaker.test.js.
const assert = require('assert');
const sinon  = require('sinon');
const { createDatabase, registerHooks } = require('./helpers/circuit_breaker_suite');

describe('Boundary: Circuit Breaker', function(){
    let db, pool;
    registerHooks();

    describe('transaction connection bypass', function(){
        it('returns transactionConnection when set (bypasses pool)', async function(){
            let txConn = { release: sinon.stub() };
            pool = {
                getConnection: sinon.stub().rejects(new Error('should not be called')),
                end: sinon.stub()
            };
            db = createDatabase(pool);
            db.transactionConnection = txConn;

            let result = await db.getConnection();
            assert.strictEqual(result, txConn);
            assert.strictEqual(pool.getConnection.called, false);
        });
    });
});
