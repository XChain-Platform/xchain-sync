// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const {
    assert, sinon, axios, ClientSync, createMockDb
} = require('../client_sync.test/support');
const Utility = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');

describe('startup incremental catch-up 404 log level', function(){
    let sync, warn, error;

    beforeEach(function(){
        let db = createMockDb();
        db.getLastBlock.resolves(100);
        sync = new ClientSync('bitcoin', 'mainnet', db, { applyIncrementalSnapshot: sinon.stub() },
            { rollback: sinon.stub() }, new HashVerifier(), { SYNC_SOURCES: 'http://source1:3006' }, new Utility());
        sync.lastAppliedBlock = 100;
        sinon.stub(console, 'log');
        sinon.stub(console, 'info');
        warn = sinon.stub(console, 'warn');
        error = sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    it('logs a 404 at warn on one line and never at error', async function(){
        let err = new Error('Request failed with status code 404');
        err.response = { status: 404 };
        sinon.stub(axios, 'get').rejects(err);

        await sync.runIncrementalCatchUp();

        assert.strictEqual(error.called, false, 'a 404 must not reach error');
        let lines = warn.getCalls().map(c => c.args.join(' ')).filter(l => l.includes('catch-up'));
        assert.strictEqual(lines.length, 1);
        assert.strictEqual(lines[0].includes('\n'), false);
        assert.strictEqual(sync.lastAppliedBlock, 100);
    });

    it('still logs a non-404 failure at error', async function(){
        let err = new Error('Request failed with status code 500');
        err.response = { status: 500 };
        sinon.stub(axios, 'get').rejects(err);
        sinon.stub(sync, 'noteUpsertDuplicateKey').resolves(false);
        sinon.stub(sync, 'healSchemaIfStale').resolves(false);

        await sync.runIncrementalCatchUp();

        assert.strictEqual(error.called, true);
    });
});
