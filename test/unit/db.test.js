// Copyright © 2025–2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

const {
    assert,
    sinon,
    Database,
    FIXTURE_HOST,
    FIXTURE_PORT,
    makeUtil,
    makeDb,
    silenceConsole,
} = require('./db.test/support/helpers');

describe('Database.close()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); });

    it('calls pool.end() and resolves', async function () {
        let ended = false;
        sinon.stub(db.pool, 'end').callsFake(async () => { ended = true; });
        await db.close();
        assert.ok(ended);
    });

    it('swallows a pool.end() error', async function () {
        sinon.stub(db.pool, 'end').rejects(new Error('boom'));
        // should not throw
        await db.close();
    });
});

describe('Database constructor: dbType default', function () {
    afterEach(async function () { sinon.restore(); });

    it('defaults dbType to "indexer" when not provided', async function () {
        silenceConsole();
        let db = new Database(FIXTURE_HOST, FIXTURE_PORT, 'db', 'u', 'p', makeUtil());
        assert.strictEqual(db.dbType, 'indexer');
        await db.close();
    });
});
