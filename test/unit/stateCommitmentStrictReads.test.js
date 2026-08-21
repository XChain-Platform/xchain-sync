// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md.
//
// M-17 regression for the db.js helpers the state-commitment path DELEGATES to.
// stateCommitment.js is strict everywhere, but the row sets it hashes are gathered
// by db.js: getBlockLeafRows for block_merkle_root, _applyStakeWeightCap plus the
// getStatusId behind it for stakes_root. Those went through fail-soft doQuery, which
// swallows a NON-transactional query error and answers []. On this path [] is not an
// error signal, it is a wrong answer: a valid-looking root over a truncated (or
// empty) leaf set, persisted into state_tree_roots, which nothing downstream ever
// compares back to the source.
//
// The CONTROL below is the fail-soft branch itself, exercised through plain doQuery
// on the same rejecting connection: it must still answer [], or a green guarded case
// only proves the harness never made a query fail.

const assert   = require('assert');
const sinon    = require('sinon');
const Database = require('../../src/db');

function makeDb() {
    const db = new Database('localhost', 3306, 'idx', 'u', 'p',
        { isNull: (x) => x == null, logError: () => {} }, 'indexer');
    // No transaction open: exactly the state the seed path and the checkpoint
    // forward-follow (ClientSync._oraclePublishSetAt) run in.
    db.transactionConnection = null;
    sinon.stub(db, 'getConnection').resolves({
        query: async () => { throw new Error('ER_LOCK_WAIT_TIMEOUT: transient'); },
        release: async () => {}
    });
    return db;
}

describe('M-17: state-commitment input reads fail closed outside a transaction', function () {

    let db;
    beforeEach(function () { db = makeDb(); });
    afterEach(function () { sinon.restore(); });

    it('CONTROL: plain doQuery still swallows the same failure and answers []', async function () {
        const rows = await db.doQuery('SELECT 1', []);
        assert.deepStrictEqual(rows, [],
            'control must reproduce the fail-soft branch, or the cases below prove nothing');
    });

    it('getBlockLeafRows throws rather than hashing a truncated leaf set', async function () {
        await assert.rejects(() => db.getBlockLeafRows(100), /transient/);
    });

    it('the stake readers throw rather than committing an empty stakes_root', async function () {
        // getStatusId is the first hop, and a swallowed error there returned null,
        // which both readers turn into an empty stake set.
        await assert.rejects(() => db.getStakeWeightsByCapability('anchor', 100, '1', 10, 'BTC', 'regtest'),
            /transient/);
        await assert.rejects(() => db.getStakeWeightsByCapabilityAsOf('anchor', 100, '1', 10, 'BTC', 'regtest'),
            /transient/);
    });

    it('_applyStakeWeightCap throws on its own read, past the getStatusId hop', async function () {
        await assert.rejects(
            () => db._applyStakeWeightCap({ sql: 'SELECT 1', args: [] }, 100, 10, null, null, 'test'),
            /transient/);
    });

    it('getStatusId keeps the fail-soft default for its operational callers', async function () {
        assert.strictEqual(await db.getStatusId('completed'), null,
            'rollback / cooldown-credit callers must not start throwing');
    });
});
