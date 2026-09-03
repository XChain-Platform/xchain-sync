// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md.
//
// Follower half of the stake-weight ordering binary-collation flag-day (see
// src/stake_weight_collation_activation.js). `source` resolves through
// index_addresses.address, declared utf8_general_ci, and the snapshot RANKS and
// TRUNCATES on it: DENSE_RANK decides which sources survive the source cap,
// ROW_NUMBER which keys survive the per-source cap, and the legacy branch's
// LIMIT keeps the first `limit` rows in that order. The surviving SET is the
// stakes_root preimage this follower recomputes and HALTs on, so the pin has to
// land on BOTH sides at the same height - a one-sided COLLATE edit IS the fork.
//
// These lock the sync side of that contract: OFF is byte-for-byte the legacy
// SQL, ON pins utf8_bin at all four ordering sites, a null coin/network stays
// inert, and the startup collation assertion fails closed on drift. The
// builder's byte-identity with the indexer twin is locked separately by
// rollback-coverage.test.js.

const assert   = require('assert');
const sinon    = require('sinon');
const Database = require('../../src/db');
const swc      = require('../../src/stake_weight_collation_activation');

// A Database whose doQueryStrict captures the emitted SQL instead of running it.
function makeDb() {
    const db = new Database('localhost', 3306, 'idx', 'u', 'p',
        { isNull: (x) => x == null, logError: () => {} }, 'indexer');
    db.transactionConnection = null;
    db._calls = [];
    sinon.stub(db, 'doQueryStrict').callsFake((query, args) => {
        db._calls.push({ query, args });
        return Promise.resolve([]);
    });
    return db;
}

async function capQuery(db, blockIndex, coin, network) {
    await db._applyStakeWeightCap({ sql: 'SELECT 1', args: [] }, blockIndex, 10, coin, network, 'test');
    assert.strictEqual(db._calls.length, 1, 'expected exactly one stake-weight query');
    return db._calls[0].query.replace(/\s+/g, ' ');
}

afterEach(function () { sinon.restore(); });

describe('stake-weight ordering collation gate (follower side)', function () {

    // The inert-on-deploy claim, asserted against a pinned literal rather than a
    // "no COLLATE" match: a whitespace or alias change would also move the
    // stakes_root on a chain where a cap bit, and only a literal catches that.
    it('OFF emits the legacy capped SQL byte-for-byte', function () {
        const off = makeDb()._cappedStakeWeightsSql({ sql: 'SELECT 1', args: ['a'] }, 1000, 64, false);
        const LEGACY = `SELECT r.pubkey AS pubkey, r.source AS source, r.weight AS weight, r._sr AS _sr
                   FROM (
                       SELECT b.pubkey AS pubkey, b.source AS source, b.weight AS weight,
                              DENSE_RANK() OVER (ORDER BY b.source)                        AS _sr,
                              ROW_NUMBER() OVER (PARTITION BY b.source ORDER BY b.pubkey)  AS _kr
                       FROM (SELECT 1) b
                   ) r
                   WHERE r._sr <= ? AND r._kr <= ?
                   ORDER BY r.source, r.pubkey`;
        assert.strictEqual(off.sql, LEGACY,
            'the gate-off string must reproduce the deployed SQL exactly, or deploying this arms a fork');
        assert.deepStrictEqual(off.args, ['a', 1001, 64]);
    });

    it('an omitted binCollation argument is OFF (no accidental arming by an unupdated caller)', function () {
        assert.doesNotMatch(makeDb()._cappedStakeWeightsSql({ sql: 'SELECT 1', args: [] }, 1000, 64).sql, /COLLATE/);
    });

    it('ON pins utf8_bin at all three ordering sites of the capped builder', function () {
        const on = makeDb()._cappedStakeWeightsSql({ sql: 'SELECT 1', args: [] }, 1000, 64, true).sql.replace(/\s+/g, ' ');
        assert.match(on, /DENSE_RANK\(\) OVER \(ORDER BY b\.source COLLATE utf8_bin\)/);
        assert.match(on, /ROW_NUMBER\(\) OVER \(PARTITION BY b\.source COLLATE utf8_bin ORDER BY b\.pubkey COLLATE utf8_bin\)/);
        assert.match(on, /ORDER BY r\.source COLLATE utf8_bin, r\.pubkey COLLATE utf8_bin$/);
    });

    it('regtest (armed from genesis) pins utf8_bin through _applyStakeWeightCap', async function () {
        assert.match(await capQuery(makeDb(), 5, 'BTC', 'regtest'), /ORDER BY b\.source COLLATE utf8_bin/);
    });

    it('mainnet is inert: no COLLATE at any height, on either branch', async function () {
        assert.doesNotMatch(await capQuery(makeDb(), 10 ** 9, 'BTC', 'mainnet'), /COLLATE/);
        assert.doesNotMatch(await capQuery(makeDb(), 1, 'BTC', 'mainnet'), /COLLATE/);
    });

    // Sync has no per-chain config, so its gate is only as good as what the caller
    // passes; an unlabelled call must not silently arm a binary ordering.
    it('a null coin/network stays inert', async function () {
        const q = await capQuery(makeDb(), 10 ** 9, null, null);
        assert.doesNotMatch(q, /COLLATE/);
        assert.strictEqual(swc.isStakeWeightBinCollationActive(10 ** 9, null, null), false);
    });

    describe('legacy LIMIT branch (fourth ordering site)', function () {
        it('is gated on too', async function () {
            const db = makeDb();
            const swqCap = require('../../src/swq_source_cap_activation');
            sinon.stub(swqCap, 'isSwqSourceCapActive').returns(false);
            assert.match(await capQuery(db, 5, 'BTC', 'regtest'),
                /ORDER BY source COLLATE utf8_bin, pubkey COLLATE utf8_bin LIMIT \?$/);
        });

        it('stays unpinned on an unarmed chain', async function () {
            const db = makeDb();
            const swqCap = require('../../src/swq_source_cap_activation');
            sinon.stub(swqCap, 'isSwqSourceCapActive').returns(false);
            const q = await capQuery(db, 10 ** 9, 'BTC', 'mainnet');
            assert.match(q, /ORDER BY source, pubkey LIMIT \?$/);
            assert.doesNotMatch(q, /COLLATE/);
        });
    });
});

describe('startup consensus-ordering collation assertion (follower side)', function () {

    function dbWithCollation(collationByColumn) {
        const db = new Database('localhost', 3306, 'idx', 'u', 'p',
            { isNull: (x) => x == null, logError: () => {} }, 'indexer');
        db.transactionConnection = null;
        sinon.stub(db, 'getConnection').resolves({
            query: async (_sql, args) => {
                const v = collationByColumn[args[2]];
                return v === undefined ? [] : [{ collation: v }];
            },
            release: async () => {}
        });
        return db;
    }

    afterEach(function () { sinon.restore(); });

    it('passes on the declared collation', async function () {
        await dbWithCollation({ address: 'utf8_general_ci', pubkey: 'utf8_general_ci' })
            ._assertConsensusOrderingCollations();
    });

    // MariaDB 10.6+/MySQL 8 report the explicit utf8mb3_* name for the same
    // collation the source's src/sql spells `utf8`. Rejecting that would turn this
    // guard into a fleet outage on a routine server upgrade.
    it('accepts the utf8mb3 alias of the same collation', async function () {
        await dbWithCollation({ address: 'utf8mb3_general_ci', pubkey: 'UTF8MB3_GENERAL_CI' })
            ._assertConsensusOrderingCollations();
        assert.strictEqual(Database.normalizeCollationName('utf8mb3_general_ci'),
                           Database.normalizeCollationName('utf8_general_ci'));
        assert.notStrictEqual(Database.normalizeCollationName('utf8mb4_general_ci'),
                              Database.normalizeCollationName('utf8_general_ci'));
        assert.notStrictEqual(Database.normalizeCollationName('utf8_unicode_ci'),
                              Database.normalizeCollationName('utf8_general_ci'));
    });

    it('fails closed on a drifted collation, naming the column and both collations', async function () {
        await assert.rejects(
            () => dbWithCollation({ address: 'utf8_unicode_ci', pubkey: 'utf8_general_ci' })._assertConsensusOrderingCollations(),
            (e) => {
                assert.match(e.message, /index_addresses\.address/);
                assert.match(e.message, /utf8_unicode_ci/);
                assert.match(e.message, /utf8_general_ci/);
                assert.match(e.message, /stakes_root/, 'the operator must be told what consensus depends on it');
                return true;
            });
    });

    it('catches drift on index_pubkeys.pubkey too, not only the first column', async function () {
        await assert.rejects(
            () => dbWithCollation({ address: 'utf8_general_ci', pubkey: 'utf8mb4_general_ci' })._assertConsensusOrderingCollations(),
            /index_pubkeys\.pubkey/);
    });

    // A decoder-shaped replica carries neither table, and a client replica has not
    // replicated the schema yet on its first pass; neither may halt the service.
    it('skips silently when the table/column is absent or non-character', async function () {
        await dbWithCollation({})._assertConsensusOrderingCollations();
        await dbWithCollation({ address: null, pubkey: null })._assertConsensusOrderingCollations();
    });

    // An assertion nothing calls protects nothing, and sync has no migration runner to
    // hang it off, so pin both startup branches of _discoverChains (client and server).
    it('is wired into both startup branches of SyncService._discoverChains', function () {
        const src = require('fs').readFileSync(require('path').resolve(__dirname, '../../src/SyncService.js'), 'utf8');
        const body = src.match(/async _discoverChains\(\)\{([\s\S]*?)\n    \}/);
        assert.ok(body, '_discoverChains not found');
        const hits = body[1].match(/_assertConsensusOrderingCollations\(\)/g) || [];
        assert.strictEqual(hits.length, 2,
            'both the client and server startup branches must assert the collation');
    });
});
