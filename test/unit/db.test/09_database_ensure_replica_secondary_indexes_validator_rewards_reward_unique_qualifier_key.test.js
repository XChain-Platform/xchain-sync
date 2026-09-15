// Copyright © 2025–2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

const {
    assert,
    sinon,
    makeDb,
    silenceConsole,
} = require('./support/helpers');

describe('Database.ensureReplicaSecondaryIndexes(): validator_rewards reward_unique qualifier key', function () {
    // Fake query layer that treats only `validator_rewards` as present, so the
    // index-ensure/attests/votes/anchor_actions steps above no-op and the test
    // isolates this migration. idxCols is reward_unique's ordered column list
    // (empty = index absent); hasColumn drives the round_qualifier probe.
    function stubDoQuery(db, idxCols, hasColumn, opts) {
        let calls = [];
        let fake = async (sql, params) => {
            calls.push({ sql, params });
            if (opts && opts.failAddColumn && /ADD COLUMN IF NOT EXISTS/.test(sql)) {
                let e = new Error('refused'); e.errno = 1142; throw e;
            }
            if (/information_schema\.tables/.test(sql)) {
                if (/table_name = 'validator_rewards'/.test(sql)) return [{ table_name: 'validator_rewards' }];
                return [];
            }
            if (/information_schema\.columns/.test(sql))
                return hasColumn ? [{ column_name: 'round_qualifier' }] : [];
            if (/information_schema\.statistics/.test(sql)) {
                if (/index_name = 'reward_unique'/.test(sql)) return idxCols.map(c => ({ column_name: c }));
                return [];
            }
            return [];
        };
        sinon.stub(db, 'doQuery').callsFake(fake);
        sinon.stub(db, 'doQueryStrict').callsFake(fake);
        return calls;
    }

    let staleKey = ['source_id', 'signing_pubkey_id', 'reward_type', 'round_reference'];
    let newKey   = staleKey.concat(['round_qualifier']);

    let db;
    beforeEach(function () { silenceConsole(); db = makeDb('indexer'); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('adds round_qualifier itself and then rebuilds the key in the same pass', async function () {
        // ADD UNIQUE INDEX naming an unknown column is errno 1072 and the whole
        // ALTER is refused, so the column has to land first. Deferring it to the next
        // startup repeats this same order, so the step closes the gap itself.
        let calls = stubDoQuery(db, staleKey, false);
        await db.ensureReplicaSecondaryIndexes();
        let ddl = calls.map(c => c.sql);
        let addAt  = ddl.findIndex(s => /ALTER TABLE `validator_rewards` ADD COLUMN IF NOT EXISTS `round_qualifier`/.test(s));
        let swapAt = ddl.findIndex(s => /validator_rewards` DROP INDEX/.test(s));
        assert.ok(addAt !== -1, 'must add the missing precondition column');
        assert.ok(swapAt !== -1, 'must still rebuild the key once the column is there');
        assert.ok(addAt < swapAt, 'the column must land before the key rebuild names it');
    });

    it('leaves the stale key alone when the column ADD is refused', async function () {
        let calls = stubDoQuery(db, staleKey, false, { failAddColumn: true });
        await db.ensureReplicaSecondaryIndexes();
        assert.ok(!calls.some(c => /validator_rewards` DROP INDEX/.test(c.sql)),
            'a rebuild naming a column that could not be added would be refused too');
    });

});

describe('Database.ensureReplicaSecondaryIndexes(): validator_rewards reward_unique qualifier key', function () {
    // Fake query layer that treats only `validator_rewards` as present, so the
    // index-ensure/attests/votes/anchor_actions steps above no-op and the test
    // isolates this migration. idxCols is reward_unique's ordered column list
    // (empty = index absent); hasColumn drives the round_qualifier probe.
    function stubDoQuery(db, idxCols, hasColumn, opts) {
        let calls = [];
        let fake = async (sql, params) => {
            calls.push({ sql, params });
            if (opts && opts.failAddColumn && /ADD COLUMN IF NOT EXISTS/.test(sql)) {
                let e = new Error('refused'); e.errno = 1142; throw e;
            }
            if (/information_schema\.tables/.test(sql)) {
                if (/table_name = 'validator_rewards'/.test(sql)) return [{ table_name: 'validator_rewards' }];
                return [];
            }
            if (/information_schema\.columns/.test(sql))
                return hasColumn ? [{ column_name: 'round_qualifier' }] : [];
            if (/information_schema\.statistics/.test(sql)) {
                if (/index_name = 'reward_unique'/.test(sql)) return idxCols.map(c => ({ column_name: c }));
                return [];
            }
            return [];
        };
        sinon.stub(db, 'doQuery').callsFake(fake);
        sinon.stub(db, 'doQueryStrict').callsFake(fake);
        return calls;
    }

    let staleKey = ['source_id', 'signing_pubkey_id', 'reward_type', 'round_reference'];
    let newKey   = staleKey.concat(['round_qualifier']);

    let db;
    beforeEach(function () { silenceConsole(); db = makeDb('indexer'); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('leaves an unexpected reward_unique definition alone', async function () {
        // Never guess at a key this migration does not describe.
        let calls = stubDoQuery(db, ['source_id', 'round_reference'], true);
        await db.ensureReplicaSecondaryIndexes();
        assert.ok(!calls.some(c => /validator_rewards` DROP INDEX/.test(c.sql)),
            'only the exact stale four-column key is migrated');
    });

    it('does nothing when reward_unique is absent', async function () {
        // Adding a UNIQUE index to a table that has been running without one can
        // fail on pre-existing duplicates; that is not the drift this heals.
        let calls = stubDoQuery(db, [], true);
        await db.ensureReplicaSecondaryIndexes();
        assert.ok(!calls.some(c => /validator_rewards` (DROP INDEX|ADD UNIQUE)/.test(c.sql)),
            'an absent key is left for the operator, not invented here');
    });

});

describe('Database.ensureReplicaSecondaryIndexes(): validator_rewards reward_unique qualifier key', function () {
    // Fake query layer that treats only `validator_rewards` as present, so the
    // index-ensure/attests/votes/anchor_actions steps above no-op and the test
    // isolates this migration. idxCols is reward_unique's ordered column list
    // (empty = index absent); hasColumn drives the round_qualifier probe.
    function stubDoQuery(db, idxCols, hasColumn, opts) {
        let calls = [];
        let fake = async (sql, params) => {
            calls.push({ sql, params });
            if (opts && opts.failAddColumn && /ADD COLUMN IF NOT EXISTS/.test(sql)) {
                let e = new Error('refused'); e.errno = 1142; throw e;
            }
            if (/information_schema\.tables/.test(sql)) {
                if (/table_name = 'validator_rewards'/.test(sql)) return [{ table_name: 'validator_rewards' }];
                return [];
            }
            if (/information_schema\.columns/.test(sql))
                return hasColumn ? [{ column_name: 'round_qualifier' }] : [];
            if (/information_schema\.statistics/.test(sql)) {
                if (/index_name = 'reward_unique'/.test(sql)) return idxCols.map(c => ({ column_name: c }));
                return [];
            }
            return [];
        };
        sinon.stub(db, 'doQuery').callsFake(fake);
        sinon.stub(db, 'doQueryStrict').callsFake(fake);
        return calls;
    }

    let staleKey = ['source_id', 'signing_pubkey_id', 'reward_type', 'round_reference'];
    let newKey   = staleKey.concat(['round_qualifier']);

    let db;
    beforeEach(function () { silenceConsole(); db = makeDb('indexer'); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('is a no-op for a decoder replica (indexer-only)', async function () {
        let dec  = makeDb('decoder');
        let spy  = sinon.stub(dec, 'doQuery').resolves([]);
        let spy2 = sinon.stub(dec, 'doQueryStrict').resolves([]);
        await dec.ensureReplicaSecondaryIndexes();
        assert.ok(spy.notCalled && spy2.notCalled, 'decoder returns before any schema query');
        await dec.close();
    });
});
