// Copyright © 2025–2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

const {
    assert,
    sinon,
    makeDb,
    silenceConsole,
} = require('./support/helpers');

describe('Database.ensureReplicaSecondaryIndexes(): votes append-only migration', function () {
    // Fake doQuery that treats only `votes` as present and all other tables
    // (index_tickers/index_addresses/attests) as absent, so the pre-existing
    // ensure/relax steps no-op and the test isolates the votes migration. The
    // votes index checks resolve from votesIndexes: keys are index names.
    function stubDoQuery(db, votesIndexes) {
        let calls = [];
        sinon.stub(db, 'doQuery').callsFake(async (sql, params) => {
            calls.push({ sql, params });
            if (/information_schema\.tables/.test(sql)) {
                // The votes/attests existence checks inline the table name; the
                // index-ensure checks pass it as a bound param. Only `votes` is present.
                if (/table_name = 'votes'/.test(sql)) return [{ table_name: 'votes' }];
                return [];
            }
            if (/information_schema\.statistics/.test(sql)) {
                if (/poll_voter_action_choice'/.test(sql))
                    return votesIndexes.poll_voter_action_choice ? [{ index_name: 'poll_voter_action_choice' }] : [];
                if (/poll_voter_choice'/.test(sql))
                    return votesIndexes.poll_voter_choice ? [{ index_name: 'poll_voter_choice' }] : [];
                return [];
            }
            return [];
        });
        return calls;
    }

    let db;
    beforeEach(function () { silenceConsole(); db = makeDb('indexer'); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('creates the widened key on a replica missing both (defensive)', async function () {
        let calls = stubDoQuery(db, { poll_voter_choice: false, poll_voter_action_choice: false });
        await db.ensureReplicaSecondaryIndexes();
        let ddl = calls.map(c => c.sql);
        assert.ok(!ddl.some(s => /DROP INDEX `poll_voter_choice`/.test(s)), 'nothing to drop');
        assert.ok(ddl.some(s => /CREATE UNIQUE INDEX `poll_voter_action_choice` ON `votes`/.test(s)),
            'still ensures the append-only key exists');
    });

    it('is a no-op for a decoder replica (indexer-only)', async function () {
        let dec = makeDb('decoder');
        let spy = sinon.stub(dec, 'doQuery').resolves([]);
        await dec.ensureReplicaSecondaryIndexes();
        assert.ok(spy.notCalled, 'decoder returns before any schema query');
        await dec.close();
    });
});


// A replica that bootstrapped before the indexer's
// 2026-08-28-anchor-actions-section-index-pk migration carries anchor_actions on
// the single-column PRIMARY KEY (action_index). The column self-heal adds
// section_index but never touches the key, so the first ANCHOR v7 bundle (N rows
// sharing one action_index) collides on ER_DUP_ENTRY, which is outside the
// {1146, 1054} schema-heal set and therefore wedges the replica permanently.
describe('Database.ensureReplicaSecondaryIndexes(): anchor_actions bundle-section primary key', function () {
    // Fake doQuery that treats only `anchor_actions` as present, so the
    // index-ensure/attests/votes steps above no-op and the test isolates this
    // migration. pk is the PRIMARY's column list; hasColumn drives the
    // section_index existence probe.
    function stubDoQuery(db, pk, hasColumn, opts) {
        let calls = [];
        let fake = async (sql, params) => {
            calls.push({ sql, params });
            if (opts && opts.failAddColumn && /ADD COLUMN IF NOT EXISTS/.test(sql)) {
                let e = new Error('refused'); e.errno = 1142; throw e;
            }
            if (/information_schema\.tables/.test(sql)) {
                if (/table_name = 'anchor_actions'/.test(sql)) return [{ table_name: 'anchor_actions' }];
                return [];
            }
            if (/information_schema\.columns/.test(sql))
                return hasColumn ? [{ column_name: 'section_index' }] : [];
            if (/information_schema\.statistics/.test(sql)) {
                if (/index_name = 'PRIMARY'/.test(sql)) return pk.map(c => ({ column_name: c }));
                return [];
            }
            return [];
        };
        sinon.stub(db, 'doQuery').callsFake(fake);
        sinon.stub(db, 'doQueryStrict').callsFake(fake);
        return calls;
    }

    let db;
    beforeEach(function () { silenceConsole(); db = makeDb('indexer'); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('widens a stale single-column PRIMARY KEY to (action_index, section_index)', async function () {
        let calls = stubDoQuery(db, ['action_index'], true);
        await db.ensureReplicaSecondaryIndexes();
        let ddl = calls.map(c => c.sql);
        assert.ok(ddl.some(s => /ALTER TABLE `anchor_actions` DROP PRIMARY KEY, ADD PRIMARY KEY \(`action_index`, `section_index`\)/.test(s)),
            'must drop and re-add the primary key in one ALTER');
    });

    it('is a no-op when the composite key is already in place (idempotent)', async function () {
        let calls = stubDoQuery(db, ['action_index', 'section_index'], true);
        await db.ensureReplicaSecondaryIndexes();
        assert.ok(!calls.some(c => /anchor_actions` DROP PRIMARY KEY/.test(c.sql)),
            'a replica already on the composite key must not be re-altered');
    });

});

describe('Database.ensureReplicaSecondaryIndexes(): anchor_actions bundle-section primary key', function () {
    // Fake doQuery that treats only `anchor_actions` as present, so the
    // index-ensure/attests/votes steps above no-op and the test isolates this
    // migration. pk is the PRIMARY's column list; hasColumn drives the
    // section_index existence probe.
    function stubDoQuery(db, pk, hasColumn, opts) {
        let calls = [];
        let fake = async (sql, params) => {
            calls.push({ sql, params });
            if (opts && opts.failAddColumn && /ADD COLUMN IF NOT EXISTS/.test(sql)) {
                let e = new Error('refused'); e.errno = 1142; throw e;
            }
            if (/information_schema\.tables/.test(sql)) {
                if (/table_name = 'anchor_actions'/.test(sql)) return [{ table_name: 'anchor_actions' }];
                return [];
            }
            if (/information_schema\.columns/.test(sql))
                return hasColumn ? [{ column_name: 'section_index' }] : [];
            if (/information_schema\.statistics/.test(sql)) {
                if (/index_name = 'PRIMARY'/.test(sql)) return pk.map(c => ({ column_name: c }));
                return [];
            }
            return [];
        };
        sinon.stub(db, 'doQuery').callsFake(fake);
        sinon.stub(db, 'doQueryStrict').callsFake(fake);
        return calls;
    }

    let db;
    beforeEach(function () { silenceConsole(); db = makeDb('indexer'); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('adds section_index itself and then swaps the key in the same pass', async function () {
        // ADD PRIMARY KEY on an unknown column is errno 1072 and the whole ALTER is
        // refused, so the column has to land first. Deferring it to the next startup
        // repeats this same order, so the step closes the gap itself.
        let calls = stubDoQuery(db, ['action_index'], false);
        await db.ensureReplicaSecondaryIndexes();
        let ddl = calls.map(c => c.sql);
        let addAt  = ddl.findIndex(s => /ALTER TABLE `anchor_actions` ADD COLUMN IF NOT EXISTS `section_index`/.test(s));
        let swapAt = ddl.findIndex(s => /anchor_actions` DROP PRIMARY KEY/.test(s));
        assert.ok(addAt !== -1, 'must add the missing precondition column');
        assert.ok(swapAt !== -1, 'must still widen the key once the column is there');
        assert.ok(addAt < swapAt, 'the column must land before the key rebuild names it');
    });

    it('leaves the stale key alone when the column ADD is refused', async function () {
        let calls = stubDoQuery(db, ['action_index'], false, { failAddColumn: true });
        await db.ensureReplicaSecondaryIndexes();
        assert.ok(!calls.some(c => /anchor_actions` DROP PRIMARY KEY/.test(c.sql)),
            'a rebuild naming a column that could not be added would be refused too');
    });

});

describe('Database.ensureReplicaSecondaryIndexes(): anchor_actions bundle-section primary key', function () {
    // Fake doQuery that treats only `anchor_actions` as present, so the
    // index-ensure/attests/votes steps above no-op and the test isolates this
    // migration. pk is the PRIMARY's column list; hasColumn drives the
    // section_index existence probe.
    function stubDoQuery(db, pk, hasColumn, opts) {
        let calls = [];
        let fake = async (sql, params) => {
            calls.push({ sql, params });
            if (opts && opts.failAddColumn && /ADD COLUMN IF NOT EXISTS/.test(sql)) {
                let e = new Error('refused'); e.errno = 1142; throw e;
            }
            if (/information_schema\.tables/.test(sql)) {
                if (/table_name = 'anchor_actions'/.test(sql)) return [{ table_name: 'anchor_actions' }];
                return [];
            }
            if (/information_schema\.columns/.test(sql))
                return hasColumn ? [{ column_name: 'section_index' }] : [];
            if (/information_schema\.statistics/.test(sql)) {
                if (/index_name = 'PRIMARY'/.test(sql)) return pk.map(c => ({ column_name: c }));
                return [];
            }
            return [];
        };
        sinon.stub(db, 'doQuery').callsFake(fake);
        sinon.stub(db, 'doQueryStrict').callsFake(fake);
        return calls;
    }

    let db;
    beforeEach(function () { silenceConsole(); db = makeDb('indexer'); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('leaves an unexpected primary key alone', async function () {
        // Never guess at a key this migration does not describe.
        let calls = stubDoQuery(db, ['id'], true);
        await db.ensureReplicaSecondaryIndexes();
        assert.ok(!calls.some(c => /anchor_actions` DROP PRIMARY KEY/.test(c.sql)),
            'only the exact stale (action_index) key is migrated');
    });
});


// A replica that bootstrapped before the indexer's
// 2026-08-24-validator-rewards-round-qualifier migration carries the OLD
// four-column UNIQUE reward_unique. sync runs no migrations and the column
// self-heal never touches indexes, so the key stays narrow while the applier
// INSERT IGNOREs validator_rewards: two distinct archive rewards differing only
// in round_qualifier collapse into one row with nothing to catch it. The index
// keeps the same NAME under both schemas, so detection must read the column list.
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

    it('rebuilds a stale four-column reward_unique with round_qualifier appended', async function () {
        let calls = stubDoQuery(db, staleKey, true);
        await db.ensureReplicaSecondaryIndexes();
        let ddl = calls.map(c => c.sql);
        assert.ok(ddl.some(s => /ALTER TABLE `validator_rewards` DROP INDEX `reward_unique`, ADD UNIQUE INDEX `reward_unique` \(`source_id`, `signing_pubkey_id`, `reward_type`, `round_reference`, `round_qualifier`\)/.test(s)),
            'must drop and re-add reward_unique in one ALTER so the table is never keyless');
    });

    it('is a no-op when the five-column key is already in place (idempotent)', async function () {
        let calls = stubDoQuery(db, newKey, true);
        await db.ensureReplicaSecondaryIndexes();
        assert.ok(!calls.some(c => /validator_rewards` DROP INDEX/.test(c.sql)),
            'a replica already on the qualifier key must not be re-altered');
    });

});
