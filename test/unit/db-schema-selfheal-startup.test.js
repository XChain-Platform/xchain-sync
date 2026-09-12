// Copyright © 2025-2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

// The follower's schema self-heal runs as two startup steps in a fixed order: the
// column step (ensureReplicatedColumns) and then the key-rebuild step
// (ensureReplicaSecondaryIndexes). The rebuilds for anchor_actions and
// validator_rewards each NAME a column the replica may not have. If only the
// source-DDL pass supplies that column, the rebuild cannot run in the startup
// that reaches it: on the client topology that pass runs later (ClientSync's
// /schema fetch) and never at all on a replica holding a durable halt, so the
// rebuild defers to a next startup that repeats the same order.
//
// These tests drive a STATEFUL fake replica through repeated startups: the deferred
// shape must converge on the first one and go idle on the next.

const assert = require('assert');
const sinon  = require('sinon');
const Database = require('../../src/db');

function makeUtil() {
    return {
        isNull:     (v) => v === null || v === undefined,
        throwError: (msg) => { throw new Error(msg); },
        sleep:      sinon.stub().resolves(),
        logError:   sinon.stub()
    };
}

// A replica schema the fake query layer reads AND writes: an ALTER issued by the code
// under test mutates it, so the next startup sees the converged shape rather than a
// frozen answer.
function deferredReplica() {
    return {
        anchor_actions: {
            columns: ['action_index', 'version', 'chain', 'block_index'],
            indexes: { PRIMARY: ['action_index'] }
        },
        validator_rewards: {
            columns: ['id', 'source_id', 'signing_pubkey_id', 'reward_type', 'round_reference'],
            indexes: { reward_unique: ['source_id', 'signing_pubkey_id', 'reward_type', 'round_reference'] }
        }
    };
}

// Table / column / index names reach the query layer either inlined into the SQL or as
// bound parameters, depending on the probe; read whichever form this statement used.
function nameFrom(sql, params, inlineRe, paramIndex) {
    let m = inlineRe.exec(sql);
    if (m) return m[1];
    let p = params || [];
    return p[paramIndex] !== undefined ? p[paramIndex] : null;
}

function fakeQueryLayer(state, opts) {
    let ddl = [];
    let fake = async (sql, params) => {
        if (/^\s*ALTER TABLE/i.test(sql)) {
            if (opts && opts.failAddColumn && /ADD COLUMN/i.test(sql)) {
                let e = new Error('ALTER refused'); e.errno = 1142; throw e;
            }
            ddl.push(sql);
            applyDdl(state, sql);
            return [];
        }
        if (/information_schema\.tables/i.test(sql)) {
            let t = nameFrom(sql, params, /table_name = '(\w+)'/i, 1);
            return state[t] ? [{ table_name: t }] : [];
        }
        if (/information_schema\.columns/i.test(sql)) {
            let t = nameFrom(sql, params, /table_name = '(\w+)'/i, 1);
            let c = nameFrom(sql, params, /column_name = '(\w+)'/i, 2);
            if (!state[t]) return [];
            if (c === null) return state[t].columns.map(x => ({ COLUMN_NAME: x, column_name: x }));
            if (!state[t].columns.includes(c)) return [];
            return [{ COLUMN_NAME: c, column_name: c, IS_NULLABLE: 'YES', EXTRA: 'auto_increment' }];
        }
        if (/information_schema\.statistics/i.test(sql)) {
            let t = nameFrom(sql, params, /table_name = '(\w+)'/i, 1);
            let i = nameFrom(sql, params, /index_name = '(\w+)'/i, 2);
            if (!state[t] || !state[t].indexes[i]) return [];
            return state[t].indexes[i].map(x => ({ column_name: x, COLUMN_NAME: x, NON_UNIQUE: 1 }));
        }
        return [];
    };
    return { fake, ddl };
}

// Apply the DDL shapes this heal emits back onto the fake schema.
function applyDdl(state, sql) {
    let table = /ALTER TABLE `(\w+)`/.exec(sql);
    if (!table || !state[table[1]]) return;
    let t = state[table[1]];

    let add = /ADD COLUMN (?:IF NOT EXISTS )?`(\w+)`/.exec(sql);
    if (add && !t.columns.includes(add[1])) t.columns.push(add[1]);

    let pk = /ADD PRIMARY KEY \(([^)]*)\)/.exec(sql);
    if (pk) t.indexes.PRIMARY = pk[1].split(',').map(s => s.trim().replace(/`/g, ''));

    let uq = /ADD UNIQUE INDEX `(\w+)` \(([^)]*)\)/.exec(sql);
    if (uq) t.indexes[uq[1]] = uq[2].split(',').map(s => s.trim().replace(/`/g, ''));
}

// One follower startup, in SyncService._discoverChains order.
async function startup(db) {
    await db.ensureReplicatedColumns();
    await db.ensureReplicaSecondaryIndexes();
}

describe('Database schema self-heal: one startup completes columns then key rebuilds', function () {
    let db, state, layer;

    beforeEach(function () {
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
        state = deferredReplica();
        db = new Database('localhost', 3306, 'replica_db', 'u', 'p', makeUtil(), 'indexer');
        layer = fakeQueryLayer(state);
        sinon.stub(db, 'doQuery').callsFake(layer.fake);
        sinon.stub(db, 'doQueryStrict').callsFake(layer.fake);
    });

    afterEach(async function () { sinon.restore(); await db.close(); });

    it('converges a follower sitting in the deferred state on its next start', async function () {
        await startup(db);
        assert.deepStrictEqual(state.anchor_actions.indexes.PRIMARY, ['action_index', 'section_index']);
        assert.ok(state.anchor_actions.columns.includes('section_index'));
        assert.deepStrictEqual(state.validator_rewards.indexes.reward_unique,
            ['source_id', 'signing_pubkey_id', 'reward_type', 'round_reference', 'round_qualifier']);
        assert.ok(state.validator_rewards.columns.includes('round_qualifier'));
    });

    it('adds each column BEFORE the rebuild that names it', async function () {
        await startup(db);
        let addPk  = layer.ddl.findIndex(s => /anchor_actions` ADD COLUMN (?:IF NOT EXISTS )?`section_index`/.test(s));
        let swapPk = layer.ddl.findIndex(s => /anchor_actions` DROP PRIMARY KEY/.test(s));
        let addUq  = layer.ddl.findIndex(s => /validator_rewards` ADD COLUMN (?:IF NOT EXISTS )?`round_qualifier`/.test(s));
        let swapUq = layer.ddl.findIndex(s => /validator_rewards` DROP INDEX `reward_unique`/.test(s));
        assert.ok(addPk !== -1 && swapPk !== -1 && addPk < swapPk, 'section_index lands before the key swap');
        assert.ok(addUq !== -1 && swapUq !== -1 && addUq < swapUq, 'round_qualifier lands before the key rebuild');
    });

    it('never defers the work to a next startup', async function () {
        await startup(db);
        let warned = console.warn.getCalls().map(c => String(c.args[0] || ''));
        assert.ok(!warned.some(m => /column self-heal must run first|re-run on the next startup|could not be added/.test(m)),
            'a deferral line in any form is what repeated at six consecutive starts');
    });

    it('is idle on the second and third startup', async function () {
        await startup(db);
        let after = layer.ddl.length;
        await startup(db);
        await startup(db);
        assert.strictEqual(layer.ddl.length, after, 'a converged replica must issue no further DDL');
    });

    // The key-rebuild step alone, as SyncService also calls it: a refused ADD must leave
    // the stale key untouched (the rebuild naming that column would be refused too).
    it('keeps the stale keys when the column ADD is refused, and says so', async function () {
        sinon.restore();
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
        state = deferredReplica();
        db = new Database('localhost', 3306, 'replica_db', 'u', 'p', makeUtil(), 'indexer');
        layer = fakeQueryLayer(state, { failAddColumn: true });
        sinon.stub(db, 'doQuery').callsFake(layer.fake);
        sinon.stub(db, 'doQueryStrict').callsFake(layer.fake);

        await db.ensureReplicaSecondaryIndexes();
        assert.deepStrictEqual(state.anchor_actions.indexes.PRIMARY, ['action_index']);
        assert.deepStrictEqual(state.validator_rewards.indexes.reward_unique,
            ['source_id', 'signing_pubkey_id', 'reward_type', 'round_reference']);
        let warned = console.warn.getCalls().map(c => String(c.args[0] || ''));
        assert.ok(warned.some(m => /section_index could not be added/.test(m)));
        assert.ok(warned.some(m => /round_qualifier could not be added/.test(m)));
    });
});
