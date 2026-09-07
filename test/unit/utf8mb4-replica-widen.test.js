// Copyright © 2025-2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC - https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.
//
// The replica half of the raw-wire-field utf8mb4 widen.
//
// The source indexer converges through a dated migration. sync runs none: a replica's
// tables are copied from the source's SHOW CREATE TABLE at bootstrap and addMissingColumns
// only ever ADDs a column, never retypes one. So the moment the widened ORIGIN accepts a
// 4-byte character (a contract whose source carries an emoji, an EXECUTE method name, a
// VOTE quorum), every replica built before the migration halts applying that block with
// errno 1366 while the source runs on. ensureReplicaUtf8mb4Columns is what closes that,
// and these are its guards: it must widen what is narrow, skip what is already wide, skip
// what is absent, and never take startup down when the DB says no.

const assert = require('assert');
const sinon  = require('sinon');

const Database      = require('../../src/db');
const utf8mb4Columns = require('../../src/utf8mb4Columns');

function makeDb(dbType){
    const util = { isNull: (v) => v === null || v === undefined, logError: () => {} };
    return new Database('localhost', 3306, 'replica_db', 'u', 'p', util, dbType || 'indexer');
}

// information_schema rows for `table`, reporting every column in the widen set as
// utf8mb3 (narrow) or utf8mb4 (already converged).
function schemaRows(table, charset){
    return utf8mb4Columns.UTF8MB4_RAW_FIELD_COLUMNS
        .filter(e => e.table === table)
        .map(e => ({ COLUMN_NAME: e.column, CHARACTER_SET_NAME: charset }));
}

const altersIn = (calls) => calls.filter(sql => /^ALTER\s+TABLE/i.test(sql));

describe('Database.ensureReplicaUtf8mb4Columns', function(){

    let db;

    beforeEach(function(){
        db = makeDb();
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
    });

    afterEach(async function(){
        sinon.restore();
        await db.close();
    });

    it('widens every narrow column, one ALTER per table, with the twin module\'s exact clause', async function(){
        const calls = [];
        sinon.stub(db, 'doQuery').callsFake(async (sql, args) => {
            calls.push(String(sql));
            if(/information_schema\.columns/i.test(sql)) return schemaRows(args[1], 'utf8mb3');
            return [];
        });

        await db.ensureReplicaUtf8mb4Columns();

        const alters = altersIn(calls);
        const tables = [...utf8mb4Columns.byTable().keys()];
        assert.strictEqual(alters.length, tables.length,
            'expected exactly one ALTER per table in the widen set');

        // Every entry's MODIFY must appear verbatim: the replica and the origin have to
        // land on the same column shape, not merely on "some utf8mb4".
        const joined = alters.join('\n');
        for(const entry of utf8mb4Columns.UTF8MB4_RAW_FIELD_COLUMNS){
            assert.ok(joined.includes(utf8mb4Columns.modifyClause(entry)),
                'the replica widen never issues ' + utf8mb4Columns.modifyClause(entry) +
                ' for ' + entry.table + '.' + entry.column);
        }
        // contracts.code is the named half of the ledger item; pin it by name so a list
        // edit cannot quietly drop the column the operator ruling called out.
        assert.ok(/ALTER TABLE `contracts` MODIFY `code` MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL/
            .test(joined), 'contracts.code is not widened on the replica');
    });

    it('is a no-op on a replica whose columns are already utf8mb4', async function(){
        const calls = [];
        sinon.stub(db, 'doQuery').callsFake(async (sql, args) => {
            calls.push(String(sql));
            if(/information_schema\.columns/i.test(sql)) return schemaRows(args[1], 'utf8mb4');
            return [];
        });

        await db.ensureReplicaUtf8mb4Columns();

        assert.deepStrictEqual(altersIn(calls), [],
            'a converged replica must pay one information_schema read per table and nothing else');
    });

    it('skips a table this replica does not carry', async function(){
        const calls = [];
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            calls.push(String(sql));
            return [];                                   // no rows = table absent
        });

        await db.ensureReplicaUtf8mb4Columns();

        assert.deepStrictEqual(altersIn(calls), []);
    });

    it('widens only the columns the replica actually has, leaving the rest of the ALTER intact', async function(){
        // A replica mid-upgrade can carry the table but not every column of it (the column
        // self-heal runs earlier and may have failed). Naming an absent column makes the
        // whole ALTER errno 1054 and NOTHING in that table converges, so the pass must
        // filter to the columns information_schema actually reports.
        const calls = [];
        sinon.stub(db, 'doQuery').callsFake(async (sql, args) => {
            calls.push(String(sql));
            if(!/information_schema\.columns/i.test(sql)) return [];
            if(args[1] !== 'polls') return schemaRows(args[1], 'utf8mb4');
            return [{ COLUMN_NAME: 'quorum', CHARACTER_SET_NAME: 'utf8mb3' }];
        });

        await db.ensureReplicaUtf8mb4Columns();

        const alters = altersIn(calls);
        assert.strictEqual(alters.length, 1);
        assert.ok(alters[0].includes('MODIFY `quorum`'));
        assert.ok(!alters[0].includes('MODIFY `decide_threshold`'),
            'the ALTER names a column this replica does not have, so the whole statement fails errno 1054');
    });

    it('does not run on a decoder replica (none of these tables exist there)', async function(){
        const decoder = makeDb('decoder');
        const doQuery = sinon.stub(decoder, 'doQuery').resolves([]);
        await decoder.ensureReplicaUtf8mb4Columns();
        assert.strictEqual(doQuery.callCount, 0);
        await decoder.close();
    });

    it('logs and carries on when a table cannot be read or cannot be altered', async function(){
        // A transient driver fault must not be read as "table absent" and must not take
        // startup down: the replica is exactly as usable as it was a moment ago, and every
        // other table still converges.
        const calls = [];
        sinon.stub(db, 'doQuery').callsFake(async (sql, args) => {
            calls.push(String(sql));
            if(/information_schema\.columns/i.test(sql)){
                if(args[1] === 'contracts') throw Object.assign(new Error('read timeout'), { errno: 2013 });
                return schemaRows(args[1], 'utf8mb3');
            }
            if(/ALTER TABLE `deploy_chunks`/.test(sql))
                throw Object.assign(new Error('Row size too large'), { errno: 1118 });
            return [];
        });

        await db.ensureReplicaUtf8mb4Columns();   // must not reject

        assert.ok(console.error.called, 'a failed read / failed ALTER must be reported, not swallowed');
        // The tables after the two failures still converge.
        assert.ok(altersIn(calls).some(sql => /ALTER TABLE `broadcasts`/.test(sql)),
            'one table failing must not abandon the rest of the widen set');
    });

    it('reads the charset case-insensitively and under either information_schema column casing', function(){
        assert.strictEqual(utf8mb4Columns.isAlreadyUtf8mb4({ CHARACTER_SET_NAME: 'utf8mb4' }), true);
        assert.strictEqual(utf8mb4Columns.isAlreadyUtf8mb4({ character_set_name: 'UTF8MB4' }), true);
        // MariaDB 10.6 renamed utf8 to utf8mb3, so both legacy spellings must read narrow.
        assert.strictEqual(utf8mb4Columns.isAlreadyUtf8mb4({ CHARACTER_SET_NAME: 'utf8mb3' }), false);
        assert.strictEqual(utf8mb4Columns.isAlreadyUtf8mb4({ CHARACTER_SET_NAME: 'utf8' }), false);
        assert.strictEqual(utf8mb4Columns.isAlreadyUtf8mb4(null), false);
    });
});
