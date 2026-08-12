// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Can the applier WRITE every table it claims to replicate?
//
// WHY THIS EXISTS. A production follower once could not replicate `contract_state`
// at ALL on a strict-mode MariaDB, live for weeks, found only when something finally
// ran a follower over a block carrying such a row. The mechanism was mundane: the
// source reads block rows with SELECT *, `ClientApplier._insertRows` names every
// carried column, and one of them was GENERATED, which is errno 1906. Nothing about
// that is specific to contract_state. It is the general shape "the applier meets a
// column shape it has never met", and the registry lists 100-plus replicated tables
// whose write path no test had ever executed.
//
// So this stops hunting instances. For every table the follower replicates through
// _insertRows, it synthesizes one row FROM THE REAL SCHEMA (including the generated
// columns the source would ship, which is precisely the condition that broke the
// production follower above) and requires the insert to land.
//
// HOW FAILURES ARE CLASSIFIED, because a generic synthesizer must not cry wolf. An
// APPLIER-class error is one where the statement itself is wrong for the schema:
// 1906 generated column, 1054 unknown column, 1064 syntax, 1146 missing table. Those
// FAIL, because they are that same failure family and no synthesized value can cause them.
// Anything else (a NOT NULL this synthesizer filled badly, a range or charset
// complaint) is counted as UNVERIFIED and printed, never asserted on: it is evidence
// about this test, not about the applier. A floor on the verified count keeps the
// suite from passing by verifying nothing.
//
// FOREIGN_KEY_CHECKS is off for the run. This asks whether the applier's INSERT is
// well-formed for the table, not whether a lone synthetic row is referentially sound.

const assert  = require('assert');
const sinon   = require('sinon');
const setup   = require('./helpers/setup');
const testDb  = require('./helpers/testDb');

const ClientApplier = require('../../src/ClientApplier');
const lifecycle     = require('../../src/tableLifecycle');

// The classes that ride a block/catch-up payload into _insertRows. 'local',
// 'follower-derived' and 'hub-mirror' tables are written by other paths and are not
// this test's subject.
const STREAMED = new Set(['stream:action', 'stream:block', 'stream:index',
                          'stream:special', 'snapshot']);

// Errors that mean the STATEMENT is wrong for the schema. No value this test could
// synthesize produces any of them, so each one is an applier defect.
const APPLIER_ERRNOS = new Map([
    [1906, 'value supplied for a GENERATED column (the failure family described above)'],
    [1054, 'unknown column in the INSERT list'],
    [1064, 'SQL syntax error'],
    [1146, 'table does not exist']
]);

// One acceptable value per SQL type, derived from the column definition rather than
// guessed per table, so a new table needs no entry here.
// Type ceilings for the auto-increment probe below. Signed maxima, so the value is
// valid whether or not the column is UNSIGNED.
const INT_CEILING = { tinyint: 99, smallint: 9999, mediumint: 999999,
                      int: 8675309, integer: 8675309, bigint: 8675309 };

function synthValue(col){
    const type = String(col.DATA_TYPE || col.data_type || '').toLowerCase();
    const full = String(col.COLUMN_TYPE || col.column_type || '').toLowerCase();
    const auto = /auto_increment/i.test(String(col.e || col.extra || ''));
    switch(type){
        case 'tinyint': case 'smallint': case 'mediumint': case 'int': case 'integer':
        case 'bigint': case 'bit':
            // An AUTO_INCREMENT surrogate gets a high value rather than 1. Several
            // replicated tables are seeded lookup tables (index_fiats ships 12 currency
            // rows) and those ride INSERT IGNORE, so id=1 collided with seed row 1 and the
            // applier correctly skipped the probe. Verifying the table beats politely
            // reporting that it could not be verified.
            return auto ? (INT_CEILING[type] || 8675309) : 1;
        case 'decimal': case 'numeric': case 'float': case 'double':
            return '1';
        case 'date':      return '2026-01-01';
        case 'datetime': case 'timestamp': return '2026-01-01 00:00:00';
        case 'time':      return '00:00:00';
        case 'year':      return 2026;
        case 'json':      return '{}';
        case 'binary': case 'varbinary': case 'tinyblob': case 'blob':
        case 'mediumblob': case 'longblob':
            return Buffer.from([1]);
        case 'enum': case 'set': {
            // First allowed member, read out of the column type: enum('a','b').
            const m = full.match(/^(?:enum|set)\((.*)\)$/);
            if(m){
                const first = m[1].split(',')[0].trim();
                return first.replace(/^'|'$/g, '');
            }
            return 'x';
        }
        default: {
            // char/varchar/text. A DISTINCTIVE value, truncated to the column's declared
            // width: some replicated tables are seeded lookup tables (index_fiats ships
            // with currency codes), and those are in the INSERT IGNORE set, so a bland
            // 'x' collided on the natural key and the applier correctly skipped the row.
            // The delta check caught that as unverified, which is what it is for, but a
            // non-colliding probe verifies the table instead of merely not lying about it.
            const max = Number(col.CHARACTER_MAXIMUM_LENGTH || col.character_maximum_length || 0);
            const probe = 'zqsynthprobe';
            return (max > 0 && max < probe.length) ? probe.slice(0, max) : probe;
        }
    }
}

describe('Integration: the applier can write every table it replicates', function() {

    let replicaDb, applier;
    let schema = new Map();      // table -> column rows
    const absent = [];           // registry tables with no table in the replica schema

    before(async function() {
        this.timeout(120000);
        await setup.globalSetup();
        replicaDb = setup.getReplicaDb();
        applier   = new ClientApplier(replicaDb, testDb.util, 'litecoin', 'regtest');

        const cols = await replicaDb.doQuery(
            'SELECT table_name AS t, column_name AS c, data_type AS DATA_TYPE, ' +
            'column_type AS COLUMN_TYPE, character_maximum_length AS CHARACTER_MAXIMUM_LENGTH, ' +
            'is_nullable AS n, extra AS e ' +
            'FROM information_schema.columns WHERE table_schema = DATABASE() ' +
            'ORDER BY table_name, ordinal_position', []);
        for(const r of cols){
            const t = String(r.t);
            if(!schema.has(t)) schema.set(t, []);
            schema.get(t).push(r);
        }
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    after(async function() {
        this.timeout(60000);
        sinon.restore();
        await setup.globalTeardown();
    });

    it('accepts a schema-shaped row for every replicated table, with no applier-class error',
       async function() {
        this.timeout(300000);

        const targets = lifecycle.TABLES
            .filter(e => STREAMED.has(e.replication))
            .map(e => e.table)
            .filter(t => { if(schema.has(t)) return true; absent.push(t); return false; })
            .sort();

        assert.ok(targets.length > 50,
                  'only ' + targets.length + ' replicated tables found in the replica schema; ' +
                  'the schema seed or the registry read is broken, and a pass here would ' +
                  'mean nothing');

        const applierBugs = [];
        const unverified  = [];
        let verified = 0;

        // One transaction for the whole sweep, for two reasons that both matter. It is
        // the only way SET FOREIGN_KEY_CHECKS applies to the connection the inserts
        // actually run on (doQuery draws from a pool unless a transaction is open, so the
        // earlier per-pool SET was luck rather than suppression), and rolling back at the
        // end leaves the replica exactly as found, with no per-table DELETE to get wrong.
        await replicaDb.beginTransaction();
        await replicaDb.doQuery('SET FOREIGN_KEY_CHECKS = 0', []);
        try {

        for(const table of targets){
            // Every column the SOURCE would ship, generated ones INCLUDED. That is the
            // condition described above: before the fix, _insertRows named them and
            // MariaDB rejected the statement under STRICT_TRANS_TABLES.
            const row = {};
            for(const col of schema.get(table)) row[String(col.c)] = synthValue(col);

            // A DELTA, not an absolute count: asserting "> 0" would pass on a table that
            // already held rows and rejected this one.
            const before = await replicaDb.doQuery('SELECT COUNT(*) AS c FROM `' + table + '`', []);
            try {
                await applier._insertRows(table, [row]);
            } catch(e){
                const errno = e && e.errno;
                if(APPLIER_ERRNOS.has(errno)){
                    applierBugs.push(table + ': errno ' + errno + ' (' + APPLIER_ERRNOS.get(errno) + ')');
                } else {
                    unverified.push(table + ': ' + (errno || (e && e.code) || 'unknown'));
                }
                continue;
            }
            const after = await replicaDb.doQuery('SELECT COUNT(*) AS c FROM `' + table + '`', []);
            if(Number(after[0].c) > Number(before[0].c)) verified++;
            else unverified.push(table + ': insert reported success but no row landed');
        }

        } finally {
            // Never commit: the sweep is a shape probe, and the replica other suites in
            // this tier share must come out of it untouched.
            await replicaDb.rollbackTransaction();
        }

        // Always printed. A guard that reports nothing on success gives no way to notice
        // it has quietly stopped covering anything, which is how the sibling-skip trap in
        // generatedColumns.test.js stayed invisible.
        process.stdout.write('\n      [' + verified + '/' + targets.length +
                             ' tables round-tripped through the applier' +
                             (applierBugs.length ? ', ' + applierBugs.length + ' APPLIER BUG' +
                                                   (applierBugs.length === 1 ? '' : 'S') : '') +
                             ']\n');
        // Printed rather than asserted: this is evidence about the synthesizer.
        if(unverified.length)
            process.stdout.write('\n      [unverified ' + unverified.length + '/' + targets.length +
                                 '] ' + unverified.slice(0, 12).join('; ') + '\n');
        if(absent.length)
            process.stdout.write('      [registry tables absent from the replica schema: ' +
                                 absent.length + '] ' + absent.slice(0, 8).join(', ') + '\n');

        assert.deepStrictEqual(applierBugs, [],
            'the applier cannot write these replicated tables. Each is the same shape as the ' +
            'incident described above: a follower would fail its block apply, roll back, retry ' +
            'the same block and never advance, on any chain that writes one of these rows.');

        // A floor, so the suite cannot pass by verifying nothing: most tables must have
        // genuinely round-tripped through the applier.
        assert.ok(verified >= Math.floor(targets.length * 0.8),
            'only ' + verified + ' of ' + targets.length + ' tables round-tripped; the ' +
            'synthesizer has decayed and this guard is no longer covering what it claims');
    });
});
