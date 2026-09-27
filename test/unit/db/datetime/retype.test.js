/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 *********************************************************************/

'use strict';

const assert     = require('assert');
const sinon      = require('sinon');
const proxyquire = require('proxyquire').noCallThru();
const { DATETIME_COLUMNS } = require('../../../../src/schema/datetime_columns');

const TABLES = Array.from(new Set(DATETIME_COLUMNS.map(entry => entry.table)));
const FOLLOWER_TABLES = DATETIME_COLUMNS.filter(e => e.scope === 'follower-derived').map(e => e.table);
const SYNC_OWNED_TABLES = DATETIME_COLUMNS.filter(e => e.scope !== 'follower-derived').map(e => e.table);

function loadMixin(){
    return require('../../../../src/db/datetime/retype');
}

function makeDb({ dbType = 'indexer', dbName = 'replica_db', zoneRow,
    columnRowsByTable = {}, alterBehavior = () => Promise.resolve([]) } = {}){
    const queries = [];
    return {
        dbType,
        dbName,
        queries,
        doQuery: sinon.stub().callsFake(async function(sql, args){
            queries.push({ sql, args });
            if(sql.indexOf('@@session.time_zone') !== -1)
                return zoneRow ? [zoneRow] : [];
            if(sql.indexOf('information_schema.columns') !== -1){
                const table = args[1];
                if(Object.prototype.hasOwnProperty.call(columnRowsByTable, table))
                    return columnRowsByTable[table];
                return [];
            }
            if(sql.indexOf('ALTER TABLE') !== -1)
                return alterBehavior(sql);
            return [];
        }),
    };
}

function rowsFor(table, dataType){
    return DATETIME_COLUMNS
        .filter(entry => entry.table === table)
        .map(entry => ({ COLUMN_NAME: entry.column, DATA_TYPE: dataType }));
}

function querySql(db, fragment){
    return db.queries.filter(query => query.sql.indexOf(fragment) !== -1);
}

describe('DATETIME retype mixin', function(){

    it('retypes every live TIMESTAMP column with one ALTER per table under UTC', async function(){
        const columnRowsByTable = {};
        for(const table of TABLES) columnRowsByTable[table] = rowsFor(table, 'timestamp');
        const db = makeDb({ zoneRow: { tz: '+00:00', sys: 'UTC' }, columnRowsByTable });

        const count = await loadMixin().ensureDatetimeColumns.call(db, {
            includeFollowerDerived: true,
        });

        assert.strictEqual(count, 3);
        const alters = querySql(db, 'ALTER TABLE');
        assert.strictEqual(alters.length, TABLES.length);
        for(const alter of alters){
            assert.match(alter.sql, /MODIFY COLUMN/);
            for(const entry of DATETIME_COLUMNS){
                if(alter.sql.indexOf('`' + entry.table + '`') !== -1)
                    assert.match(alter.sql, new RegExp('`' + entry.column + '`.*DATETIME'));
            }
        }
    });

    it('does not alter columns that are already DATETIME', async function(){
        const columnRowsByTable = {};
        for(const table of TABLES) columnRowsByTable[table] = rowsFor(table, 'datetime');
        const db = makeDb({ zoneRow: { tz: 'UTC' }, columnRowsByTable });

        const count = await loadMixin().ensureDatetimeColumns.call(db, {
            includeFollowerDerived: true,
        });

        assert.strictEqual(count, 0);
        assert.strictEqual(querySql(db, 'ALTER TABLE').length, 0);
    });
});

describe('DATETIME retype selection', function(){

    it('skips an absent table', async function(){
        const db = makeDb({ zoneRow: { tz: 'UTC' } });

        const count = await loadMixin().ensureDatetimeColumns.call(db, {
            includeFollowerDerived: true,
        });

        assert.strictEqual(count, 0);
        assert.strictEqual(querySql(db, 'ALTER TABLE').length, 0);
    });

    it('excludes follower-derived tables when requested', async function(){
        const columnRowsByTable = {};
        for(const table of TABLES) columnRowsByTable[table] = rowsFor(table, 'timestamp');
        const db = makeDb({ zoneRow: { tz: '+00:00' }, columnRowsByTable });

        const count = await loadMixin().ensureDatetimeColumns.call(db, {
            includeFollowerDerived: false,
        });

        assert.strictEqual(count, 2);
        for(const table of FOLLOWER_TABLES){
            for(const query of db.queries){
                assert.strictEqual((query.args || []).indexOf(table), -1);
                assert.strictEqual(query.sql.indexOf('`' + table + '`'), -1);
            }
        }
        assert.strictEqual(querySql(db, 'information_schema.columns').length,
            SYNC_OWNED_TABLES.length);
    });

    it('does not query a decoder database', async function(){
        const db = makeDb({ dbType: 'decoder', zoneRow: { tz: 'UTC' } });

        const count = await loadMixin().ensureDatetimeColumns.call(db, {
            includeFollowerDerived: true,
        });

        assert.strictEqual(count, 0);
        sinon.assert.notCalled(db.doQuery);
    });
});

describe('DATETIME retype failure isolation', function(){

    for(const zoneRow of [
        { tz: '-06:00', sys: 'CST' },
        { tz: 'SYSTEM', sys: 'CST' },
    ]){
        it('does not alter columns in the unsafe ' + zoneRow.tz + ' session zone', async function(){
            const db = makeDb({ zoneRow });

            const count = await loadMixin().ensureDatetimeColumns.call(db, {
                includeFollowerDerived: true,
            });

            assert.strictEqual(count, 0);
            assert.strictEqual(querySql(db, 'information_schema.columns').length, 0);
            assert.strictEqual(querySql(db, 'ALTER TABLE').length, 0);
        });
    }

    it('continues with the next table when one ALTER rejects', async function(){
        const columnRowsByTable = {};
        for(const table of TABLES) columnRowsByTable[table] = rowsFor(table, 'timestamp');
        const db = makeDb({
            zoneRow: { tz: 'UTC' },
            columnRowsByTable,
            alterBehavior: sql => {
                if(sql.indexOf('`' + TABLES[0] + '`') !== -1)
                    return Promise.reject(new Error('ALTER refused'));
                return Promise.resolve([]);
            },
        });

        const count = await loadMixin().ensureDatetimeColumns.call(db, {
            includeFollowerDerived: true,
        });

        assert.strictEqual(count, 2);
        assert.strictEqual(querySql(db, 'ALTER TABLE').length, TABLES.length);
    });
});

describe('Database DATETIME retype registration', function(){

    it('installs ensureDatetimeColumns on Database.prototype', function(){
        const Database = proxyquire('../../../../src/db', {
            mariadb: {
                createPool: () => ({
                    end: () => Promise.resolve(),
                    getConnection: () => Promise.resolve(),
                }),
                createConnection: () => Promise.resolve(),
                '@noCallThru': true,
            },
        });

        assert.strictEqual(typeof Database.prototype.ensureDatetimeColumns, 'function');
    });
});
