// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

'use strict';

const assert = require('assert');
const { splitSqlStatements } = require('../../src/db/sql_util');
const { DATETIME_COLUMNS } = require('../../src/schema/datetime_columns');
const {
    readCreateSql,
    agedModifySql,
    zoneReadSql,
    snapshotColumnShapes,
} = require('./helpers/datetime_aged_shape');

const AGED_DB = 'xchain_sync_test_datetime_retype_aged';
const FRESH_DB = 'xchain_sync_test_datetime_retype_fresh';
const LITERAL = '2026-01-15 12:34:56';
const TABLES = DATETIME_COLUMNS.map(entry => entry.table);

async function createTables(db){
    for(const table of TABLES){
        for(const statement of splitSqlStatements(readCreateSql(table)))
            await db.doQuery(statement);
    }
}

async function ageColumns(db){
    for(const entry of DATETIME_COLUMNS)
        await db.doQuery(agedModifySql(entry));
}

async function insertAgedRows(conn){
    const root = 'a'.repeat(64);
    await conn.query(`INSERT INTO merkle_epochs
        (epoch, start_block, end_block, merkle_root, leaf_count, created_at)
        VALUES (?, ?, ?, ?, ?, ?)`, [1, 1, 2, root, 1, LITERAL]);
    await conn.query(`INSERT INTO merkle_reorgs
        (reorg_block, epoch, start_block, end_block, old_root, detected_at)
        VALUES (?, ?, ?, ?, ?, ?)`, [3, 1, 1, 2, root, LITERAL]);
    await conn.query(`INSERT INTO state_tree_roots
        (chain, network, block_index, balances_root, stakes_root, state_root,
         block_merkle_root, computed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    ['BTC', 'regtest', 1, root, root, root, root, LITERAL]);
}

async function readValues(conn){
    const values = {};
    for(const entry of DATETIME_COLUMNS){
        const rows = await conn.query(zoneReadSql(entry));
        values[`${entry.table}.${entry.column}`] = rows[0].v;
    }
    return values;
}

function metadataOnly(shapes){
    const metadata = {};
    for(const [key, row] of Object.entries(shapes)){
        metadata[key] = {
            COLUMN_TYPE: row.COLUMN_TYPE,
            IS_NULLABLE: row.IS_NULLABLE,
            COLUMN_DEFAULT: row.COLUMN_DEFAULT,
            EXTRA: row.EXTRA,
        };
    }
    return metadata;
}

function expectedValues(value){
    return Object.fromEntries(DATETIME_COLUMNS.map(
        entry => [`${entry.table}.${entry.column}`, value]
    ));
}

let testDb;
let agedDb;
let freshDb;
let agedConn;
let migratedShapes;
let migratedValues;

async function setUp(){
    if(process.env.TEST_DB_PASS === undefined){
        this.skip();
        return;
    }

    testDb = require('./helpers/testDb');
    await testDb.dropDatabase(AGED_DB);
    await testDb.dropDatabase(FRESH_DB);
    agedDb = await testDb.createDatabase(AGED_DB);
    freshDb = await testDb.createDatabase(FRESH_DB);
    agedDb.dbType = 'indexer';
    freshDb.dbType = 'indexer';
    await createTables(agedDb);
    await createTables(freshDb);
    await ageColumns(agedDb);

    agedConn = await agedDb.pool.getConnection();
    await agedConn.query("SET time_zone = '+00:00'");
    await insertAgedRows(agedConn);
    await agedConn.query("SET time_zone = '-06:00'");
}

async function tearDown(){
    if(agedConn) agedConn.release();
    if(agedDb) await agedDb.close();
    if(freshDb) await freshDb.close();
    if(testDb){
        await testDb.dropDatabase(AGED_DB);
        await testDb.dropDatabase(FRESH_DB);
    }
}

async function retypeAgedColumns(){
    assert.deepStrictEqual(await readValues(agedConn), expectedValues('2026-01-15 06:34:56'));

    const changed = await agedDb.ensureDatetimeColumns({ includeFollowerDerived: true });
    assert.strictEqual(changed, 3);
    migratedShapes = await snapshotColumnShapes(
        agedDb.doQuery.bind(agedDb), AGED_DB, DATETIME_COLUMNS
    );
    for(const shape of Object.values(migratedShapes))
        assert.strictEqual(shape.DATA_TYPE.toLowerCase(), 'datetime');

    migratedValues = await readValues(agedConn);
    assert.deepStrictEqual(migratedValues, expectedValues(LITERAL));
}

async function assertIdempotence(){
    const metadata = metadataOnly(migratedShapes);
    assert.strictEqual(
        await agedDb.ensureDatetimeColumns({ includeFollowerDerived: true }),
        0
    );
    const secondShapes = await snapshotColumnShapes(
        agedDb.doQuery.bind(agedDb), AGED_DB, DATETIME_COLUMNS
    );
    assert.deepStrictEqual(metadataOnly(secondShapes), metadata);
    assert.deepStrictEqual(await readValues(agedConn), migratedValues);
}

async function assertFreshParity(){
    const freshShapes = await snapshotColumnShapes(
        freshDb.doQuery.bind(freshDb), FRESH_DB, DATETIME_COLUMNS
    );
    assert.deepStrictEqual(metadataOnly(freshShapes), metadataOnly(migratedShapes));
}

describe('Integration: startup DATETIME retype on real MariaDB', function(){
    this.timeout(300000);
    before(setUp);
    after(tearDown);
    it('preserves wall-clock values while retyping all three aged columns', retypeAgedColumns);
    it('is idempotent without changing metadata or values', assertIdempotence);
    it('matches the column metadata created by fresh sync SQL', assertFreshParity);
});
