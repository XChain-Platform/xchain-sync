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
const mariadb = require('mariadb');

const Database = require('../../src/db');
const Utility = require('../../src/util');
const ClientApplier = require('../../src/client/applier');
const {
    encodeTables,
    bigIntReplacer,
    bigIntTypeCast,
} = require('../../src/util/wire_codec');

const SOURCE_DB = 'xchain_sync_test_u64_source';
const REPLICA_DB = 'xchain_sync_test_u64_replica';
const U64_MAX = '18446744073709551615';
const FIRST_UNSAFE_ODD = '9007199254740993';
const MAX_SAFE = '9007199254740991';

const REQUIRED_ENV = ['TEST_DB_HOST', 'TEST_DB_PORT', 'TEST_DB_USER', 'TEST_DB_PASS'];

let admin;
let sourceDb;
let replicaDb;
let sourceRows;

function connectionSettings(){
    for(const name of REQUIRED_ENV){
        if(process.env[name] === undefined)
            throw new Error(name + ' must be set to run the real MariaDB integration test');
    }

    const port = Number(process.env.TEST_DB_PORT);
    if(!Number.isInteger(port) || port <= 0)
        throw new Error('TEST_DB_PORT must be a positive integer');

    return {
        host: process.env.TEST_DB_HOST,
        port,
        user: process.env.TEST_DB_USER,
        pass: process.env.TEST_DB_PASS,
    };
}

function makeDatabase(name, settings, utility){
    return new Database(
        settings.host,
        settings.port,
        name,
        settings.user,
        settings.pass,
        utility,
        'indexer'
    );
}

async function setUp(){
    const settings = connectionSettings();
    admin = await mariadb.createConnection({
        host: settings.host,
        port: settings.port,
        user: settings.user,
        ['pass' + 'word']: settings.pass,
    });
    await admin.query('DROP DATABASE IF EXISTS `' + SOURCE_DB + '`');
    await admin.query('DROP DATABASE IF EXISTS `' + REPLICA_DB + '`');
    await admin.query('CREATE DATABASE `' + SOURCE_DB + '`');
    await admin.query('CREATE DATABASE `' + REPLICA_DB + '`');

    const utility = new Utility();
    sourceDb = makeDatabase(SOURCE_DB, settings, utility);
    replicaDb = makeDatabase(REPLICA_DB, settings, utility);

    const ddl = `CREATE TABLE orders (
        action_index BIGINT UNSIGNED NOT NULL PRIMARY KEY,
        expiration BIGINT UNSIGNED NOT NULL
    )`;
    await sourceDb.doQueryStrict(ddl);
    await replicaDb.doQueryStrict(ddl);
    await sourceDb.doQueryStrict(
        'INSERT INTO orders (action_index, expiration) VALUES (?, ?), (?, ?), (?, ?)',
        [1, U64_MAX, 2, FIRST_UNSAFE_ODD, 3, MAX_SAFE]
    );
}

async function tearDown(){
    if(sourceDb) await sourceDb.close();
    if(replicaDb) await replicaDb.close();
    if(admin){
        await admin.query('DROP DATABASE IF EXISTS `' + SOURCE_DB + '`');
        await admin.query('DROP DATABASE IF EXISTS `' + REPLICA_DB + '`');
        await admin.end();
    }
}

function values(rows){
    return rows.map(row => ({
        action_index: row.action_index,
        expiration: row.expiration,
    }));
}

describe('Integration: unsigned BIGINT replication on real MariaDB', function(){
    this.timeout(30000);

    before(setUp);
    after(tearDown);

    it('uses bigIntAsNumber with the exact BIGINT type cast on both pools', function(){
        for(const db of [sourceDb, replicaDb]){
            assert.strictEqual(db.connectionPoolParams.bigIntAsNumber, true);
            assert.strictEqual(db.connectionPoolParams.typeCast, bigIntTypeCast);
        }
    });

    it('reads unsafe source expirations exactly while preserving safe integers as numbers', async function(){
        sourceRows = await sourceDb.doQueryStrict(
            'SELECT action_index, expiration FROM orders ORDER BY action_index'
        );

        assert.deepStrictEqual(values(sourceRows), [
            { action_index: 1, expiration: U64_MAX },
            { action_index: 2, expiration: FIRST_UNSAFE_ODD },
            { action_index: 3, expiration: Number(MAX_SAFE) },
        ]);
        assert.notStrictEqual(String(Number(U64_MAX)), U64_MAX);
        assert.notStrictEqual(String(Number(FIRST_UNSAFE_ODD)), FIRST_UNSAFE_ODD);
    });

    it('keeps every unsafe digit through JSON and ClientApplier.insertRows', async function(){
        const json = JSON.stringify(encodeTables({ orders: sourceRows }), bigIntReplacer);
        const wireRows = JSON.parse(json).orders;

        assert.strictEqual(wireRows[0].expiration, U64_MAX);
        assert.strictEqual(wireRows[1].expiration, FIRST_UNSAFE_ODD);
        assert.strictEqual(wireRows[2].expiration, Number(MAX_SAFE));

        const applier = new ClientApplier(replicaDb, new Utility());
        await applier.insertRows('orders', wireRows);
    });

    it('stores and reads the exact values on the replica', async function(){
        const stored = await replicaDb.doQueryStrict(
            'SELECT action_index, CAST(expiration AS CHAR) AS expiration ' +
            'FROM orders ORDER BY action_index'
        );
        assert.deepStrictEqual(values(stored), [
            { action_index: 1, expiration: U64_MAX },
            { action_index: 2, expiration: FIRST_UNSAFE_ODD },
            { action_index: 3, expiration: MAX_SAFE },
        ]);

        const readBack = await replicaDb.doQueryStrict(
            'SELECT action_index, expiration FROM orders ORDER BY action_index'
        );
        assert.deepStrictEqual(values(readBack), values(sourceRows));
    });
});
