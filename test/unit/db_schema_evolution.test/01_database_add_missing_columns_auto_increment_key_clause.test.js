// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const sinon  = require('sinon');
const { makeTestDatabase } = require('../support/fake_db');

// Minimal util stub: addMissingColumns only touches this.doQuery (stubbed
// below) and the pure validation helpers, so util is never exercised here.
function makeDb(){
    let util = { isNull: (v) => v === null || v === undefined };
    return makeTestDatabase('replica_db', 'u', 'p', util, 'indexer');
}

// The decoder replicas' pubkeys table pre-dates the monotonic-id migration, so
// the self-heal had to ADD an AUTO_INCREMENT column. MariaDB refuses that
// without a key in the same statement (errno 1075, "there can be only one auto
// column and it must be defined as a key"), the generated ALTER carried none,
// and the failure was reported as "Added column" while the BTC:testnet decoder
// sat 345 blocks behind for two days.

// The source shape the fleet actually runs: a composite-free primary key on
// address_id and the monotonic id carried by its own UNIQUE key.
const PUBKEYS_DDL = [
    'CREATE TABLE `pubkeys` (',
    '  `address_id` bigint(20) unsigned NOT NULL,',
    '  `pubkey` varchar(66) NOT NULL,',
    '  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,',
    '  PRIMARY KEY (`address_id`),',
    '  UNIQUE KEY `id` (`id`)',
    ') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
].join('\n');

let db;

// Replica has address_id + pubkey only; `id` is the gap. hasPk drives the
// information_schema.statistics probe for an existing primary key.
function stubReplica(opts){
    let o = opts || {};
    let alters = [];
    sinon.stub(db, 'doQuery').callsFake(async (sql) => {
        if(/information_schema\.columns/i.test(sql))
            return [{ column_name: 'address_id' }, { column_name: 'pubkey' }];
        if(/information_schema\.statistics/i.test(sql))
            return o.hasPk ? [{ index_name: 'PRIMARY' }] : [];
        if(/ALTER TABLE/i.test(sql)){
            alters.push(sql);
            if(o.alterErrno){
                let e = new Error('alter refused');
                e.errno = o.alterErrno;
                throw e;
            }
            return [];
        }
        return [];
    });
    return alters;
}

describe('Database.addMissingColumns: AUTO_INCREMENT key clause', function(){

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

    it('appends the source UNIQUE key so the ALTER is not refused with errno 1075', async function(){
        let alters = stubReplica({ hasPk: true });

        let added = await db.addMissingColumns('pubkeys', PUBKEYS_DDL);

        assert.strictEqual(added, 1);
        assert.deepStrictEqual(alters, [
            'ALTER TABLE `pubkeys` ADD COLUMN `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT, ' +
            'ADD UNIQUE KEY `id` (`id`)'
        ]);
    });

    it('reproduces a source PRIMARY KEY when the replica has no primary key', async function(){
        const ddl = [
            'CREATE TABLE `t` (',
            '  `id` int(11) NOT NULL AUTO_INCREMENT,',
            '  `address_id` bigint(20) unsigned NOT NULL,',
            '  `pubkey` varchar(66) NOT NULL,',
            '  PRIMARY KEY (`id`)',
            ') ENGINE=InnoDB'
        ].join('\n');
        let alters = stubReplica({ hasPk: false });

        await db.addMissingColumns('t', ddl);

        assert.deepStrictEqual(alters, [
            'ALTER TABLE `t` ADD COLUMN `id` int(11) NOT NULL AUTO_INCREMENT, ADD PRIMARY KEY (`id`)'
        ]);
    });
});

describe('Database.addMissingColumns: AUTO_INCREMENT key clause', function(){

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

    it('falls back to a UNIQUE key when the replica already has a different primary key', async function(){
        // ADD PRIMARY KEY on a table that has one fails with errno 1068, so the
        // auto-increment requirement is satisfied with a unique key instead.
        const ddl = [
            'CREATE TABLE `t` (',
            '  `id` int(11) NOT NULL AUTO_INCREMENT,',
            '  `address_id` bigint(20) unsigned NOT NULL,',
            '  `pubkey` varchar(66) NOT NULL,',
            '  PRIMARY KEY (`id`)',
            ') ENGINE=InnoDB'
        ].join('\n');
        let alters = stubReplica({ hasPk: true });

        await db.addMissingColumns('t', ddl);

        assert.deepStrictEqual(alters, [
            'ALTER TABLE `t` ADD COLUMN `id` int(11) NOT NULL AUTO_INCREMENT, ADD UNIQUE KEY `id` (`id`)'
        ]);
    });

    it('synthesises a UNIQUE key when the source declares no single-column key on the auto column', async function(){
        const ddl = [
            'CREATE TABLE `t` (',
            '  `id` int(11) NOT NULL AUTO_INCREMENT,',
            '  `address_id` bigint(20) unsigned NOT NULL,',
            '  `pubkey` varchar(66) NOT NULL,',
            '  KEY `combo` (`id`,`address_id`)',
            ') ENGINE=InnoDB'
        ].join('\n');
        let alters = stubReplica({ hasPk: true });

        await db.addMissingColumns('t', ddl);

        assert.deepStrictEqual(alters, [
            'ALTER TABLE `t` ADD COLUMN `id` int(11) NOT NULL AUTO_INCREMENT, ADD UNIQUE KEY `id` (`id`)'
        ]);
    });
});

describe('Database.addMissingColumns: AUTO_INCREMENT key clause', function(){

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

    it('adds no key clause for an ordinary column, including one whose COMMENT says auto_increment', async function(){
        const ddl = [
            'CREATE TABLE `t` (',
            '  `address_id` bigint(20) unsigned NOT NULL,',
            '  `pubkey` varchar(66) NOT NULL,',
            "  `note` varchar(32) DEFAULT NULL COMMENT 'auto_increment',",
            '  PRIMARY KEY (`address_id`)',
            ') ENGINE=InnoDB'
        ].join('\n');
        let alters = stubReplica({ hasPk: true });

        await db.addMissingColumns('t', ddl);

        assert.deepStrictEqual(alters, [
            "ALTER TABLE `t` ADD COLUMN `note` varchar(32) DEFAULT NULL COMMENT 'auto_increment'"
        ]);
    });

    it('reports a refused ALTER as a failure and never logs "Added column"', async function(){
        stubReplica({ hasPk: true, alterErrno: 1075 });

        let thrown = null;
        try { await db.addMissingColumns('pubkeys', PUBKEYS_DDL); }
        catch(e){ thrown = e; }

        assert.ok(thrown, 'a refused ALTER must reach the caller');
        assert.strictEqual(thrown.errno, 1075);
        assert.deepStrictEqual(thrown.failedColumns.map(f => f.column), ['id']);
        let successLines = console.log.getCalls().map(c => String(c.args[0]));
        assert.ok(!successLines.some(l => /^Added column/.test(l)),
            'a failed ALTER must not report "Added column"');
        assert.ok(console.error.getCalls().some(c => /FAILED to add column id/.test(String(c.args[0]))));
    });
});

describe('Database.addMissingColumns: AUTO_INCREMENT key clause', function(){

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

    it('emits an UNQUALIFIED ALTER against the pool default database, so Replicate_Do_DB forwards it', async function(){
        // A fully-qualified `db`.`tbl` DDL executed with no default database is
        // dropped by the downstream tier's Replicate_Do_DB filter, which is why the
        // hand-applied fix had to be repeated directly on each web node.
        let alters = stubReplica({ hasPk: true });

        await db.addMissingColumns('pubkeys', PUBKEYS_DDL);

        assert.strictEqual(db.connectionPoolParams.database, 'replica_db',
            'pool connections must carry a default database for the DDL to replicate');
        assert.ok(alters[0].indexOf('`replica_db`') === -1 && alters[0].indexOf('replica_db.') === -1,
            'the ALTER must not qualify the table with the database name');
        assert.ok(/^ALTER TABLE `pubkeys` /.test(alters[0]));
    });
});
