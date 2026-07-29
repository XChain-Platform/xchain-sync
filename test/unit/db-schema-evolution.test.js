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
const Database = require('../../src/db');

// Minimal util stub: addMissingColumns only touches this.doQuery (stubbed
// below) and the pure validation helpers, so util is never exercised here.
function makeDb(){
    let util = { isNull: (v) => v === null || v === undefined };
    return new Database('localhost', 3306, 'replica_db', 'u', 'p', util, 'indexer');
}

const DDL = [
    'CREATE TABLE `stakes` (',
    '  `id` int(11) NOT NULL AUTO_INCREMENT,',
    '  `pubkey` varchar(66) NOT NULL,',
    '  `contract` varchar(66) DEFAULT NULL,',
    "  `cooldown_blocks` int(11) NOT NULL DEFAULT '0',",
    '  PRIMARY KEY (`id`)',
    ') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
].join('\n');

describe('Database.addMissingColumns', function(){

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

    it('issues ALTER TABLE ADD COLUMN for each column the replica lacks', async function(){
        // Replica has id + pubkey; source DDL also has contract + cooldown_blocks.
        let calls = [];
        sinon.stub(db, 'doQuery').callsFake(async (sql, args) => {
            calls.push(sql);
            if(/information_schema\.columns/i.test(sql))
                return [{ column_name: 'id' }, { column_name: 'pubkey' }];
            return [];
        });

        let added = await db.addMissingColumns('stakes', DDL);

        assert.strictEqual(added, 2);
        let alters = calls.filter(s => /ALTER TABLE/i.test(s));
        assert.deepStrictEqual(alters, [
            'ALTER TABLE `stakes` ADD COLUMN `contract` varchar(66) DEFAULT NULL',
            "ALTER TABLE `stakes` ADD COLUMN `cooldown_blocks` int(11) NOT NULL DEFAULT '0'"
        ]);
    });

    it('is a no-op when the replica already has every source column', async function(){
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if(/information_schema\.columns/i.test(sql))
                return ['id', 'pubkey', 'contract', 'cooldown_blocks'].map(c => ({ column_name: c }));
            return [];
        });

        let added = await db.addMissingColumns('stakes', DDL);
        assert.strictEqual(added, 0);
    });

    it('skips (does not abort) a column whose definition cannot be parsed', async function(){
        // Source reports a column via INFORMATION_SCHEMA-style name list that
        // does not appear as a parseable line in the DDL → best-effort skip.
        let ddlMissingLine = [
            'CREATE TABLE `stakes` (',
            '  `id` int(11) NOT NULL,',
            '  PRIMARY KEY (`id`)',
            ') ENGINE=InnoDB'
        ].join('\n');

        let altered = false;
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if(/ALTER TABLE/i.test(sql)) altered = true;
            if(/information_schema\.columns/i.test(sql)) return [{ column_name: 'id' }];
            return [];
        });

        // Only `id` is in the DDL and the replica already has it → nothing added.
        let added = await db.addMissingColumns('stakes', ddlMissingLine);
        assert.strictEqual(added, 0);
        assert.strictEqual(altered, false);
    });

    it('returns 0 for an unparseable DDL', async function(){
        sinon.stub(db, 'doQuery').resolves([]);
        let added = await db.addMissingColumns('stakes', 'not a ddl');
        assert.strictEqual(added, 0);
    });
});

describe('Database.ensureReplicatedColumns: nullability relaxation', function(){

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

    // The orders/swaps add-column drift loop queries information_schema.tables
    // first; returning [] there makes those four entries no-ops so each test
    // exercises only the contract_emissions.action_index relaxation.
    it('relaxes contract_emissions.action_index NOT NULL -> NULL when the replica column is NOT NULL', async function(){
        let alters = [];
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if(/information_schema\.tables/i.test(sql)) return [];          // orders/swaps absent
            if(/IS_NULLABLE/i.test(sql))               return [{ IS_NULLABLE: 'NO' }];
            if(/ALTER TABLE/i.test(sql)){ alters.push(sql); return []; }
            return [];
        });

        await db.ensureReplicatedColumns();

        assert.deepStrictEqual(alters, [
            'ALTER TABLE `contract_emissions` MODIFY COLUMN `action_index` BIGINT UNSIGNED NULL'
        ]);
    });

    it('is a no-op when contract_emissions.action_index is already nullable (idempotent)', async function(){
        let altered = false;
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if(/information_schema\.tables/i.test(sql)) return [];
            if(/IS_NULLABLE/i.test(sql))               return [{ IS_NULLABLE: 'YES' }];
            if(/ALTER TABLE/i.test(sql))               altered = true;
            return [];
        });

        await db.ensureReplicatedColumns();
        assert.strictEqual(altered, false);
    });

    it('is a no-op when contract_emissions / the column is absent on the replica', async function(){
        let altered = false;
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if(/information_schema\.tables/i.test(sql))  return [];
            if(/information_schema\.columns/i.test(sql)) return [];   // column not found
            if(/ALTER TABLE/i.test(sql))                 altered = true;
            return [];
        });

        await db.ensureReplicatedColumns();
        assert.strictEqual(altered, false);
    });

    it('never tightens: leaves a nullable column alone and issues no MODIFY', async function(){
        // Defensive: even if upstream were NOT NULL, this method only relaxes;
        // a replica reporting YES must never be MODIFYed back to NOT NULL.
        let modifies = 0;
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if(/information_schema\.tables/i.test(sql)) return [];
            if(/IS_NULLABLE/i.test(sql))               return [{ IS_NULLABLE: 'YES' }];
            if(/MODIFY COLUMN/i.test(sql))             modifies++;
            return [];
        });

        await db.ensureReplicatedColumns();
        assert.strictEqual(modifies, 0);
    });

    it('adds state_tree_roots.contract_state_root on an aged replica that already has the table', async function(){
        // The hazard this closes: state_tree_roots is FOLLOWER-DERIVED, so
        // verifySyncTables only creates it when ABSENT and an aged replica never
        // gains a column added to src/sql/state_tree_roots.sql afterwards. The
        // first recomputed block then fails its INSERT with errno 1054 on every
        // follower at once, on deploy rather than at an armed height.
        let alters = [];
        sinon.stub(db, 'doQuery').callsFake(async (sql, args) => {
            if(/information_schema\.tables/i.test(sql))
                return (args && args[1] === 'state_tree_roots') ? [{ table_name: 'state_tree_roots' }] : [];
            if(/information_schema\.columns/i.test(sql) && /COLUMN_NAME = \?/i.test(sql)) return [];   // column absent
            if(/IS_NULLABLE/i.test(sql))  return [{ IS_NULLABLE: 'YES' }];
            if(/ALTER TABLE/i.test(sql)){ alters.push(sql); return []; }
            return [];
        });

        await db.ensureReplicatedColumns();

        assert.deepStrictEqual(alters, [
            'ALTER TABLE `state_tree_roots` ADD COLUMN `contract_state_root` CHAR(64) NULL AFTER `block_merkle_root`',
            'ALTER TABLE `state_tree_roots` ADD COLUMN `contract_state_root_shadow` CHAR(64) NULL AFTER `contract_state_root`',
            'ALTER TABLE `state_tree_roots` ADD COLUMN `balances_root_escrow_shadow` CHAR(64) NULL AFTER `contract_state_root_shadow`'
        ]);
    });

    it('the added definition matches src/sql/state_tree_roots.sql (one column, two declarations)', function(){
        // The drift entry and the definition file are two spellings of the same
        // column, and a fresh install uses the file while an aged replica uses the
        // entry. If they disagree the two populations diverge in schema, which is
        // how a "converged" fleet ends up with a CHAR(64) on some nodes and
        // something else on others.
        const fs   = require('fs');
        const path = require('path');
        const ddl  = fs.readFileSync(path.resolve(__dirname, '../../src/sql/state_tree_roots.sql'), 'utf8');
        assert.ok(/contract_state_root\s+CHAR\(64\)\s+NULL/i.test(ddl),
            'src/sql/state_tree_roots.sql must declare contract_state_root CHAR(64) NULL');
        assert.ok(/contract_state_root_shadow\s+CHAR\(64\)\s+NULL/i.test(ddl),
            'and the §7 shadow column beside it');
        // And it sits after block_merkle_root there, matching the AFTER anchor, so
        // migrated and fresh tables converge on the same column order.
        assert.ok(ddl.indexOf('block_merkle_root') < ddl.indexOf('contract_state_root'),
            'contract_state_root must follow block_merkle_root in the definition, matching the AFTER anchor');
    });

    it('does nothing on a decoder replica (early return, no queries)', async function(){
        let decoderDb = new Database('localhost', 3306, 'replica_db', 'u', 'p', { isNull: (v) => v === null || v === undefined }, 'decoder');
        let called = false;
        sinon.stub(decoderDb, 'doQuery').callsFake(async () => { called = true; return []; });

        await decoderDb.ensureReplicatedColumns();

        assert.strictEqual(called, false);
        await decoderDb.close();
    });
});
