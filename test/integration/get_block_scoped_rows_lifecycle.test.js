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
const setup  = require('./helpers/setup');
const testDb = require('./helpers/testDb');

// Prove the harness's getBlockScopedRows reads lifecycle.blockKey(table) against
// a real schema: rollcalls scopes by close_block and has no block_index column,
// so a fixed block_index query would throw errno 1054 on it.
describe('Integration: TestDatabase.getBlockScopedRows reads the lifecycle key', function() {

    let db;

    before(async function() {
        await setup.globalSetup();
        db = setup.getSourceDb();
    });

    after(async function() {
        await setup.globalTeardown();
    });

    beforeEach(async function() {
        await testDb.truncateAll(db);
    });

    it('scopes a close_block-keyed table (rollcalls) by close_block, not block_index', async function() {
        await db.doQuery(
            'INSERT INTO rollcalls (epoch_height, snapshot_block, close_block, rolled, responsible_set_json) VALUES (?, ?, ?, ?, ?)',
            [100, 90, 120, 1, '[]']);
        // A second epoch at a different close_block: proves the query is
        // actually filtering, not just returning every row in the table.
        await db.doQuery(
            'INSERT INTO rollcalls (epoch_height, snapshot_block, close_block, rolled, responsible_set_json) VALUES (?, ?, ?, ?, ?)',
            [200, 190, 220, 0, null]);

        let rows = await db.getBlockScopedRows('rollcalls', 120);

        assert.strictEqual(rows.length, 1);
        assert.strictEqual(Number(rows[0].epoch_height), 100);
        assert.strictEqual(Number(rows[0].close_block), 120);
    });

    it('still scopes an ordinary block_index-keyed table (transactions) by block_index', async function() {
        await db.doQuery(
            'INSERT INTO transactions (tx_index, block_index, tx_hash_id) VALUES (?, ?, ?)',
            [1, 5, 1]);
        await db.doQuery(
            'INSERT INTO transactions (tx_index, block_index, tx_hash_id) VALUES (?, ?, ?)',
            [2, 6, 2]);

        let rows = await db.getBlockScopedRows('transactions', 5);

        assert.strictEqual(rows.length, 1);
        assert.strictEqual(Number(rows[0].tx_index), 1);
    });
});
