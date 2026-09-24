/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md.
 *
 **********************************************************************
 * Emissions-streaming consensus drill (real DB).
 *
 * Internal contract emissions (e.g. SLASH) deduct ledger state but mint no
 * on-wire action, so contract_emissions.action_index is NULL for them. The
 * consensus contract_hash (the block hasher) counts those rows by walking
 * execution_index -> contract_executions -> actions rather than action_index, so
 * the server must stream them the same way; the old getActionScopedRows() path
 * joined on action_index directly and its INNER JOIN silently dropped every
 * NULL-action_index row. A follower then received fewer emissions than the hash
 * counted, recomputed a divergent contract_hash and halted.
 *
 * This drill proves the fix at the SQL layer against a real MariaDB. It seeds a
 * block whose single contract execution emits one on-wire emission (action_index
 * set) and one internal SLASH (action_index NULL), then asserts that
 *   - getEmissionRowsForBlock() returns BOTH, the streamed, hash-aligned set, and
 *   - getActionScopedRows()     returns only ONE, dropping the SLASH: the bug.
 *
 * Requires the integration MariaDB; run via `npm run test:integration`. It cannot
 * run where no real database is reachable.
 ********************************************************************/

const assert   = require('assert');
const setup    = require('./helpers/setup');
const testDb   = require('./helpers/testDb');
const fixtures = require('./helpers/fixtures');
const { assertStreamedEmissions } = require('./emissions_parity.test/helpers/emissions_parity_suite');

describe('Integration: contract_emissions reorg-safe streaming (emissions fix) @regression', function () {
    this.timeout(30000);

    let sourceDb;

    before(async function () {
        await setup.globalSetup();
    });

    after(async function () {
        await setup.globalTeardown();
    });

    beforeEach(async function () {
        sourceDb = setup.getSourceDb();
        await testDb.truncateAll(sourceDb);
    });

    it('getEmissionRowsForBlock includes NULL-action_index (SLASH) emissions the action-scoped path drops', async function () {
        const B = 5;
        // Real block with transactions + actions (the execution anchors to one of them).
        await fixtures.seedBlocks(sourceDb, B, B);

        const actionRows = await sourceDb.doQuery(
            "SELECT a.action_index FROM actions a " +
            "INNER JOIN transactions tx ON (tx.tx_index = a.tx_index) " +
            "WHERE tx.block_index = ? ORDER BY a.action_index ASC LIMIT 1",
            [B]);
        assert.ok(actionRows.length > 0, 'seedBlocks must have produced an action in block ' + B);
        const execAction = Number(actionRows[0].action_index);

        // The EXECUTE action's contract_executions row. emitted_count is descriptive only;
        // block scope flows through execution_index == this action_index.
        await sourceDb.doQuery(
            "INSERT INTO contract_executions " +
            "(action_index, contract_index, caller_id, gas_used, gas_limit, status_id, emitted_count, block_index) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [execAction, 1, 1, 0, 100, 1, 2, B]);

        // Two emissions for that execution: a normal on-wire one (action_index set) and an
        // internal SLASH (action_index NULL).
        await sourceDb.doQuery(
            "INSERT INTO contract_emissions (execution_index, emitted_action, action_index, position) VALUES (?, ?, ?, ?)",
            [execAction, 'ORDER', execAction, 0]);
        await sourceDb.doQuery(
            "INSERT INTO contract_emissions (execution_index, emitted_action, action_index, position) VALUES (?, ?, ?, ?)",
            [execAction, 'SLASH', null, 1]);

        const streamed = await sourceDb.getEmissionRowsForBlock(B);
        assertStreamedEmissions(streamed);

        // The bug it fixes: the generic action-scoped INNER JOIN silently drops the NULL row,
        // so a follower fed by this path would recompute a divergent contract_hash and halt.
        const actionScoped = await sourceDb.getActionScopedRows('contract_emissions', B);
        assert.strictEqual(actionScoped.length, 1,
            'action-scoped path drops the NULL-action_index SLASH row (the divergence bug)');
        assert.strictEqual(actionScoped[0].emitted_action, 'ORDER');
    });
});
