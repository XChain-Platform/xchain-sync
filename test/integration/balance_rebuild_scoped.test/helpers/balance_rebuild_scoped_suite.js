// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Provides scoped balance fixtures and hooks. One part of balance_rebuild_scoped.test.js.
const sinon  = require('sinon');
const setup  = require('../../helpers/setup');
const testDb = require('../../helpers/testDb');

let db;

// Behavioural proof for the scoped rebuild optimisation: against a real
// MariaDB, a rebuild scoped to the ids a batch of new rows touched must
// leave the aggregate table EXACTLY as a full-table rebuild would, with the same
// rows, same string amounts, untouched pairs preserved, zeroed pairs
// pruned, invalid-status custody excluded. The unit suite pins the SQL
// shape; this pins the arithmetic the database actually performs.

async function snapshotBalances(db) {
    let rows = await db.doQuery(
        'SELECT address_id, tick_id, amount FROM balances ORDER BY address_id, tick_id');
    return rows.map(r => [Number(r.address_id), Number(r.tick_id), String(r.amount)]);
}

async function insertLedgerRows(db, table, rows) {
    for (let r of rows) {
        await db.doQuery(
            'INSERT INTO `' + table + '` (action_index, address_id, tick_id, amount) VALUES (?, ?, ?, ?)',
            [r.action_index, r.address_id, r.tick_id, r.amount]);
    }
}

async function insertCustodyRows(db, table, rows) {
    for (let r of rows) {
        await db.doQuery(
            'INSERT INTO `' + table + '` (action_index, contract_index, source_id, tick_id, amount, status_id, block_index) VALUES (?, ?, ?, ?, ?, ?, ?)',
            [r.action_index, r.contract_index, r.source_id || 1, r.tick_id, r.amount, r.status_id, r.block_index || 1]);
    }
}

// History across addresses 1-5 / ticks 1-3, with big-number amounts.
// Pair (5,3) nets to exactly zero in history (must never appear).
const history = {
    credits: [
        { action_index: 1, address_id: 1, tick_id: 1, amount: '1000' },
        { action_index: 2, address_id: 2, tick_id: 1, amount: '250' },
        { action_index: 3, address_id: 2, tick_id: 2, amount: '99999999999999999999999999999999999999' },
        { action_index: 4, address_id: 3, tick_id: 2, amount: '7' },
        { action_index: 5, address_id: 4, tick_id: 3, amount: '12345678901234567890' },
        { action_index: 6, address_id: 5, tick_id: 3, amount: '500' }
    ],
    debits: [
        { action_index: 7, address_id: 1, tick_id: 1, amount: '400' },
        { action_index: 8, address_id: 2, tick_id: 1, amount: '50' },
        { action_index: 9, address_id: 5, tick_id: 3, amount: '500' }
    ]
};

// New rows touch addresses {2,3} × ticks {1,2}; they drive pair (2,1)
// to exactly zero, so the scoped rebuild must also PRUNE it. The scope
// rectangle includes pair (3,1) (no rows at all) and pair (2,2)
// (history only, untouched by the new rows); both must come out
// exactly as the full rebuild leaves them.
const newCredits = [
    { action_index: 10, address_id: 3, tick_id: 2, amount: '13' }
];
const newDebits = [
    { action_index: 11, address_id: 2, tick_id: 1, amount: '200' }
];
const scope = { addressIds: [2, 3], tickIds: [1, 2] };

function installHooks() {
    before(async function() {
        await setup.globalSetup();
        db = setup.getReplicaDb();
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    after(async function() {
        sinon.restore();
        await setup.globalTeardown();
    });
    beforeEach(async function() {
        await testDb.truncateAll(db);
    });
}

module.exports = {
    get db() { return db; },
    history,
    newCredits,
    newDebits,
    scope,
    testDb,
    installHooks,
    snapshotBalances,
    insertLedgerRows,
    insertCustodyRows
};
