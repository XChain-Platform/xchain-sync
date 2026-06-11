// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available —
// contact legal@dankest.llc.

const assert = require('assert');
const sinon  = require('sinon');
const setup  = require('./helpers/setup');
const testDb = require('./helpers/testDb');
const balanceHelpers = require('../../src/balance-helpers');
const ClientApplier  = require('../../src/ClientApplier');

// Behavioural proof for the scoped rebuild optimisation: against a real
// MariaDB, a rebuild scoped to the ids a batch of new rows touched must
// leave the aggregate table EXACTLY as a full-table rebuild would — same
// rows, same string amounts, untouched pairs preserved, zeroed pairs
// pruned, invalid-status custody excluded. The unit suite pins the SQL
// shape; this pins the arithmetic the database actually performs.

async function snapshotBalances(db) {
    let rows = await db.doQuery(
        'SELECT address_id, tick_id, amount FROM balances ORDER BY address_id, tick_id');
    return rows.map(r => [Number(r.address_id), Number(r.tick_id), String(r.amount)]);
}

async function snapshotContractBalances(db) {
    let rows = await db.doQuery(
        'SELECT contract_index, tick_id, amount FROM contract_balances ORDER BY contract_index, tick_id');
    return rows.map(r => [Number(r.contract_index), Number(r.tick_id), String(r.amount)]);
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

describe('Integration: scoped balance rebuilds', function() {

    let db;

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

    describe('balances', function() {

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
        // (history only, untouched by the new rows) — both must come out
        // exactly as the full rebuild leaves them.
        const newCredits = [
            { action_index: 10, address_id: 3, tick_id: 2, amount: '13' }
        ];
        const newDebits = [
            { action_index: 11, address_id: 2, tick_id: 1, amount: '200' }
        ];
        const scope = { addressIds: [2, 3], tickIds: [1, 2] };

        it('a scoped rebuild leaves the table exactly as a full rebuild would', async function() {
            await insertLedgerRows(db, 'credits', history.credits);
            await insertLedgerRows(db, 'debits',  history.debits);
            await balanceHelpers.rebuildBalances(db);

            await insertLedgerRows(db, 'credits', newCredits);
            await insertLedgerRows(db, 'debits',  newDebits);

            await balanceHelpers.rebuildBalances(db, scope);
            let scoped = await snapshotBalances(db);

            await balanceHelpers.rebuildBalances(db);
            let full = await snapshotBalances(db);

            assert.deepStrictEqual(scoped, full);

            // Spot-checks on the interesting rows so a both-paths-wrong
            // regression can't slip through the equivalence assertion.
            assert.ok(!scoped.some(r => r[0] === 2 && r[1] === 1), 'zeroed pair (2,1) pruned');
            assert.ok(scoped.some(r => r[0] === 1 && r[1] === 1 && r[2] === '600'), 'untouched pair (1,1) intact');
            assert.ok(scoped.some(r => r[0] === 2 && r[1] === 2 && r[2] === '99999999999999999999999999999999999999'),
                'big-number amount survives the scoped recompute byte-for-byte');
            assert.ok(scoped.some(r => r[0] === 3 && r[1] === 2 && r[2] === '20'), 'touched pair (3,2) recomputed');
        });

        it('ClientApplier.applyBlock drives the scoped rebuild end-to-end', async function() {
            await insertLedgerRows(db, 'credits', history.credits);
            await insertLedgerRows(db, 'debits',  history.debits);
            await balanceHelpers.rebuildBalances(db);

            db.dbType = 'indexer';
            let applier = new ClientApplier(db, testDb.util);
            let spy = sinon.spy(balanceHelpers, 'rebuildBalances');
            try {
                await applier.applyBlock({ block_index: 999, data: { credits: newCredits, debits: newDebits } });
                assert.deepStrictEqual(spy.firstCall.args[1], { addressIds: [3, 2], tickIds: [2, 1] },
                    'applier passed a scope (not a full rebuild)');
            } finally {
                spy.restore();
                delete db.dbType;
            }

            let applied = await snapshotBalances(db);
            await balanceHelpers.rebuildBalances(db);
            assert.deepStrictEqual(applied, await snapshotBalances(db));
        });
    });

    describe('contract_balances', function() {

        let validId, invalidId;

        beforeEach(async function() {
            await db.doQuery("INSERT INTO index_statuses (status) VALUES ('valid'), ('invalid')");
            let rows = await db.doQuery("SELECT id, status FROM index_statuses");
            validId   = Number(rows.find(r => r.status === 'valid').id);
            invalidId = Number(rows.find(r => r.status === 'invalid').id);
        });

        it('a scoped rebuild leaves the table exactly as a full rebuild would', async function() {
            // History: contracts 1-3, fractional amounts, one invalid deposit
            // that must never count, pair (3,2) fully withdrawn (custody 0).
            await insertCustodyRows(db, 'deposits', [
                { action_index: 1, contract_index: 1, tick_id: 1, amount: '10.5',                  status_id: validId },
                { action_index: 2, contract_index: 2, tick_id: 1, amount: '0.000000000000000001',  status_id: validId },
                { action_index: 3, contract_index: 2, tick_id: 2, amount: '1000000',               status_id: validId },
                { action_index: 4, contract_index: 3, tick_id: 2, amount: '77',                    status_id: validId },
                { action_index: 5, contract_index: 1, tick_id: 1, amount: '500',                   status_id: invalidId }
            ]);
            await insertCustodyRows(db, 'withdrawals', [
                { action_index: 6, contract_index: 1, tick_id: 1, amount: '0.5', status_id: validId },
                { action_index: 7, contract_index: 3, tick_id: 2, amount: '77',  status_id: validId }
            ]);
            await balanceHelpers.rebuildContractBalances(db);

            // New rows touch contracts {2} × ticks {1,2}, including an invalid
            // withdrawal that must NOT reduce custody.
            await insertCustodyRows(db, 'deposits', [
                { action_index: 8, contract_index: 2, tick_id: 1, amount: '5', status_id: validId }
            ]);
            await insertCustodyRows(db, 'withdrawals', [
                { action_index: 9,  contract_index: 2, tick_id: 2, amount: '999999', status_id: validId },
                { action_index: 10, contract_index: 2, tick_id: 2, amount: '1',      status_id: invalidId }
            ]);

            await balanceHelpers.rebuildContractBalances(db, { contractIndexes: [2], tickIds: [1, 2] });
            let scoped = await snapshotContractBalances(db);

            await balanceHelpers.rebuildContractBalances(db);
            let full = await snapshotContractBalances(db);

            assert.deepStrictEqual(scoped, full);

            assert.ok(scoped.some(r => r[0] === 1 && r[1] === 1 && r[2] === '10'),
                'untouched contract 1 intact (invalid deposit excluded)');
            assert.ok(scoped.some(r => r[0] === 2 && r[1] === 1 && r[2] === '5.000000000000000001'),
                'touched fractional custody recomputed at full precision');
            assert.ok(scoped.some(r => r[0] === 2 && r[1] === 2 && r[2] === '1'),
                'invalid withdrawal did not reduce custody');
            assert.ok(!scoped.some(r => r[0] === 3 && r[1] === 2), 'zero-custody pair stays pruned');
        });
    });
});
