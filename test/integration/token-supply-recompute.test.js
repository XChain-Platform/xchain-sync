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
const setup  = require('./helpers/setup');
const testDb = require('./helpers/testDb');
const balanceHelpers = require('../../src/balance-helpers');

// Behavioural proof for recomputeTokenSupplies (the reorg tokens.supply fix). The
// unit suite pins the SQL *shape*; this pins the arithmetic MariaDB actually
// performs: supply = (credits - debits) + escrows, summed at the token's OWN
// DECIMAL(60,decimals), rendered in the source indexer's minimal-decimal format so
// the recomputed string is byte-identical to what xchain-indexer getTokenSupply
// would write. It also proves the headline regression: an inflated supply left by
// an orphaned in-place MINT mutation is CORRECTED, not preserved.

async function insertLedger(db, table, rows) {
    for (let r of rows) {
        await db.doQuery(
            'INSERT INTO `' + table + '` (action_index, address_id, tick_id, amount) VALUES (?, ?, ?, ?)',
            [r.action_index, r.address_id, r.tick_id, r.amount]);
    }
}

async function seedToken(db, tickId, decimals, supply) {
    await db.doQuery(
        'INSERT INTO tokens (tick_id, supply, decimals) VALUES (?, ?, ?)',
        [tickId, supply, decimals]);
}

async function supplyOf(db, tickId) {
    let rows = await db.doQuery('SELECT supply FROM tokens WHERE tick_id=? LIMIT 1', [tickId]);
    return rows.length ? String(rows[0].supply) : null;
}

describe('Integration: token supply recompute', function() {

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

    it('recomputes each token supply byte-identically to (credits - debits) + escrows', async function() {
        // tick 1 (decimals 8): a divisible token whose tokens.supply row is INFLATED
        // (9999) as if an orphaned MINT mutated it in place; the surviving ledger nets
        // to 1000 + 500 (mint) - 200 + 50 (escrow) = 1350.
        await seedToken(db, 1, 8, '9999');
        await insertLedger(db, 'credits', [
            { action_index: 1, address_id: 1, tick_id: 1, amount: '1000' },
            { action_index: 2, address_id: 1, tick_id: 1, amount: '500' },
        ]);
        await insertLedger(db, 'debits', [
            { action_index: 3, address_id: 1, tick_id: 1, amount: '200' },
        ]);
        await insertLedger(db, 'escrows', [
            { action_index: 4, address_id: 1, tick_id: 1, amount: '50' },
        ]);

        // tick 2 (decimals 0): non-divisible; the minimal-decimal render must NOT trim
        // integer zeros (700 stays 700, not 7).
        await seedToken(db, 2, 0, '1');
        await insertLedger(db, 'credits', [
            { action_index: 5, address_id: 2, tick_id: 2, amount: '700' },
        ]);

        // tick 3 (decimals 8): a surviving token with NO ledger rows must resolve to '0'
        // (LEFT JOIN), matching getTokenSupply's all-zero result - not left inflated.
        await seedToken(db, 3, 8, '9999');

        // tick 4 (decimals 8): a fractional supply renders minimally (10.5, not 10.50000000).
        await seedToken(db, 4, 8, '0');
        await insertLedger(db, 'credits', [
            { action_index: 6, address_id: 4, tick_id: 4, amount: '10.5' },
        ]);

        // tick 5 (decimals 8): a supply that nets to exactly zero renders as '0'.
        await seedToken(db, 5, 8, '123');
        await insertLedger(db, 'credits', [{ action_index: 7, address_id: 5, tick_id: 5, amount: '100' }]);
        await insertLedger(db, 'debits',  [{ action_index: 8, address_id: 5, tick_id: 5, amount: '100' }]);

        // tick 6 (decimals 0): a >16-digit amount must survive exactly (a DOUBLE promotion
        // would corrupt it; the fix sums at DECIMAL, not the bare VARCHAR).
        let big = '99999999999999999999999999999999999999'; // 38 nines
        await seedToken(db, 6, 0, '0');
        await insertLedger(db, 'credits', [{ action_index: 9, address_id: 6, tick_id: 6, amount: big }]);

        await balanceHelpers.recomputeTokenSupplies(db);

        assert.strictEqual(await supplyOf(db, 1), '1350', 'inflated supply corrected to the surviving ledger sum');
        assert.strictEqual(await supplyOf(db, 2), '700',  'decimals=0 integer render keeps trailing zeros');
        assert.strictEqual(await supplyOf(db, 3), '0',    'no-ledger token resolves to 0, not left inflated');
        assert.strictEqual(await supplyOf(db, 4), '10.5', 'fractional supply renders minimally');
        assert.strictEqual(await supplyOf(db, 5), '0',    'net-zero supply renders as 0');
        assert.strictEqual(await supplyOf(db, 6), big,    '>16-digit supply survives byte-for-byte');
    });

    it('is idempotent: a second recompute leaves every supply unchanged', async function() {
        await seedToken(db, 1, 8, '0');
        await insertLedger(db, 'credits', [{ action_index: 1, address_id: 1, tick_id: 1, amount: '42.25' }]);

        await balanceHelpers.recomputeTokenSupplies(db);
        let once = await supplyOf(db, 1);
        await balanceHelpers.recomputeTokenSupplies(db);
        let twice = await supplyOf(db, 1);

        assert.strictEqual(once, '42.25');
        assert.strictEqual(twice, once, 'recompute is a pure function of the surviving ledger');
    });

});
