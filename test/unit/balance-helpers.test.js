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
const { rebuildBalances, recomputeTokenSupplies } = require('../../src/balance-helpers');

// These helpers are pure SQL emitters (the real arithmetic happens in MariaDB),
// so a unit test can only guard the SQL *semantics*: call order, target tables,
// credit/debit sign handling, the precision casts, and the HAVING pruning that
// drops zeroed/negative rows. A behavioural rebuild test belongs in the Tier-2
// DB suite; this is the regression net that catches a silent SQL edit.
function fakeDb() {
    const queries = [];
    const argsLog = [];
    return { queries, argsLog, doQuery: async (sql, args) => { queries.push(sql.replace(/\s+/g, ' ').trim()); argsLog.push(args || []); return []; } };
}

describe('balance-helpers @money @regression', function () {

    describe('rebuildBalances()', function () {
        let db;
        beforeEach(async function () { db = fakeDb(); await rebuildBalances(db); });

        it('clears the table before reinserting (DELETE then INSERT, in order)', function () {
            assert.strictEqual(db.queries.length, 2);
            assert.ok(/^DELETE FROM balances/i.test(db.queries[0]));
            assert.ok(/INSERT INTO balances \(address_id, tick_id, amount\)/i.test(db.queries[1]));
        });

        it('aggregates credits minus debits per (address_id, tick_id)', function () {
            const sql = db.queries[1];
            assert.ok(/FROM credits/i.test(sql) && /FROM debits/i.test(sql), 'unions credits + debits');
            assert.ok(/UNION ALL/i.test(sql));
            assert.ok(/WHEN t\.type = 'credit' THEN CAST\(t\.amount AS DECIMAL\(65,18\)\) ELSE -CAST\(t\.amount AS DECIMAL\(65,18\)\) END/i.test(sql), 'credit positive, debit negative');
            assert.ok(/GROUP BY address_id, tick_id/i.test(sql));
        });

        it('sums at DECIMAL(65,18) (bare VARCHAR amounts promote to DOUBLE and corrupt >16-digit amounts)', function () {
            const sql = db.queries[1];
            assert.ok(!/THEN t\.amount/i.test(sql), 'no un-cast amount may reach the SUM');
            assert.ok(/CAST\(t\.amount AS DECIMAL\(65,18\)\)/i.test(sql));
        });

        it('renders amounts in the source indexer\'s minimal-decimal format', function () {
            assert.ok(/TRIM\(TRAILING '\.' FROM TRIM\(TRAILING '0' FROM CAST\(/i.test(db.queries[1]),
                'trailing zeros then the bare dot are trimmed so rebuilt bytes match source bytes');
        });

        it('prunes rows that net to exactly zero (HAVING ... != 0)', function () {
            assert.ok(/HAVING SUM\(.*\) != 0/i.test(db.queries[1]));
        });

        it('rejects if the DB layer fails (caller owns error handling)', async function () {
            const boom = { doQuery: async () => { throw new Error('1146 no table'); } };
            await assert.rejects(() => rebuildBalances(boom), /1146/);
        });
    });

    describe('rebuildBalances(): scoped', function () {
        const scope = { addressIds: [7, 9], tickIds: [3] };
        let db;
        beforeEach(async function () { db = fakeDb(); await rebuildBalances(db, scope); });

        it('scopes the DELETE to the touched ids (parameterized IN-lists)', function () {
            assert.ok(/^DELETE FROM balances WHERE address_id IN \(\?, \?\) AND tick_id IN \(\?\)$/i.test(db.queries[0]));
            assert.deepStrictEqual(db.argsLog[0], [7, 9, 3]);
        });

        it('scopes BOTH union branches so the source scans stay bounded', function () {
            const sql = db.queries[1];
            const branchPreds = sql.match(/FROM (credits|debits) WHERE address_id IN \(\?, \?\) AND tick_id IN \(\?\)/gi) || [];
            assert.strictEqual(branchPreds.length, 2, 'credits and debits both scoped');
            assert.deepStrictEqual(db.argsLog[1], [7, 9, 3, 7, 9, 3], 'args repeated per branch');
        });

        it('keeps the full-rebuild aggregation semantics (signs, grouping, zero pruning)', function () {
            const sql = db.queries[1];
            assert.ok(/WHEN t\.type = 'credit' THEN CAST\(t\.amount AS DECIMAL\(65,18\)\) ELSE -CAST\(t\.amount AS DECIMAL\(65,18\)\) END/i.test(sql));
            assert.ok(/GROUP BY address_id, tick_id/i.test(sql));
            assert.ok(/HAVING SUM\(.*\) != 0/i.test(sql));
        });

        it('is a no-op when the scope is empty (nothing was touched)', async function () {
            const empty = fakeDb();
            await rebuildBalances(empty, { addressIds: [], tickIds: [] });
            assert.strictEqual(empty.queries.length, 0);
        });

        it('an absent scope still issues the unscoped full rebuild', async function () {
            const full = fakeDb();
            await rebuildBalances(full);
            assert.ok(/^DELETE FROM balances$/i.test(full.queries[0]));
            assert.ok(!/WHERE/i.test(full.queries[0]));
        });
    });

    // Fix #5 (MED): ClientRollback rebuilt balances but not tokens.supply on reorg,
    // so a surviving token whose supply was mutated in place by an orphaned MINT kept
    // the inflated value. recomputeTokenSupplies mirrors xchain-indexer getTokenSupply.
    describe('recomputeTokenSupplies() @money @regression', function () {
        // fakeDb variant that returns the distinct-precision rows for the first
        // SELECT, then [] for the per-precision UPDATEs.
        function precisionDb(decimalsRows) {
            const queries = [];
            const argsLog = [];
            return {
                queries, argsLog,
                doQuery: async (sql, args) => {
                    queries.push(sql.replace(/\s+/g, ' ').trim());
                    argsLog.push(args || []);
                    if (/SELECT DISTINCT decimals FROM tokens/i.test(sql)) return decimalsRows;
                    return [];
                }
            };
        }

        it('runs one UPDATE per distinct token precision, keyed by decimals', async function () {
            const db = precisionDb([{ decimals: 8 }, { decimals: 0 }]);
            await recomputeTokenSupplies(db);
            const updates = db.queries.filter(q => /^UPDATE tokens/i.test(q));
            assert.strictEqual(updates.length, 2, 'one UPDATE per distinct precision');
            // Precision is the bind arg (a DECIMAL scale can't be parameterized, but the
            // WHERE key can), so tokens are fixed precision-by-precision.
            const updateArgs = db.argsLog.filter((_, i) => /^UPDATE tokens/i.test(db.queries[i]));
            assert.deepStrictEqual(updateArgs.sort(), [[0], [8]]);
        });

        // Rows are summed EXACTLY (DECIMAL(60,18)) and the TOTAL is rounded once
        // to the token's own scale: per-ROW rounding agreed with the indexer only
        // while every amount sat on the token's grid, and inflates supply once
        // fee amounts go finer than the tick.
        it('sums (credits - debits) + escrows at the EXACT scale and rounds ONCE at the token scale', async function () {
            const db = precisionDb([{ decimals: 8 }]);
            await recomputeTokenSupplies(db);
            const sql = db.queries.find(q => /^UPDATE tokens/i.test(q));
            assert.ok(/FROM credits/i.test(sql) && /FROM debits/i.test(sql) && /FROM escrows/i.test(sql),
                'unions credits + debits + escrows (escrows folds into supply, unlike balances)');
            assert.ok(/SELECT tick_id, ?CAST\(amount AS DECIMAL\(60,18\)\) AS amt FROM credits/i.test(sql), 'credit is positive');
            assert.ok(/- ?CAST\(amount AS DECIMAL\(60,18\)\) AS amt FROM debits/i.test(sql), 'debit is negative');
            assert.ok(/CAST\(amount AS DECIMAL\(60,18\)\) AS amt FROM escrows/i.test(sql), 'escrow is positive');
            assert.ok(/CAST\(SUM\(amt\) AS DECIMAL\(60,8\)\)/i.test(sql),
                'the TOTAL is rounded once at the token scale, so it stays byte-identical to the source supply');
            assert.ok(!/CAST\(amount AS DECIMAL\(60,8\)\)/i.test(sql),
                'must NOT round each ROW to the token scale (the per-row rounding overcharge shape)');
            assert.ok(!/DECIMAL\(65,18\)/i.test(sql), 'must NOT use the fixed 65,18 scale (byte-identity needs the token scale)');
            assert.ok(/GROUP BY tick_id/i.test(sql));
        });

        it('LEFT JOINs so a token with no surviving ledger rows resolves to supply 0', async function () {
            const db = precisionDb([{ decimals: 8 }]);
            await recomputeTokenSupplies(db);
            const sql = db.queries.find(q => /^UPDATE tokens/i.test(q));
            assert.ok(/LEFT JOIN/i.test(sql), 'LEFT JOIN keeps tokens with no ledger rows');
            assert.ok(/SET tok\.supply = COALESCE\(agg\.supply, '0'\)/i.test(sql), 'no-ledger token -> supply 0');
        });

        it('renders supply in the source minimal-decimal format for divisible tokens', async function () {
            const db = precisionDb([{ decimals: 8 }]);
            await recomputeTokenSupplies(db);
            const sql = db.queries.find(q => /^UPDATE tokens/i.test(q));
            assert.ok(/TRIM\(TRAILING '\.' FROM TRIM\(TRAILING '0' FROM CAST\(CAST\(SUM\(amt\) AS DECIMAL\(60,8\)\) AS CHAR\)\)\)/i.test(sql),
                'divisible: round the total once, then trim trailing zeros and the bare dot');
        });

        it('renders an integer (no zero-trim) for a non-divisible token (decimals=0)', async function () {
            const db = precisionDb([{ decimals: 0 }]);
            await recomputeTokenSupplies(db);
            const sql = db.queries.find(q => /^UPDATE tokens/i.test(q));
            // decimals=0: CAST AS CHAR has no '.', so the trailing-zero trim would eat
            // integer zeros ('100' -> '1'); it must emit the integer verbatim.
            assert.ok(/CAST\(CAST\(SUM\(amt\) AS DECIMAL\(60,0\)\) AS CHAR\) AS supply/i.test(sql));
            assert.ok(!/TRIM\(TRAILING '0'/i.test(sql), 'no zero-trim on an integer supply');
        });

        it('clamps an out-of-range precision to [0,18] (no DECIMAL-scale injection)', async function () {
            const db = precisionDb([{ decimals: 99 }]);
            await recomputeTokenSupplies(db);
            const sql = db.queries.find(q => /^UPDATE tokens/i.test(q));
            assert.ok(/DECIMAL\(60,18\)/i.test(sql), 'clamped to the max scale');
            assert.ok(!/DECIMAL\(60,99\)/i.test(sql));
        });

        it('is a no-op when there are no tokens', async function () {
            const db = precisionDb([]);
            await recomputeTokenSupplies(db);
            assert.ok(!db.queries.some(q => /^UPDATE tokens/i.test(q)), 'no UPDATE without any token precisions');
        });

        it('rejects if the DB layer fails (caller owns error handling)', async function () {
            const boom = { doQuery: async () => { throw new Error('1146 no table'); } };
            await assert.rejects(() => recomputeTokenSupplies(boom), /1146/);
        });
    });

});
