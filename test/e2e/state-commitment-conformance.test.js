// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Byte-diff conformance/fuzz guard for the follower's independent getNetBalance
// reimplementation . The indexer (SOURCE) renders each SMT balance leaf
// value via mathjs: util.bcstr(util.bcsub(cr, dr, 18)). The sync follower has an
// independent reimplementation that renders the SAME net inside SQL
// (balance-helpers.minimalDecimal over the DECIMAL(60,18) credit/debit sums).
// The two strings feed merkle.canonicalAmount and MUST be byte-identical, or
// the follower's balances_root forks from the source and VERIFY_STATE_COMMITMENT
// halts every replica. Nothing previously compared the two renders directly;
// this suite runs the real SQL path against the real MariaDB (fixture tables
// created from the canonical indexer DDL) and compares byte-for-byte against a
// verbatim extraction of the indexer's mathjs pipeline, over both hand-picked
// edge vectors and a fast-check fuzz sweep.
//
// The mathjs reference below is an EXTRACTION of:
//   xchain-indexer/src/utility.js       bcnum / bcstr / bcsub (default mathjs config)
//   xchain-indexer/src/stateCommitment.js getNetBalance tail: bcstr(bcsub(cr, dr, 18))
// If the indexer's rendering ever changes, change it there first, then here.

const assert  = require('assert');
const fs      = require('fs');
const path    = require('path');
const fc      = require('fast-check');
const mathjs  = require('mathjs');
const testDb  = require('./helpers/testDb');
const { splitSqlStatements } = require('../../src/sqlUtil');
const { getNetBalance } = require('../../src/stateCommitment.js');
const M = require('../../src/merkle.js');

const DB_NAME  = 'xchain_e2e_state_commitment_conformance';
// DB-backed fuzz: each run round-trips inserts + the net-balance SELECT, so the
// default is lower than the pure in-process fuzz tiers. FUZZ_RUNS overrides.
const NUM_RUNS = parseInt(process.env.FUZZ_RUNS || '200', 10);

// ---- Indexer-side mathjs reference (extraction, see header) -----------------

function refBcnum(num) {
    return mathjs.bignumber(String(num).trim());
}

// utility.bcsub(a, b, 18): exact subtract, fixed-notation render at 18 dp,
// re-wrapped as a bignumber.
function refBcsub(a, b) {
    return refBcnum(mathjs.format(
        mathjs.subtract(mathjs.bignumber(a), mathjs.bignumber(b)),
        { notation: 'fixed', precision: 18 }));
}

// utility.bcstr: minimal fixed notation, never exponential.
function refBcstr(num) {
    return refBcnum(num).toFixed();
}

// The indexer feeds bcsub the SQL DECIMAL sums as strings; SUM(CAST(amount AS
// DECIMAL(60,18))) renders with all 18 decimals, mimicked here with the same
// fixed-notation format.
function refSum(amounts) {
    let acc = mathjs.bignumber(0);
    for (const a of amounts) acc = mathjs.add(acc, mathjs.bignumber(a));
    return mathjs.format(acc, { notation: 'fixed', precision: 18 });
}

// Full reference pipeline: what the indexer's getNetBalance returns for a
// (address, tick) whose credits/debits carry these amounts.
function refNetBalance(creditAmounts, debitAmounts) {
    return refBcstr(refBcsub(refSum(creditAmounts), refSum(debitAmounts)));
}

// ---- Fixture DB --------------------------------------------------------------

let db;
let nextId = 1;          // shared counter for address/ticker ids
let nextAction = 1;      // credits/debits action_index

// Seed only the four tables getNetBalance's SQL touches, from the CANONICAL
// indexer DDL files (same source seedSchema uses), so column types (VARCHAR
// amounts, id columns) cannot drift from the real schema.
async function seedTables(database) {
    const sqlDir = process.env.XCHAIN_INDEXER_SQL_PATH
        || path.join(__dirname, '..', '..', '..', 'xchain-indexer', 'src', 'sql');
    for (const file of ['index_addresses.sql', 'index_tickers.sql', 'credits.sql', 'debits.sql']) {
        const text = fs.readFileSync(path.join(sqlDir, file), 'utf8');
        for (const stmt of splitSqlStatements(text))
            await database.doQuery(stmt);
    }
}

// Insert a fresh (address, tick) pair plus its credit/debit rows; returns the
// canonical strings for the getNetBalance call. Fresh ids per case, so no
// cleanup between runs is needed and cases never interfere.
async function seedCase(creditAmounts, debitAmounts) {
    const id = nextId++;
    const address = 'addr_conf_' + id;
    const tick    = 'TICKCONF' + id;
    await db.doQuery('INSERT INTO index_addresses (id, address, block_index) VALUES (?, ?, 1)', [id, address]);
    await db.doQuery('INSERT INTO index_tickers   (id, tick,    block_index) VALUES (?, ?, 1)', [id, tick]);
    for (const [table, amounts] of [['credits', creditAmounts], ['debits', debitAmounts]]) {
        if (amounts.length === 0) continue;
        const values = amounts.map(() => '(?, ?, ?, ?)').join(', ');
        const args = [];
        for (const amt of amounts) args.push(nextAction++, id, id, amt);
        await db.doQuery(`INSERT INTO ${table} (action_index, address_id, tick_id, amount) VALUES ${values}`, args);
    }
    return { address, tick };
}

async function assertConformance(creditAmounts, debitAmounts) {
    const { address, tick } = await seedCase(creditAmounts, debitAmounts);
    const sqlNet = await getNetBalance(db, address, tick);
    const refNet = refNetBalance(creditAmounts, debitAmounts);
    assert.strictEqual(sqlNet, refNet,
        `byte-diff: SQL minimalDecimal render '${sqlNet}' != indexer bcsub/bcstr '${refNet}' ` +
        `for credits=${JSON.stringify(creditAmounts)} debits=${JSON.stringify(debitAmounts)}`);
    // The render must also be a form canonicalAmount ACCEPTS (the indexer once
    // wedged on exponential notation below 1e-7); equal bytes + a clean
    // canonicalisation means the leaf value is identical on both sides.
    // canonicalAmount rejects negatives BY DESIGN: a debits-over-credits net
    // never occurs on a consistent ledger, so the leaf path only ever sees
    // non-negative renders. The fuzzer still byte-diffs negative nets above
    // (both sides must agree even on impossible inputs), just not this check.
    if (!sqlNet.startsWith('-'))
        assert.doesNotThrow(() => M.canonicalAmount(sqlNet));
}

// ---- Amount generator ---------------------------------------------------------
// Indexer-written amounts are non-negative mathjs bcstr strings: up to tens of
// integer digits, at most 18 fractional digits, minimal decimal form. Integer
// part capped at 24 digits so a 6-term sum stays far inside DECIMAL(60,18).
const arbAmount = fc.tuple(
    fc.bigInt({ min: 0n, max: 10n ** 24n - 1n }),
    fc.integer({ min: 0, max: 18 }),
    fc.bigInt({ min: 0n, max: 10n ** 18n - 1n })
).map(([intPart, fracLen, fracSeed]) => {
    if (fracLen === 0) return intPart.toString();
    const frac = (fracSeed % (10n ** BigInt(fracLen))).toString().padStart(fracLen, '0');
    return intPart.toString() + '.' + frac;
});

const arbLedger = fc.record({
    credits: fc.array(arbAmount, { minLength: 0, maxLength: 6 }),
    debits:  fc.array(arbAmount, { minLength: 0, maxLength: 6 })
});

// ---- Suite --------------------------------------------------------------------

describe('E2E: state-commitment getNetBalance conformance (SQL minimalDecimal vs indexer mathjs bcsub)', function() {

    before(async function() {
        db = await testDb.createDatabase(DB_NAME);
        await seedTables(db);
    });

    after(async function() {
        if (db && db.pool) { try { await db.pool.end(); } catch (_) {} }
        await testDb.dropDatabase(DB_NAME);
    });

    it('renders hand-picked edge vectors byte-identically', async function() {
        const vectors = [
            // [credits, debits]
            [[], []],                                                        // no rows at all -> '0'
            [['100'], ['100']],                                              // exact cancel -> '0'
            [['0'], []],                                                     // explicit zero credit
            [['0.000000000000000001'], []],                                  // 1e-18: exponential-notation trap
            [['0.000000000000000001'], ['0.000000000000000002']],            // negative 1e-18
            [[], ['0.00000001']],                                            // sub-1e-7 negative net
            [['0.1', '0.2'], []],                                            // classic float trap, exact in DECIMAL
            [['10.500000000000000000'], []],                                 // non-minimal stored form
            [['123.450000000000000000'], ['23.45']],                         // trailing-zero trim across the sum
            [['1'], ['0.999999999999999999']],                               // 1e-18 remainder
            [['999999999999999999999999.999999999999999999'], []],           // 24-digit int + full 18 dp
            [['999999999999999999999999'], ['0.000000000000000001']],        // borrow across the decimal point
            [['20000000000000000000'], ['0.5']],                             // >16 significant digits (DOUBLE corruption zone)
            [['1000000'], ['1000000.000000000000000000']],                   // cancel against non-minimal debit
            [['3', '4', '5'], ['12']],                                       // multi-row exact cancel
            [['0.000000100000000000'], []],                                  // minimal render must keep inner zeros
        ];
        for (const [credits, debits] of vectors)
            await assertConformance(credits, debits);
    });

    it(`fuzz: ${NUM_RUNS} random ledgers render byte-identically`, async function() {
        await fc.assert(
            fc.asyncProperty(arbLedger, async ({ credits, debits }) => {
                await assertConformance(credits, debits);
            }),
            { numRuns: NUM_RUNS }
        );
    });
});
