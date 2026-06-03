const assert = require('assert');
const { rebuildBalances, rebuildContractBalances } = require('../../src/balance-helpers');

// These helpers are pure SQL emitters (the real arithmetic happens in MariaDB),
// so a unit test can only guard the SQL *semantics* — call order, target tables,
// credit/debit sign handling, the precision casts, and the HAVING pruning that
// drops zeroed/negative rows. A behavioural rebuild test belongs in the Tier-2
// DB suite; this is the regression net that catches a silent SQL edit.
function fakeDb() {
    const queries = [];
    return { queries, doQuery: async (sql) => { queries.push(sql.replace(/\s+/g, ' ').trim()); return []; } };
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
            assert.ok(/WHEN t\.type = 'credit' THEN t\.amount ELSE -t\.amount END/i.test(sql), 'credit positive, debit negative');
            assert.ok(/GROUP BY address_id, tick_id/i.test(sql));
        });

        it('prunes rows that net to exactly zero (HAVING ... != 0)', function () {
            assert.ok(/HAVING SUM\(.*\) != 0/i.test(db.queries[1]));
        });

        it('rejects if the DB layer fails (caller owns error handling)', async function () {
            const boom = { doQuery: async () => { throw new Error('1146 no table'); } };
            await assert.rejects(() => rebuildBalances(boom), /1146/);
        });
    });

    describe('rebuildContractBalances()', function () {
        let db;
        beforeEach(async function () { db = fakeDb(); await rebuildContractBalances(db); });

        it('clears then reinserts contract_balances', function () {
            assert.strictEqual(db.queries.length, 2);
            assert.ok(/^DELETE FROM contract_balances/i.test(db.queries[0]));
            assert.ok(/INSERT INTO contract_balances \(contract_index, tick_id, amount\)/i.test(db.queries[1]));
        });

        it('nets valid deposits minus valid withdrawals at DECIMAL(65,18) precision', function () {
            const sql = db.queries[1];
            assert.ok(/FROM deposits/i.test(sql) && /FROM withdrawals/i.test(sql));
            assert.ok(/WHEN t\.type = 'deposit' THEN CAST\(t\.amount AS DECIMAL\(65,18\)\)/i.test(sql), 'deposit cast');
            assert.ok(/ELSE -CAST\(t\.amount AS DECIMAL\(65,18\)\)/i.test(sql), 'withdrawal negative cast');
        });

        it('only counts rows whose status is valid', function () {
            const sql = db.queries[1];
            const matches = sql.match(/status_id = \(SELECT id FROM index_statuses WHERE status = 'valid'\)/gi) || [];
            assert.strictEqual(matches.length, 2, 'both deposits and withdrawals filtered to valid');
        });

        it('retains only positive custody (HAVING ... > 0)', function () {
            assert.ok(/HAVING SUM\(.*\) > 0/i.test(db.queries[1]));
        });
    });
});
