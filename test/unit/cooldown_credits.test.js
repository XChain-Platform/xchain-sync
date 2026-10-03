//  doctrine test-coverage program: unit coverage for src/server/cooldown_credits.js.
// A follower that misses matured cooldown-refund credits drifts into hash-blind
// balance divergence, so the collector's early-return, dedup, and
// missing-table tolerance are consensus-relevant. Exercised against a mock
// source Database (no live MariaDB): the collector only calls getStatusId and
// doQuery.

const assert = require('assert');
const { collectMaturedCooldownCredits: collectFromRealFake } = require('../../src/server/cooldown_credits.js');
// The collector reads through a named Database method, and these fakes stand in
// for the database with nothing but doQuery. Mixing the REAL mixin onto whatever
// fake reaches the collector keeps every assertion below exactly as it was: the
// real method still hands its SQL, args and connection to the fake's doQuery.
const CREDITS_MIXIN = require('../../src/db/credits.js');
const collectMaturedCooldownCredits = (db, ...rest) => collectFromRealFake(Object.assign(db, CREDITS_MIXIN), ...rest);

function mockDb({ statusId = 1, queries = [] } = {}) {
    let call = 0;
    return {
        async getStatusId() { return statusId; },
        async doQuery() {
            const r = queries[call] || [];
            call += 1;
            if (r instanceof Error) throw r;
            return r;
        },
        _calls: () => call,
    };
}

describe('collectMaturedCooldownCredits', function () {
    it('returns [] when the completed status id is unresolved', async function () {
        const rows = await collectMaturedCooldownCredits(mockDb({ statusId: null }), 100, 200);
        assert.deepStrictEqual(rows, []);
    });

    it('aggregates GAS and contract refund rows from both channels', async function () {
        const gas = [{ action_index: 10, address_id: 5, tick_id: 1, amount: '3' }];
        const contract = [{ action_index: 11, address_id: 6, tick_id: 2, amount: '4' }];
        const rows = await collectMaturedCooldownCredits(mockDb({ queries: [gas, contract] }), 1, 999);
        assert.strictEqual(rows.length, 2);
        const keys = rows.map((r) => `${r.action_index}:${r.address_id}:${r.tick_id}`).sort();
        assert.deepStrictEqual(keys, ['10:5:1', '11:6:2']);
    });

    it('dedups rows sharing the (action_index, address_id, tick_id) identity', async function () {
        const dup = { action_index: 10, address_id: 5, tick_id: 1, amount: '3' };
        const rows = await collectMaturedCooldownCredits(mockDb({ queries: [[dup], [dup]] }), 1, 999);
        assert.strictEqual(rows.length, 1);
    });

    it('skips a missing table (errno 1146) rather than throwing', async function () {
        const noTable = Object.assign(new Error('no such table'), { errno: 1146 });
        const contract = [{ action_index: 12, address_id: 7, tick_id: 3, amount: '5' }];
        const rows = await collectMaturedCooldownCredits(mockDb({ queries: [noTable, contract] }), 1, 999);
        assert.strictEqual(rows.length, 1);
        assert.strictEqual(rows[0].action_index, 12);
    });

    it('re-throws an unexpected DB error (not a missing table/column)', async function () {
        const fatal = Object.assign(new Error('deadlock'), { errno: 1213 });
        await assert.rejects(
            () => collectMaturedCooldownCredits(mockDb({ queries: [fatal] }), 1, 999),
            /deadlock/,
        );
    });
});

// The escrow release paired with each matured refund rides the same collector over
// the escrows finders, so the follower's escrows table (and the supply its rollback
// recomputes from it) keeps every release the source wrote.
describe('collectMaturedCooldownEscrows', function () {
    const { collectMaturedCooldownEscrows: collectEscrowsFromRealFake } = require('../../src/server/cooldown_credits.js');
    const collectMaturedCooldownEscrows = (db, ...rest) => collectEscrowsFromRealFake(Object.assign(db, CREDITS_MIXIN), ...rest);

    // A fake that records each statement, answering in call order like mockDb.
    function recordingDb(queries) {
        const sqls = [];
        const db = mockDb({ queries });
        const inner = db.doQuery;
        db.doQuery = async (sql, ...rest) => { sqls.push(sql); return inner(sql, ...rest); };
        return { db, sqls };
    }

    it('reads the escrows table by the unstake keys, never credits', async function () {
        const { db, sqls } = recordingDb([[], []]);
        await collectMaturedCooldownEscrows(db, 1, 999);
        assert.strictEqual(sqls.length, 2);
        assert.ok(/^SELECT e\.\* FROM escrows e JOIN unstakes u ON u\.action_index = e\.action_index AND u\.source_id = e\.address_id/.test(sqls[0]), sqls[0]);
        assert.ok(/^SELECT e\.\* FROM escrows e JOIN contract_unstakes cu ON cu\.action_index = e\.action_index AND cu\.source_id = e\.address_id AND cu\.tick_id = e\.tick_id/.test(sqls[1]), sqls[1]);
        assert.ok(sqls.every((s) => !/credits/.test(s)), 'an escrow collector must never read credits');
    });

    it('returns [] when the completed status id is unresolved', async function () {
        const rows = await collectMaturedCooldownEscrows(mockDb({ statusId: null }), 100, 200);
        assert.deepStrictEqual(rows, []);
    });

    it('aggregates the GAS and contract releases and dedups on the logical identity', async function () {
        const gas = { action_index: 10, address_id: 5, tick_id: 1, amount: '-3' };
        const contract = { action_index: 11, address_id: 6, tick_id: 2, amount: '-4' };
        const rows = await collectMaturedCooldownEscrows(mockDb({ queries: [[gas, gas], [contract]] }), 1, 999);
        const keys = rows.map((r) => `${r.action_index}:${r.address_id}:${r.tick_id}`).sort();
        assert.deepStrictEqual(keys, ['10:5:1', '11:6:2']);
    });

    it('skips a schema gap and re-throws anything else', async function () {
        const noTable = Object.assign(new Error('no such table'), { errno: 1146 });
        const rows = await collectMaturedCooldownEscrows(mockDb({ queries: [noTable, []] }), 1, 999);
        assert.deepStrictEqual(rows, []);
        const fatal = Object.assign(new Error('deadlock'), { errno: 1213 });
        await assert.rejects(() => collectMaturedCooldownEscrows(mockDb({ queries: [fatal] }), 1, 999), /deadlock/);
    });
});

describe('mergeMaturedRows', function () {
    const { mergeMaturedRows } = require('../../src/server/cooldown_credits.js');

    it('returns the payload rows unchanged (even undefined) when nothing matured', function () {
        assert.strictEqual(mergeMaturedRows(undefined, []), undefined);
        const rows = [{ action_index: 1, address_id: 1, tick_id: 1 }];
        assert.strictEqual(mergeMaturedRows(rows, []), rows);
    });

    it('appends only the matured rows whose identity the payload does not already carry', function () {
        const kept = { action_index: 1, address_id: 1, tick_id: 1, amount: '-1' };
        const fresh = { action_index: 2, address_id: 1, tick_id: 1, amount: '-2' };
        const out = mergeMaturedRows([kept], [Object.assign({}, kept), fresh]);
        assert.deepStrictEqual(out, [kept, fresh]);
        assert.deepStrictEqual(mergeMaturedRows(undefined, [fresh]), [fresh]);
    });
});
