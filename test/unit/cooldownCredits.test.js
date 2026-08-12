// Unit coverage for src/cooldownCredits.js. A follower that misses matured
// cooldown-refund credits drifts into hash-blind balance divergence, so the
// collector's early-return, dedup, and missing-table tolerance are
// consensus-relevant. Exercised against a mock source Database (no live
// MariaDB): the collector only calls getStatusId and doQuery.

const assert = require('assert');
const { collectMaturedCooldownCredits } = require('../../src/cooldownCredits.js');

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
