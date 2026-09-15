// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const {
    assert, sinon, axios, ClientSync, createMockDb, registerClientSyncHooks
} = require('./support');

let sync, db, applier, rollback, hashVerifier, config, util;

function assignState(state){
    ({ sync, db, applier, rollback, hashVerifier, config, util } = state);
}

function registerVerifyTableCountsGroup1Tests(){
    describe('verifyTableCounts (replica-completeness)', function(){
        it('flags a table the source has rows in but the follower has zeroed', async function(){
            // The core gap this guards: ledger/actions/contract hashes still agree,
            // yet contract_stakes never replicated to the follower.
            db.getTableCount = async (t) => ({ blocks: 100, actions: 5000, contract_stakes: 0 })[t];
            let mismatches = await sync.verifyTableCounts({ blocks: 100, actions: 5000, contract_stakes: 7 });
            assert.strictEqual(mismatches.length, 1);
            assert.strictEqual(mismatches[0].table, 'contract_stakes');
            assert.strictEqual(mismatches[0].sourceCount, 7);
            assert.strictEqual(mismatches[0].localCount, 0);
            assert.strictEqual(mismatches[0].delta, 7);
        });

        it('reports a table missing entirely from the follower as a full shortfall', async function(){
            // getTableCount throws (table absent in this replica's schema) → treated as 0.
            db.getTableCount = async () => { throw new Error('no such table'); };
            let mismatches = await sync.verifyTableCounts({ attests: 3 });
            assert.strictEqual(mismatches.length, 1);
            assert.strictEqual(mismatches[0].table, 'attests');
            assert.strictEqual(mismatches[0].localCount, 0);
            assert.strictEqual(mismatches[0].delta, 3);
        });

        it('heals the schema when a replicated table is missing locally (errno 1146)', async function(){
            // The live bug: a table the source ADDED but has never written a row to
            // (polls / poll_results / vote_delegations on the BTC replicas) streams
            // nothing, so the apply-path heal never fires. The completeness check is
            // the only place that touches it. Swallowing the 1146 when the source
            // count is also 0 prevents any mismatch from being raised. Result:
            // ER_NO_SUCH_TABLE logged forever, table never created.
            let healed = [];
            sync.healSchemaIfStale = async (e) => { healed.push(e.errno); return true; };
            let err = new Error("Table 'X.polls' doesn't exist"); err.errno = 1146;
            db.getTableCount = async () => { throw err; };

            let mismatches = await sync.verifyTableCounts({ polls: 0 });
            assert.deepStrictEqual(healed, [1146], 'missing table must trigger the schema heal');
            // Source has 0 rows, so it is correctly NOT a count mismatch.
            assert.strictEqual(mismatches.length, 0);
        });
    });
}

function registerVerifyTableCountsGroup2Tests(){
    describe('verifyTableCounts (replica-completeness)', function(){
        it('does not fault the completeness check when the schema heal itself throws', async function(){
            sync.healSchemaIfStale = async () => { throw new Error('DDL rejected'); };
            let err = new Error('missing'); err.errno = 1146;
            db.getTableCount = async () => { throw err; };
            // Advisory path: still returns, still reports the shortfall.
            let mismatches = await sync.verifyTableCounts({ polls: 4 });
            assert.strictEqual(mismatches.length, 1);
            assert.strictEqual(mismatches[0].localCount, 0);
        });

        it('returns no mismatches when the follower is complete (local >= source)', async function(){
            db.getTableCount = async (t) => ({ blocks: 100, actions: 5000, deposits: 12 })[t];
            // Without a same-height gate a follower ahead on a table is not flagged: the
            // /status counts and the local count can straddle a block (ordinary skew).
            let mismatches = await sync.verifyTableCounts({ blocks: 100, actions: 4999, deposits: 12 });
            assert.strictEqual(mismatches.length, 0);
        });

        it('at the same height, reports a replica-AHEAD delta on exact-parity tables as reason replica-ahead', async function(){
            // An un-replicated source-side forward DELETE (the anchor-reward winner
            // collapse on validator_rewards) leaves the replica strictly ahead; the
            // shortfall-only check was structurally blind to it (#5610).
            db.getTableCount = async (t) => ({ validator_rewards: 12, actions: 5000 })[t];
            let mismatches = await sync.verifyTableCounts({ validator_rewards: 10, actions: 5000 }, undefined,
                { remoteHeight: 900, localHeight: 900 });
            assert.strictEqual(mismatches.length, 1);
            assert.deepStrictEqual(mismatches[0], { table: 'validator_rewards', sourceCount: 10, localCount: 12, delta: -2, reason: 'replica-ahead' });
        });
    });
}

function registerVerifyTableCountsGroup3Tests(){
    describe('verifyTableCounts (replica-completeness)', function(){
        it('replica-ahead is gated on equal heights and on the registry exact-parity class', async function(){
            db.getTableCount = async (t) => ({ validator_rewards: 12, events: 50, markets: 9 })[t];
            // Height skew: no replica-ahead report (ordinary lag between the status read and the local count).
            let skew = await sync.verifyTableCounts({ validator_rewards: 10 }, undefined, { remoteHeight: 899, localHeight: 900 });
            assert.strictEqual(skew.length, 0);
            // Same height, but events (snapshot class) and markets (snapshot/special) may legitimately differ.
            let classed = await sync.verifyTableCounts({ events: 40, markets: 8 }, undefined, { remoteHeight: 900, localHeight: 900 });
            assert.strictEqual(classed.length, 0);
            // Shortfalls keep their shape and stay reported regardless of the gate.
            db.getTableCount = async () => 3;
            let short = await sync.verifyTableCounts({ validator_rewards: 5 }, undefined, { remoteHeight: 900, localHeight: 900 });
            assert.deepStrictEqual(short, [{ table: 'validator_rewards', sourceCount: 5, localCount: 3, delta: 2 }]);
        });

        it('treats absent/invalid table_counts as nothing to check (older source builds)', async function(){
            db.getTableCount = async () => 100;
            assert.deepStrictEqual(await sync.verifyTableCounts(undefined), []);
            assert.deepStrictEqual(await sync.verifyTableCounts(null), []);
            assert.deepStrictEqual(await sync.verifyTableCounts({ blocks: 'not-a-number' }), []);
        });

        it('skips a malicious table name without passing it to getTableCount', async function(){
            // A hostile/MITM'd source could put a backtick-injection payload in a
            // table_counts key. It must be rejected at the boundary, never reaching
            // the identifier interpolation in getTableCount, and must not manufacture
            // a false mismatch.
            let queried = [];
            db.getTableCount = async (t) => { queried.push(t); return 0; };
            let evilKey = 'blocks` WHERE 1=1 UNION SELECT password FROM mysql.user -- ';
            let mismatches = await sync.verifyTableCounts({ [evilKey]: 999, blocks: 100 });

            assert.ok(!queried.includes(evilKey), 'malicious key must never reach getTableCount');
            assert.deepStrictEqual(queried, ['blocks'], 'only the valid identifier is queried');
            assert.ok(!mismatches.some(m => m.table === evilKey), 'malicious key produces no mismatch');
        });
    });
}

describe('ClientSync', function(){
    registerClientSyncHooks(assignState);
    registerVerifyTableCountsGroup1Tests();
    registerVerifyTableCountsGroup2Tests();
    registerVerifyTableCountsGroup3Tests();
});
