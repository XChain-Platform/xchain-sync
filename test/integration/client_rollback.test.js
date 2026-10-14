// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert   = require('assert');
const sinon    = require('sinon');
const setup    = require('./helpers/setup');
const testDb   = require('./helpers/testDb');
const fixtures = require('./helpers/fixtures');
const ClientRollback = require('../../src/client/rollback');

describe('Integration: ClientRollback', function() {

    let replicaDb, rollback;

    before(async function() {
        await setup.globalSetup();
        replicaDb = setup.getReplicaDb();
        rollback = new ClientRollback(replicaDb, testDb.util, undefined, 'regtest');
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    after(async function() {
        sinon.restore();
        await setup.globalTeardown();
    });

    beforeEach(async function() {
        await testDb.truncateAll(replicaDb);
    });

    describe('rollback deletes correct rows', function() {
        it('removes blocks at and after rollback point', async function() {
            await fixtures.seedBlocks(replicaDb, 1, 10);

            let blocksBefore = await testDb.getRowCount(replicaDb, 'blocks');
            assert.strictEqual(blocksBefore, 10);

            await rollback.rollback(8);

            let blocksAfter = await testDb.getRowCount(replicaDb, 'blocks');
            assert.strictEqual(blocksAfter, 7);

            // Verify blocks 1-7 remain
            let rows = await replicaDb.doQuery("SELECT block_index FROM blocks ORDER BY block_index");
            assert.strictEqual(rows.length, 7);
            assert.strictEqual(Number(rows[0].block_index), 1);
            assert.strictEqual(Number(rows[6].block_index), 7);
        });

        it('removes transactions at and after rollback point', async function() {
            await fixtures.seedBlocks(replicaDb, 1, 10);
            await rollback.rollback(8);

            let txCount = await testDb.getRowCount(replicaDb, 'transactions');
            assert.strictEqual(txCount, 7);
        });

        it('removes actions with action_index >= firstActionIndex', async function() {
            await fixtures.seedBlocks(replicaDb, 1, 10);
            await rollback.rollback(8);

            let actionsCount = await testDb.getRowCount(replicaDb, 'actions');
            assert.strictEqual(actionsCount, 7);
        });

        it('removes credits with action_index >= firstActionIndex', async function() {
            await fixtures.seedBlocks(replicaDb, 1, 10);
            await rollback.rollback(8);

            let creditsCount = await testDb.getRowCount(replicaDb, 'credits');
            assert.strictEqual(creditsCount, 7);
        });

        it('removes sync_meta entries at and after rollback point', async function() {
            await fixtures.seedBlocks(replicaDb, 1, 10);
            // Insert sync_meta entries
            for (let i = 1; i <= 10; i++) {
                await replicaDb.doQuery(
                    "INSERT IGNORE INTO sync_meta (block_index, block_time, ledger_hash) VALUES (?, ?, ?)",
                    [i, 1700000000 + i, 'lh' + i]
                );
            }

            await rollback.rollback(8);

            let syncMetaCount = await testDb.getRowCount(replicaDb, 'sync_meta');
            assert.strictEqual(syncMetaCount, 7);
        });
    });

    describe('balance recalculation', function() {
        it('rebuilds balances from remaining credits/debits', async function() {
            // Seed blocks with credits
            await fixtures.seedBlocks(replicaDb, 1, 10, { creditAmount: '500' });

            // Verify balances before rollback
            let balancesBefore = await replicaDb.doQuery("SELECT * FROM balances");
            assert.ok(balancesBefore.length > 0);

            // Rollback to block 6 (removes blocks 6-10, keeping 1-5)
            await rollback.rollback(6);

            let creditsRemaining = await testDb.getRowCount(replicaDb, 'credits');
            assert.strictEqual(creditsRemaining, 5);

            // Seeding already rebuilt 10 balance rows, so only the rollback's own rebuild
            // can bring them to the five surviving addresses of 500 each and drop the rest.
            let balancesAfter = await replicaDb.doQuery("SELECT * FROM balances");
            assert.strictEqual(balancesAfter.length, 5, 'one balance row per surviving credited address');
            for (let row of balancesAfter)
                assert.strictEqual(Number(row.amount), 500, 'stale balance amount ' + row.amount);
            let orphaned = await replicaDb.doQuery(
                "SELECT COUNT(*) AS n FROM balances b LEFT JOIN credits c " +
                "ON c.address_id = b.address_id AND c.tick_id = b.tick_id WHERE c.address_id IS NULL");
            assert.strictEqual(Number(orphaned[0].n), 0, 'balances kept rows for rolled-back credits');
        });

        it('excludes zero-balance entries', async function() {
            // Seed blocks with matching credits and debits
            await fixtures.seedBlocks(replicaDb, 1, 5, {
                creditAmount: '1000', debitAmount: '1000',
                sourceAddr: 'same_addr', tickName: 'ZERO'
            });

            // Seeding leaves balances empty, so plant a stale non-zero row that only the
            // rollback's own rebuild can remove.
            let pair = await replicaDb.doQuery("SELECT address_id, tick_id FROM credits LIMIT 1");
            await replicaDb.doQuery("INSERT INTO balances (address_id, tick_id, amount) VALUES (?, ?, ?)",
                [pair[0].address_id, pair[0].tick_id, '1000']);

            await rollback.rollback(3);

            // Balances should be empty (credits = debits for remaining blocks)
            let balances = await replicaDb.doQuery("SELECT * FROM balances");
            assert.strictEqual(balances.length, 0);
        });
    });

    describe('rollback with no actions', function() {
        it('handles block with no actions cleanly', async function() {
            // fixtures.seedBlocks always attaches an action, so insert directly here
            // to get a block with none.
            // Insert a block without actions
            await replicaDb.doQuery(
                "INSERT INTO blocks (block_index, block_time) VALUES (?, ?)",
                [1, 1700000001]
            );
            await replicaDb.doQuery(
                "INSERT INTO blocks (block_index, block_time) VALUES (?, ?)",
                [2, 1700000002]
            );

            await rollback.rollback(2);

            let blockCount = await testDb.getRowCount(replicaDb, 'blocks');
            assert.strictEqual(blockCount, 1);
        });
    });

    // tokens.bridged is set in place on a surviving token row by the first applied XBRIDGE
    // v3 lock; the generic delete cannot reach it, so the rollback re-derives it from the
    // surviving xbridges rows exactly as the source indexer does (rederiveTokenBridged).
    describe('tokens.bridged re-derive', function() {
        async function idOf(table, column, value) {
            await replicaDb.doQuery("INSERT IGNORE INTO `" + table + "` (`" + column + "`) VALUES (?)", [value]);
            const rows = await replicaDb.doQuery("SELECT id FROM `" + table + "` WHERE `" + column + "` = ?", [value]);
            return Number(rows[0].id);
        }

        // One bridged token row per tick, issued at action 150 (survives a rollback to block 6,
        // whose first action is 600), and its v3 locks as [action_index, status_id] pairs.
        async function seedBridgedToken(tick, locks, version = 3) {
            const tickId = await idOf('index_tickers', 'tick', tick);
            await replicaDb.doQuery(
                "INSERT INTO tokens (tick_id, action_index, last_action_index, supply, bridged) VALUES (?, 150, 150, '0', 1)",
                [tickId]);
            for (const [actionIndex, statusId] of locks)
                await replicaDb.doQuery(
                    "INSERT INTO xbridges (action_index, version, tick_id, status_id, block_index) VALUES (?, ?, ?, ?, ?)",
                    [actionIndex, version, tickId, statusId, Math.floor(actionIndex / 100)]);
            return tickId;
        }

        async function bridgedOf(tickId) {
            const rows = await replicaDb.doQuery("SELECT bridged FROM tokens WHERE tick_id = ?", [tickId]);
            return Number(rows[0].bridged);
        }

        it('clears bridged when the only v3 lock is orphaned and keeps it when one survives', async function() {
            await fixtures.seedBlocks(replicaDb, 1, 10);
            const valid = await idOf('index_statuses', 'status', 'valid');
            const invalid = await idOf('index_statuses', 'status', 'invalid: AMOUNT');
            const orphanedOnly = await seedBridgedToken('BRGORPHAN', [[700, valid]]);
            const survivor = await seedBridgedToken('BRGSURVIVE', [[250, valid], [800, valid]]);
            const refusedOnly = await seedBridgedToken('BRGREFUSED', [[300, invalid], [900, valid]]);
            const xchainLock = await seedBridgedToken('BRGV0', [[350, valid]], 0);

            await rollback.rollback(6);

            assert.strictEqual(await bridgedOf(orphanedOnly), 0, 'a token whose only lock was orphaned must read unbridged');
            assert.strictEqual(await bridgedOf(survivor), 1, 'a surviving valid v3 lock keeps the bit');
            assert.strictEqual(await bridgedOf(refusedOnly), 0, 'a refused v3 lock never bridged the token');
            assert.strictEqual(await bridgedOf(xchainLock), 0, 'only a v3 token lock sets the bit');
            const locksLeft = await replicaDb.doQuery("SELECT COUNT(*) AS n FROM xbridges WHERE action_index >= 600");
            assert.strictEqual(Number(locksLeft[0].n), 0, 'the orphaned locks are deleted');
        });
    });

    describe('rollback then re-sync', function() {
        it('allows new data to be applied after rollback', async function() {
            await fixtures.seedBlocks(replicaDb, 1, 10);
            await rollback.rollback(6);

            let blocksAfterRollback = await testDb.getRowCount(replicaDb, 'blocks');
            assert.strictEqual(blocksAfterRollback, 5);

            // Re-seed blocks 6-8 with different data
            await fixtures.seedBlocks(replicaDb, 6, 8, { creditAmount: '9999' });

            let blocksAfterResync = await testDb.getRowCount(replicaDb, 'blocks');
            assert.strictEqual(blocksAfterResync, 8);

            // Verify new credit amounts
            let credits = await replicaDb.doQuery(
                "SELECT c.amount FROM credits c INNER JOIN actions a ON a.action_index = c.action_index INNER JOIN transactions t ON t.tx_index = a.tx_index WHERE t.block_index >= 6"
            );
            for (let row of credits) {
                assert.strictEqual(row.amount, '9999');
            }
        });
    });
});
