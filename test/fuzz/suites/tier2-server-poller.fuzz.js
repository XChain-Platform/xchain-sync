/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available —
 * contact legal@dankest.llc.
 *
 **********************************************************************
 * Tier 2 Fuzz: ServerPoller
 *
 * Tests that _buildBlockPayload never crashes regardless of what the mocked
 * source database returns. Exercises the 40+ table reads, try/catch error
 * handling per table, and payload assembly logic.
 */

const assert = require('assert');
const fc = require('fast-check');
const sinon = require('sinon');
const {
    NUM_RUNS, createMockDb, createMockUtil,
    createMockBroadcaster, createMockTransparencyLog, createMockConfig,
} = require('../setup/harness');
const { blockIndex, blockTime, hashString } = require('../generators/values');
const { blocksRow, transactionsRow, actionsRow, rowArray } = require('../generators/rows');

let crashLog = [];

describe('Tier 2 - ServerPoller @tier2', function () {
    this.timeout(0);
    let ServerPoller, poller, db, broadcaster, log, config, util;

    before(function () {
        ServerPoller = require('../../../src/ServerPoller');
    });

    beforeEach(function () {
        db = createMockDb();
        broadcaster = createMockBroadcaster();
        log = createMockTransparencyLog();
        config = createMockConfig();
        util = createMockUtil();
        poller = new ServerPoller('bitcoin', 'mainnet', db, broadcaster, log, config, util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function () {
        sinon.restore();
    });

    describe('_buildBlockPayload', function () {

        it('returns null when getBlockHashRow returns null', function () {
            return fc.assert(fc.asyncProperty(
                blockIndex(),
                async (bi) => {
                    db.getBlockHashRow.resolves(null);
                    let result = await poller._buildBlockPayload(bi);
                    assert.strictEqual(result, null);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('never throws when all DB methods resolve normally', function () {
            return fc.assert(fc.asyncProperty(
                fc.record({
                    block_index: blockIndex(),
                    block_time: blockTime(),
                    ledger_hash: hashString(),
                    actions_hash: hashString(),
                    contract_hash: hashString(),
                }),
                async (hashRow) => {
                    db.getBlockHashRow.resolves(hashRow);
                    db.getBlockScopedRows.resolves([]);
                    db.getActionScopedRows.resolves([]);
                    db.getTransactions.resolves([]);
                    db.getActions.resolves([]);
                    db.doQuery.resolves([]);

                    let result = await poller._buildBlockPayload(hashRow.block_index);
                    assert.ok(result !== null);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('payload always has correct top-level shape', function () {
            return fc.assert(fc.asyncProperty(
                fc.record({
                    block_index: blockIndex(),
                    block_time: blockTime(),
                    ledger_hash: hashString(),
                    actions_hash: hashString(),
                    contract_hash: hashString(),
                }),
                async (hashRow) => {
                    db.getBlockHashRow.resolves(hashRow);
                    let result = await poller._buildBlockPayload(hashRow.block_index);

                    assert.strictEqual(result.type, 'block');
                    assert.strictEqual(result.chain, 'bitcoin');
                    assert.strictEqual(result.network, 'mainnet');
                    assert.strictEqual(typeof result.data, 'object');
                    assert.ok(result.data !== null);
                    assert.strictEqual(result.block_index, Number(hashRow.block_index));
                    assert.strictEqual(result.ledger_hash, hashRow.ledger_hash);
                    assert.strictEqual(result.actions_hash, hashRow.actions_hash);
                    assert.strictEqual(result.contract_hash, hashRow.contract_hash);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('never throws when individual DB table reads throw', function () {
            return fc.assert(fc.asyncProperty(
                fc.integer({ min: 0, max: 20 }),
                async (failIndex) => {
                    db.getBlockHashRow.resolves({
                        block_index: 1, block_time: 100,
                        ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c',
                    });

                    let callCount = 0;
                    db.getBlockScopedRows.callsFake(async () => {
                        if (callCount++ === failIndex) throw new Error('fuzz-table-fail');
                        return [];
                    });
                    db.getActionScopedRows.callsFake(async () => {
                        if (callCount++ === failIndex) throw new Error('fuzz-table-fail');
                        return [];
                    });
                    db.getTransactions.resolves([]);
                    db.getActions.resolves([]);
                    db.doQuery.resolves([]);

                    // Should not throw — errors are caught per table
                    let result = await poller._buildBlockPayload(1);
                    assert.ok(result !== null);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('never throws when DB returns fuzzed row objects', function () {
            return fc.assert(fc.asyncProperty(
                rowArray(blocksRow(), 0, 3),
                rowArray(transactionsRow(), 0, 10),
                rowArray(actionsRow(), 0, 10),
                async (blockRows, txRows, actionRows) => {
                    db.getBlockHashRow.resolves({
                        block_index: 1, block_time: 100,
                        ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c',
                    });
                    db.getBlockScopedRows.resolves(blockRows);
                    db.getTransactions.resolves(txRows);
                    db.getActions.resolves(actionRows);
                    db.getActionScopedRows.resolves([]);
                    db.doQuery.resolves([]);

                    let result = await poller._buildBlockPayload(1);
                    assert.ok(result !== null);
                    assert.strictEqual(typeof result.data, 'object');
                }
            ), { numRuns: NUM_RUNS });
        });
    });

    after(function () {
        if (crashLog.length > 0) {
            let unique = [...new Set(crashLog.map(c => c.error))];
            console.log('\n    [FUZZ CRASH REPORT] ' + crashLog.length + ' crashes across ' + unique.length + ' unique error types:');
            for (let err of unique) {
                let count = crashLog.filter(c => c.error === err).length;
                console.log('      - (' + count + 'x) ' + err.substring(0, 120));
            }
        }
    });
});
