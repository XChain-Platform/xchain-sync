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
const ServerPoller = require('../../../src/server/poller');
const Utility = require('../../../src/util');
const { withDbMixins } = require('../../helpers/db_mixins.js');

function createMockDb(){
    // Queries read through named Database methods. The real ones are installed for
    // any this fake does not stub, so they still reach doQuery below and every
    // doQuery call count these suites assert keeps counting them.
    return withDbMixins({
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getBlockScopedRows: sinon.stub().resolves([]),
        getTxScopedRows: sinon.stub().resolves([]),
        getActionScopedRows: sinon.stub().resolves([]),
        getEmissionRowsForBlock: sinon.stub().resolves([]),
        getStateRootsRow: sinon.stub().resolves({
            balances_root: 'br', block_merkle_root: 'bmr', state_root: 'sr'
        }),
        getTransactions: sinon.stub().resolves([]),
        getActions: sinon.stub().resolves([]),
        // Used by collectMaturedCooldownCredits; null short-circuits it to no credits.
        getStatusId: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        // The forward batch is pinned to a REPEATABLE READ snapshot (H-P2).
        beginReadSnapshot: sinon.stub().resolves({ mockSnapshotConn: true }),
        commitReadSnapshot: sinon.stub().resolves(),
        rollbackReadSnapshot: sinon.stub().resolves()
    });
}

function createMockBroadcaster(){
    return {
        broadcast: sinon.stub(),
        updateStatus: sinon.stub(),
        getSubscribers: sinon.stub().returns([]),
        getSubscriberCount: sinon.stub().returns(0)
    };
}

function createMockLog(){
    return {
        epochSize:        100,
        recordBlock:      sinon.stub().resolves(),
        pruneFrom:        sinon.stub().resolves(),
        getHighWaterMark: sinon.stub().resolves(null),
        getRecordedHash:  sinon.stub().resolves(null),
        findGaps:         sinon.stub().resolves([]),
        recommitEpoch:    sinon.stub().resolves()
    };
}

describe('ServerPoller', function(){

    let poller, db, broadcaster, log, config, util;

    beforeEach(function(){
        db = createMockDb();
        broadcaster = createMockBroadcaster();
        log = createMockLog();
        config = { BLOCK_POLL_INTERVAL: 3000 };
        util = new Utility();
        poller = new ServerPoller('bitcoin', 'mainnet', db, broadcaster, log, config, util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    describe('buildBlockPayload', function(){
        it('returns null when block hash row is missing', async function(){
            db.getBlockHashRow.resolves(null);
            let result = await poller.buildBlockPayload(100);
            assert.strictEqual(result, null);
        });

        it('builds complete payload with correct structure', async function(){
            db.getBlockHashRow.resolves({
                block_index: 100, block_time: 1700000000,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch'
            });
            db.getBlockScopedRows.resolves([{ block_index: 100 }]);
            db.getTransactions.resolves([{ tx_index: 1, block_index: 100, source_id: 10, tx_hash_id: 20 }]);
            db.getActions.resolves([{ action_index: 50, tx_index: 1 }]);

            let payload = await poller.buildBlockPayload(100);

            assert.strictEqual(payload.type, 'block');
            assert.strictEqual(payload.chain, 'bitcoin');
            assert.strictEqual(payload.network, 'mainnet');
            assert.strictEqual(payload.block_index, 100);
            assert.strictEqual(payload.block_time, 1700000000);
            assert.strictEqual(payload.ledger_hash, 'lh');
            assert.ok(payload.data);
            // 5250: live block payloads must carry schema_version so ClientApplier
            // can enforce the version-pin gate the snapshot paths enforce.
            const { SCHEMA_VERSION } = require('../../../src/schema/version');
            assert.strictEqual(payload.schema_version, SCHEMA_VERSION['indexer'],
                'live block payload must carry schema_version');
        });

        it('merges derived anchor/archive rewards (block_index = earn-block E, derive_block_index = this block) into validator_rewards', async function(){
            // The BTC-side derivation mints the row while processing block 961700 but stamps
            // block_index = SNAPSHOT_BLOCK 961500, so getBlockScopedRows(961700) never sees it
            // and a continuously-live follower never received it (#5605).
            db.getBlockHashRow.resolves({ block_index: 961700, block_time: 1700000000, ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch' });
            let derived = { id: 9, source_id: 1, signing_pubkey_id: 2, reward_type: 'anchor_BTC', round_reference: 961500,
                amount: '1.00000000', block_index: 961500, derive_block_index: 961700 };
            db.doQuery.withArgs(sinon.match(/vr\.derive_block_index BETWEEN \? AND \?/)).resolves([derived]);
            let payload = await poller.buildBlockPayload(961700);
            assert.ok(payload.data.validator_rewards, 'derived reward must ride the validator_rewards payload');
            assert.deepStrictEqual(payload.data.validator_rewards, [derived]);
            let q = db.doQuery.getCalls().find(c => /vr\.derive_block_index BETWEEN \? AND \?/.test(c.args[0]));
            assert.deepStrictEqual(q.args[1], [961700, 961700], 'keyed on THIS block as the materialization window');
        });

    });

    describe('buildBlockPayload', function(){
        it('streams contract_emissions via getEmissionRowsForBlock, not getActionScopedRows', async function(){
            db.getBlockHashRow.resolves({
                block_index: 100, block_time: 1700000000,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch'
            });
            // Internal SLASH emission: action_index is NULL; getActionScopedRows would drop it.
            db.getEmissionRowsForBlock.resolves([
                { execution_index: 10, emitted_action: 'SLASH', action_index: null, position: 0 }
            ]);

            let payload = await poller.buildBlockPayload(100);

            assert.ok(db.getEmissionRowsForBlock.calledOnceWith(100),
                'contract_emissions must be sourced via getEmissionRowsForBlock');
            assert.ok(!db.getActionScopedRows.getCalls().some(c => c.args[0] === 'contract_emissions'),
                'contract_emissions must NOT go through getActionScopedRows');
            assert.ok(payload.data['contract_emissions'], 'emission rows present in payload');
            assert.strictEqual(payload.data['contract_emissions'][0].action_index, null);
        });

        it('includes a sync_meta transparency row in the indexer payload', async function(){
            db.getBlockHashRow.resolves({
                block_index: 100, block_time: 1700000000,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch'
            });
            let payload = await poller.buildBlockPayload(100);

            assert.ok(payload.data['sync_meta'], 'sync_meta present in indexer payload');
            assert.strictEqual(payload.data['sync_meta'].length, 1);
            let row = payload.data['sync_meta'][0];
            assert.strictEqual(row.block_index, 100);
            assert.strictEqual(row.block_time, 1700000000);
            assert.strictEqual(row.ledger_hash, 'lh');
            assert.strictEqual(row.actions_hash, 'ah');
            assert.strictEqual(row.contract_hash, 'ch');
        });

        it('ships state_hash NULL for burst-built blocks (viewTip ahead of B)', async function(){
            db.getBlockHashRow.resolves({
                block_index: 100, block_time: 1700000000,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch', state_hash: 'sh'
            });
            // Catch-up burst: the pinned view's tip sits ahead of the block being
            // built, so updated_rows carry tip-state and the follower's apply-time
            // recompute at B must be skipped via the existing NULL gate.
            let payload = await poller.buildBlockPayload(100, null, 105);
            assert.strictEqual(payload.state_hash, null,
                'burst-built payload must ship state_hash NULL');
            assert.strictEqual(payload.ledger_hash, 'lh',
                'B-scoped hashes stay verified on the burst path');
        });

    });

    describe('buildBlockPayload', function(){
        it('keeps state_hash when the view tip IS the block (steady state) or is unknown', async function(){
            db.getBlockHashRow.resolves({
                block_index: 100, block_time: 1700000000,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch', state_hash: 'sh'
            });
            let steady = await poller.buildBlockPayload(100, null, 100);
            assert.strictEqual(steady.state_hash, 'sh', 'steady-state payload keeps state_hash');
            let noTip = await poller.buildBlockPayload(100, null);
            assert.strictEqual(noTip.state_hash, 'sh', 'unknown view tip keeps state_hash');
        });

        it('ships state_root NULL for burst blocks but keeps balances/merkle roots (@regression)', async function(){
            // block 1000000 is past the state-commitment flag-day for bitcoin/mainnet.
            db.getBlockHashRow.resolves({
                block_index: 1000000, block_time: 1700000000,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch', state_hash: 'sh'
            });
            // Catch-up burst: state_root folds the follower-recomputed stakes_root, which
            // is read from live tip-state stake tables during a burst, so it must be NULLed
            // exactly like state_hash. balances_root/block_merkle_root are B-scoped and stay.
            let burst = await poller.buildBlockPayload(1000000, null, 1000005);
            assert.strictEqual(burst.state_root, null,
                'burst-built payload must ship state_root NULL');
            assert.strictEqual(burst.balances_root, 'br',
                'balances_root stays verified on the burst path');
            assert.strictEqual(burst.block_merkle_root, 'bmr',
                'block_merkle_root stays verified on the burst path');

            let steady = await poller.buildBlockPayload(1000000, null, 1000000);
            assert.strictEqual(steady.state_root, 'sr',
                'steady-state payload keeps state_root');
        });

        it('omits sync_meta from the decoder payload', async function(){
            let decoderDb = createMockDb();
            decoderDb.dbType = 'decoder';
            decoderDb.getBlockHashRow.resolves({
                block_index: 100, block_time: 1700000000, block_hash: 'bh'
            });
            // Decoder has no transparency log.
            let decoderPoller = new ServerPoller('bitcoin', 'mainnet', decoderDb, broadcaster, null, config, util);

            let payload = await decoderPoller.buildBlockPayload(100);
            assert.strictEqual(payload.data['sync_meta'], undefined, 'decoder payload has no sync_meta');
            assert.strictEqual(payload.block_hash, 'bh');
        });

    });

    describe('buildBlockPayload', function(){
        it('includes block-scoped table rows in data', async function(){
            db.getBlockHashRow.resolves({
                block_index: 1, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });
            let blockRows = [{ block_index: 1, block_time: 100 }];
            db.getBlockScopedRows.resolves(blockRows);

            let payload = await poller.buildBlockPayload(1);
            assert.ok(payload.data['blocks']);
        });

        it('skips a per-table SCHEMA-GAP read error (errno 1146) and still builds the block @regression', async function(){
            db.getBlockHashRow.resolves({
                block_index: 1, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });
            let gap = new Error('table missing'); gap.errno = 1146;
            db.getBlockScopedRows.rejects(gap);
            db.getTransactions.resolves([]);
            db.getActions.resolves([]);

            let payload = await poller.buildBlockPayload(1);
            assert.ok(payload); // Should not be null
            assert.strictEqual(payload.type, 'block');
        });

        it('fails closed on a TRANSIENT per-table read error (deadlock 1213) so the block is retried, not broadcast incomplete @regression', async function(){
            db.getBlockHashRow.resolves({
                block_index: 1, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });
            let transient = new Error('Deadlock found when trying to get lock'); transient.errno = 1213;
            db.getBlockScopedRows.rejects(transient);
            db.getTransactions.resolves([]);
            db.getActions.resolves([]);

            let threw = false;
            try {
                await poller.buildBlockPayload(1);
            } catch(e){
                threw = true;
                assert.strictEqual(e.errno, 1213);
            }
            assert.ok(threw, 'a transient DB fault must propagate out of buildBlockPayload');
        });

    });
});
