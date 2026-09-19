// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Behavioural proof for the tokens-EDIT updated-rows class, against real MariaDB and
// the real indexer schema.
//
// WHY THIS EXISTS. xchain-indexer#39: after a valid ownership transfer
// (ISSUE|0|<TICK>||||||<new owner>) the explorer served the OLD owner indefinitely,
// and flipped to the new one only once some unrelated later action touched the token.
// The source indexer was right the whole time; the replica the explorer reads was not.
// `tokens.action_index` is pinned at the tick's FIRST issuance, so an edit never
// re-enters the action-scoped stream's window, and the only carry that existed keyed
// on the ticks touched by a credit / debit / escrow row - which an ownership transfer
// does not write. That is why it looked intermittent: an edit that also moved a
// balance rode the supply class and looked fine.
//
// The unit tier pins the SQL shape. This pins what MariaDB actually selects over the
// real schema, and carries the row the whole way: source rows -> collectUpdatedRows
// -> ClientApplier.upsertRows -> the replica's own tokens.owner_id.

const assert = require('assert');
const sinon  = require('sinon');
const setup  = require('../helpers/setup');
const testDb = require('../helpers/testDb');

const { collectUpdatedRows } = require('../../../src/server/updated_rows');
const ClientApplier = require('../../../src/client/applier');

const TICK   = 'MGRTEST';
const OLD    = 'nOLDownerAddressForTheIssue39Repro';
const NEW    = 'nNEWownerAddressForTheIssue39Repro';
const GENESIS_BLOCK = 100;   // the tick's first issuance
const EDIT_BLOCK    = 200;   // the ownership transfer, far above it

// One ISSUE: its actions row (what pins it to a block), its transactions row and its
// issues row. `transfer_id` null on the genesis issuance, the new owner on the edit.
async function seedIssue(db, ids, opts){
    await db.doQuery(
        'INSERT INTO transactions (tx_index, block_index, tx_hash_id, source_id) VALUES (?, ?, ?, ?)',
        [opts.action_index, opts.block_index, opts.action_index, ids.old]);
    await db.doQuery(
        'INSERT INTO actions (action_index, block_index, tx_index, tx_vout, action_id, action_format, source_id) ' +
            'VALUES (?, ?, ?, 0, ?, 0, ?)',
        [opts.action_index, opts.block_index, opts.action_index, ids.action, ids.old]);
    await db.doQuery(
        'INSERT INTO issues (action_index, tick_id, transfer_id, status_id) VALUES (?, ?, ?, ?)',
        [opts.action_index, ids.tick, opts.transfer_id, opts.status_id]);
}

// The interned ids every row above is keyed by, plus the tokens row the genesis
// issuance produced (owner = OLD, action_index pinned at the genesis action).
async function seedTick(db){
    await db.doQuery('INSERT INTO index_tickers (tick) VALUES (?)', [TICK]);
    await db.doQuery('INSERT INTO index_addresses (address) VALUES (?), (?)', [OLD, NEW]);
    await db.doQuery('INSERT INTO index_actions (action) VALUES (?)', ['ISSUE']);
    await db.doQuery('INSERT INTO index_statuses (status) VALUES (?), (?)', ['valid', 'invalid: issued by another address']);
    await db.doQuery('INSERT INTO index_transactions (hash) VALUES (?), (?)', ['tx_genesis_issue', 'tx_owner_transfer']);
    let ids = {
        tick:    (await db.doQuery('SELECT id FROM index_tickers WHERE tick=?', [TICK]))[0].id,
        old:     (await db.doQuery('SELECT id FROM index_addresses WHERE address=?', [OLD]))[0].id,
        new:     (await db.doQuery('SELECT id FROM index_addresses WHERE address=?', [NEW]))[0].id,
        action:  (await db.doQuery('SELECT id FROM index_actions WHERE action=?', ['ISSUE']))[0].id,
        valid:   (await db.doQuery('SELECT id FROM index_statuses WHERE status=?', ['valid']))[0].id,
        invalid: (await db.doQuery('SELECT id FROM index_statuses WHERE status=?', ['invalid: issued by another address']))[0].id
    };
    await seedIssue(db, ids, { action_index: 1, block_index: GENESIS_BLOCK, transfer_id: null, status_id: ids.valid });
    await db.doQuery(
        'INSERT INTO tokens (tick_id, owner_id, action_index, last_action_index, supply, decimals) VALUES (?, ?, 1, 1, ?, 0)',
        [ids.tick, ids.old, '1000']);
    return ids;
}

// What the source indexer does when the transfer settles: the issues row, then the
// in-place tokens UPDATE (createToken's UPDATE binds owner_id and leaves action_index
// at the first issuance). No credit / debit / escrow row: a transfer moves no balance,
// which is the whole point of the repro.
async function applyTransferOnSource(db, ids){
    await seedIssue(db, ids, { action_index: 2, block_index: EDIT_BLOCK, transfer_id: ids.new, status_id: ids.valid });
    await db.doQuery('UPDATE tokens SET owner_id=?, last_action_index=1 WHERE tick_id=?', [ids.new, ids.tick]);
}

async function ownerOf(db, ids){
    let rows = await db.doQuery('SELECT owner_id FROM tokens WHERE tick_id=? LIMIT 1', [ids.tick]);
    return rows.length ? Number(rows[0].owner_id) : null;
}

describe('Integration: tokens owner refresh after an edit-ISSUE (xchain-indexer#39)', function(){

    let sourceDb, replicaDb;

    before(async function(){
        await setup.globalSetup();
        sourceDb  = setup.getSourceDb();
        replicaDb = setup.getReplicaDb();
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    after(async function(){
        sinon.restore();
        await setup.globalTeardown();
    });

    beforeEach(async function(){
        await setup.resetDatabases();
    });

    it('carries the new owner to the replica in the transfer block, with no ledger row anywhere', async function(){
        let ids = await seedTick(sourceDb);
        // The replica is where the explorer reads: it holds the pre-transfer row.
        let replicaIds = await seedTick(replicaDb);
        assert.strictEqual(await ownerOf(replicaDb, replicaIds), replicaIds.old);

        await applyTransferOnSource(sourceDb, ids);
        assert.strictEqual(await ownerOf(sourceDb, ids), ids.new, 'source indexer is right immediately');

        // No ledger row exists at all, so the supply class contributes nothing and the
        // edit class is the only thing that can carry this row.
        for(let table of ['credits', 'debits', 'escrows'])
            assert.strictEqual(await testDb.getRowCount(sourceDb, table), 0);

        let updated = await collectUpdatedRows(sourceDb, EDIT_BLOCK, EDIT_BLOCK, 6);
        assert.ok(updated.tokens, 'the transfer block must carry the tokens row');
        assert.strictEqual(updated.tokens.length, 1);
        assert.strictEqual(Number(updated.tokens[0].owner_id), ids.new);

        await new ClientApplier(replicaDb, testDb.util).upsertRows('tokens', updated.tokens);
        assert.strictEqual(await ownerOf(replicaDb, replicaIds), replicaIds.new,
            'the replica the explorer reads must serve the new owner in this same block');
    });

});

describe('Integration: tokens owner refresh, the windows that must stay quiet', function(){

    let sourceDb;

    before(async function(){
        await setup.globalSetup();
        sourceDb = setup.getSourceDb();
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    after(async function(){
        sinon.restore();
        await setup.globalTeardown();
    });

    beforeEach(async function(){
        await setup.resetDatabases();
    });

    it('does not carry the row for a block whose only ISSUE on the tick is invalid', async function(){
        let ids = await seedTick(sourceDb);
        // An ISSUE from the wrong address never reaches createToken, so no tokens row
        // was mutated and there is nothing to refresh.
        await seedIssue(sourceDb, ids, { action_index: 2, block_index: EDIT_BLOCK, transfer_id: ids.new, status_id: ids.invalid });

        let updated = await collectUpdatedRows(sourceDb, EDIT_BLOCK, EDIT_BLOCK, 6);
        assert.ok(!updated.tokens, 'an invalid ISSUE mutates nothing and must not re-emit the row');
    });

    it('does not carry the row for a block the tick had no ISSUE in', async function(){
        let ids = await seedTick(sourceDb);
        await applyTransferOnSource(sourceDb, ids);

        // The window the reporter polled through: every block after the transfer, none
        // of which touches the tick. The class is keyed on the edit's own block.
        let updated = await collectUpdatedRows(sourceDb, EDIT_BLOCK + 1, EDIT_BLOCK + 24, 6);
        assert.ok(!updated.tokens, 'only the edit block re-emits the row');
    });
});
