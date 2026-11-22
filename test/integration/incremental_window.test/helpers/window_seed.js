// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// A real-database incremental window large enough that a streamed table
// cannot fit in the driver's 16-row object buffer, so any read queued behind
// an unconsumed stream on the snapshot connection would never return.
//
// Window [SINCE, LAST]: blocks 1..LAST are seeded, blocks QUIET_FROM..LAST
// carry no actions. Every side set the snapshot merges (legacy cooldown
// maturities on both legs, derived and redriven validator rewards) is seeded
// both below the window and inside it, so a side set that is not bounded to
// the rows below the window comes back window-sized.

const fixtures = require('../../helpers/fixtures');

const SINCE = 11;
const LAST = 33;
const QUIET_FROM = 31;
const WINDOW_BLOCKS = [];
for (let b = SINCE; b < QUIET_FROM; b++) WINDOW_BLOCKS.push(b);

// Rows the payload must carry, counted from the seed below, not from the code.
// The driver pauses its socket once a stream's buffer is full, but rows
// already received in that socket read are still parsed, so a small window
// never blocks. BULK rows per streamed table put several megabytes behind
// the pause, which is what holds every later query on the connection.
const BULK = 20000;

const EXPECTED = {
    credits: 20 + 60 + 20 + 6 + 2 + BULK,
    escrows: 60 + 20 + 6 + 2 + BULK,
    validator_rewards: 20 + 18 + 1 + 1 + 2 + 1 + 4 + BULK,
    // The most rows any bounded side-set read may return: the backdated rows.
    maxSideSetRead: 6,
};

async function insertAll(db, conn, sql, rows) {
    for (const r of rows) await db.doQuery(sql, r, conn);
}

async function lookupIds(db, conn) {
    await db.doQuery("INSERT IGNORE INTO index_statuses (status) VALUES ('completed'), ('pending')", [], conn);
    await db.doQuery("INSERT IGNORE INTO index_tickers (tick) VALUES ('XCHAIN'), ('TOKEN')", [], conn);
    const pk = 'ab'.repeat(32);
    await db.doQuery('INSERT IGNORE INTO index_pubkeys (pubkey) VALUES (?)', [pk], conn);
    const one = async (sql, arg) => Number((await db.doQuery(sql, [arg], conn))[0].id);
    return {
        completed: await one('SELECT id FROM index_statuses WHERE status = ?', 'completed'),
        pending: await one('SELECT id FROM index_statuses WHERE status = ?', 'pending'),
        gas: await one('SELECT id FROM index_tickers WHERE tick = ?', 'XCHAIN'),
        token: await one('SELECT id FROM index_tickers WHERE tick = ?', 'TOKEN'),
        pubkeyId: await one('SELECT id FROM index_pubkeys WHERE pubkey = ?', pk),
        pubkey: pk,
    };
}

function ledgerRows(ids) {
    const credits = [];
    const escrows = [];
    const unstakes = [];
    const contractUnstakes = [];
    for (const b of WINDOW_BLOCKS) {
        for (let k = 1; k <= 3; k++) {
            credits.push([b * 100 + k, 1000 + k, ids.token, String(b * 10 + k)]);
            escrows.push([b * 100 + k, 1000 + k, ids.token, String(b * 10 + k)]);
        }
        // An unstake created and matured inside the window: the base stream
        // already carries its refund, so the cooldown side set must not.
        const ai = b * 100 + 50;
        unstakes.push([ai, 2000 + b, ids.pubkeyId, Math.min(b + 2, LAST), '5', ids.completed, b]);
        credits.push([ai, 2000 + b, ids.gas, '5']);
        escrows.push([ai, 2000 + b, ids.gas, '-5']);
    }
    for (let k = 1; k <= 5; k++) {
        unstakes.push([200 + k, 3000 + k, ids.pubkeyId, 12 + k, '6', ids.completed, 2]);
        credits.push([200 + k, 3000 + k, ids.gas, '6']);
        escrows.push([200 + k, 3000 + k, ids.gas, '-6']);
    }
    // Matures inside the quiet tail, where the window has no first action.
    unstakes.push([210, 3010, ids.pubkeyId, QUIET_FROM + 1, '6', ids.completed, 2]);
    credits.push([210, 3010, ids.gas, '6']);
    escrows.push([210, 3010, ids.gas, '-6']);
    for (let k = 1; k <= 2; k++) {
        contractUnstakes.push([300 + k, 4000 + k, ids.pubkeyId, ids.token, 20 + k, '8', ids.completed, 3]);
        credits.push([300 + k, 4000 + k, ids.token, '8']);
        escrows.push([300 + k, 4000 + k, ids.token, '-8']);
    }
    // Neither of these matures in the window: one too late, one not completed.
    unstakes.push([400, 4900, ids.pubkeyId, 40, '1', ids.completed, 4]);
    unstakes.push([401, 4901, ids.pubkeyId, 15, '1', ids.pending, 4]);
    credits.push([400, 4900, ids.gas, '1'], [401, 4901, ids.gas, '1']);
    return { credits, escrows, unstakes, contractUnstakes };
}

function rewardRows(ids) {
    const vr = [];
    const rpr = [];
    const row = (src, type, ref, block, derive) => [src, ids.pubkeyId, type, ref, '7', block, derive];
    for (const b of WINDOW_BLOCKS) vr.push(row(5000 + b, 'oracle_round', b, b, null));
    for (let k = 1; k <= 18; k++) vr.push(row(7000 + k, 'anchor_bundle', 200 + k, 12, 20));
    for (let k = 1; k <= 4; k++) vr.push(row(6000 + k, 'anchor_bundle', 100 + k, 5, 15));
    // NULL round_reference: the unique key admits both, the snapshot key
    // collapses them, so the backdated twin is dropped.
    vr.push(row(9, 'anchor_bundle', null, 25, null), row(9, 'anchor_bundle', null, 4, 16));
    const redriven = [[8001, 6, 18], [8002, 6, 18], [8100, 7, 19], [8200, 14, 22]];
    for (const [src, block, applied] of redriven) {
        vr.push(row(src, 'oracle_round', 300 + src, block, src === 8100 ? 17 : null));
        rpr.push(['addr_' + src, ids.pubkey, 'oracle_round', 300 + src, '7', block, src, 1, applied]);
    }
    return { vr, rpr };
}

// BULK in-window rows per streamed table, generated by the sequence engine.
async function seedBulk(db, conn, ids) {
    const seq = ' FROM seq_1_to_' + BULK;
    await db.doQuery('INSERT INTO credits (action_index, address_id, tick_id, amount) ' +
        "SELECT 100000 + seq, 1, ?, LPAD(seq, 40, '7')" + seq, [ids.token], conn);
    await db.doQuery('INSERT INTO escrows (action_index, address_id, tick_id, amount) ' +
        "SELECT 100000 + seq, 1, ?, LPAD(seq, 40, '7')" + seq, [ids.token], conn);
    await db.doQuery('INSERT INTO validator_rewards (source_id, signing_pubkey_id, reward_type, round_reference, amount, block_index) ' +
        "SELECT 100000 + seq, ?, 'oracle_round', seq, LPAD(seq, 40, '7'), ?" + seq, [ids.pubkeyId, QUIET_FROM - 1], conn);
}

async function seedWindow(db) {
    await fixtures.seedBlocks(db, 1, LAST);
    await db.withTransaction(async (conn) => {
        await db.doQuery('DELETE FROM actions WHERE block_index >= ?', [QUIET_FROM], conn);
        await db.doQuery('DELETE FROM credits WHERE action_index >= ?', [QUIET_FROM * 100], conn);
        const ids = await lookupIds(db, conn);
        const l = ledgerRows(ids);
        await insertAll(db, conn, 'INSERT INTO credits (action_index, address_id, tick_id, amount) VALUES (?, ?, ?, ?)', l.credits);
        await insertAll(db, conn, 'INSERT INTO escrows (action_index, address_id, tick_id, amount) VALUES (?, ?, ?, ?)', l.escrows);
        await insertAll(db, conn, 'INSERT INTO unstakes (action_index, source_id, signing_pubkey_id, cooldown_end_block, amount, status_id, block_index) VALUES (?, ?, ?, ?, ?, ?, ?)', l.unstakes);
        await insertAll(db, conn, 'INSERT INTO contract_unstakes (action_index, source_id, signing_pubkey_id, tick_id, cooldown_end_block, amount, status_id, block_index) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', l.contractUnstakes);
        const r = rewardRows(ids);
        await insertAll(db, conn, 'INSERT INTO validator_rewards (source_id, signing_pubkey_id, reward_type, round_reference, amount, block_index, derive_block_index) VALUES (?, ?, ?, ?, ?, ?, ?)', r.vr);
        await insertAll(db, conn, 'INSERT INTO recovery_pending_rewards (source_address, validator_pubkey, reward_type, round_reference, amount, block_index, source_id, applied, applied_block) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', r.rpr);
        await seedBulk(db, conn, ids);
    });
}

module.exports = { SINCE, LAST, QUIET_FROM, EXPECTED, seedWindow };
