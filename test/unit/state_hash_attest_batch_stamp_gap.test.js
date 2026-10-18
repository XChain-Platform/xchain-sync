/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md.
 *
 * The index-map class stays live so this fixture follows the canonical
 * regtest preimage and query order.
 *
 ********************************************************************/
'use strict';

const assert  = require('assert');
const Utility = require('../../src/util');
const {
    buildStateHashData, isAttestBatchHeadStateHashActive, ATTEST_BATCH_HEAD_STATE_HASH_ACTIVATION,
    POLL_FINALIZE_STATE_HASH_ACTIVATION, BET_STATUS_STATE_HASH_ACTIVATION,
    TOKEN_SUPPLY_STATE_HASH_ACTIVATION,
} = require('../../src/consensus/state_hash');
const G = require('../../src/consensus/state_hash/gated_sections');
const updatedRows = require('../../src/server/updated_rows');

const util = new Utility();
const PREFEATURE_KEYS = ['deactivations', 'slashes', 'request_status', 'cooldown', 'credits', 'anchor_invalid',
                         'index_addresses_new', 'index_tickers_new', 'block_index', 'state_hash_version'];
const STAMPED = 'invalid: ATTEST_BATCH (reassembly CRC mismatch)' + G.ATTEST_BATCH_COMPLETION_STAMP;

function dbFor(results, calls){
    let i = 0;
    return {
        doQuery: async (sql, args) => { if(calls) calls.push({ sql, args }); return results[i++]; },
        getStatusId: async () => null,
    };
}

function baseResults(){ return [[], [], [], [], [], [], [], [], [], [], []]; }

async function build(results, calls){
    const data = await buildStateHashData(dbFor(results || baseResults(), calls), 7,
        { activationDelay: null, gasTick: 'XCHAIN', network: 'regtest', coin: 'BTC' });
    return { data, hash: util.getDataHash(data) };
}

async function assertActiveFold(){    const source = baseResults().concat([[{ action_index: 100, status: STAMPED }]]);
    const stale  = baseResults().concat([[]]);
    const calls  = [];
    const s = await build(source, calls);
    assert.deepStrictEqual(Object.keys(s.data),
        ['deactivations', 'slashes', 'request_status', 'cooldown', 'credits', 'anchor_invalid',
         'index_addresses_new', 'index_tickers_new', 'attest_batch_head', 'block_index', 'state_hash_version']);
    assert.deepStrictEqual(s.data.attest_batch_head, [{ action_index: 100, status: STAMPED }]);
    assert.strictEqual(calls.length, 12);
    assert.deepStrictEqual(calls[11].args,
        ['%' + G.ATTEST_BATCH_COMPLETION_STAMP, G.ATTEST_BATCH_HEAD_VERSION,
         G.ATTEST_BATCH_CONTINUATION_VERSION, 7, G.ATTEST_BATCH_CONTINUATION_VERSION, 7]);
    const dropped = await build(stale);
    assert.notStrictEqual(dropped.hash, s.hash);
    const same = await build(baseResults().concat([[{ action_index: 100, status: STAMPED }]]));
    assert.strictEqual(same.hash, s.hash);
}

describe('state_hash attest_batch_head class, follower twin @regression', function(){
    const maps = { poll: POLL_FINALIZE_STATE_HASH_ACTIVATION,
                   bet: BET_STATUS_STATE_HASH_ACTIVATION, token: TOKEN_SUPPLY_STATE_HASH_ACTIVATION };
    let prev;
    before(function(){
        prev = {};
        for(const k of Object.keys(maps)){ prev[k] = maps[k].regtest; maps[k].regtest = 999999999; }
    });
    after(function(){
        for(const k of Object.keys(maps)) maps[k].regtest = prev[k];
    });

    it('gate: regtest armed from genesis, mainnet inert, testnet armed by the v0.23.0 cut, unknown network off', function(){
        assert.strictEqual(isAttestBatchHeadStateHashActive(1, 'regtest', 'DOGE'), true);
        assert.strictEqual(isAttestBatchHeadStateHashActive(999999999, 'mainnet', 'DOGE'), false);
        assert.strictEqual(isAttestBatchHeadStateHashActive(999999999, 'testnet', 'DOGE'), true);
        assert.strictEqual(isAttestBatchHeadStateHashActive(1, 'testnet', 'DOGE'), false);
        assert.strictEqual(isAttestBatchHeadStateHashActive(999999999, 'mainnet', null), false);
        assert.strictEqual(isAttestBatchHeadStateHashActive(7, 'nonexistent', 'BTC'), false);
        assert.strictEqual(isAttestBatchHeadStateHashActive('x', 'regtest', 'BTC'), false);
    });

    it('below threshold: no attest_batch_head key and no extra query', async function(){
        const prevH = ATTEST_BATCH_HEAD_STATE_HASH_ACTIVATION.regtest;
        ATTEST_BATCH_HEAD_STATE_HASH_ACTIVATION.regtest = 999999999;
        try {
            const calls = [];
            const { data } = await build(null, calls);
            assert.deepStrictEqual(Object.keys(data), PREFEATURE_KEYS);
            assert.strictEqual(calls.length, 11);
        } finally { ATTEST_BATCH_HEAD_STATE_HASH_ACTIVATION.regtest = prevH; }
    });

    it('active: stamped heads fold in before block_index and a dropped stamp changes the hash', assertActiveFold);

    it('selection: block-prefiltered stamped v5 head keyed by the completing chunk block, resolved status, total order', async function(){
        const calls = [];
        await G.collectAttestBatchHead(dbFor([[]], calls), 7);
        const sql = calls[0].sql;
        assert.match(sql, /s\.status LIKE \?/);
        assert.match(sql, /p\.version = \? AND p\.batch_chunk_index = 0/);
        assert.match(sql, /EXISTS \( SELECT 1 FROM attests e/);
        assert.match(sql, /e\.version = \? AND e\.request_id = p\.request_id AND e\.block_index = \?/);
        assert.match(sql, /ca\.source_id = pa\.source_id/);
        assert.match(sql, /cs\.status = 'valid'/);
        assert.match(sql, /\) = \? ORDER BY p\.action_index ASC$/);
        assert.doesNotMatch(sql, /status_id,|p\.status_id AS/);
        assert.deepStrictEqual(calls[0].args,
            ['%' + G.ATTEST_BATCH_COMPLETION_STAMP, G.ATTEST_BATCH_HEAD_VERSION,
             G.ATTEST_BATCH_CONTINUATION_VERSION, 7, G.ATTEST_BATCH_CONTINUATION_VERSION, 7]);
    });

    it('literal twins equal the follower forward-carry and rollback constants', function(){
        assert.strictEqual(G.ATTEST_BATCH_HEAD_VERSION, updatedRows.ATTEST_BATCH_HEAD_VERSION);
        assert.strictEqual(G.ATTEST_BATCH_CONTINUATION_VERSION, updatedRows.ATTEST_BATCH_CONTINUATION_VERSION);
        assert.strictEqual(G.ATTEST_BATCH_COMPLETION_STAMP, updatedRows.ATTEST_BATCH_COMPLETION_STAMP);
        assert.strictEqual(/[%_]/.test(G.ATTEST_BATCH_COMPLETION_STAMP), false);
    });
});
