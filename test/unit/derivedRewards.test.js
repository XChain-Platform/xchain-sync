// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.
//
// Coverage for the derived anchor/archive validator-reward forward collector: the
// BTC-side derivation stamps block_index = the checkpoint's SNAPSHOT_BLOCK E with
// derive_block_index = the minting block B, so every block-keyed forward channel missed
// the row (#5605). This collector selects by derive_block_index, the forward twin of
// ClientRollback's derive_block_index >= B reverse delete.

const assert = require('assert');
const { collectDerivedAnchorRewards } = require('../../src/derivedRewards');

function row(over){
    return Object.assign({
        id: 9, source_id: 1, signing_pubkey_id: 2, reward_type: 'anchor_BTC',
        round_reference: 961500, amount: '1.00000000', block_index: 961500, derive_block_index: 961700
    }, over || {});
}

describe('collectDerivedAnchorRewards', function(){
    it('returns [] (no throw) when the source has no derived rows', async function(){
        let db = { doQuery: async () => [] };
        assert.deepStrictEqual(await collectDerivedAnchorRewards(db, 961700, 961700), []);
    });

    it('selects by the derive_block_index window with the backdated-only guard', async function(){
        let captured = null;
        let db = { doQuery: async (sql, args, conn) => { captured = { sql, args, conn }; return []; } };
        await collectDerivedAnchorRewards(db, 961700, 961700);
        assert.match(captured.sql, /FROM validator_rewards vr/);
        assert.match(captured.sql, /vr\.derive_block_index BETWEEN \? AND \?/);
        assert.match(captured.sql, /vr\.block_index < vr\.derive_block_index/,
            'a row whose earn-block is its own materialization block already streams block-scoped');
        assert.deepStrictEqual(captured.args, [961700, 961700]);
    });

    it('projects vr.* so derived rows stay column-compatible with block-scoped rows (carry the id PK)', async function(){
        let captured = null;
        let db = { doQuery: async (sql) => { captured = sql; return []; } };
        await collectDerivedAnchorRewards(db, 1, 2);
        assert.match(captured, /SELECT\s+vr\.\*/, 'must project the full validator_rewards row (vr.*)');
    });

    it('passes the snapshot connection through for the REPEATABLE READ view', async function(){
        let captured = null;
        let db = { doQuery: async (sql, args, conn) => { captured = conn; return []; } };
        let fakeConn = { id: 'snap-conn' };
        await collectDerivedAnchorRewards(db, 10, 200, fakeConn);
        assert.strictEqual(captured, fakeConn);
    });

    it('maps rows through and dedups by the validator_rewards UNIQUE identity', async function(){
        let db = { doQuery: async () => [ row(), row(), row({ signing_pubkey_id: 3 }) ] };
        let out = await collectDerivedAnchorRewards(db, 961700, 961700);
        assert.strictEqual(out.length, 2);
        assert.strictEqual(out[0].block_index, 961500, 'block_index stays the earn-block E');
        assert.strictEqual(out[0].derive_block_index, 961700);
    });

    it('swallows ONLY a schema gap (1054/1146); a transient fault propagates so the block is retried', async function(){
        let gap = Object.assign(new Error('Unknown column derive_block_index'), { errno: 1054 });
        assert.deepStrictEqual(await collectDerivedAnchorRewards({ doQuery: async () => { throw gap; } }, 1, 1), []);
        let transient = Object.assign(new Error('Lock wait timeout'), { errno: 1205 });
        await assert.rejects(() => collectDerivedAnchorRewards({ doQuery: async () => { throw transient; } }, 1, 1), /Lock wait/);
    });
});
