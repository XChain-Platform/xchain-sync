/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 *********************************************************************/

'use strict';

// Unit coverage for collectRedrivenValidatorRewards (review #5087): the forward collector
// that delivers a reorg-redriven survivor validator_rewards row (block_index = earn-block
// E < B) to followers, selected by recovery_pending_rewards.applied_block. Mock-based (no
// live MariaDB): locks the query SHAPE, the arg order, the dedup, and error tolerance. The
// end-to-end semantic proof (reorg re-drain -> follower receives the E-row) runs at the
// integration venue.

const assert = require('assert');
const { collectRedrivenValidatorRewards } = require('../../src/recoveryRewards');

function row(over){
    return Object.assign({
        source_id: 1, signing_pubkey_id: 2, reward_type: 'anchor_BTC',
        round_reference: null, amount: '10.00000000', block_index: 100
    }, over || {});
}

describe('collectRedrivenValidatorRewards (#5087)', function(){
    it('returns [] (no throw) when the source has no re-driven rows', async function(){
        let db = { doQuery: async () => [] };
        let out = await collectRedrivenValidatorRewards(db, 150, 150);
        assert.deepStrictEqual(out, []);
    });

    it('selects by the applied_block window with the survivors-only backdating guard', async function(){
        let captured = null;
        let db = { doQuery: async (sql, args, conn) => { captured = { sql, args, conn }; return []; } };
        await collectRedrivenValidatorRewards(db, 150, 150);
        assert.match(captured.sql, /FROM validator_rewards vr/);
        assert.match(captured.sql, /JOIN recovery_pending_rewards rpr/);
        assert.match(captured.sql, /rpr\.round_reference <=> vr\.round_reference/, 'NULL-safe round_reference join');
        assert.match(captured.sql, /JOIN index_pubkeys ip ON ip\.id = vr\.signing_pubkey_id AND ip\.pubkey = rpr\.validator_pubkey/);
        assert.match(captured.sql, /rpr\.applied = 1 AND rpr\.applied_block IS NOT NULL/);
        assert.match(captured.sql, /rpr\.applied_block BETWEEN \? AND \?/);
        assert.match(captured.sql, /vr\.block_index < rpr\.applied_block/);
        assert.deepStrictEqual(captured.args, [150, 150]);
    });

    it('projects vr.* so redriven rows stay column-compatible with block-scoped rows (carry the id PK)', async function(){
        // Regression: a narrow projection (source_id, signing_pubkey_id, reward_type,
        // round_reference, amount, block_index) omits the validator_rewards AUTO_INCREMENT id.
        // These rows merge into the same payload array as SELECT * block-scoped rows, and
        // ClientApplier._insertRows keys its column list off rows[0], so a shape mismatch makes
        // the replica mint a divergent local id. Must select the full row.
        let captured = null;
        let db = { doQuery: async (sql) => { captured = sql; return []; } };
        await collectRedrivenValidatorRewards(db, 150, 150);
        assert.match(captured, /SELECT\s+vr\.\*/, 'must project the full validator_rewards row (vr.*)');
        assert.doesNotMatch(captured, /vr\.source_id\s+AS/, 'must not use the narrow id-omitting projection');
    });

    it('passes the snapshot connection through for the REPEATABLE READ view', async function(){
        let captured = null;
        let db = { doQuery: async (sql, args, conn) => { captured = conn; return []; } };
        let fakeConn = { id: 'snap-conn' };
        await collectRedrivenValidatorRewards(db, 10, 200, fakeConn);
        assert.strictEqual(captured, fakeConn);
    });

    it('maps rows through and dedups by the validator_rewards UNIQUE identity', async function(){
        let db = { doQuery: async () => [
            row(),
            row(),                                              // exact duplicate (live+snapshot overlap)
            row({ signing_pubkey_id: 3, amount: '5.00000000' }) // distinct identity
        ] };
        let out = await collectRedrivenValidatorRewards(db, 150, 150);
        assert.strictEqual(out.length, 2, 'duplicate logical identity collapses to one');
        assert.ok(out.some(r => r.signing_pubkey_id === 2 && r.block_index === 100));
        assert.ok(out.some(r => r.signing_pubkey_id === 3));
    });

    it('distinguishes rows that share columns but differ in round_reference', async function(){
        let db = { doQuery: async () => [
            row({ reward_type: 'oracle_round', round_reference: 5 }),
            row({ reward_type: 'oracle_round', round_reference: 6 })
        ] };
        let out = await collectRedrivenValidatorRewards(db, 150, 150);
        assert.strictEqual(out.length, 2);
    });

    it('swallows a query error (non-recovery stack / old schema) and returns []', async function(){
        let db = { doQuery: async () => { throw new Error('no such table: recovery_pending_rewards'); } };
        let out = await collectRedrivenValidatorRewards(db, 150, 150);
        assert.deepStrictEqual(out, []);
    });
});
