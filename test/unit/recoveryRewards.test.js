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

// Unit coverage for collectRedrivenValidatorRewards: the forward collector
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

describe('collectRedrivenValidatorRewards', function(){
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

    it('swallows ONLY a schema gap (1146/1054); a transient fault propagates so the block is retried', async function(){
        // Regression: a bare catch here made both callers' isSchemaGapError gates dead
        // code (ServerPoller freezes the cursor, SnapshotBuilder aborts the stream), so a
        // deadlock/lock-wait/connection drop shipped the block short its backdated rows
        // with no consensus-hash signal. Mirrors derivedRewards on the same rail.
        let missingTable = Object.assign(new Error("Table 'recovery_pending_rewards' doesn't exist"), { errno: 1146 });
        assert.deepStrictEqual(await collectRedrivenValidatorRewards(
            { doQuery: async () => { throw missingTable; } }, 150, 150), []);

        let missingColumn = Object.assign(new Error('Unknown column applied_block'), { errno: 1054 });
        assert.deepStrictEqual(await collectRedrivenValidatorRewards(
            { doQuery: async () => { throw missingColumn; } }, 150, 150), []);

        let transient = Object.assign(new Error('Lock wait timeout exceeded'), { errno: 1205 });
        await assert.rejects(() => collectRedrivenValidatorRewards(
            { doQuery: async () => { throw transient; } }, 150, 150), /Lock wait/);

        let deadlock = Object.assign(new Error('Deadlock found when trying to get lock'), { errno: 1213 });
        await assert.rejects(() => collectRedrivenValidatorRewards(
            { doQuery: async () => { throw deadlock; } }, 150, 150), /Deadlock/);

        // An errno-less fault is not a schema gap either: both callers' gates classify it
        // as fatal (isSchemaGapError is false), so it must reach them rather than be eaten.
        await assert.rejects(() => collectRedrivenValidatorRewards(
            { doQuery: async () => { throw new Error('socket hang up'); } }, 150, 150), /socket hang up/);
    });
});
