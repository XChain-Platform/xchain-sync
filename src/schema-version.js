/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************
 *
 * XChain Sync - Snapshot Schema Version
 *
 * SCHEMA_VERSION carries an independent version per dbType ({ indexer, decoder }).
 * Bump only the key whose replicated DB had a DDL change (column added, dropped,
 * renamed, or type changed; index added, dropped, or changed; or a primary-key,
 * foreign-key, or constraint change) OR a wire-encoding change to its row values. Keeping
 * the two versions separate means a schema change to one DB does not force the
 * other dbType's validators to restart; only validators of the changed dbType
 * see a mismatch. Mismatched versions cause those validators to refuse the
 * snapshot and log a clear error rather than silently corrupting replica state.
 * After bumping a key, that dbType's validators must be restarted (or will
 * restart automatically on the next bootstrap) so that _fetchAndApplySchema
 * re-runs against the new schema.
 *
 * Version history:
 *   1 - initial replicated schema (both dbTypes).
 *   2 - binary (BLOB/binary) column values are base64-encoded on the wire
 *       as a { "__xbin__": "<base64>" } sentinel and decoded back to Buffers
 *       on apply (see src/wireCodec.js). Prior versions corrupted every
 *       binary column. A v1 peer fails closed against a v2 snapshot. This wire
 *       change affected both dbTypes, so both keys advanced to 2 together.
 *   3 - (indexer only) incremental snapshots and live block payloads gained an
 *       `updated_rows` channel carrying the current state of SURVIVING rows the source
 *       mutated in place (deactivation_block, SLASH amounts, v0 request_status), which
 *       the action_index-scoped paths cannot reach. The follower UPSERTs them and
 *       re-derives the escrow gate locally; a v2 follower silently ignores the field
 *       and stays divergent, so the bump forces a coordinated server+follower upgrade.
 *       Decoder is unaffected (none of these tables exist there) and stays at 2.
 *   4 - (indexer only) two DDL changes to `attests`, both on the action_index-scoped
 *       stream: the `request_status` ENUM widened with the terminal value 'rejected',
 *       and the `request_id_version` index relaxed from UNIQUE to non-unique so a
 *       request can carry one v1 row per retry round. A v3 follower keeps the narrow
 *       ENUM, so a streamed 'rejected' row either fails the block apply in strict mode
 *       or coerces to '' and flows through as serviceable; it also keeps the stale
 *       UNIQUE, so the second v1 row hits ER_DUP_ENTRY under ClientApplier's plain
 *       INSERT and stalls the queue. Both migrations are mode=auto, so they self-heal
 *       fleet-wide on the forced restart. Decoder is unaffected and stays at 3.
 *   3 - (decoder only) `dispensers.expiration` converted from DATETIME to
 *       BIGINT UNSIGNED holding raw unix seconds. The old DATETIME path silently
 *       NULLed any expiration past Y2038 while the protocol accepts values up to
 *       4294967295 (year 2106), so a v2 replica still on DATETIME disagrees with the
 *       source about which dispensers are expired. That migration is mode=manual and
 *       does NOT auto-apply on follower startup, so this bump is what makes the
 *       ordering enforced rather than advisory: a v2 follower must run it (or re-sync
 *       to the fresh BIGINT base schema) before it can consume v3. Indexer is
 *       unaffected by this key and stays at 4.
 *   5 - (indexer only) `block_index BIGINT NULL` added to the replicated lookup tables
 *       `index_addresses` and `index_tickers`, recording the block at which each dense
 *       index id was first assigned so a reorg can delete index rows created in
 *       orphaned blocks, which is what makes a wire ^<id> reference resolve to the same
 *       entity on every node. It enters no block-hash preimage (getBlockHashes resolves
 *       ids to canonical strings), so this is purely a replication-DDL concern.
 *       ClientApplier derives its INSERT column list from the SERVER row's keys, so a
 *       v4 follower that has not run the migration hits Unknown column 'block_index'
 *       and stalls its queue with a cryptic error; the bump replaces that with an
 *       explicit mismatch. The migration is mode=auto and the index_* tables resume via
 *       INSERT IGNORE paged from a MAX(id) cursor, so no full re-snapshot is needed.
 *       Decoder is unaffected (these tables do not exist there) and stays at 3.
 *   6 - (indexer only) BET parimutuel betting: four new replicated tables (bet_feeds,
 *       bets, bet_feed_statuses, bet_statuses; stream:action, base-schema files so
 *       fresh replicas create them from the schema fetch) plus two updated_rows classes
 *       for surviving bet_feeds rows latched or terminal-flipped in place
 *       (closed_block / terminal_block) and surviving bets rows settled in place
 *       (settled_block), with matching ClientRollback resets. A v5 follower would
 *       silently ignore both and serve permanently-open feeds after any latch, flip or
 *       settlement it missed. Decoder is unaffected and stays at 3.
 *   7 - (indexer only) BET cancel/resolve status rows: two more replicated tables,
 *       bet_cancels and bet_resolves (stream:action, base-schema files plus a dated
 *       migration). BET formats 1 and 3 previously owned no row of their own, so a
 *       chain-rejected cancel or resolve persisted no parse status and every API
 *       consumer had to assume it succeeded. They are pure reporting (no validation,
 *       settlement or hash path reads them) and add no updated_rows class, but a v6
 *       follower lacks the tables entirely and could not apply the streamed rows.
 *       Decoder is unaffected and stays at 3.
 *   8 - (indexer only) deferred chunked-DEPLOY assembly: two columns on
 *       contract_executions (assembler_action_index, fee_payment_mode) and a
 *       (source_id, code_hash) index on contracts, shipped by a dated migration.
 *       Neither column enters a block-hash preimage, but a v7 follower has no
 *       column to receive the streamed assembler_action_index, so the rows the
 *       completing carrier writes from DEPLOY_DEFERRED_ASSEMBLY on cannot apply
 *       there. Decoder is unaffected and stays at 3.
 *   9 - (indexer only) ROLLCALL v1 gates: a new close_block-scoped table
 *       rollcall_gates (one row per verified signer of a rolled epoch, its
 *       re-signed gate list) and a nullable gates column on rollcall_signers,
 *       shipped by the 2026-09-07-rollcall-gates migration. Neither enters a
 *       block-hash preimage, but a v8 follower has no table to receive the
 *       streamed gates rows the epoch close writes from ROLLCALL_GATES_ACTIVATION
 *       on, and the rules-aware attestation set it derives would then differ
 *       from its source. Decoder is unaffected and stays at 3.
 *  10 - (indexer) frontier catch-up: every replicated-DDL migration dated on or
 *       before MIGRATION_FRONTIER.indexer is accounted for at this version. The
 *       history above had drifted behind the migration ledger (entries named the
 *       migrations that motivated a bump, so DDL that landed without one was
 *       recorded nowhere), and the backlog folded in here is what a v9 follower
 *       can be missing: the utf8mb4 widenings of the raw wire and user-text
 *       columns across ~35 replicated tables (2026-08-19, 2026-09-02) plus
 *       index_memos.memo, which truncate or reject a 4-byte character a migrated
 *       source stores; the anchor_actions primary key restated over
 *       (action_index, section_index) (2026-08-28), which is what lets a
 *       multi-section anchor apply at all; the destroys UNIQUE action_index drop
 *       (2026-08-15) and the destroys/sends leg_ordinal column (2026-09-09),
 *       without which a multi-leg row hits ER_DUP_ENTRY under ClientApplier's
 *       plain INSERT; new replicated tables escrow_leaf_journal,
 *       contract_delegation_rotations and the ROLLCALL set (rollcalls,
 *       rollcall_signers, rollcall_absences); added columns on attests
 *       (relay origin, relay identity index, batch action_index and chunk
 *       columns), gated_files, lists.memo, attest_validator_stats,
 *       validator_rewards, anchor_reward_reconcile_log, prices v2 batch,
 *       markets native-coin side, contracts meta, and the pubkeys uncompressed
 *       widening. None of it enters a block-hash preimage; all of it changes
 *       which streamed rows a follower can store.
 *   4 - (decoder only) `transactions.data` converted to utf8mb4 by the
 *       2026-08-10-action-data-utf8mb4 migration. `transactions` is in the
 *       replicated decoder set, and an unmigrated replica quarantines a
 *       non-BMP ACTION its source stored, so the two disagree on chain state
 *       rather than merely lagging; that migration is a coordinated stop-the-
 *       decoders window, and this bump is what makes the ordering enforced
 *       instead of advisory. The 2026-07-24 pubkeys widening rides along.
 *       Indexer is unaffected by this key and stays at 10.
 *
 * MIGRATION_FRONTIER is the machine-readable half of that accounting: `through`
 * is the newest migration DATE whose replicated DDL is folded into the version
 * above, and `accounted` names the migration files bearing exactly that date
 * (dates are not unique, so the tail has to be enumerated or a same-day
 * migration would hide behind the cursor). test/unit/schema-version-gate.test.js
 * reads both, walks the sibling migration ledgers, and fails when a migration
 * past the frontier carries DDL against a wire-replicated table of that dbType
 * while the frontier stands still. Pure-DML backfills do not change what a
 * follower can store and are not flagged. Per the operator ruling of 2026-09-09
 * the answer to a red gate is a bump carried by the next fleet release (the
 * version decides what peers ACCEPT, so a drive-by edit strands the fleet),
 * never a frontier advance on its own.
 *
 ********************************************************************/

const SCHEMA_VERSION = { indexer: 10, decoder: 4 };

const MIGRATION_FRONTIER = {
    indexer: {
        through: '2026-09-11',
        accounted: [
            '2026-09-11-contract-meta-columns.sql',
            '2026-09-11-cross-chain-btc-chain-id.sql'
        ]
    },
    decoder: {
        through: '2026-08-22',
        accounted: [
            '2026-08-22-mempool-first-seen.sql'
        ]
    }
};

module.exports = { SCHEMA_VERSION, MIGRATION_FRONTIER };
