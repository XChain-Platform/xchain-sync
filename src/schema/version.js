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
 * Bump only the key whose replicated DB had a DDL change that affects what rows a
 * follower can store (a column, primary key, foreign key, unique index or constraint
 * change) OR a wire-encoding change to its row values. A non-unique secondary-index-
 * only migration changes query performance rather than replicated columns or payload
 * shape, so it is recorded at MIGRATION_FRONTIER without a version bump. Keeping the
 * two versions separate means a schema change to one DB does not force the other
 * dbType's validators to restart; only validators of the changed dbType see a mismatch.
 * Mismatched versions cause those validators to refuse the snapshot and log a clear
 * error rather than silently corrupting replica state.
 * After bumping a key, that dbType's validators must be restarted (or will
 * restart automatically on the next bootstrap) so that fetchAndApplySchema
 * re-runs against the new schema.
 *
 * Version history:
 *   1 - initial replicated schema (both dbTypes).
 *   2 - binary (BLOB/binary) column values are base64-encoded on the wire
 *       as a { "__xbin__": "<base64>" } sentinel and decoded back to Buffers
 *       on apply (see src/util/wire_codec.js). Prior versions corrupted every
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
 *  11 - (indexer only) the XCHAIN bridge and token-bridge set. Two new replicated
 *       tables: `xbridges`, the local action record for every user-broadcast XBRIDGE
 *       (v0 lock, v1 burn, v3 token lock, v4 bridged-copy burn), and
 *       `bridge_settlements`, the local idempotency and rollback record for every
 *       applied settle leg and every applied policy snapshot (both stream:action,
 *       shipped by the 2026-09-12-bridge-tables migration alongside the two
 *       hub-mirrored tables, which never ride the wire). Plus the ISSUE format 7
 *       bridge opt-in columns of 2026-09-12-token-bridge-fields: raw wire strings
 *       `bridge_chains` / `min_depth` / `lock_bridge` on `issues`, and the parsed
 *       `bridge_chains` / `min_depth` / `lock_bridge` / `bridged` on `tokens`. A v10
 *       follower has no table to receive a streamed lock, burn or settlement and no
 *       column to receive the streamed opt-in fields, so it would apply neither the
 *       source side nor the destination side of a transfer and would answer
 *       bridgeability from an empty column while its source refuses the same lock.
 *       Also folded in here, having landed on the old frontier date without a bump of
 *       its own: `price_snapshots.batch_block_time` and its
 *       (coin_pair, batch_block_time, round_number) index
 *       (2026-09-11-price-snapshots-batch-block-time). price_snapshots is hub-mirrored
 *       rather than wire-replicated, so it changes nothing a follower can store and the
 *       gate never flagged it; it is named so the frontier move does not bury it.
 *       Nothing here enters a block-hash preimage: the bridge tables are action-derived
 *       and the format-7 fields are derived from `issues`, which actions_hash already
 *       covers. Decoder is unaffected and stays at 4.
 *   4 - (decoder only) `transactions.data` converted to utf8mb4 by the
 *       2026-08-10-action-data-utf8mb4 migration. `transactions` is in the
 *       replicated decoder set, and an unmigrated replica quarantines a
 *       non-BMP ACTION its source stored, so the two disagree on chain state
 *       rather than merely lagging; that migration is a coordinated stop-the-
 *       decoders window, and this bump is what makes the ordering enforced
 *       instead of advisory. The 2026-07-24 pubkeys widening rides along.
 *       Indexer is unaffected by this key and stays at 11.
 *  12 - (indexer only) `leg_ordinal SMALLINT UNSIGNED NOT NULL DEFAULT 0` added to
 *       `destroys` and `sends`, plus a composite (action_index, leg_ordinal) index
 *       on each, by the 2026-09-13-destroys-sends-leg-ordinal migration. Both
 *       tables are stream:action wire-replicated and a multi-leg SEND or DESTROY
 *       writes one row per leg under a single action_index, so the column is where
 *       the broadcast order of those legs is recorded. A v11 follower has no column
 *       to receive the streamed value: under ClientApplier's strict apply the row
 *       fails the block, and where it flows through, every leg lands on the implicit
 *       default and the follower answers leg order from nothing while its source
 *       answers from the wire. Nothing here enters a block-hash preimage: `destroys`
 *       and `sends` are DERIVED in src/table_lifecycle.js, a deterministic projection
 *       of already-hashed actions in no hash class of their own, rolled back by
 *       `action_index >= ?`, which is agnostic to the row's column set. The
 *       migration is mode=auto, so it self-heals fleet-wide on the forced restart.
 *       Decoder is unaffected and stays at 4.
 *  13 - (indexer only) `list_transfers`, a new table (transferred-list ownership
 *       history, LIST format 3) created by the 2026-10-01-list-transfers migration.
 *       It is stream:action wire-replicated, so a v12 follower has no table to
 *       receive the streamed rows and fails the block under ClientApplier's strict
 *       apply. Nothing here enters a block-hash preimage: `list_transfers` is
 *       DERIVED in src/table_lifecycle/action_tables.js and rolled back by
 *       `action_index >= ?`. No row is written below LIST_TRANSFER_ACTIVATION, and
 *       the migration is mode=auto.
 *       Also folded in here, having landed inside the same frontier move without a
 *       bump of its own: `list_share_mirrors`, a stream:action table (DERIVED, rolled
 *       back by `action_index >= ?`) created by the 2026-09-30-list-share-tables
 *       migration beside the hub-mirrored `list_snapshots`, which never rides the
 *       wire. A v12 follower has no table to receive its streamed rows either.
 *       Unlike list_transfers, that migration is mode=manual
 *       deploy-precondition=required: it does not self-heal on the forced restart,
 *       and the indexer refuses to start until it has run. Decoder is unaffected and
 *       stays at 4.
 *  14 - (indexer only) `list_metas`, a new table for resolved LIST names and
 *       descriptions created by the 2026-10-01-list-metas migration. It is
 *       stream:action wire-replicated, so a v13 follower has no table to receive
 *       the streamed rows and fails the block under ClientApplier's strict apply.
 *       Nothing here enters a block-hash preimage: `list_metas` is DERIVED in
 *       src/table_lifecycle/action_tables.js and rolled back by
 *       `action_index >= ?`. No row is written below LIST_META_ACTIVATION, and
 *       the migration is mode=auto. Decoder is unaffected and stays at 4.
 *  15 - (indexer only) secondary indexes added by the 2026-10-06-resolved-block-idx
 *       migration: (version, resolved_block) on `attests` and `xcalls`, and
 *       resolved_block and callback_due_block on `polls`, so the per-block state-hash
 *       collectors, the resolved-row sync reads and the reorg resets stop scanning
 *       whole tables. A v14 follower can still store every streamed row; the bump
 *       records the index change as this file's rule requires, and an aged follower
 *       gains the same four indexes at startup from ensureReplicaBlockIndexes.
 *       Nothing here enters a block-hash preimage. The migration is mode=auto.
 *       Decoder is unaffected and stays at 4.
 * Index-only frontier record (no version bump): the
 *       2026-10-07-ledger-covering-index migration adds non-unique
 *       (tick_id, action_index, amount) covering indexes to `credits`, `debits`
 *       and `escrows`. It changes neither replicated columns nor payload shape,
 *       so an older follower can still store every streamed row.
 * Migration-rename frontier record (no version bump):
 *       2026-10-08-token-bridge-fields is the byte-identical re-key of
 *       2026-09-12-token-bridge-fields, whose `issues` and `tokens` columns were
 *       already folded into indexer version 11. The indexer migration ledger
 *       re-keys the old applied name to the new one, so this changes neither the
 *       live schema nor what a follower can store. The same frontier date also
 *       carries 2026-10-08-state-tree-roots-block-index-idx, a non-unique
 *       secondary index on the follower-derived `state_tree_roots` table.
 *
 * MIGRATION_FRONTIER is the machine-readable half of that accounting: `through`
 * is the newest migration DATE whose replicated DDL is folded into the version
 * above, and `accounted` names the migration files bearing exactly that date
 * (dates are not unique, so the tail has to be enumerated or a same-day
 * migration would hide behind the cursor). `indexOnly` identifies the accounted
 * files that did not receive a version bump, so the gate also verifies their
 * replicated DDL remains non-unique secondary-index-only.
 * test/unit/schema_version_gate.test.js reads both, walks the sibling migration
 * ledgers, and fails when a migration past the frontier carries DDL against a
 * wire-replicated table of that dbType while the frontier stands still. Pure-DML
 * backfills do not change what a follower can store and are not flagged. Per the
 * operator ruling of 2026-09-09,
 * payload-affecting DDL requires a bump carried by the next fleet release because
 * the version decides what peers ACCEPT. A reviewed non-unique secondary-index-only
 * migration can advance the frontier without a bump when an accounting comment names
 * the migration and records that it changes neither replicated columns nor payload shape.
 *
 ********************************************************************/

const SCHEMA_VERSION = { indexer: 15, decoder: 4 };

const MIGRATION_FRONTIER = {
    indexer: {
        through: '2026-10-08',
        accounted: [
            // Index-only on a follower-derived table; no replicated payload changed.
            '2026-10-08-state-tree-roots-block-index-idx.sql',
            // Renamed byte-identically; its replicated columns were accounted at v11.
            '2026-10-08-token-bridge-fields.sql'
        ],
        indexOnly: [
            '2026-10-08-state-tree-roots-block-index-idx.sql'
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
