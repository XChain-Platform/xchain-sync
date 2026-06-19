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
 * renamed, or type changed) OR a wire-encoding change to its row values. Keeping
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
 *       `updated_rows` channel carrying the current state of SURVIVING rows the
 *       source mutated in place (deactivation_block, SLASH amounts, v0
 *       request_status); rows the action_index-scoped paths can't reach. The
 *       follower UPSERTs them (ClientApplier) and re-derives the escrow gate
 *       locally. A v2 follower silently ignores the field and stays divergent,
 *       so the bump forces a coordinated server+follower upgrade: a v2 follower
 *       fails closed against a v3 incremental/full snapshot. Decoder is
 *       unaffected (none of these tables exist there) and stays at 2.
 *   4 - (indexer only) two DDL changes to `attests`, both on the action_index-
 *       scoped stream: (a) `request_status` ENUM widened to add the terminal
 *       value 'rejected' (migration 2026-06-13-attests-request-status-add-rejected,
 *       mode=auto); (b) the `request_id_version` index relaxed from UNIQUE to
 *       non-unique so a request can carry multiple v1 rows, one per retry round
 *       (migration 2026-06-17-attests-drop-unique-request-id-version, mode=auto).
 *       A v3 follower keeps the narrow ENUM (a streamed 'rejected' row is rejected
 *       in strict mode -> block apply fails -> replication stalls, or coerced to ''
 *       in non-strict mode -> a protocol-rejected request flows through as
 *       serviceable) AND keeps the stale UNIQUE (the second v1 row hits ER_DUP_ENTRY
 *       under ClientApplier's plain INSERT -> the apply transaction fails and the
 *       follower's queue stalls). The bump fails a v3 follower closed against a v4
 *       snapshot; both migrations are mode=auto, so they self-heal the ENUM + index
 *       fleet-wide on the forced restart. Decoder is unaffected and stays at 3.
 *   3 - (decoder only) `dispensers.expiration` converted from DATETIME to
 *       BIGINT UNSIGNED storing raw unix seconds (migration
 *       2026-06-13-dispensers-expiration-bigint, mode=manual: a one-time column
 *       TYPE change with a value conversion, harmless no-op on fresh DBs whose
 *       base schema already ships BIGINT). The old DATETIME path silently NULLed
 *       any expiration past Y2038, yet the protocol accepts values up to
 *       4294967295 (year 2106), so a v2 replica still on DATETIME coerces a large
 *       integer expiration to an invalid datetime (NULL or failed apply) and
 *       disagrees with master on which dispensers are expired. Because the
 *       migration is mode=manual it does NOT auto-apply on follower startup, so the
 *       bump makes the ordering enforced rather than advisory: a v2 follower fails
 *       closed against a v3 snapshot and must run the migration against its replica
 *       (or re-sync to the fresh BIGINT base schema) before it can consume v3.
 *       Indexer is unaffected by this key and stays at 4.
 *
 ********************************************************************/

// Per-dbType schema version. Bump only the key whose DDL or wire encoding changed
// so the unaffected dbType's validators need not restart.
const SCHEMA_VERSION = { indexer: 4, decoder: 3 };

module.exports = { SCHEMA_VERSION };
