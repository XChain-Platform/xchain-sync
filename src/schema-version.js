/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available —
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
 * other dbType's validators to restart — only validators of the changed dbType
 * see a mismatch. Mismatched versions cause those validators to refuse the
 * snapshot and log a clear error rather than silently corrupting replica state.
 * After bumping a key, that dbType's validators must be restarted (or will
 * restart automatically on the next bootstrap) so that _fetchAndApplySchema
 * re-runs against the new schema.
 *
 * Version history:
 *   1 — initial replicated schema (both dbTypes).
 *   2 — binary (BLOB/binary) column values are base64-encoded on the wire
 *       as a { "__xbin__": "<base64>" } sentinel and decoded back to Buffers
 *       on apply (see src/wireCodec.js). Prior versions corrupted every
 *       binary column. A v1 peer fails closed against a v2 snapshot. This wire
 *       change affected both dbTypes, so both keys advanced to 2 together.
 *
 ********************************************************************/

// Per-dbType schema version. Bump only the key whose DDL or wire encoding changed
// so the unaffected dbType's validators need not restart.
const SCHEMA_VERSION = { indexer: 2, decoder: 2 };

module.exports = { SCHEMA_VERSION };
