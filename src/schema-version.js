/*********************************************************************
 *
 * Copyright © 2025 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * Licensed under the Dankest Community License (Apache License 2.0 + Additional Terms).
 * You may not use this file except in compliance with that License.
 *
 * A copy of the License is available at:
 *     https://dankest.llc/license
 *
 * This software is provided "AS IS", without warranties or conditions of any kind.
 *
 **********************************************************************
 *
 * XChain Sync - Snapshot Schema Version
 *
 * Increment SCHEMA_VERSION whenever any replicated table's DDL changes
 * (column added, dropped, renamed, or type changed) OR the wire encoding
 * of row values changes. Mismatched versions cause validators to refuse
 * the snapshot and log a clear error rather than silently corrupting
 * replica state. After incrementing, validators must be restarted (or
 * will restart automatically on the next bootstrap) so that
 * _fetchAndApplySchema re-runs against the new schema.
 *
 * Version history:
 *   1 — initial replicated schema.
 *   2 — binary (BLOB/binary) column values are base64-encoded on the wire
 *       as a { "__xbin__": "<base64>" } sentinel and decoded back to Buffers
 *       on apply (see src/wireCodec.js). Prior versions corrupted every
 *       binary column. A v1 peer fails closed against a v2 snapshot.
 *
 ********************************************************************/

// Increment this integer whenever a replicated table's DDL or wire encoding changes.
const SCHEMA_VERSION = 2;

module.exports = { SCHEMA_VERSION };
