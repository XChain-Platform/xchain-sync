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
 * (column added, dropped, renamed, or type changed). Mismatched versions
 * cause validators to refuse the snapshot and log a clear error rather
 * than silently corrupting replica state. After incrementing, validators
 * must be restarted (or will restart automatically on the next bootstrap)
 * so that _fetchAndApplySchema re-runs against the new schema.
 *
 ********************************************************************/

// Increment this integer whenever a replicated table's DDL changes.
const SCHEMA_VERSION = 1;

module.exports = { SCHEMA_VERSION };
