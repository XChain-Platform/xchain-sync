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
 **********************************************************************/

'use strict'

/**
 * Parse the CORS_ORIGIN env var into a value the `cors` package matches per-origin.
 *
 * Passing the raw string through does not work: `cors` treats a String origin as
 * ONE exact origin and echoes it back verbatim, so `CORS_ORIGIN="a,b"` emits
 * `Access-Control-Allow-Origin: a,b` to a, to b and to a hostile origin alike, a
 * multi-value header no browser accepts. That blocks every caller while a header
 * dump still reads as configured. An allowlist is the real shape of the setting.
 *
 * Fails CLOSED on anything ambiguous: an empty or all-blank value disables CORS
 * exactly as an unset var does, and `*` only means "any origin" when it is the
 * entire value. Mixed in with real origins it stays a literal list entry, which
 * no browser sends, so a fat-fingered `*,https://x` grants x and nothing more
 * rather than silently opening the service to everyone.
 *
 * Unlike the sibling services this is applied in src/config.js rather than at the
 * cors() call, because sync resolves every setting into `cfg` first and api.js
 * reads cfg['CORS_ORIGIN']. Parsing at the config seam is what keeps the raw env
 * string from reaching `cors` at all. The other XChain services carry the same
 * parser; keep them in step.
 *
 * @param {string|undefined|null} raw - the raw CORS_ORIGIN value
 * @returns {false|string|string[]} `false` (disabled), `'*'` (any), one origin, or an allowlist
 */
function parseCorsOrigin (raw) {
    if (raw == null) return false
    const list = String(raw).split(',').map(s => s.trim()).filter(Boolean)
    if (list.length === 0) return false
    if (list.length === 1) return list[0]
    return list
}

module.exports = { parseCorsOrigin }
