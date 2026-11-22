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
 * Refusal filter for applier fuzz properties: a payload with no data or table map
 * is refused by name; any other error is a crash.
 */

'use strict';

// A payload with no data or table map is refused by name; any other error is a crash.
const MALFORMED = /^Malformed (live block payload|full snapshot|incremental snapshot)/;

async function tolerateMalformed(apply) {
    try {
        await apply();
    } catch (e) {
        if (!MALFORMED.test(e && e.message)) throw e;
    }
}

module.exports = { tolerateMalformed };
