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
 **********************************************************************
 *
 * The carrier_logic_digest /health field (C7): the logic half that armed-map
 * fingerprint v2 stopped covering when the carriers became registry shims.
 *
 * The value is READ from the committed pin, bin/pins/carrier-logic.json, and
 * never measured from the tree. Measuring needs the acorn tokenizer and the
 * pin module under bin/lib, and the sync image copies neither (only src/ and
 * the pin file ship); a serving process has no business tokenizing its own
 * sources, and the guard that holds the tree to the pin already runs in CI
 * (test/unit/repo_guards/carrier_logic_pin.test.js). The digest formula is the
 * pin module's (sha256 over the sorted `id=hash` lines) restated here for the
 * same reason, and test/unit/health/carrier_logic.test.js holds the two equal.
 *
 * UNREADABLE, NEVER A GUESS. A checkout or image without the pin publishes the
 * literal UNREADABLE, the shape fingerprint v2 uses, so a fleet sweep sees a
 * process that cannot answer rather than one that happens to match.
 *
 ********************************************************************/

'use strict';

const crypto = require('crypto');
const fs     = require('fs');
const path   = require('path');

const PIN_PATH = path.join(__dirname, '..', '..', 'bin', 'pins', 'carrier-logic.json');
const UNREADABLE = 'UNREADABLE';

// Read once per process: the pin is a committed file, and a /health poll that
// re-read it would only ever reproduce the first reading.
let cached = null;

function digestOf(pin) {
    const hash = crypto.createHash('sha256');
    for (const id of Object.keys(pin.entries).sort()) hash.update(`${id}=${pin.entries[id].hash}\n`);
    return hash.digest('hex');
}

function carrierLogicDigest() {
    if (cached !== null) return cached;
    try {
        cached = digestOf(JSON.parse(fs.readFileSync(PIN_PATH, 'utf8')));
    } catch (e) {
        cached = UNREADABLE;
    }
    return cached;
}

module.exports = { carrierLogicDigest, digestOf, PIN_PATH, UNREADABLE };
