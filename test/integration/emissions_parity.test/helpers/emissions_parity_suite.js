/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md.
 *
 ********************************************************************/

// Covers streamed emission assertions. One part of emissions_parity.test.js.
const assert = require('assert');

function assertStreamedEmissions(streamed){
    // The fix: execution_index-scoped stream returns BOTH, ORDER BY execution_index, position.
    assert.strictEqual(streamed.length, 2,
        'getEmissionRowsForBlock must include the NULL-action_index SLASH row the hash counts');
    assert.deepStrictEqual(streamed.map(r => r.emitted_action), ['ORDER', 'SLASH'],
        'rows ordered by execution_index then position');
    const slash = streamed.find(r => r.emitted_action === 'SLASH');
    assert.ok(slash, 'SLASH emission present in the streamed set');
    assert.strictEqual(slash.action_index, null, 'SLASH emission carries NULL action_index');
}

module.exports = { assertStreamedEmissions };
