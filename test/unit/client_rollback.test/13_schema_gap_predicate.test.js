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

// test/unit/client_rollback.test/13_schema_gap_predicate.test.js
//
// Keeps the reverse twin on the forward channels' schema-gap rule: every catch in
// ClientRollback that skips a schema gap must ask isSchemaGapError, so the two sides
// cannot disagree about which faults are skipped and which abort the rollback.

'use strict';

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');

const ROLLBACK = path.join(__dirname, '../../../src/client/rollback.js');

// The catches that deliberately skip only a missing table (1054 aborts there), each named by its comment.
const MISSING_TABLE_ONLY_SITES = 4;

describe('ClientRollback schema-gap catches', function(){
    it('skip a schema gap only through isSchemaGapError, never an inline errno pair', function(){
        const src = fs.readFileSync(ROLLBACK, 'utf8');
        assert.ok(/const \{ isSchemaGapError \} = require\('\.\.\/db\/tables\/schema_gap'\);/.test(src),
            'src/client/rollback.js must import isSchemaGapError from db/tables/schema_gap');
        const pairs = src.match(/errno\s*!==?\s*1146\s*&&\s*\w+(\.\w+)*\s*!==?\s*1054/g) || [];
        assert.deepStrictEqual(pairs, [],
            'src/client/rollback.js restates the schema-gap rule inline; use isSchemaGapError(e) so it cannot drift');
        assert.ok((src.match(/!isSchemaGapError\(e\)/g) || []).length >= 30,
            'expected the schema-gap catches to call isSchemaGapError');
        assert.strictEqual((src.match(/errno !== 1146\)/g) || []).length, MISSING_TABLE_ONLY_SITES,
            'a missing-table-only catch was added or removed in src/client/rollback.js; if it is deliberate, ' +
            'say in its comment why 1054 must abort there and update MISSING_TABLE_ONLY_SITES');
    });
});
