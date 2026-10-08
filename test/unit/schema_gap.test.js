// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.
//
// Coverage for the one schema-gap predicate the forward collectors share: only a
// missing table or column on an older source may be skipped, and anything else
// must re-throw so the block is retried rather than broadcast short.

const assert = require('assert');
const { isSchemaGapError } = require('../../src/db/schema_gap');

describe('schemaGap.isSchemaGapError', function(){

    it('treats a missing table (1146) and a missing column (1054) as a schema gap', function(){
        assert.strictEqual(isSchemaGapError({ errno: 1146 }), true);
        assert.strictEqual(isSchemaGapError({ errno: 1054 }), true);
    });

    it('rejects transient driver faults such as a deadlock or a lock-wait timeout', function(){
        assert.strictEqual(isSchemaGapError({ errno: 1213 }), false);
        assert.strictEqual(isSchemaGapError({ errno: 1205 }), false);
    });

    it('rejects an error with no numeric errno, and a thrown falsy value', function(){
        assert.strictEqual(isSchemaGapError(new TypeError('db.find is not a function')), false);
        assert.strictEqual(isSchemaGapError({ errno: '1146' }), false);
        assert.strictEqual(isSchemaGapError(null), false);
        assert.strictEqual(isSchemaGapError(undefined), false);
    });
});
