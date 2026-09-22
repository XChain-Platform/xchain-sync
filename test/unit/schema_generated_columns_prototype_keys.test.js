/********************************************************************
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
 ********************************************************************
 * test/unit/schema_generated_columns_prototype_keys.test.js
 *
 * generatedColumns(table) used a plain `GENERATED_COLUMNS[table]` lookup, so a
 * prototype key such as `constructor` resolved through Object.prototype to a
 * function, and `new Set(function)` throws instead of yielding an empty Set. An
 * unknown table name must always answer an empty Set, prototype keys included.
 */

'use strict';

const assert = require('assert');

const { generatedColumns } = require('../../src/schema/generated_columns');

describe('generatedColumns: prototype-key table names @regression', function(){

    it('returns an empty Set for constructor, __proto__ and toString', function(){
        assert.strictEqual(generatedColumns('constructor').size, 0);
        assert.strictEqual(generatedColumns('__proto__').size, 0);
        assert.strictEqual(generatedColumns('toString').size, 0);
    });

    it('still returns contract_state\'s own generated column', function(){
        assert.deepStrictEqual([...generatedColumns('contract_state')], ['state_key_bin']);
    });

    it('caches the same Set across calls for a prototype-key table', function(){
        const first = generatedColumns('constructor');
        assert.strictEqual(generatedColumns('constructor'), first);
    });
});
