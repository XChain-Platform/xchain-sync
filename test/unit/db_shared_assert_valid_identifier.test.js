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
 *********************************************************************/

'use strict';

const assert = require('assert');
const { assertValidIdentifier } = require('../../src/db/shared');

describe('db shared assertValidIdentifier', function(){
    it('accepts a plain identifier', function(){
        assert.strictEqual(assertValidIdentifier('blocks_2024'), undefined);
    });

    it('accepts a 64-character identifier', function(){
        assert.strictEqual(assertValidIdentifier('a'.repeat(64)), undefined);
    });

    it('rejects a backtick with the exact unsafe identifier message', function(){
        const identifier = 'bl' + String.fromCharCode(96) + 'ocks';
        assert.throws(
            () => assertValidIdentifier(identifier),
            error => error instanceof Error && error.message ===
                'Refusing to query unsafe table identifier: Identifier contains invalid characters'
        );
    });

    it('rejects null', function(){
        assert.throws(() => assertValidIdentifier(null), /Identifier is null or undefined/);
    });

    it('rejects an empty identifier', function(){
        assert.throws(() => assertValidIdentifier(''), /Identifier is empty/);
    });

    it('rejects a 65-character identifier', function(){
        assert.throws(() => assertValidIdentifier('a'.repeat(65)), /Identifier exceeds 64 characters/);
    });

    it('rejects a non-string identifier', function(){
        assert.throws(() => assertValidIdentifier(7), /Identifier is not a string/);
    });
});
