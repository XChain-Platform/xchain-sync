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
const { isUtcSessionZone } = require('../../src/db/datetime_session_zone');

describe('DATETIME session-zone validation', function(){
    it('accepts the zero UTC offset', function(){
        assert.strictEqual(isUtcSessionZone({ tz: ' +00:00 ', sys: 'CST' }), true);
    });

    it('accepts UTC case-insensitively', function(){
        assert.strictEqual(isUtcSessionZone({ tz: ' utc ', sys: 'CST' }), true);
    });

    it('accepts SYSTEM when the system zone is UTC', function(){
        assert.strictEqual(isUtcSessionZone({ tz: ' system ', sys: ' utc ' }), true);
    });

    it('rejects a null row', function(){
        assert.strictEqual(isUtcSessionZone(null), false);
    });

    it('rejects an undefined row', function(){
        assert.strictEqual(isUtcSessionZone(undefined), false);
    });

    it('rejects a row with a missing time-zone field', function(){
        assert.strictEqual(isUtcSessionZone({ sys: 'UTC' }), false);
    });

    it('rejects a negative non-UTC offset', function(){
        assert.strictEqual(isUtcSessionZone({ tz: '-06:00', sys: 'UTC' }), false);
    });

    it('rejects a positive non-UTC offset', function(){
        assert.strictEqual(isUtcSessionZone({ tz: '+01:00', sys: 'UTC' }), false);
    });

    it('rejects SYSTEM when the system zone is not UTC', function(){
        assert.strictEqual(isUtcSessionZone({ tz: 'SYSTEM', sys: 'CST' }), false);
    });
});
