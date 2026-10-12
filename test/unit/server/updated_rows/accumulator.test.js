// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const { add } = require('../../../../src/server/updated_rows/accumulator');

describe('updatedRows accumulator', function(){

    it('creates a table entry on the first add', function(){
        const acc = {};
        const row = { action_index: 7, status: 'pending' };
        add(acc, 'requests', [row]);
        assert.ok(acc.requests instanceof Map);
        assert.strictEqual(acc.requests.size, 1);
        assert.strictEqual(acc.requests.get('7'), row);
    });

    it('merges repeated adds without duplicating an action index', function(){
        const acc = {};
        const original = { action_index: 7, status: 'pending' };
        const replacement = { action_index: '7', status: 'complete' };
        const added = { action_index: 8, status: 'pending' };
        add(acc, 'requests', [original]);
        const table = acc.requests;
        add(acc, 'requests', [replacement, added]);
        assert.strictEqual(acc.requests, table);
        assert.strictEqual(table.size, 2);
        assert.strictEqual(table.get('7'), replacement);
        assert.strictEqual(table.get('8'), added);
    });

    it('does not mutate an input row', function(){
        const acc = {};
        const row = { action_index: 9, status: 'pending' };
        const before = { ...row };
        add(acc, 'requests', [row]);
        assert.deepStrictEqual(row, before);
        assert.strictEqual(acc.requests.get('9'), row);
    });
});
