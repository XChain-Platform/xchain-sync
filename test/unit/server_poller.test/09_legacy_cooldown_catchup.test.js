// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md.

const assert = require('assert');
const ServerPoller = require('../../../src/server/poller');
const CREDITS_MIXIN = require('../../../src/db/credits');

function makeDb(refund, release) {
    return Object.assign({
        async getStatusId() { return 3; },
        async doQuery(sql, args) {
            if (/cooldown_end_block >/.test(sql)) {
                if (/FROM credits c/.test(sql)) return [refund];
                if (/FROM escrows e/.test(sql)) return [release];
            }
            if (/cooldown_end_block BETWEEN/.test(sql) && args.includes(120)) {
                if (/FROM credits c/.test(sql)) return [refund];
                if (/FROM escrows e/.test(sql)) return [release];
            }
            return [];
        },
    }, CREDITS_MIXIN);
}

describe('ServerPoller legacy cooldown catch-up @regression', function () {
    it('ships a later-maturing refund only at its maturity block', async function () {
        const refund = { action_index: 9, address_id: 7, tick_id: 1, amount: '100' };
        const release = { action_index: 9, address_id: 7, tick_id: 1, amount: '-100' };
        const credit = { action_index: 8, address_id: 6, tick_id: 1, amount: '2' };
        const escrow = { action_index: 8, address_id: 6, tick_id: 1, amount: '-2' };
        const context = {
            db: makeDb(refund, release),
            async addRedrivenRewardPayloadRows(payload) { return payload; },
        };
        const creation = { data: { credits: [refund, credit], escrows: [release, escrow] } };

        await ServerPoller.prototype.addCooldownPayloadRows.call(context, creation, 100);

        const maturity = { data: {} };
        await ServerPoller.prototype.addCooldownPayloadRows.call(context, maturity, 120);

        assert.deepStrictEqual(creation.data.credits, [credit]);
        assert.deepStrictEqual(creation.data.escrows, [escrow]);
        assert.deepStrictEqual(maturity.data.credits, [refund]);
        assert.deepStrictEqual(maturity.data.escrows, [release]);
    });
});
