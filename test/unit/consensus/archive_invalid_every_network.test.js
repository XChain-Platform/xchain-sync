'use strict';

// Copyright © 2025-2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC - https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const { collectAnchorInvalid } = require('../../../src/consensus/state_hash/archive_invalid_section.js');
const { archiveHeadPredicate,
        ARCHIVE_HEAD_VERSIONS_SQL } = require('../../../src/consensus/state_hash/activation.js');

describe('state hash archive-invalid selection on every network', function () {
    it('queries the widened archive-head class for every coin, network and sampled block', async function () {
        const expectedPredicate = archiveHeadPredicate('p') + ' AND p.version ' + ARCHIVE_HEAD_VERSIONS_SQL;
        const networks = ['mainnet', 'testnet', 'regtest'];
        const coins = ['BTC', 'LTC', 'DOGE'];
        const blocks = [7, 900000];

        for(const network of networks){
            for(const coin of coins){
                for(const block of blocks){
                    const calls = [];
                    const expectedRows = [{ action_index: 100, status: 'invalid_archive' }];
                    const db = {
                        doQuery: async (sql, args) => {
                            calls.push({ sql, args });
                            return expectedRows;
                        }
                    };

                    const rows = await collectAnchorInvalid(db, block, network, coin);
                    assert.strictEqual(calls.length, 1, coin + ':' + network + ' at ' + block);
                    assert.strictEqual(rows, expectedRows, coin + ':' + network + ' must return the query rows');
                    assert.ok(calls[0].sql.includes('FROM anchor_actions p'),
                        coin + ':' + network + ' at ' + block + ' must query anchor_actions');
                    assert.ok(calls[0].sql.includes(expectedPredicate),
                        coin + ':' + network + ' at ' + block + ' must select every archive-head version');
                    assert.strictEqual(calls[0].sql.includes('p.version = 1'), false,
                        coin + ':' + network + ' at ' + block + ' must not select only version 1');
                    assert.deepStrictEqual(calls[0].args, [block, block]);
                }
            }
        }
    });

    it('reads errno 1146 as an empty archive-invalid class', async function () {
        const missingTable = Object.assign(new Error('anchor_actions is absent'), { errno: 1146 });
        const db = { doQuery: async () => { throw missingTable; } };

        assert.deepStrictEqual(await collectAnchorInvalid(db, 7, 'testnet', 'BTC'), []);
    });
});
