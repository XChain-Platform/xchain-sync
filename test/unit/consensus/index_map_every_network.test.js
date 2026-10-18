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
const { buildStateHashData } = require('../../../src/consensus/state_hash.js');

const ADDRESS_QUERY = 'FROM index_addresses WHERE block_index = ?';
const TICKER_QUERY = 'FROM index_tickers WHERE block_index = ?';

describe('state hash index-map inclusion on every network', function () {
    it('includes each index-map class for every coin, network and sampled block', async function () {
        const networks = ['mainnet', 'testnet', 'regtest'];
        const coins = ['BTC', 'LTC', 'DOGE'];
        const blocks = [7, 900000];

        for(const network of networks){
            for(const coin of coins){
                for(const block of blocks){
                    const calls = [];
                    const addressRows = [{ id: 5, address: 'address-' + block }];
                    const tickerRows = [{ id: 3, tick: 'TICK-' + block }];
                    const db = {
                        doQuery: async (sql, args) => {
                            calls.push({ sql, args });
                            if(sql.includes(ADDRESS_QUERY)) return addressRows;
                            if(sql.includes(TICKER_QUERY)) return tickerRows;
                            return [];
                        },
                        getStatusId: async () => null
                    };

                    const preimage = await buildStateHashData(db, block, { network, coin });
                    const keys = Object.keys(preimage);
                    const anchorIndex = keys.indexOf('anchor_invalid');
                    const label = coin + ':' + network + ' at ' + block;
                    const addressCalls = calls.filter(({ sql }) => sql.includes(ADDRESS_QUERY));
                    const tickerCalls = calls.filter(({ sql }) => sql.includes(TICKER_QUERY));

                    assert.strictEqual(preimage.index_addresses_new, addressRows,
                        label + ' must preserve the address rows');
                    assert.strictEqual(preimage.index_tickers_new, tickerRows,
                        label + ' must preserve the ticker rows');
                    assert.strictEqual(keys[anchorIndex + 1], 'index_addresses_new',
                        label + ' must place addresses directly after anchor_invalid');
                    assert.strictEqual(keys[anchorIndex + 2], 'index_tickers_new',
                        label + ' must place tickers directly after addresses');
                    assert.strictEqual(addressCalls.length, 1,
                        label + ' must read index_addresses once');
                    assert.strictEqual(tickerCalls.length, 1,
                        label + ' must read index_tickers once');
                    assert.deepStrictEqual(addressCalls[0].args, [block]);
                    assert.deepStrictEqual(tickerCalls[0].args, [block]);
                }
            }
        }
    });

    it('reads errno 1146 as empty index-map classes', async function () {
        const missingTable = Object.assign(new Error('index table is absent'), { errno: 1146 });
        const missingQueries = [];
        const db = {
            doQuery: async (sql) => {
                if(sql.includes(ADDRESS_QUERY)){
                    missingQueries.push(ADDRESS_QUERY);
                    throw missingTable;
                }
                if(sql.includes(TICKER_QUERY)){
                    missingQueries.push(TICKER_QUERY);
                    throw missingTable;
                }
                return [];
            },
            getStatusId: async () => null
        };

        const preimage = await buildStateHashData(db, 7, { network: 'testnet', coin: 'BTC' });

        assert.deepStrictEqual(preimage.index_addresses_new, []);
        assert.deepStrictEqual(preimage.index_tickers_new, []);
        assert.deepStrictEqual(missingQueries, [ADDRESS_QUERY, TICKER_QUERY]);
    });
});
