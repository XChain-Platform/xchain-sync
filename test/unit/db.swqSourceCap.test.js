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
 **********************************************************************
 * test/unit/db.swqSourceCap.test.js
 *
 * SWQ-TRUNC-1 liveness half on the follower side. The BTC stakes_root the follower
 * recomputes MUST select the SAME source-capped set as the indexer at/after
 * SWQ_SOURCE_CAP_ACTIVATION, or the state-commitment check false-halts. These
 * mock-based tests (doQuery stubbed) lock the gate + arg shape on BOTH follower
 * stake-weight paths - the live getStakeWeightsByCapability and the SPV forward-
 * follow getStakeWeightsByCapabilityAsOf - and confirm the legacy uncapped path is
 * preserved when no coin/network is threaded (backward compatibility). The window
 * SQL itself is proven by the real-MariaDB drill.
 */

'use strict';

const assert   = require('assert');
const sinon    = require('sinon');
const Database = require('../../src/db');
const swqCap   = require('../../src/swq_source_cap_activation');

const MAX_SOURCES = swqCap.STAKE_WEIGHT_MAX_SOURCES;
const MAX_KEYS    = swqCap.STAKE_WEIGHT_MAX_KEYS_PER_SOURCE;

function makeUtil() {
    return {
        isNull:     (v) => v === null || v === undefined,
        throwError: (m) => { throw new Error(m); },
        sleep:      sinon.stub().resolves(),
        logError:   sinon.stub()
    };
}

function dbFor(rows) {
    const db = new Database('localhost', 3306, 'replica_db', 'u', 'p', makeUtil(), 'indexer');
    const calls = [];
    sinon.stub(console, 'warn');
    sinon.stub(db, 'getStatusId').resolves(1);
    sinon.stub(db, 'doQuery').callsFake((query, args) => { calls.push({ query, args }); return Promise.resolve(rows || []); });
    db._calls = calls;
    return db;
}

afterEach(function () { sinon.restore(); });

describe('SWQ source-cap follower gate (SWQ-TRUNC-1 liveness) @regression @tier1', function () {

    describe('live getStakeWeightsByCapability', function () {

        it('below activation (BTC:mainnet < 960000) uses the legacy uncapped LIMIT', async function () {
            const db = dbFor([]);
            await db.getStakeWeightsByCapability('oracle_publish', 900000, '500', 1000, 'BTC', 'mainnet');
            const { query, args } = db._calls[0];
            assert.match(query, /ORDER BY source, pubkey\s+LIMIT \?/);
            assert.doesNotMatch(query, /DENSE_RANK/);
            assert.strictEqual(args[args.length - 1], 1000);
        });

        it('at/after activation (BTC:mainnet >= 960000) uses the windowed source-cap', async function () {
            const db = dbFor([]);
            await db.getStakeWeightsByCapability('oracle_publish', 960000, '500', 1000, 'BTC', 'mainnet');
            const { query, args } = db._calls[0];
            assert.match(query, /DENSE_RANK\(\) OVER \(ORDER BY b\.source\)/);
            assert.match(query, /ROW_NUMBER\(\) OVER \(PARTITION BY b\.source ORDER BY b\.pubkey\)/);
            assert.strictEqual(args[args.length - 2], MAX_SOURCES + 1);
            assert.strictEqual(args[args.length - 1], MAX_KEYS);
        });

        it('stays on the legacy path when no coin/network is threaded (backward compatible)', async function () {
            const db = dbFor([]);
            await db.getStakeWeightsByCapability('oracle_publish', 960000, '500', 1000);
            assert.doesNotMatch(db._calls[0].query, /DENSE_RANK/, 'inert without coin/network - legacy uncapped');
        });

        it('drops the overflow source above maxSources (follower selects the same set the source commits)', async function () {
            const rows = [
                { pubkey: 'a', source: 's-0001', weight: '5', _sr: 1 },
                { pubkey: 'z', source: 's-1001', weight: '9', _sr: MAX_SOURCES + 1 },
            ];
            const db = dbFor(rows);
            const out = await db.getStakeWeightsByCapability('oracle_publish', 5, '500', 1000, 'BTC', 'regtest');
            assert.deepStrictEqual(out.map(r => r.source), ['s-0001']);
        });
    });

    describe('SPV forward-follow getStakeWeightsByCapabilityAsOf', function () {

        it('at/after activation the reconstruction is source-capped too (matches the committed root)', async function () {
            const db = dbFor([]);
            await db.getStakeWeightsByCapabilityAsOf('oracle_publish', 960000, '500', 1000, 'BTC', 'mainnet');
            const { query, args } = db._calls[0];
            assert.match(query, /DENSE_RANK\(\) OVER \(ORDER BY b\.source\)/, 'AsOf reconstruction is capped');
            assert.match(query, /capability_slash_debits csd/, 'still folds in the post-snapshot slash add-back');
            assert.strictEqual(args[args.length - 2], MAX_SOURCES + 1);
            assert.strictEqual(args[args.length - 1], MAX_KEYS);
        });

        it('below activation the reconstruction keeps the legacy uncapped LIMIT (unchanged pre-flag-day)', async function () {
            const db = dbFor([]);
            await db.getStakeWeightsByCapabilityAsOf('oracle_publish', 900000, '500', 1000, 'BTC', 'mainnet');
            assert.doesNotMatch(db._calls[0].query, /DENSE_RANK/);
            assert.strictEqual(db._calls[0].args[db._calls[0].args.length - 1], 1000);
        });
    });
});
