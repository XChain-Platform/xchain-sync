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
const swc      = require('../../src/stake_weight_collation_activation');

const MAX_SOURCES = swqCap.STAKE_WEIGHT_MAX_SOURCES;
const MAX_KEYS    = swqCap.STAKE_WEIGHT_MAX_KEYS_PER_SOURCE;

// The stake-weight ordering collation gate splices a ` COLLATE utf8_bin` suffix into
// the very ORDER BY clauses this suite matches on, and it is armed on mainnet from
// genesis (2026-09-09 ruling) while testnet stays unpinned. Derive the suffix from
// that gate instead of freezing a literal: this suite stays about the SOURCE CAP,
// and it additionally proves the two gates COMPOSE. That composition is the thing
// that would break liveness here, because the follower must emit the same suffix the
// source indexer does or it orders the capped set differently and forks stakes_root.
function collateSuffix(blockIndex, coin, network) {
    return swc.stakeWeightCollate(swc.isStakeWeightBinCollationActive(blockIndex, network, coin));
}
function escapeRe(s) { return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); }

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
            const c = escapeRe(collateSuffix(900000, 'BTC', 'mainnet'));
            assert.match(query, new RegExp('ORDER BY source' + c + ', pubkey' + c + '\\s+LIMIT \\?'));
            assert.doesNotMatch(query, /DENSE_RANK/);
            assert.strictEqual(args[args.length - 1], 1000);
        });

        it('at/after activation (BTC:mainnet >= 960000) uses the windowed source-cap', async function () {
            const db = dbFor([]);
            await db.getStakeWeightsByCapability('oracle_publish', 960000, '500', 1000, 'BTC', 'mainnet');
            const { query, args } = db._calls[0];
            const c = escapeRe(collateSuffix(960000, 'BTC', 'mainnet'));
            assert.match(query, new RegExp('DENSE_RANK\\(\\) OVER \\(ORDER BY b\\.source' + c + '\\)'));
            assert.match(query, new RegExp('ROW_NUMBER\\(\\) OVER \\(PARTITION BY b\\.source' + c + ' ORDER BY b\\.pubkey' + c + '\\)'));
            assert.strictEqual(args[args.length - 2], MAX_SOURCES + 1);
            assert.strictEqual(args[args.length - 1], MAX_KEYS);
        });

        // The control that keeps the helper above honest: a venue where the collation
        // gate is OFF must emit the bare window, so a helper that returned a suffix
        // unconditionally (or never) could not pass both this and the mainnet case.
        // testnet caps from genesis but is not collation-pinned, so it is that venue.
        it('an un-collated venue keeps the bare window: the cap and the collation are separate gates', async function () {
            assert.strictEqual(swc.STAKE_WEIGHT_COLLATION_ACTIVATION['BTC:testnet'], null,
                'this control needs a capped-but-uncollated venue; re-point it if testnet is ever pinned');
            // Built through the SAME helper, so an always-suffix helper fails HERE while an
            // always-empty one fails the mainnet cases above: neither degenerate form passes both.
            const c = collateSuffix(900000, 'BTC', 'testnet');
            assert.strictEqual(c, '', 'the collation gate must be off on an unpinned chain');
            const db = dbFor([]);
            await db.getStakeWeightsByCapability('oracle_publish', 900000, '500', 1000, 'BTC', 'testnet');
            const { query } = db._calls[0];
            assert.match(query, new RegExp('DENSE_RANK\\(\\) OVER \\(ORDER BY b\\.source' + escapeRe(c) + '\\)'),
                'testnet caps from genesis');
            assert.doesNotMatch(query, /COLLATE/, 'an unpinned chain must order exactly as it does today');
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
            const c = escapeRe(collateSuffix(960000, 'BTC', 'mainnet'));
            assert.match(query, new RegExp('DENSE_RANK\\(\\) OVER \\(ORDER BY b\\.source' + c + '\\)'),
                'AsOf reconstruction is capped');
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
