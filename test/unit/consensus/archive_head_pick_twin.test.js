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
const ours = require('../../../src/consensus/archive_head_pick');
const { UNARMED } = require('../../../src/consensus/gate_registry/core');
const { siblingCheckout, skipOrFail } = require('../../helpers/sibling_checkout.js');

const INDEXER_PICK = '../../../../xchain-indexer/src/db/anchors/archive_head_pick.js';
const NETWORKS = ['mainnet', 'testnet', 'regtest', 'unknown', undefined, null];
const COINS = ['DOGE', 'BTC', 'LTC', undefined];

describe('archive parent-pick predicate twin', function () {
    // The replica's reorg reset must select exactly the heads the source's chunk path can stamp.
    it('matches the xchain-indexer parent pick, function body and floor, on every network', function () {
        const verdict = siblingCheckout(__dirname, INDEXER_PICK);
        if (!skipOrFail(this, verdict, 'the archive parent-pick twin guard')) return;
        const theirs = require(verdict.path);

        for (const name of ['archiveHeadPickPredicate', 'foldArchiveHeadFloor'])
            assert.strictEqual(ours[name].toString(), theirs[name].toString(), name + ' differs from ' + verdict.path);
        assert.strictEqual(ours.archiveHeadPickPredicate('p'), theirs.archiveHeadPickPredicate('p'));
        for (const network of NETWORKS)
            for (const coin of COINS)
                assert.strictEqual(ours.foldArchiveHeadFloor(network, coin), theirs.foldArchiveHeadFloor(network, coin),
                    'fold floor differs for ' + coin + ':' + network);
    });

    it('arms the DOGE testnet floor and leaves mainnet UNARMED', function () {
        const testnet = ours.foldArchiveHeadFloor('testnet', 'DOGE');
        assert.ok(Number.isFinite(testnet) && testnet < UNARMED, 'testnet DOGE fold floor is armed');
        assert.strictEqual(ours.foldArchiveHeadFloor('mainnet', 'DOGE'), UNARMED);
        assert.strictEqual(ours.foldArchiveHeadFloor(undefined, 'DOGE'), UNARMED);
    });
});
