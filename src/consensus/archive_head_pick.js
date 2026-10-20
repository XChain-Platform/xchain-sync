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
 **********************************************************************
 *
 * The archive parent-pick predicate and DOGE fold floor, ported from
 * xchain-indexer/src/db/anchors/archive_head_pick.js. The source picks a v2
 * chunk's parent with them and stamps that parent on a failed reassembly, so
 * the replica's reorg reset must select the same rows; a twin test compares
 * the two function bodies and their output on every network.
 *
 ********************************************************************/

'use strict';

const gateRegistry = require('./gate_registry');
const { UNARMED } = require('./gate_registry/core');

const FOLD_GATE = 'anchor_fold_activation.ANCHOR_FOLD_ACTIVATION';

function archiveHeadPickPredicate(alias) {
    return alias + '.match_batch_seq IS NOT NULL AND (' +
        alias + '.version = 1 OR (' + alias + '.version = 3 AND ' +
        alias + '.chain IS NULL AND ' + alias + '.block_index_doge >= ?))';
}

// `coin` is optional for network-only callers; DOGE-height readers supply DOGE.
function foldArchiveHeadFloor(network, coin) {
    const table = gateRegistry.get(FOLD_GATE);
    if(typeof network !== 'string')
        return UNARMED;
    const coinKey = coin + ':' + network;
    const floor = coin !== null && coin !== undefined &&
        Object.prototype.hasOwnProperty.call(table, coinKey)
        ? table[coinKey]
        : Object.prototype.hasOwnProperty.call(table, network)
            ? table[network] : undefined;
    return (typeof floor === 'number' && Number.isFinite(floor)) ? floor : UNARMED;
}

module.exports = { archiveHeadPickPredicate, foldArchiveHeadFloor };
