// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

'use strict';

async function seedReorgWindow(poller, logger){
    const cursor = poller.lastPolledBlock;
    if(cursor === null) return;

    const floor = Math.max(1, cursor - poller.recentHashCap + 1);
    const suffix = [];
    for(let blockIndex = cursor; blockIndex >= floor; blockIndex--){
        const hash = poller.transparencyLog
            ? await poller.transparencyLog.getRecordedHash(blockIndex)
            : await poller.sourceBlockHash(blockIndex);
        if(hash === null){
            logger.warn('Reorg window seed stopped for ' + poller.chain + '/' + poller.network + '/' + poller.dbType
                + ': no durable hash at block ' + blockIndex);
            break;
        }
        suffix.push([blockIndex, hash]);
    }

    for(let i = suffix.length - 1; i >= 0; i--)
        poller.recentBroadcastHashes.set(suffix[i][0], suffix[i][1]);
}

module.exports = seedReorgWindow;
