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
 * Complete an indexer snapshot's transparency rows through its pinned tip.
 *
 ********************************************************************/

'use strict';

function syncMetaKey(row){
    return String(row.block_index);
}

async function collectUnrecordedSyncMetaRows(db, fromBlock, lastBlock, conn){
    if(((db && db.dbType) || 'indexer') !== 'indexer') return [];
    const rows = await db.getSyncMetaTip(conn);
    const tip = rows && rows.length ? rows[0].tip : null;
    if(tip === null || tip === undefined) return [];
    const start = Math.max(Number(tip) + 1, Number(fromBlock));
    if(start > lastBlock) return [];
    return db.getBlockHashRowsInRange(start, lastBlock, conn);
}

module.exports = { collectUnrecordedSyncMetaRows, syncMetaKey };
