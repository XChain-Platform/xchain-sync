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
 * The DATETIME retype set for sync-owned and follower-derived columns.
 *********************************************************************/

'use strict';

const DATETIME_COLUMNS = Object.freeze([
    Object.freeze({
        table: 'merkle_epochs',
        column: 'created_at',
        columnDef: 'DATETIME DEFAULT CURRENT_TIMESTAMP',
        scope: 'sync-owned',
    }),
    Object.freeze({
        table: 'merkle_reorgs',
        column: 'detected_at',
        columnDef: 'DATETIME DEFAULT CURRENT_TIMESTAMP',
        scope: 'sync-owned',
    }),
    Object.freeze({
        table: 'state_tree_roots',
        column: 'computed_at',
        columnDef: 'DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP',
        scope: 'follower-derived',
    }),
]);

function byTable({ includeFollowerDerived }){
    const out = new Map();
    for(const entry of DATETIME_COLUMNS){
        if(entry.scope === 'follower-derived' && !includeFollowerDerived) continue;
        if(!out.has(entry.table)) out.set(entry.table, []);
        out.get(entry.table).push(entry);
    }
    return out;
}

function modifyClause(entry){
    return 'MODIFY COLUMN `' + entry.column + '` ' + entry.columnDef;
}

function needsRetype(row){
    if(!row) return false;
    const dataType = row.DATA_TYPE || row.data_type;
    return String(dataType || '').toLowerCase() === 'timestamp';
}

module.exports = {
    DATETIME_COLUMNS,
    byTable,
    modifyClause,
    needsRetype,
};
