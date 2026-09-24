/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
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
 * XChain Sync - Updated rows: the row accumulator
 *
 ********************************************************************/

// table -> Map(action_index -> row). The Map dedups rows reached by more than
// one class (e.g. a stake both deactivated and slashed in the same window) by
// their UNIQUE action_index, so each table emits each surviving row once.
function add(acc, table, rows){
    if(!rows || rows.length === 0) return;
    let m = acc[table] || (acc[table] = new Map());
    for(let r of rows){
        if(r && r.action_index !== undefined && r.action_index !== null)
            m.set(String(r.action_index), r);
    }
}

module.exports = { add };
