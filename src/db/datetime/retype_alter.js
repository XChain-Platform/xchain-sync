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
 * Builds one table's DATETIME retype statement from its live metadata.
 *********************************************************************/

'use strict';

const { modifyClause, needsRetype } = require('../../schema/datetime_columns');

function rowsByColumn(liveRows){
    const rows = new Map();
    for(const row of (liveRows || [])){
        const name = row.COLUMN_NAME || row.column_name;
        if(name !== undefined && name !== null)
            rows.set(String(name).toLowerCase(), row);
    }
    return rows;
}

function retypeAlterSql(table, entries, liveRows){
    const rows = rowsByColumn(liveRows);
    const pending = entries.filter(entry =>
        needsRetype(rows.get(String(entry.column).toLowerCase()))
    );
    if(pending.length === 0) return null;
    return {
        sql: 'ALTER TABLE `' + table + '` ' + pending.map(modifyClause).join(', '),
        columns: pending.map(entry => entry.column),
    };
}

module.exports = { retypeAlterSql };
