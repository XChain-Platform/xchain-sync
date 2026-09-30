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
 * Retypes aged TIMESTAMP columns that fresh database DDL now creates as DATETIME.
 *********************************************************************/

'use strict';

const util = require('node:util');
const { getLogger } = require('../../observability');
const { isUtcSessionZone } = require('../datetime_session_zone');
const { retypeAlterSql } = require('./retype_alter');
const { byTable } = require('../../schema/datetime_columns');
const logger = getLogger();

async function retypeTable(db, table, entries){
    let liveRows;
    try {
        liveRows = await db.doQuery(
            'SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns WHERE table_schema = ? AND table_name = ?',
            [db.dbName, table], null, { rethrow: true }
        );
    } catch(e){
        logger.error(util.format('Error reading column metadata for ' + table + ':', e));
        return 0;
    }

    const alter = retypeAlterSql(table, entries, liveRows);
    if(!alter) return 0;

    try {
        await db.doQuery(alter.sql, [], null, { rethrow: true });
        logger.info('Retyped ' + table + ' columns to DATETIME: ' + alter.columns.join(', '));
        return alter.columns.length;
    } catch(e){
        logger.error(util.format('Failed to retype ' + table + ' to DATETIME:', e));
        return 0;
    }
}

module.exports = {

    // Require UTC because MariaDB applies the session zone when TIMESTAMP becomes DATETIME.
    async ensureDatetimeColumns({ includeFollowerDerived }){
        if(this.dbType !== 'indexer') return 0;

        const zoneRows = await this.doQuery(
            'SELECT @@session.time_zone AS tz, @@system_time_zone AS sys',
            [], null, { rethrow: true }
        );
        const zoneRow = zoneRows && zoneRows[0];
        if(!isUtcSessionZone(zoneRow)){
            logger.error('Skipping DATETIME retype: session time zone is not UTC (tz=' +
                (zoneRow && zoneRow.tz) + ', system=' + (zoneRow && zoneRow.sys) + ')');
            return 0;
        }

        let retyped = 0;
        for(const [table, entries] of byTable({ includeFollowerDerived }))
            retyped += await retypeTable(this, table, entries);
        return retyped;
    },

};
