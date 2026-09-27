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

const fs = require('fs');
const path = require('path');

const SQL_DIR = path.join(__dirname, '..', '..', '..', 'src', 'sql');
const TABLE_NAME_RE = /^[A-Za-z0-9_]+$/;

const COLUMN_SHAPE_SQL = `SELECT DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA
    FROM information_schema.columns
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`;

function stripSqlComments(sql){
    return sql
        .replace(/\/\*[\s\S]*?\*\//g, '')
        .replace(/--.*$/gm, '');
}

function readCreateSql(table){
    if(!TABLE_NAME_RE.test(table)) throw new Error(`invalid table name: ${table}`);
    const filePath = path.join(SQL_DIR, `${table}.sql`);
    return stripSqlComments(fs.readFileSync(filePath, 'utf8'));
}

function agedColumnDef(entry){
    if(!entry.columnDef.startsWith('DATETIME'))
        throw new Error(`columnDef does not start with DATETIME: ${entry.columnDef}`);
    return 'TIMESTAMP' + entry.columnDef.slice('DATETIME'.length);
}

function agedModifySql(entry){
    return `ALTER TABLE \`${entry.table}\` MODIFY COLUMN \`${entry.column}\` ${agedColumnDef(entry)}`;
}

function zoneReadSql(entry){
    return `SELECT DATE_FORMAT(\`${entry.column}\`, '%Y-%m-%d %H:%i:%s') AS v FROM \`${entry.table}\``;
}

async function snapshotColumnShapes(query, dbName, entries){
    const shapes = {};
    for(const entry of entries){
        const rows = await query(COLUMN_SHAPE_SQL, [dbName, entry.table, entry.column]);
        const row = rows && rows[0];
        if(!row) throw new Error(`column absent: ${entry.table}.${entry.column}`);
        shapes[`${entry.table}.${entry.column}`] = {
            DATA_TYPE: row.DATA_TYPE,
            COLUMN_TYPE: row.COLUMN_TYPE,
            IS_NULLABLE: row.IS_NULLABLE,
            COLUMN_DEFAULT: row.COLUMN_DEFAULT,
            EXTRA: row.EXTRA,
        };
    }
    return shapes;
}

module.exports = {
    stripSqlComments,
    readCreateSql,
    agedColumnDef,
    agedModifySql,
    zoneReadSql,
    COLUMN_SHAPE_SQL,
    snapshotColumnShapes,
};
