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
 * Loads named, dated SQL migrations and provides the schema-evolution
 * helpers whose statements are assembled from validated source metadata.
 *
 ********************************************************************/

'use strict';

const fs         = require('fs');
const path       = require('path');
const util       = require('node:util');
const validation = require('../../util/validation');
const { splitSqlStatements } = require('../../db/sql_util');
const { getLogger } = require('../../observability');
const logger = getLogger();

const MIGRATION_DIR = __dirname;
const KEY_REBUILD_PRECONDITION_COLUMNS = [
    { table: 'anchor_actions', column: 'section_index', key: 'anchor-actions-section-index' },
    { table: 'validator_rewards', column: 'round_qualifier', key: 'validator-rewards-round-qualifier' },
    { table: 'anchor_reward_reconcile_log', column: 'round_qualifier', key: 'anchor-reward-reconcile-round-qualifier' }
];

function migrationStatements(file){
    if(path.basename(file) !== file || !/^\d{4}-\d{2}-\d{2}-[a-z0-9-]+\.sql$/.test(file))
        throw new Error('Invalid SQL migration filename: ' + file);
    const text = fs.readFileSync(path.join(MIGRATION_DIR, file), 'utf8');
    const statements = new Map();
    const markers = Array.from(text.matchAll(/^\s*--\s*migration-key:\s*([a-z0-9-]+)\s*$/gmi));
    for(let i = 0; i < markers.length; i++){
        const start = markers[i].index + markers[i][0].length;
        const end = i + 1 < markers.length ? markers[i + 1].index : text.length;
        const sql = splitSqlStatements(text.slice(start, end));
        if(sql.length !== 1)
            throw new Error('SQL migration key must contain exactly one statement: ' + file + '#' + markers[i][1]);
        statements.set(markers[i][1], sql[0]);
    }
    return statements;
}

function renderStatement(sql, replacements){
    return sql.replace(/\{\{([a-z_]+)\}\}/g, (match, name) => {
        if(!Object.prototype.hasOwnProperty.call(replacements || {}, name))
            throw new Error('Missing SQL migration replacement: ' + name);
        return replacements[name];
    });
}

module.exports = {

    async runSqlMigrationStatement(file, key, replacements, opts){
        let sql = migrationStatements(file).get(key);
        if(!sql) throw new Error('SQL migration key not found: ' + file + '#' + key);
        sql = renderStatement(sql, replacements);
        if(opts && opts.strict) return await this.doQueryStrict(sql);
        return await this.doQuery(sql, undefined, null, opts && opts.rethrow ? { rethrow: true } : undefined);
    },

    async addMissingColumns(tableName, sourceDdl){
        const sourceColumns = validation.extractColumnNames(sourceDdl);
        if(sourceColumns.length === 0) return 0;

        const destRows = await this.doQuery(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?",
            [this.dbName, tableName]
        );
        const destSet = new Set(destRows.map(r => r.column_name || r.COLUMN_NAME));

        let added = 0;
        const failed = [];
        for(const col of sourceColumns){
            if(destSet.has(col)) continue;
            const colCheck = validation.validateIdentifier(col);
            if(!colCheck.valid){
                logger.error('Skipping invalid column name ' + col + ' on ' + tableName + ' (' + colCheck.reason + ')');
                continue;
            }
            const def = validation.extractColumnDefinition(sourceDdl, col);
            if(!def){
                logger.warn('Could not extract definition for column ' + col + ' on ' + tableName + '; skipping (manual schema change may be required)');
                continue;
            }
            const actions = ['ADD COLUMN ' + def];
            if(validation.isAutoIncrementDefinition(def))
                actions.push(await this.autoIncrementKeyAction(tableName, col, sourceDdl));
            const statement = renderStatement(
                migrationStatements('2026-10-19-replica-columns.sql').get('source-column'),
                { table: tableName, actions: actions.join(', ') }
            );
            try {
                await this.doQueryStrict(statement);
                logger.info('Added column ' + col + ' to ' + tableName);
                added++;
            } catch(e){
                failed.push({ column: col, errno: (e && e.errno) || null, message: (e && e.message) || String(e) });
                logger.error(util.format('FAILED to add column ' + col + ' to ' + tableName +
                    ' (errno ' + ((e && e.errno) || 'unknown') + '); the column is still missing. Statement was: ' + statement, e));
            }
        }
        if(failed.length){
            const err = new Error('Schema column self-heal failed on ' + tableName + ': ' +
                failed.map(f => f.column + ' (errno ' + f.errno + ')').join(', '));
            err.errno = failed[0].errno;
            err.failedColumns = failed;
            throw err;
        }
        return added;
    },

    async autoIncrementKeyAction(tableName, col, sourceDdl){
        const key = validation.extractKeyForColumn(sourceDdl, col);
        if(key && key.type === 'primary' && !(await this.hasPrimaryKey(tableName)))
            return 'ADD PRIMARY KEY (`' + col + '`)';
        if(key && key.type === 'unique' && key.name)
            return 'ADD UNIQUE KEY `' + key.name + '` (`' + col + '`)';
        if(key && key.type === 'index' && key.name)
            return 'ADD KEY `' + key.name + '` (`' + col + '`)';
        return 'ADD UNIQUE KEY `' + col + '` (`' + col + '`)';
    },

    async hasPrimaryKey(tableName){
        try {
            const rows = await this.doQueryStrict(
                "SELECT index_name FROM information_schema.statistics WHERE table_schema = ? AND table_name = ? AND index_name = 'PRIMARY' LIMIT 1",
                [this.dbName, tableName]
            );
            return rows.length > 0;
        } catch(e){
            logger.error(util.format('Could not read primary-key state for ' + tableName + '; assuming one exists:', e));
            return true;
        }
    },

    async ensureKeyRebuildColumn(table, column){
        const spec = KEY_REBUILD_PRECONDITION_COLUMNS.find(c => c.table === table && c.column === column);
        if(!spec) return false;
        try {
            await this.runSqlMigrationStatement(
                '2026-10-19-replica-columns.sql', spec.key, undefined, { strict: true }
            );
            logger.info('Added ' + table + '.' + column + ' on ' + this.dbName +
                '; the key rebuild that names it can run in this same startup.');
            return true;
        } catch(e){
            logger.error(util.format('Failed to add ' + table + '.' + column + ' on ' + this.dbName +
                ' (errno ' + ((e && e.errno) || 'unknown') + '); the key rebuild that names it cannot run', e));
            return false;
        }
    },

    async readReplicatedCreateStatement(sourceDb, tableName, retry){
        const idCheck = validation.validateIdentifier(tableName);
        if(!idCheck.valid){
            if(!retry) logger.error('Skipping invalid table name: ' + tableName + ' (' + idCheck.reason + ')');
            return null;
        }
        const ddlRows = await sourceDb.doQuery('SHOW ' + 'CREATE TABLE `' + tableName + '`');
        if(ddlRows.length === 0) return null;
        const createSql = ddlRows[0]['Create Table'];
        if(!createSql) return null;
        const ddlCheck = validation.validateDdl(createSql);
        if(!ddlCheck.valid){
            const suffix = retry ? ' (retry)' : '';
            logger.error('Rejected DDL for ' + tableName + suffix + ': ' + ddlCheck.reason);
            return null;
        }
        return createSql;
    }
};
