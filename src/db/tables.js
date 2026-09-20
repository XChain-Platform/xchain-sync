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
 * Table-generic access: row windows, counts, streaming and truncation, for any
 * replicated table named at run time rather than compiled in.
 *
 * A Database mixin: every method below is installed on Database.prototype by
 * db/index.js, so `this` is the Database instance and call sites are unchanged.
 *
 ********************************************************************/

const path       = require('path');
const lifecycle = require('../table_lifecycle');
const { assertValidIdentifier } = require('./shared.js');
const { ARCHIVE_HEAD_VERSIONS_SQL, ARCHIVE_CHUNK_HEIGHT_COL } = require('../consensus/state_hash');

module.exports = {

    // Advisory content-parity reads.
    //
    // Every row of `table` inside a block window, reached through the SAME scope
    // join the per-block stream uses, so what the checksum sees is exactly what
    // replication was supposed to deliver. Bounds come from
    // replicatedTables.contentParityPlan; the caller hashes the rows.
    //
    // doQueryStrict, not doQuery: outside a transaction doQuery is fail-soft and
    // returns [] on error, and an empty result here is indistinguishable from
    // "this table has no rows in the window", which would silently drop the table
    // from the comparison on BOTH sides and read as parity. The caller catches per
    // table and omits it explicitly (advisory), so a real fault stays visible as an
    // omission rather than being laundered into a pass.
    //
    // No ORDER BY: the caller canonicalizes and SORTS the rows before hashing, so
    // the digest is independent of storage order and collation on either side.
    async getContentWindowRows(table, bound, fromBlock, toBlock, conn){
        assertValidIdentifier(table);
        let query;
        if(bound === 'action'){
            query = "SELECT t.* FROM `" + table + "` t" +
                    " INNER JOIN actions a ON (a.action_index = t.action_index)" +
                    " WHERE a.block_index BETWEEN ? AND ?";
        } else if(bound === 'tx'){
            query = "SELECT t.* FROM `" + table + "` t" +
                    " INNER JOIN transactions tx ON (tx.tx_index = t.tx_index)" +
                    " WHERE tx.block_index BETWEEN ? AND ?";
        } else if(bound === 'emission'){
            // contract_emissions carries a NULL action_index for internal emissions,
            // which the generic INNER JOIN above would drop. Reach them through the
            // execution_index chain, the same route ServerPoller streams them by.
            query = "SELECT em.* FROM contract_emissions em" +
                    " INNER JOIN contract_executions ce ON (ce.action_index = em.execution_index)" +
                    " INNER JOIN actions a ON (a.action_index = ce.action_index)" +
                    " WHERE a.block_index BETWEEN ? AND ?";
        } else {
            // 'block': the two reorg-scoped lookups also land here, and their
            // block_index is NULL for rows assigned outside a consensus block tx
            // (recovery pre-seed, API read-path createAddress). Those are benign
            // source-local drift, excluded exactly as computeIndexMapChecksum
            // excludes them, so they can never raise a false alarm.
            //
            // Window by the registry's scope column, never the literal: a close_block-keyed
            // table raises errno 1054 on `block_index`, the caller's per-table catch drops
            // it, and it stays reported as parity-covered while nothing ever checks it.
            let key = lifecycle.blockKey(table);
            assertValidIdentifier(key);
            query = "SELECT * FROM `" + table + "` WHERE " + key + " IS NOT NULL AND " + key + " BETWEEN ? AND ?";
        }
        return await this.doQueryStrict(query, [fromBlock, toBlock], conn);
    },

    // The current highest `id` in an append-only lookup, or null when it is empty.
    // The SOURCE publishes this ceiling and the follower reuses it verbatim, so
    // both sides checksum the same id range even though the follower's own tail
    // may lag or lead by rows the window then ignores.
    async getMaxRowId(table, conn){
        assertValidIdentifier(table);
        let rows = await this.doQueryStrict("SELECT MAX(id) AS m FROM `" + table + "`", null, conn);
        let m = rows && rows.length ? rows[0].m : null;
        return (m === null || m === undefined) ? null : Number(m);
    },

    // Rows of an append-only lookup inside an id window (fromId, toId]. Same
    // doQueryStrict / no-ORDER-BY reasoning as getContentWindowRows above.
    async getContentIdWindowRows(table, fromId, toId, conn){
        assertValidIdentifier(table);
        return await this.doQueryStrict(
            "SELECT * FROM `" + table + "` WHERE id > ? AND id <= ?", [fromId, toId], conn);
    },

    // Get all rows from a table for a given block (block-scoped tables).
    // ORDER BY the scope column, then by the first column for deterministic ordering
    // across sources with differing insert histories (matches the snapshot path).
    //
    // Scope by the registry's blockKey, never the literal `block_index`: a table keyed
    // by another column raises errno 1054, which ServerPoller classifies as an older
    // source schema and drops from the payload with no log line and no delivery.
    async getBlockScopedRows(table, block_index, conn){
        let key = lifecycle.blockKey(table);
        assertValidIdentifier(key);
        let query = "SELECT * FROM `" + table + "` WHERE " + key + " = ? ORDER BY " + key + " ASC, 1 ASC";
        return await this.doQuery(query, [block_index], conn);
    },

    // Stream every row of a table in ONE ordered pass on the given (dedicated
    // snapshot) connection. Returns the driver's row Readable (async-iterable);
    // the caller must consume it fully or destroy() it.
    //
    // This deliberately replaces LIMIT/OFFSET paging (`ORDER BY 1 LIMIT ? OFFSET ?`):
    // most replicated ledger tables have NO primary key and a non-unique first
    // column (e.g. credits/debits share one action_index across a match's rows), so
    // `ORDER BY 1` is not a total order and SQL does not guarantee a stable tie
    // order across separate OFFSET executions. A boundary tie could then be emitted
    // in two adjacent pages (duplicate -> plain-INSERT apply aborts, or silently
    // doubles a keyless row) or in neither (skip -> replica short, caught only by
    // the advisory count check). A single query execution reads each row exactly
    // once, so no cross-execution tie order exists to disagree. Keyset paging is
    // not an option here: the keyless tables have nothing unique to key on.
    // ORDER BY 1 is kept so the emitted row order matches the historical snapshot
    // shape (and getBlockScopedRows' "matches the snapshot path" comment).
    streamTableRows(table, conn){
        assertValidIdentifier(table);
        return conn.queryStream("SELECT * FROM `" + table + "` ORDER BY 1");
    },

    // The set of BASE TABLEs that actually exist in this database.
    //
    // Exists so a caller can skip a table instead of discovering its absence by
    // failing a query against it. That distinction is not cosmetic: the /status
    // handlers enumerate the STATIC replicated-table list, which grows whenever a
    // new family lands in this repo, so on any replica whose source predates that
    // family a poll that queries such a table raises ER_NO_SUCH_TABLE, logs a
    // multi-line SqlError and swallows it, every time. That error is expected and
    // tolerated, yet indistinguishable in the journal from a real fault, and it is
    // raised for tables the SOURCE does not have either.
    //
    // doQueryStrict, so a failure to LIST is never silently read as "nothing
    // exists": that would empty table_counts and make an incomplete replica look
    // complete to verifyTableCounts. Callers fall back to probing instead.
    async listExistingTables(conn){
        let rows = await this.doQueryStrict(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
            [this.dbName], conn);
        return new Set(rows.map(r => r.table_name || r.TABLE_NAME));
    },

    // Get total row count for a table.
    //
    // doQueryStrict, NOT doQuery, and the reason is a live defect rather than
    // tidiness. Outside a transaction doQuery is fail-soft: it logs the
    // SqlError and returns [], so `rows[0].cnt` then threw a TypeError with NO
    // errno. Every caller that classifies the failure by errno was therefore
    // reading a different error than the one the database raised, and the one
    // that matters is ClientSync.verifyTableCounts: its catch routes errno 1146
    // ("the source's schema moved ahead of this replica") into the debounced
    // schema heal that CREATEs the missing table. A TypeError carries no errno,
    // so that heal could never fire from here, and the replica stayed missing the
    // table forever while logging the same SqlError on every status poll.
    // Observed on a production BTC regtest replica: `bet_resolves` absent and
    // erroring 11,542 times over two days with the heal wired and unreachable.
    //
    // Strict here is safe for the tolerant callers too: /status wraps this in a
    // try/catch and omits the table, which is unchanged. What changes is that the
    // error they swallow, and the one ClientSync inspects, is now the database's
    // own errno-bearing SqlError.
    async getTableCount(table, conn){
        assertValidIdentifier(table);
        let query = "SELECT COUNT(*) as cnt FROM `" + table + "`";
        let rows = await this.doQueryStrict(query, null, conn);
        return Number(rows[0].cnt);
    },

    // Per-database size + table-count stats for every replicated XChain_* DB on this
    // server. information_schema is server-wide, so one connection reports them all.
    // Used by the /catalog endpoint. Rows: {db_name, tables, data_bytes, index_bytes}.
    async getDatabaseStats(){
        let query = "SELECT table_schema AS db_name, COUNT(*) AS tables, " +
                    "COALESCE(SUM(data_length),0) AS data_bytes, " +
                    "COALESCE(SUM(index_length),0) AS index_bytes " +
                    "FROM information_schema.tables " +
                    "WHERE table_type='BASE TABLE' AND table_schema LIKE 'XChain\\_%' " +
                    "GROUP BY table_schema";
        return await this.doQuery(query);
    },

    async truncateTable(table){
        assertValidIdentifier(table);
        await this.doQuery("TRUNCATE TABLE `" + table + "`");
    },

    /**
     * The information_schema row for one table in this database, or none. Used to
     * decide whether a table must be created before rows are written into it.
     *
     * @param {string} tableName
     * @returns {Promise<object[]>} the driver's row array, empty when the table is absent
     */
    async findTableInSchema(tableName){
        return await this.doQuery(
            "SELECT * FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
            [this.dbName, tableName]
        );
    },

    /**
     * The highest value in one column of one table. Both names are interpolated,
     * because an identifier cannot be a bind parameter; the caller passes names it
     * read from the replicated-table registry, never user input.
     *
     * @param {string} table
     * @param {string} col
     * @returns {Promise<object[]>} the driver's row array, one row carrying `m`
     */
    async getMaxColumnValue(table, col){
        return await this.doQuery('SELECT MAX(`' + col + '`) AS m FROM `' + table + '`');
    },

    /**
     * Rows of any replicated table for a set of ids. The table is interpolated,
     * because an identifier cannot be a bind parameter; the caller iterates the
     * replicated-table registry, never user input.
     *
     * @param {string} table
     * @param {Array<number>} ids at least one
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findRowsByIds(table, ids, conn){
        return await this.doQuery("SELECT * FROM `" + table + "` WHERE id IN (" + ids.map(() => '?').join(',') + ")", ids, conn);
    },

    /**
     * The name of every base table in this database, in name order. Views are
     * excluded, because only a base table has DDL a client can recreate.
     *
     * @returns {Promise<object[]>} the driver's row array, one row per table
     */
    async findBaseTableNames(){
        return await this.doQuery(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name",
            [this.dbName]
        );
    },

    /**
     * The name of every base table in this database, unordered: the snapshot
     * builder imposes its own dependency order.
     *
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array, one row per table
     */
    async findStreamableTableNames(conn){
        return await this.doQuery(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
            [this.dbName],
            conn
        );
    },

    /**
     * Every row of a table from a block onward, keyed by a block_index column.
     * The table is interpolated because an identifier cannot be a bind parameter;
     * the caller passes a name from the replicated-table topology.
     *
     * @param {string} table
     * @param {number} sinceBlock first block, inclusive
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findRowsFromBlockIndex(table, sinceBlock, conn){
        return await this.doQuery("SELECT * FROM `" + table + "` WHERE block_index >= ? ORDER BY block_index", [sinceBlock], conn);
    },

    /**
     * Every row of a table. The table is interpolated from the replicated-table
     * topology, never user input.
     *
     * @param {string} table
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findAllRows(table, conn){
        return await this.doQuery("SELECT * FROM `" + table + "`", null, conn);
    },

    /**
     * Every row of a table from a block onward, keyed by the column the table
     * lifecycle registry names for it. Both names are interpolated from the
     * registry, never user input.
     *
     * @param {string} table
     * @param {string} key the table's block scope column
     * @param {number} sinceBlock first block, inclusive
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findRowsFromBlockKey(table, key, sinceBlock, conn){
        return await this.doQuery("SELECT * FROM `" + table + "` WHERE " + key + " >= ? ORDER BY " + key, [sinceBlock], conn);
    },

    /**
     * Every row of a table from an action index onward. The table is interpolated
     * from the replicated-table topology, never user input.
     *
     * @param {string} table
     * @param {number} firstActionIndex first action, inclusive
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findRowsFromActionIndex(table, firstActionIndex, conn){
        return await this.doQuery("SELECT * FROM `" + table + "` WHERE action_index >= ? ORDER BY action_index", [firstActionIndex], conn);
    },

    /**
     * One page of an append-only lookup table after an id cursor. The table and
     * cursor column come from the replicated-table allowlist, never user input.
     *
     * @param {string} table
     * @param {string} col the cursor column
     * @param {number} after the last cursor value already read
     * @param {number} limit the page size
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findLookupPageAfter(table, col, after, limit, conn){
        return await this.doQuery(
            "SELECT * FROM `" + table + "` WHERE `" + col + "` > ? ORDER BY `" + col + "` ASC LIMIT ?",
            [after, limit],
            conn
        );
    },

    /**
     * The decoder's dispensers rows after a keyset cursor, read strictly so a
     * query error throws rather than reading as an empty table.
     *
     * @param {number} afterTx   the cursor's tx_index
     * @param {number} afterAddr the cursor's address_id
     * @returns {Promise<object[]>} the driver's row array
     */
    async findDispensersAfter(afterTx, afterAddr){
        return await this.doQueryStrict(
            "SELECT * FROM `dispensers` WHERE (tx_index > ? OR (tx_index = ? AND address_id > ?)) " +
            "ORDER BY tx_index ASC, address_id ASC",
            [afterTx, afterTx, afterAddr]
        );
    },

    /**
     * Every row of the decoder's dispensers table, read strictly.
     *
     * @returns {Promise<object[]>} the driver's row array
     */
    async findAllDispensers(){
        return await this.doQueryStrict(
            "SELECT * FROM `dispensers` ORDER BY tx_index ASC, address_id ASC"
        );
    },

    /**
     * Empty one table on the replica before a full snapshot re-imports it. DELETE
     * rather than TRUNCATE, because MariaDB refuses TRUNCATE on a table a foreign
     * key references. The caller validates the name first.
     *
     * @param {string} table
     * @returns {Promise<object>} the driver's result
     */
    async deleteAllRows(table){
        return await this.doQuery('DELETE FROM `' + table + '`');
    },

    /**
     * Empty the decoder's dispensers table ahead of a reconcile re-insert, inside
     * the caller's transaction.
     *
     * @returns {Promise<object>} the driver's result
     */
    async deleteAllDispensers(){
        return await this.doQuery('DELETE FROM `dispensers`');
    },

    /**
     * Clear every row already holding one of these natural-key values, so a
     * re-sent row replaces rather than collides. Both names are interpolated
     * because an identifier cannot be a bind parameter; the caller validates them.
     *
     * @param {string} table
     * @param {string} naturalKey the column the values belong to
     * @param {Array} slice       the values, a bounded chunk of them
     * @returns {Promise<object>} the driver's result
     */
    async deleteRowsByKeyValues(table, naturalKey, slice){
        return await this.doQuery(
            'DELETE FROM `' + table + '` WHERE `' + naturalKey + '` IN (' +
                slice.map(() => '?').join(', ') + ')',
            slice);
    },

    /**
     * One multi-row INSERT of `rowCount` rows over `columns`, with `args` holding
     * every row's values in column order. `useIgnore` skips a row whose key already
     * exists; `useUpsert` overwrites the existing row with the carried values. The
     * caller validates every identifier and decodes the values.
     *
     * @param {string} table
     * @param {Array<string>} columns
     * @param {number} rowCount
     * @param {Array} args
     * @param {boolean} useIgnore
     * @param {boolean} useUpsert
     * @returns {Promise<object>} the driver's result
     */
    async insertRowValues(table, columns, rowCount, args, useIgnore, useUpsert){
        let colList      = columns.map(c => '`' + c + '`').join(', ');
        let placeholders = columns.map(() => '?').join(', ');

        let insertPrefix = useIgnore
            ? 'INSERT IGNORE INTO `' + table + '` (' + colList + ') VALUES '
            : 'INSERT INTO `' + table + '` (' + colList + ') VALUES ';
        // VALUES(col) back-reference is the MariaDB idiom for "the value this row
        // would have inserted"; updating the key column to itself is a harmless no-op.
        let updateSuffix = useUpsert
            ? ' ON DUPLICATE KEY UPDATE ' + columns.map(c => '`' + c + '` = VALUES(`' + c + '`)').join(', ')
            : '';

        let valueClauses = [];
        for(let i = 0; i < rowCount; i++) valueClauses.push('(' + placeholders + ')');

        let query = insertPrefix + valueClauses.join(', ') + updateSuffix;
        return await this.doQuery(query, args);
    },

    /**
     * The row holding one surrogate id, if any.
     *
     * @param {string} table
     * @param {number} id
     * @returns {Promise<object[]>} the driver's row array, at most one row
     */
    async findRowIdById(table, id){
        return await this.doQuery('SELECT id FROM `' + table + '` WHERE id = ? LIMIT 1', [id]);
    },

    /**
     * The ids of rows matching one natural key. LIMIT 2, because the caller only
     * needs to tell "exactly one holder" from "none" or "ambiguous".
     *
     * @param {string} table
     * @param {Array<string>} keyColumns validated column names of the key
     * @param {Array} values             one value per key column
     * @returns {Promise<object[]>} the driver's row array, at most two rows
     */
    async findRowIdsByKeyColumns(table, keyColumns, values){
        return await this.doQuery(
            'SELECT id FROM `' + table + '` WHERE ' +
                keyColumns.map(c => '`' + c + '` = ?').join(' AND ') + ' LIMIT 2',
            values);
    },

    /**
     * Delete one row by its surrogate id.
     *
     * @param {string} table
     * @param {number} holderId
     * @returns {Promise<object>} the driver's result
     */
    async deleteRowById(table, holderId){
        return await this.doQuery('DELETE FROM `' + table + '` WHERE id = ?', [holderId]);
    },

    /**
     * The ordered column names of one index of one table in this database.
     *
     * @param {string} table
     * @param {string} indexName
     * @returns {Promise<object[]>} the driver's row array, one row per column in index order
     */
    async findIndexColumnNames(table, indexName){
        return await this.doQuery(
            "SELECT column_name FROM information_schema.statistics " +
            "WHERE table_schema = ? AND table_name = ? AND index_name = ? ORDER BY seq_in_index ASC",
            [this.dbName, table, indexName]);
    },

    // The updated-rows channel's reads (server/updated_rows.js). Each returns the
    // CURRENT full state of surviving rows mutated in place inside a block window,
    // which the action-scoped stream cannot carry because the row's own action is
    // older than the window. Table names are interpolated because an identifier
    // cannot be a bind parameter; every one comes from that module's fixed lists.

    /**
     * Rows whose deactivation_block stamp falls in the window. The caller shifts
     * the window by the chain's activation delay, because a stamp is written that
     * many blocks ahead of the action that set it.
     *
     * @param {string} table
     * @param {number} fromStamp first stamp value, inclusive
     * @param {number} toStamp   last stamp value, inclusive
     * @param {object} [conn]    a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findDeactivationStampedRows(table, fromStamp, toStamp, conn){
        return await this.doQuery(
            "SELECT * FROM `" + table + "` WHERE deactivation_block IS NOT NULL AND deactivation_block BETWEEN ? AND ?",
            [fromStamp, toStamp], conn);
    },

    /**
     * Stake or unstake rows a SLASH reduced in this window, reached through the
     * debit log entry that records the reduction.
     *
     * @param {{table: string, debits: string, target: string}} spec one SLASH_SPECS entry
     * @param {number} from   first block of the window, inclusive
     * @param {number} to     last block of the window, inclusive
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findSlashDebitedRows(spec, from, to, conn){
        return await this.doQuery(
            "SELECT t.* FROM `" + spec.table + "` t " +
            "JOIN `" + spec.debits + "` d ON d.stake_action_index = t.action_index " +
            "WHERE d.target_table = ? AND d.block_index BETWEEN ? AND ?",
            [spec.target, from, to], conn);
    },

    /**
     * Contract stake rows a DELEGATE v1 signing-key rotation rewrote in this
     * window, pinned to a block only by the rotations journal.
     *
     * @param {string} rotTbl one ROTATION_TABLES entry
     * @param {number} from   first block of the window, inclusive
     * @param {number} to     last block of the window, inclusive
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findRotatedStakeRows(rotTbl, from, to, conn){
        return await this.doQuery(
            "SELECT t.* FROM `" + rotTbl + "` t " +
            "JOIN `contract_delegation_rotations` r ON r.stake_action_index = t.action_index " +
            "WHERE r.target_table = ? AND r.block_index BETWEEN ? AND ?",
            [rotTbl, from, to], conn);
    },

    /**
     * Version 0 request rows whose request_status resolved in this window.
     *
     * @param {string} table  one REQUEST_STATUS_TABLES entry
     * @param {number} from   first block of the window, inclusive
     * @param {number} to     last block of the window, inclusive
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findResolvedRequestRows(table, from, to, conn){
        return await this.doQuery(
            "SELECT * FROM `" + table + "` WHERE version = 0 AND resolved_block BETWEEN ? AND ?",
            [from, to], conn);
    },

    /**
     * Poll rows finalized in this window, or whose deferred binding callback
     * fired at a due block in this window.
     *
     * @param {string} table  one POLL_FINALIZE_TABLES entry
     * @param {number} from   first block of the window, inclusive
     * @param {number} to     last block of the window, inclusive
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findFinalizedPollRows(table, from, to, conn){
        return await this.doQuery(
            "SELECT * FROM `" + table + "` WHERE resolved_block BETWEEN ? AND ? " +
            "OR (callback_due_block BETWEEN ? AND ? AND callback_execute_action_index IS NOT NULL)",
            [from, to, from, to], conn);
    },

    /**
     * Unstake rows whose cooldown matured in this window.
     *
     * @param {string} table  one COOLDOWN_STATUS_TABLES entry
     * @param {number} from   first block of the window, inclusive
     * @param {number} to     last block of the window, inclusive
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findMaturedCooldownRows(table, from, to, conn){
        return await this.doQuery(
            "SELECT * FROM `" + table + "` WHERE cooldown_end_block BETWEEN ? AND ?",
            [from, to], conn);
    },

    /**
     * BET rows matching a window predicate the caller built over the spec's
     * stamp columns.
     *
     * @param {{table: string}} spec one BET_STATUS_SPECS entry
     * @param {string} where         the OR-joined stamp predicate
     * @param {Array<number>} args   one window pair per stamp column
     * @param {object} [conn]        a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findBetStampedRows(spec, where, args, conn){
        return await this.doQuery(
            "SELECT * FROM `" + spec.table + "` WHERE " + where, args, conn);
    },

    /**
     * Archive-head anchor parents stamped invalid_archive by a completing chunk
     * that landed in this window. The height key is the shared chunk-height column
     * from stateHash.js, which a v2 continuation row actually populates.
     *
     * @param {number} from   first block of the window, inclusive
     * @param {number} to     last block of the window, inclusive
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findInvalidArchiveHeadRows(from, to, conn){
        return await this.doQuery(
            "SELECT DISTINCT p.* FROM anchor_actions p " +
            "JOIN anchor_actions c ON c.version = 2 AND c.match_batch_seq = p.match_batch_seq " +
            "JOIN index_statuses ps ON ps.id = p.status_id AND ps.status = 'invalid_archive' " +
            "JOIN index_statuses cs ON cs.id = c.status_id AND cs.status = 'valid' " +
            "WHERE p.version " + ARCHIVE_HEAD_VERSIONS_SQL + " AND " + ARCHIVE_CHUNK_HEIGHT_COL + " BETWEEN ? AND ?",
            [from, to], conn);
    },

    /**
     * ATTEST batch heads whose failure verdict was stamped when a valid
     * continuation by the same author completed the batch inside this window.
     *
     * @param {number} headVersion         the batch head's attests version
     * @param {number} continuationVersion the continuation chunk's attests version
     * @param {string} completionStamp     the status suffix a completion stamp carries
     * @param {number} from                first block of the window, inclusive
     * @param {number} to                  last block of the window, inclusive
     * @param {object} [conn]              a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findFailedAttestBatchHeads(headVersion, continuationVersion, completionStamp, from, to, conn){
        return await this.doQuery(
            "SELECT ah.* FROM attests ah " +
            "JOIN index_statuses ahs ON ahs.id = ah.status_id AND ahs.status LIKE ? " +
            "JOIN actions aha ON aha.action_index = ah.action_index " +
            "JOIN attests ac ON ac.request_id = ah.request_id " +
                "AND ac.version = " + continuationVersion + " AND ac.batch_chunk_index IS NOT NULL " +
            "JOIN index_statuses acs ON acs.id = ac.status_id AND acs.status = 'valid' " +
            "JOIN actions aca ON aca.action_index = ac.action_index AND aca.source_id = aha.source_id " +
            "WHERE ah.version = " + headVersion + " AND ah.batch_chunk_index = 0 " +
                "AND ac.block_index BETWEEN ? AND ?",
            ['%' + completionStamp, from, to], conn);
    },

};
