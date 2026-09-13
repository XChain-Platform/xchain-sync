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
const lifecycle = require('../tableLifecycle');
const { assertValidIdentifier } = require('./shared.js');

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

};
