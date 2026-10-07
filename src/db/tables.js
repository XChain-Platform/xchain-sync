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

const lifecycle = require('../table_lifecycle');
const { assertValidIdentifier } = require('./shared.js');
const {
    archiveHeadPredicate,
    ARCHIVE_HEAD_VERSIONS_SQL,
    ARCHIVE_CHUNK_HEIGHT_COL
} = require('../consensus/state_hash');
const tableReads      = require('./tables/table_reads.js');
const tableStreaming  = require('./tables/table_streaming.js');
const tableMutations  = require('./tables/table_mutations.js');
const tableLifecycle  = require('./tables/table_lifecycle_ops.js');

const ARCHIVE_HEAD_VERSION_WINDOW_SQL =
    "WHERE p.version " + ARCHIVE_HEAD_VERSIONS_SQL + " AND " + ARCHIVE_CHUNK_HEIGHT_COL + " BETWEEN ? AND ?";

const windowReads = {

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
     * COINPay matches whose settling COINPAY landed in this window: the match row
     * named by a 'fulfilled' coinpay_statuses row whose own action is in the window.
     *
     * @param {number} from   first block of the window, inclusive
     * @param {number} to     last block of the window, inclusive
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findCoinpaySettledMatches(from, to, conn){
        return await this.doQuery(
            "SELECT m.* FROM `order_matches` m WHERE m.settlement_type = 'coinpay' AND m.action_index IN (" +
                "SELECT cs.coinpay_action_index FROM coinpay_statuses cs " +
                "JOIN actions a ON a.action_index = cs.action_index " +
                "JOIN index_statuses si ON si.id = cs.status_id " +
                "WHERE si.status = 'fulfilled' AND a.block_index BETWEEN ? AND ?)",
            [from, to], conn);
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
        const where = "WHERE " + archiveHeadPredicate('p') + " AND " +
            ARCHIVE_HEAD_VERSION_WINDOW_SQL.slice("WHERE ".length);
        return await this.doQuery(
            "SELECT DISTINCT p.* FROM anchor_actions p " +
            "JOIN anchor_actions c ON c.version = 2 AND c.match_batch_seq = p.match_batch_seq " +
            "JOIN index_statuses ps ON ps.id = p.status_id AND ps.status = 'invalid_archive' " +
            "JOIN index_statuses cs ON cs.id = c.status_id AND cs.status = 'valid' " +
            where,
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

module.exports = Object.assign({}, tableReads, tableStreaming, tableMutations, tableLifecycle, windowReads);
