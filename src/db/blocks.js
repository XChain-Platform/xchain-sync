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
 * The replicated blocks table and the chain-position questions asked of it:
 * how far the replica has got, and what a block's stored hashes are.
 *
 * A Database mixin: every method below is installed on Database.prototype by
 * db/index.js, so `this` is the Database instance and call sites are unchanged.
 *
 ********************************************************************/

const path       = require('path');

module.exports = {

    // `opts` is forwarded to doQuery, so a cursor caller that must not mistake an
    // unreadable tip for an empty replica can pass { rethrow: true } (M-17). The
    // default stays fail-soft for the /status readers, where a partial answer beats
    // a 500.
    async getLastBlock(conn, opts){
        let query = "SELECT MAX(block_index) AS block_index FROM blocks";
        let rows  = await this.doQuery(query, null, conn, opts);
        if(rows.length > 0 && rows[0].block_index !== null)
            return Number(rows[0].block_index);
        return null;
    },

    // Read the replication engine's own view of this node's freshness.
    // getLastBlock above reads the SERVED database, so on a node fronting a native
    // SQL replica the source and served heights are one failure domain: replication
    // stops applying, both freeze at the same number, and lag_blocks publishes 0
    // while the node is hours behind. Only the replication subsystem can tell those
    // apart. Returns { isReplica, running, secondsBehind }: isReplica false on a
    // primary/co-located source (empty result set), null when the read itself was
    // refused (missing REPLICATION CLIENT grant), which callers must treat as stale.
    async getReplicaStatus(conn){
        // Multi-source first: `SHOW REPLICA STATUS` reports only the UNNAMED
        // connection, so a replica whose connections are all named answers it with
        // zero rows, which is indistinguishable from a primary. Fall back for MySQL.
        let rows;
        let attempts = ['SHOW ALL SLAVES STATUS', 'SHOW REPLICA STATUS', 'SHOW SLAVE STATUS'];
        let lastError = null;
        for (const sql of attempts){
            try { rows = await this.doQueryStrict(sql, null, conn); lastError = null; break; }
            catch (error){ lastError = error; }
        }
        if(lastError){
            if(!this._replicaStatusWarned){
                this._replicaStatusWarned = true;
                // Name both spellings: MariaDB 10.5 split the privilege, where
                // REPLICATION CLIENT aliases BINLOG MONITOR and does not cover
                // slave status.
                this.util.logError('Cannot read replication status (needs SLAVE MONITOR on MariaDB, ' +
                    'REPLICATION CLIENT on MySQL):', lastError);
            }
            return { isReplica: null, running: null, secondsBehind: null };
        }
        // No connections at all: a primary, or a server whose databases are fed by
        // the sync protocol rather than native replication. Fresh by this probe; the
        // client-side staleness window covers that path.
        if(!rows || rows.length === 0)
            return { isReplica: false, running: null, secondsBehind: null };

        const readRow = (row) => {
            const io     = row.Replica_IO_Running  != null ? row.Replica_IO_Running  : row.Slave_IO_Running;
            const sql    = row.Replica_SQL_Running != null ? row.Replica_SQL_Running : row.Slave_SQL_Running;
            const behind = row.Seconds_Behind_Source != null ? row.Seconds_Behind_Source : row.Seconds_Behind_Master;
            return {
                name:    row.Connection_name != null ? String(row.Connection_name) : '',
                running: io === 'Yes' && sql === 'Yes',
                // NULL here means the SQL thread is not applying at all, never "0 behind".
                secondsBehind: behind == null ? null : Number(behind)
            };
        };
        let parsed = rows.map(readRow);

        // A named-but-absent connection is an assertion that no longer matches the
        // server, not an absence of replication: fail closed rather than silently
        // measuring a different stream.
        const wanted = this.replicaConnectionName;
        if(wanted){
            const hit = parsed.find(p => p.name === wanted);
            if(!hit){
                if(!this._replicaConnectionMissingWarned){
                    this._replicaConnectionMissingWarned = true;
                    this.util.logError('Configured replica connection "' + wanted + '" is not present on this server; ' +
                        'saw [' + parsed.map(p => p.name || '(unnamed)').join(', ') + ']. Reporting unknown.', null);
                }
                return { isReplica: null, running: null, secondsBehind: null };
            }
            return { isReplica: true, running: hit.running, secondsBehind: hit.secondsBehind };
        }

        // Reduce across every connection, worst-case: served data is only as fresh as
        // its laggiest stream, and one stopped SQL thread makes the server stale.
        const running = parsed.every(p => p.running);
        const anyUnknown = parsed.some(p => p.secondsBehind == null);
        const secondsBehind = anyUnknown ? null
            : parsed.reduce((max, p) => Math.max(max, p.secondsBehind), 0);
        return { isReplica: true, running, secondsBehind };
    },

    // `opts` is forwarded to doQuery, so a caller whose NULL answer suppresses a guard
    // can pass { rethrow: true }: outside a transaction doQuery turns a query error into
    // [], which reads here as "this block was never applied" (M-17). The default stays
    // fail-soft for the /status readers.
    async getBlockHashRow(block_index, conn, opts){
        let query;
        if(this.dbType === 'decoder'){
            query = `SELECT
                    b.block_index,
                    b.block_time,
                    t1.hash as block_hash
                FROM
                    blocks b
                    LEFT JOIN index_transactions t1 ON (t1.id=b.block_hash_id)
                WHERE
                    b.block_index=?`;
        } else {
            query = `SELECT
                    b.block_index,
                    b.block_time,
                    t1.hash as ledger_hash,
                    t2.hash as actions_hash,
                    t3.hash as contract_hash,
                    t4.hash as state_hash
                FROM
                    blocks b
                    LEFT JOIN index_transactions t1 ON (t1.id=b.ledger_hash_id)
                    LEFT JOIN index_transactions t2 ON (t2.id=b.actions_hash_id)
                    LEFT JOIN index_transactions t3 ON (t3.id=b.contract_hash_id)
                    LEFT JOIN index_transactions t4 ON (t4.id=b.state_hash_id)
                WHERE
                    b.block_index=?`;
        }
        let rows = await this.doQuery(query, [block_index], conn, opts);
        if(rows.length > 0)
            return rows[0];
        return null;
    },

    // Get block data for a range of blocks (for building payloads).
    // Same indexer-vs-decoder branching as getBlockHashRow.
    async getBlockRows(startBlock, endBlock){
        let query;
        if(this.dbType === 'decoder'){
            query = `SELECT
                    b.block_index,
                    b.block_time,
                    t1.hash as block_hash
                FROM
                    blocks b
                    LEFT JOIN index_transactions t1 ON (t1.id=b.block_hash_id)
                WHERE
                    b.block_index >= ? AND b.block_index <= ?
                ORDER BY b.block_index ASC`;
        } else {
            query = `SELECT
                    b.block_index,
                    b.block_time,
                    t1.hash as ledger_hash,
                    t2.hash as actions_hash,
                    t3.hash as contract_hash
                FROM
                    blocks b
                    LEFT JOIN index_transactions t1 ON (t1.id=b.ledger_hash_id)
                    LEFT JOIN index_transactions t2 ON (t2.id=b.actions_hash_id)
                    LEFT JOIN index_transactions t3 ON (t3.id=b.contract_hash_id)
                WHERE
                    b.block_index >= ? AND b.block_index <= ?
                ORDER BY b.block_index ASC`;
        }
        return await this.doQuery(query, [startBlock, endBlock]);
    },

};
