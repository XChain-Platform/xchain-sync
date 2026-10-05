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
 * XChain Indexer Sync - Snapshot Builder
 *
 * Builds full and incremental JSON snapshots from the indexer database.
 * Snapshots are streamed with gzip compression to avoid OOM on large DBs.
 *
 * The stream writer, table classification, full snapshot, incremental
 * snapshot and table streaming live under ./snapshot_builder/; the
 * incremental per-table row selection stays here because the forward-parity
 * guards read this file for the collectors it must call.
 *
 ********************************************************************/

const poolSizing = require('../db/pool_sizing');
const envConfig = require('../config');
const replicatedTables = require('../schema/replicated_tables');
const tableLifecycle = require('../table_lifecycle');
const { collectMaturedCooldownCredits, collectMaturedCooldownEscrows, mergeMaturedRows } = require('./cooldown_credits');
const { collectRedrivenValidatorRewards } = require('./recovery_rewards');
const { collectDerivedAnchorRewards } = require('./derived_rewards');
const { SnapshotStreamWriter } = require('./snapshot_builder/stream_writer');
const {
    OPERATOR_LOCAL_TABLES,
    SOURCE_UNSTREAMED_TABLES,
    PRIORITY_TABLES,
    TRAILING_TABLES,
    orderSnapshotTables,
    decoderIncrementalSets,
} = require('./snapshot_builder/table_sets');
const fullSnapshot = require('./snapshot_builder/full_snapshot');
const incrementalSnapshot = require('./snapshot_builder/incremental_snapshot');
const tableStreaming = require('./snapshot_builder/table_streaming');

// The dedup key is the FULL five-column identity: the archive leg's
// round_reference (MATCH_BATCH_SEQ) is a dense hub counter a rebase
// reissues, so two distinct archive rewards can share the four older
// columns and the narrower key silently drops one from the payload.
function rewardKey(r){
    return r.source_id + ':' + r.signing_pubkey_id + ':' + r.reward_type + ':' + r.round_reference + ':' + r.round_qualifier;
}

function appendNewRewardRows(rows, extra){
    let seen = new Set(rows.map(rewardKey));
    for(let r of extra){
        let k = rewardKey(r);
        if(!seen.has(k)){ seen.add(k); rows.push(r); }
    }
}

class SnapshotBuilder {

    constructor(util) {
        this.util = util;

        this.priorityTables = PRIORITY_TABLES;
        this.trailingTables = TRAILING_TABLES;

        this.pageSize = 10000;

        // In-flight long-lived snapshot streams per Database instance.
        // ServerPoller and the /snapshot routes share ONE Database (pool sized
        // per dbType, see poolSizing.js) per chain:network:dbType, and each full or
        // incremental snapshot pins a pool connection for the whole stream
        // (beginReadSnapshot). The rate limiter is per-IP only, so N validators
        // bootstrapping at once (a flag-day cohort) could pin every connection
        // and starve the poller's ~3s getLastBlock acquire, stalling live block
        // broadcast. This semaphore caps concurrent streams per Database so at
        // least one connection is always free for the poller; excess requests
        // get 503 + Retry-After and the client retries.
        this._inflightSnapshots = new Map();
    }

    // Per-Database cap on concurrent long-lived snapshot streams. Default is
    // poolSize - 2 (one connection reserved for ServerPoller, one for other
    // short reads like /status). MAX_CONCURRENT_SNAPSHOTS overrides, but is
    // always clamped to [1, poolSize - 1] so no configuration can hand the
    // poller's last connection to a snapshot stampede.
    snapshotCap(db){
        let poolSize = (db && db.connectionPoolParams && db.connectionPoolParams.connectionLimit)
            || poolSizing.resolvePoolSize(db && db.dbType);
        let cap = envConfig.maxConcurrentSnapshotsFromEnv();
        if(!Number.isFinite(cap)) cap = poolSize - 2;
        return Math.max(1, Math.min(cap, poolSize - 1));
    }

    // Try to reserve a snapshot-stream slot for this Database. On saturation,
    // answers the request itself with 503 + Retry-After (fail fast rather than
    // queue: a queued acquire would still pin the caller and hide the overload
    // from the client's retry logic) and returns false.
    acquireSnapshotSlot(db, res){
        let inflight = this._inflightSnapshots.get(db) || 0;
        if(inflight >= this.snapshotCap(db)){
            this.snapshotsRejected = (this.snapshotsRejected || 0) + 1;
            res.setHeader('Retry-After', '30');
            res.status(503).json({
                error: 'Too many concurrent snapshot streams; retry later',
                code: 'SNAPSHOT_BUSY'
            });
            return false;
        }
        this._inflightSnapshots.set(db, inflight + 1);
        return true;
    }

    releaseSnapshotSlot(db){
        let inflight = this._inflightSnapshots.get(db) || 0;
        if(inflight <= 1) this._inflightSnapshots.delete(db);
        else this._inflightSnapshots.set(db, inflight - 1);
    }

    // Discover all tables in the database and return them in dependency order.
    // Priority tables come first, trailing tables last, everything else alphabetically in between.
    async getOrderedTables(db, conn){
        let rows = await db.findStreamableTableNames(conn);
        let allTables = rows.map(r => r.table_name || r.TABLE_NAME)
            .filter(t => !OPERATOR_LOCAL_TABLES.has(t) && !SOURCE_UNSTREAMED_TABLES.has(t));
        return orderSnapshotTables(allTables);
    }

    // Resolve one table's rows for an incremental snapshot. Returns null when
    // the table is skipped for this window. `ctx` carries the per-request
    // scoping sets built by streamIncrementalSnapshotLocked.
    async selectIncrementalRows(db, table, ctx){
        const { dbType, sinceBlock, lastBlock, conn } = ctx;
        let rows = (dbType === 'decoder')
            ? await this.selectDecoderRows(db, table, ctx)
            : await this.selectIndexerRows(db, table, ctx);
        if(rows === null) return null;

        if(dbType === 'indexer' && (table === 'credits' || table === 'escrows')){
            rows = await this.mergeMaturedCooldownRows(db, table, rows, sinceBlock, lastBlock, conn);
        }
        if(dbType === 'indexer' && table === 'validator_rewards'){
            rows = await this.mergeValidatorRewardRows(db, rows, sinceBlock, lastBlock, conn);
        }
        return rows;
    }

    async selectDecoderRows(db, table, ctx){
        const { sinceBlock, conn, skipLookups, lookupSet, decoderSkip, decoderBlockScoped,
                decoderTxScoped, decoderFullDump } = ctx;
        if(decoderSkip.has(table)) return null;
        if(decoderBlockScoped.has(table)) return db.findRowsFromBlockIndex(table, sinceBlock, conn);
        if(decoderTxScoped.has(table)) return db.findTxScopedRowsFromBlock(table, sinceBlock, conn);
        if(decoderFullDump.has(table)){
            if(skipLookups && lookupSet.has(table)) return null;
            return db.findAllRows(table, conn);
        }
        return null;
    }

    async selectIndexerRows(db, table, ctx){
        const { sinceBlock, conn, skipLookups, lookupSet, firstActionIndex,
                indexerBlockScoped, indexerFullDump } = ctx;
        if(indexerBlockScoped.has(table)){
            // Scope by the registry's blockKey, never the literal: a table
            // keyed by another column raises errno 1054, which this loop's
            // catch tolerates as an older source schema and skips forever.
            let key = tableLifecycle.blockKey(table);
            return db.findRowsFromBlockKey(table, key, sinceBlock, conn);
        }
        if(indexerFullDump.has(table)){
            if(skipLookups && lookupSet.has(table)) return null;
            return db.findAllRows(table, conn);
        }
        if(table === 'contract_emissions') return this.selectEmissionRows(db, sinceBlock, conn);
        if(firstActionIndex !== null) return this.selectActionScopedRows(db, table, firstActionIndex, conn);
        if(table === 'credits' || table === 'escrows'){
            // firstActionIndex is null: a catch-up window with zero actions.
            // The action-scoped base query would be empty, but the matured-
            // cooldown merge below keys off the maturity block, not
            // action_index, and a legacy-era cooldown maturity mints NO
            // actions row (ClientRollback moves its reverse cooldown delete
            // OUTSIDE this same firstActionIndex guard for exactly this
            // reason). Fall through with an empty base so the merge still
            // ships the backdated refund credit and its escrow release; without
            // this the follower receives the updated_rows status flip but not
            // the ledger rows and its balances silently diverge over quiet windows.
            return [];
        }
        return null;
    }

    // contract_emissions.action_index is NULL for INTERNAL emissions
    // (SLASH and friends, which move ledger state without minting an
    // on-wire action), so the generic `action_index >= ?` branch silently
    // drops every one of them from a catch-up window. The consensus
    // contract_hash counts them (BlockHasher reaches them through the
    // execution_index chain), and interior blocks arrive with their committed
    // hashes verbatim, so nothing halts: the follower just carries a short
    // table and any later recompute of an interior block diverges. Reach them
    // the same way the live stream does (db.getEmissionRowsForBlock), widened
    // from one block to the catch-up window.
    //
    // Block-scoped, not action-scoped, so it is correct with a null
    // firstActionIndex too, and it names the four protocol columns rather
    // than `em.*`: the AUTO_INCREMENT id is local to each node (the live
    // stream never carries it, so the follower's sequence is already offset
    // from the source's), and shipping the source's id into a plain INSERT is
    // the ER_DUP_ENTRY freeze that ClientApplier.localSurrogateIdTables
    // documents.
    async selectEmissionRows(db, sinceBlock, conn){
        try {
            return await db.findEmissionRowsFromBlock(sinceBlock, conn);
        } catch(e){
            // Swallow ONLY a genuine schema gap on an older source (1146
            // missing table / 1054 missing column), propagate anything transient.
            if(e && e.errno !== 1146 && e.errno !== 1054) throw e;
            return null;
        }
    }

    async selectActionScopedRows(db, table, firstActionIndex, conn){
        try {
            return await db.findRowsFromActionIndex(table, firstActionIndex, conn);
        } catch(e){
            // Swallow ONLY a genuine schema gap (1146 missing table /
            // 1054 missing action_index column on an older source);
            // skip the table for incremental. A transient/operational
            // error (deadlock 1213, lock-wait 1205, connection drop)
            // must propagate so the stream aborts rather than silently
            // omitting the table's window (matches ClientRollback).
            if(e && e.errno !== 1146 && e.errno !== 1054) throw e;
            return null;
        }
    }

    // Cooldown-maturity refund credits reuse the unstake's earlier-block
    // action_index (and carry no block_index), so they sit below the
    // action_index cursor and are missed. Merge them by maturity
    // block over the same [sinceBlock, lastBlock] window the in-place
    // updated_rows channel uses, deduped on the credit's logical identity
    // (the forward mirror of ClientRollback's reverse cooldown delete). An
    // unstake created AND matured inside this window can be reached both
    // here and by the action_index scope, hence the dedup. The escrow
    // release paired with each refund shares its action_index and merges
    // into escrows the same way.
    async mergeMaturedCooldownRows(db, table, rows, sinceBlock, lastBlock, conn){
        try {
            let matured = (table === 'credits')
                ? await collectMaturedCooldownCredits(db, sinceBlock, lastBlock, conn)
                : await collectMaturedCooldownEscrows(db, sinceBlock, lastBlock, conn);
            return mergeMaturedRows(rows, matured);
        } catch(e){
            // Swallow ONLY a genuine schema gap (1146 missing table / 1054
            // missing column on an older source). A transient/operational
            // error must abort the stream, not silently drop the matured
            // credits from the catch-up payload (mirrors ClientRollback).
            if(e && e.errno !== 1146 && e.errno !== 1054) throw e;
            return rows;
        }
    }

    // Recovery-redriven validator rewards: a reorg re-drain re-materializes
    // a survivor at block_index = earn-block E < B, so the block_index >=
    // sinceBlock scope misses it. Merge by applied_block over the same
    // [sinceBlock, lastBlock] window, deduped on the row's UNIQUE identity
    // (the forward analogue of ClientRollback's reverse block_index delete).
    //
    // Derived anchor/archive rewards: materialized (derive_block_index)
    // inside [sinceBlock, lastBlock] but stamped block_index = the
    // checkpoint's SNAPSHOT_BLOCK E below the cursor, so the block_index
    // >= sinceBlock scope misses them unless the gap already spans
    // back to E. Same merge + dedup; the forward twin of ClientRollback's
    // derive_block_index >= B reverse delete.
    //
    // Swallow ONLY a genuine schema gap (1146 missing table / 1054 missing
    // column on an older source); a transient/operational error must abort
    // the stream, not silently drop the rewards from the catch-up payload.
    async mergeValidatorRewardRows(db, rows, sinceBlock, lastBlock, conn){
        const channels = [
            () => collectRedrivenValidatorRewards(db, sinceBlock, lastBlock, conn),
            () => collectDerivedAnchorRewards(db, sinceBlock, lastBlock, conn),
        ];
        for(let collect of channels){
            try {
                let extra = await collect();
                if(extra.length > 0){
                    rows = rows || [];
                    appendNewRewardRows(rows, extra);
                }
            } catch(e){
                if(e && e.errno !== 1146 && e.errno !== 1054) throw e;
            }
        }
        return rows;
    }

    static get ROWS_PAGE_DEFAULT(){ return tableStreaming.ROWS_PAGE_DEFAULT; }
    static get ROWS_PAGE_MAX(){ return tableStreaming.ROWS_PAGE_MAX; }

}

Object.assign(SnapshotBuilder.prototype, fullSnapshot, incrementalSnapshot, tableStreaming.methods);

// Hung on the class rather than on module.exports so the file has ONE export
// shape. Every call site reads the same property off the same object either way.
SnapshotBuilder.SnapshotStreamWriter = SnapshotStreamWriter;
SnapshotBuilder.OPERATOR_LOCAL_TABLES = OPERATOR_LOCAL_TABLES;
SnapshotBuilder.SOURCE_UNSTREAMED_TABLES = SOURCE_UNSTREAMED_TABLES;
SnapshotBuilder.orderSnapshotTables = orderSnapshotTables;
SnapshotBuilder.decoderIncrementalSets = decoderIncrementalSets;

module.exports = SnapshotBuilder;
