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
 * XChain Sync - Full Snapshot
 * SnapshotBuilder mixin: streams a full snapshot under one REPEATABLE READ view.
 * Installed on SnapshotBuilder.prototype, so `this` is the builder.
 *
 ********************************************************************/

const zlib = require('zlib');
const { SCHEMA_VERSION } = require('../../schema/version');
const { encodeRow, bigIntReplacer } = require('../../util/wire_codec');
const { getLogger } = require('../../observability');
const { SnapshotStreamWriter } = require('./stream_writer');
const logger = getLogger();

module.exports = {

    // Stream a full snapshot to an HTTP response.
    //
    // The block-height anchor, the hash headers, and every paginated table read
    // run inside a single REPEATABLE READ snapshot (opened before getLastBlock).
    // This keeps the advertised hashes and the streamed rows consistent to one
    // block height even if the source commits new blocks mid-stream; without it
    // a busy source produces mixed-block payloads that fail validator hash
    // verification on bootstrap.
    async streamFullSnapshot(db, res){
        // Concurrency gate: reject with 503 instead of pinning yet
        // another pool connection when the per-Database stream cap is reached.
        if(!this.acquireSnapshotSlot(db, res)) return;
        try {
            await this.streamFullSnapshotLocked(db, res);
        } finally {
            this.releaseSnapshotSlot(db);
        }
    },

    setFullSnapshotHeaders(res, lastBlock, schemaVersion, hashRow){
        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Content-Encoding', 'gzip');
        res.setHeader('X-Block-Height', lastBlock);
        res.setHeader('X-Snapshot-Schema-Version', schemaVersion);
        if(hashRow){
            res.setHeader('X-Ledger-Hash', hashRow.ledger_hash || '');
            res.setHeader('X-Actions-Hash', hashRow.actions_hash || '');
            res.setHeader('X-Contract-Hash', hashRow.contract_hash || '');
        }
    },

    createFullSnapshotWriter(res){
        let gzip = zlib.createGzip();
        gzip.pipe(res);
        return new SnapshotStreamWriter(gzip, res);
    },

    async streamFullSnapshotTables(db, conn, writer){
        let tableOrder = await this.getOrderedTables(db, conn);
        let first = true;
        let totalRows = 0;
        // Any table read error aborts the snapshot before its closing frame.
        for(let table of tableOrder){
            let count = await db.getTableCount(table, conn);
            if(count === 0) continue;

            if(!first) await writer.write(',');
            first = false;
            await writer.write('"' + table + '":[');

            let firstRow = true;
            let rowStream = db.streamTableRows(table, conn);
            try {
                for await (let row of rowStream){
                    if(!firstRow) await writer.write(',');
                    firstRow = false;
                    await writer.write(JSON.stringify(encodeRow(row), bigIntReplacer));
                    totalRows++;
                }
            } finally {
                // An abort stops the driver from buffering unread rows.
                rowStream.destroy();
            }

            await writer.write(']');
        }
        return totalRows;
    },

    recordFullSnapshotSuccess(db, lastBlock, totalRows, startedAt){
        let duration = Date.now() - startedAt;
        let chain  = (db && db.chain)  || '?';
        let network = (db && db.network) || '?';
        logger.info('[SnapshotBuilder] full-snapshot served: dbType=' + (db && db.dbType) +
            ' chain=' + chain + '/' + network +
            ' block_height=' + lastBlock +
            ' rows=' + totalRows +
            ' duration=' + duration + 'ms');
        this.snapshotsServed = (this.snapshotsServed || 0) + 1;
    },

    async streamFullSnapshotLocked(db, res){
        let startedAt = Date.now();
        let conn = await db.beginReadSnapshot();
        let snapshotOpen = true;
        let writer = null;
        try {
            let lastBlock = await db.getLastBlock(conn);
            if(lastBlock === null){
                await db.commitReadSnapshot(conn);
                snapshotOpen = false;
                res.status(404).json({ error: 'No blocks in database' });
                return;
            }

            let dbType  = (db && db.dbType) || 'indexer';
            let schemaVersion = SCHEMA_VERSION[dbType];
            let hashRow = await db.getBlockHashRow(lastBlock, conn);

            this.setFullSnapshotHeaders(res, lastBlock, schemaVersion, hashRow);
            writer = this.createFullSnapshotWriter(res);

            await writer.write('{"schema_version":' + schemaVersion + ',"block_height":' + lastBlock + ',"tables":{');
            let totalRows = await this.streamFullSnapshotTables(db, conn, writer);

            // Release the read view only after the final page is read. The data
            // is already buffered into gzip, so committing before gzip.end()
            // does not race the stream flush.
            await writer.write('}}');
            await db.commitReadSnapshot(conn);
            snapshotOpen = false;

            this.recordFullSnapshotSuccess(db, lastBlock, totalRows, startedAt);
            writer.finish();
        } catch(e){
            if(writer) writer.dispose();
            if(snapshotOpen) await db.rollbackReadSnapshot(conn);
            // A client abort returns quietly after releasing the read view.
            if(e && e.aborted) return;
            // Destroying a started response makes a truncated transfer fail immediately.
            // The route handles errors that occur before headers are sent.
            if(res.headersSent && typeof res.destroy === 'function') res.destroy();
            throw e;
        }
    },

};
