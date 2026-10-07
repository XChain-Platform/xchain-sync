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
 * XChain Sync - Table Streaming
 * SnapshotBuilder mixin: paged and single-table streams (lookup paging, id-cursor
 * pages, the decoder dispensers dump). Installed on SnapshotBuilder.prototype.
 *
 ********************************************************************/

const zlib = require('zlib');
const { SCHEMA_VERSION } = require('../../schema/version');
const { encodeRow, bigIntReplacer } = require('../../util/wire_codec');
const replicatedTables = require('../../schema/replicated_tables');
const { SnapshotStreamWriter } = require('./stream_writer');

// Default + ceiling page sizes for streamTableRowsById. The ceiling bounds the
// server's single-query result set and the client's per-page buffer so no one
// request can approach SNAPSHOT_MAX_CONTENT, which is the whole point.
const ROWS_PAGE_DEFAULT = 50000;
const ROWS_PAGE_MAX = 100000;

const REISSUING_LOOKUPS = new Set(['index_addresses', 'index_tickers']);

// The optional max_block query bound: ids in these tables are minted in block order
// and reissued on a reorg, so a page never carries a row from above the caller's tip.
function parseMaxBlock(res){
    let raw = res && res.req && res.req.query ? res.req.query.max_block : undefined;
    if(raw === undefined || raw === null || raw === '') return null;
    let n = parseInt(raw, 10);
    return Number.isInteger(n) && n >= 0 ? n : null;
}

const methods = {

    // Stream one append-only id-PK lookup table into the snapshot body by id cursor
    // in pageSize batches, so a multi-million-row table never lands in the driver
    // array (or gzip buffer) all at once. `first` tracks whether any table key has
    // been written yet (for the inter-table comma); returns the updated value.
    // Uses the shared REPEATABLE READ conn, so paging is consistent across batches.
    // `cursorCol` overrides the lookup cursor for a table paged only in-band.
    async streamLookupPaged(writer, db, table, conn, first, cursorCol){
        let col = cursorCol || replicatedTables.lookupCursorColumn(table);
        let after = 0;
        let wrote = false;
        let firstRow = true;
        while(true){
            let page = await db.findLookupPageAfter(table, col, after, this.pageSize, conn);
            if(!page.length) break;
            if(!wrote){
                if(!first) await writer.write(',');
                first = false;
                await writer.write('"' + table + '":[');
                wrote = true;
            }
            for(let r of page){
                if(!firstRow) await writer.write(',');
                firstRow = false;
                await writer.write(JSON.stringify(encodeRow(r), bigIntReplacer));
            }
            after = Number(page[page.length - 1][col]);
            if(page.length < this.pageSize) break;
        }
        if(wrote) await writer.write(']');
        return first;
    },

    // Stream one id-ordered page of an append-only lookup table to an HTTP response.
    // Powers the truncated/fast-chain replica's out-of-band lookup sync: instead of
    // a single full-dump of a multi-million-row table (which exceeds
    // SNAPSHOT_MAX_CONTENT and aborts the client), the client pages by id cursor and
    // applies each bounded page. Only the append-only id-PK lookup tables (the
    // topology `.index` set: index_*, plus pubkeys/events for the decoder) are
    // pageable; they are INSERT-only with a monotonic AUTO_INCREMENT `id`, so
    // `id > cursor ORDER BY id` is stable across requests without a long-lived read
    // view. (Decoder pubkeys also pages by this surrogate `id`: its PK address_id is
    // non-monotonic w.r.t. insert order, so it would skip rows; see
    // replicatedTables.lookupCursorColumn and the pubkeys monotonic-id migration.)
    // Returns {schema_version, table, max_id, has_more, rows:[...]}.
    async streamTableRowsById(db, table, afterId, limit, res){
        let dbType = (db && db.dbType) || 'indexer';
        let allowed = new Set(replicatedTables.getTopology(dbType).index);
        if(!allowed.has(table)){
            // Not an allowlisted append-only lookup table. Refuse rather than run an
            // arbitrary SELECT * (the allowlist is also the SQL-identifier guard:
            // only these hardcoded names are ever interpolated into the query).
            return res.status(400).json({ error: 'Table not pageable: ' + table });
        }
        let after = Number.isFinite(afterId) ? Math.max(0, Math.floor(afterId)) : 0;
        let lim   = Number.isFinite(limit) ? Math.floor(limit) : ROWS_PAGE_DEFAULT;
        lim = Math.max(1, Math.min(ROWS_PAGE_MAX, lim));

        // Per-table cursor column. All pageable lookup tables (including decoder
        // pubkeys, via its surrogate monotonic `id`) page by `id`; address_id is
        // non-monotonic w.r.t. insert order and must NOT be used as the cursor.
        let col = replicatedTables.lookupCursorColumn(table);
        let rows = await db.findLookupPageAfter(table, col, after, lim);
        let hasMore = rows.length === lim;
        let maxBlock = REISSUING_LOOKUPS.has(table) ? parseMaxBlock(res) : null;
        if(maxBlock != null){
            let cut = rows.findIndex(r => r.block_index != null && Number(r.block_index) > maxBlock);
            if(cut !== -1){
                rows = rows.slice(0, cut);
                hasMore = false;
            }
        }
        let maxId   = rows.length ? Number(rows[rows.length - 1][col]) : after;
        let schemaVersion = SCHEMA_VERSION[dbType];

        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Content-Encoding', 'gzip');
        res.setHeader('X-Snapshot-Schema-Version', schemaVersion);
        res.setHeader('X-Max-Id', String(maxId));
        res.setHeader('X-Has-More', hasMore ? 'true' : 'false');

        let gzip = zlib.createGzip();
        gzip.pipe(res);
        // Backpressure + client-abort teardown, same as the snapshot streams. No read
        // view is held here (the doQuery connection is released before streaming), but
        // the writer still bounds gzip's buffered output on a slow reader and stops
        // writing to a socket the client already closed.
        let writer = new SnapshotStreamWriter(gzip, res);
        try {
            await writer.write('{"schema_version":' + schemaVersion + ',"table":"' + table +
                '","max_id":' + maxId + ',"has_more":' + (hasMore ? 'true' : 'false') + ',"rows":[');
            for(let i = 0; i < rows.length; i++){
                if(i > 0) await writer.write(',');
                await writer.write(JSON.stringify(encodeRow(rows[i]), bigIntReplacer));
            }
            await writer.write(']}');
            writer.finish();
        } catch(e){
            writer.dispose();
            if(e && e.aborted) return;
            throw e;
        }
    },

    // Stream the whole decoder `dispensers` table, keyset-ordered. dispensers
    // is excluded from both the incremental block stream and the id-cursor lookup
    // paging (streamTableRowsById): it has no monotonic surrogate id (PK is
    // (tx_index, address_id)) and the decoder soft-expires then hard-purges rows, so
    // an insert-only delta cannot replay the UPDATE/DELETE convergence. A truncated
    // replica therefore never seeds it and an incrementally-caught-up replica drifts.
    // This endpoint powers the client's periodic replace-table reconcile
    // (ClientSync.reconcileDispensers): it serves the FULL current table in ONE
    // response so the client rebuilds it from a single point-in-time image.
    // Decoder-only. Returns {schema_version, max_tx, max_addr, has_more, rows:[...]}.
    //
    // Deliberately NOT paged: the keyset-cursor stability rationale
    // (borrowed from streamTableRowsById) only holds for INSERT-only tables, and
    // dispensers is exactly the table it does not hold for - the decoder
    // soft-expires rows in place (UPDATE expired_block_index), so a soft-expire
    // landing on an already-served page after that page shipped left the client's
    // assembled walk carrying a torn cross-instant image, which
    // applyDispensersReplace then wrote in as authoritative. The count backstop
    // (verifyTableCounts) structurally cannot see it: an UPDATE leaves counts
    // equal on both sides. A single SELECT is statement-consistent in InnoDB, so
    // one response can never mix instants; dispensers is small next to the
    // multi-million-row lookups that forced paging onto the id-cursor rail. The
    // cursor params stay honoured (filtered within the same single query) and
    // has_more is always false, so an old paging client simply completes its walk
    // in one round trip.
    async streamDispensers(db, afterTx, afterAddr, res){
        let dbType = (db && db.dbType) || 'indexer';
        if(dbType !== 'decoder'){
            return res.status(400).json({ error: 'dispensers reconcile is decoder-only' });
        }

        // Read STRICTLY: this dump is authoritative for the whole follower table.
        // doQuery is fail-soft outside a transaction (a query error becomes []), which
        // is indistinguishable here from a genuinely empty table, so a transient
        // source-DB fault would ship a 200 dump of zero rows; the follower's
        // applyDispensersReplace then runs DELETE with no insert and stamps the
        // reconcile a success, wiping its table. doQueryStrict throws instead, the
        // route handler turns it into a 500, and the follower's reconcile catch leaves
        // the local rows intact.
        let rows;
        if(Number.isFinite(afterTx) && Number.isFinite(afterAddr)){
            rows = await db.findDispensersAfter(afterTx, afterAddr);
        } else {
            rows = await db.findAllDispensers();
        }
        let hasMore = false;
        let maxTx   = rows.length ? Number(rows[rows.length - 1].tx_index)   : (Number.isFinite(afterTx)   ? afterTx   : 0);
        let maxAddr = rows.length ? Number(rows[rows.length - 1].address_id) : (Number.isFinite(afterAddr) ? afterAddr : 0);
        let schemaVersion = SCHEMA_VERSION[dbType];

        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Content-Encoding', 'gzip');
        res.setHeader('X-Snapshot-Schema-Version', schemaVersion);
        res.setHeader('X-Has-More', hasMore ? 'true' : 'false');

        let gzip = zlib.createGzip();
        gzip.pipe(res);
        // Backpressure + client-abort teardown (see streamTableRowsById).
        let writer = new SnapshotStreamWriter(gzip, res);
        try {
            await writer.write('{"schema_version":' + schemaVersion + ',"max_tx":' + maxTx +
                ',"max_addr":' + maxAddr + ',"has_more":' + (hasMore ? 'true' : 'false') + ',"rows":[');
            for(let i = 0; i < rows.length; i++){
                if(i > 0) await writer.write(',');
                await writer.write(JSON.stringify(encodeRow(rows[i]), bigIntReplacer));
            }
            await writer.write(']}');
            writer.finish();
        } catch(e){
            writer.dispose();
            if(e && e.aborted) return;
            throw e;
        }
    },

};

module.exports = { methods, ROWS_PAGE_DEFAULT, ROWS_PAGE_MAX };
