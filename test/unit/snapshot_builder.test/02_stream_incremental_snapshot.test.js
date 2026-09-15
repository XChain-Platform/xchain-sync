// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const {
    assert,
    sinon,
    PassThrough,
    Readable,
    EventEmitter,
    zlib,
    SnapshotBuilder,
    SnapshotStreamWriter,
    Utility,
    poolSizing,
    createMockDb,
    createMockRes
} = require('./helpers/support');

let builder;

function prepareSnapshotBuilder(){
    const util = new Utility();
    builder = new SnapshotBuilder(util);
    sinon.stub(console, 'error');
}

function restoreSnapshotBuilder(){
    sinon.restore();
}

function streamIncrementalSnapshotStatusTests(){
    it('returns 404 when no blocks after sinceBlock', async function(){
        let db = createMockDb();
        db.getLastBlock.resolves(50);
        let res = createMockRes();
        await builder.streamIncrementalSnapshot(db, 100, res);
        assert.strictEqual(res.status.calledWith(404), true);
    });

    it('returns 404 when no blocks at all', async function(){
        let db = createMockDb();
        db.getLastBlock.resolves(null);
        let res = createMockRes();
        await builder.streamIncrementalSnapshot(db, 10, res);
        assert.strictEqual(res.status.calledWith(404), true);
    });

    it('streams incremental data with since_block field', async function(){
        let db = createMockDb();
        db.getLastBlock.resolves(100);
        db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
        db.getFirstActionIndex.resolves(500);
        db.doQuery.callsFake(async (query, args) => {
            if(query.includes('information_schema'))
                return [{ table_name: 'blocks' }];
            if(query.includes('SELECT * FROM'))
                return [{ block_index: 90 }];
            return [];
        });

        let res = new PassThrough();
        let chunks = [];
        res.on('data', c => chunks.push(c));
        res.setHeader = sinon.stub();

        await new Promise((resolve) => {
            res.on('finish', resolve);
            builder.streamIncrementalSnapshot(db, 80, res);
        });

        let json = zlib.gunzipSync(Buffer.concat(chunks)).toString();
        let parsed = JSON.parse(json);
        assert.strictEqual(parsed.block_height, 100);
        assert.strictEqual(parsed.since_block, 80);
    });
}

function streamIncrementalSnapshotDecoderTests(){

    // Regression: events has no block_index/tx_index cursor, so a decoder
    // incremental snapshot must re-dump it in full. Keeping events in decoderSkip
    // omits it incrementally and lets a caught-up follower silently drift behind
    // the source.
    it('decoder: re-dumps the events table in full on incremental', async function(){
        let db = createMockDb('decoder_db');
        db.dbType = 'decoder';
        db.getLastBlock.resolves(100);
        db.getBlockHashRow.resolves({ block_hash: 'h' });
        db.doQuery.callsFake(async (query) => {
            if(query.includes('information_schema'))
                return [{ table_name: 'events' }];
            // events uses a full dump streamed by id cursor in pages (a single
            // unbounded SELECT * would OOM on a large table). 2 rows < pageSize, so
            // one page then the loop stops.
            if(/SELECT \* FROM `events` WHERE `id` > \? ORDER BY `id` ASC LIMIT \?/.test(query))
                return [{ id: 1, code: 'x', data: 'y' }, { id: 2, code: 'z', data: 'w' }];
            return [];
        });

        let res = new PassThrough();
        let chunks = [];
        res.on('data', c => chunks.push(c));
        res.setHeader = sinon.stub();

        await new Promise((resolve) => {
            res.on('finish', resolve);
            builder.streamIncrementalSnapshot(db, 80, res);
        });

        let parsed = JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
        assert.ok(parsed.tables.events, 'events table present in incremental snapshot');
        assert.strictEqual(parsed.tables.events.length, 2, 'all events rows re-dumped');
    });
}

function streamIncrementalSnapshotLookupTests(){

    // Regression: indexer index_* lookup tables have no block_index/action_index
    // cursor. Sending them through the action_index branch throws on the missing
    // column and skips them; an incremental gap-heal leaves the
    // follower short on index rows (row-count + ledger-hash mismatch). They must be
    // re-dumped in full (the client applies them with INSERT IGNORE).
    it('indexer: re-dumps index_* lookup tables in full on incremental', async function(){
        let db = createMockDb();   // dbType defaults to indexer
        db.getLastBlock.resolves(100);
        db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
        db.getFirstActionIndex.resolves(500);
        db.doQuery.callsFake(async (query) => {
            if(query.includes('information_schema'))
                return [{ table_name: 'index_addresses' }];
            // index_* lookup tables use full dumps streamed by id cursor in
            // pages. If the code instead tried the action_index branch the query
            // would carry a `WHERE action_index >=` and this would not match, so the
            // table would come back empty and the assert below would fail, exactly
            // the bug being guarded against.
            if(/SELECT \* FROM `index_addresses` WHERE `id` > \? ORDER BY `id` ASC LIMIT \?/.test(query))
                return [{ id: 1, address: 'a1' }, { id: 2, address: 'a2' }, { id: 3, address: 'a3' }];
            return [];
        });

        let res = new PassThrough();
        let chunks = [];
        res.on('data', c => chunks.push(c));
        res.setHeader = sinon.stub();

        await new Promise((resolve) => {
            res.on('finish', resolve);
            builder.streamIncrementalSnapshot(db, 80, res);
        });

        let parsed = JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
        assert.ok(parsed.tables.index_addresses, 'index_addresses present in incremental snapshot');
        assert.strictEqual(parsed.tables.index_addresses.length, 3, 'all index_addresses rows re-dumped');
    });
}

function streamIncrementalSnapshotEventTests(){

    // Regression: the indexer `events` audit log has no block_index/action_index
    // cursor. Sending it through the action_index branch raises errno 1054 on the
    // missing column, and the error is swallowed; an incrementally-caught-up
    // follower freezes its events table at bootstrap height (silent, since events is
    // replication:'snapshot' and outside the /status count). It must be re-dumped in
    // full (the client applies it with INSERT IGNORE). Mirrors the decoder events
    // fix above. The re-dump is PAGED by id cursor: events is append-only on an
    // AUTO_INCREMENT id PK, while a bundled `SELECT * FROM events` materializes the
    // whole audit log on every catch-up.
    it('indexer: re-dumps the events audit log in full on incremental, paged by id', async function(){
        let db = createMockDb();   // dbType defaults to indexer
        db.getLastBlock.resolves(100);
        db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
        db.getFirstActionIndex.resolves(500);
        let unbounded = 0;
        db.doQuery.callsFake(async (query) => {
            if(query.includes('information_schema'))
                return [{ table_name: 'events' }];
            if(/SELECT \* FROM `events`$/.test(query)){ unbounded++; return []; }
            // events takes the id-cursor pager, NOT the bundled full-dump and NOT the
            // action_index branch (which would carry `WHERE action_index >=`, match
            // nothing here, and leave the table empty: the frozen-at-bootstrap bug
            // this case has always guarded). 2 rows < pageSize, so one page and stop.
            if(/SELECT \* FROM `events` WHERE `id` > \? ORDER BY `id` ASC LIMIT \?/.test(query))
                return [{ id: 1, event: 'REORG', data: 'x' }, { id: 2, event: 'REORG', data: 'y' }];
            return [];
        });

        let res = new PassThrough();
        let chunks = [];
        res.on('data', c => chunks.push(c));
        res.setHeader = sinon.stub();

        await new Promise((resolve) => {
            res.on('finish', resolve);
            builder.streamIncrementalSnapshot(db, 80, res);
        });

        let parsed = JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
        assert.ok(parsed.tables.events, 'events audit log present in incremental snapshot');
        assert.strictEqual(parsed.tables.events.length, 2, 'all events rows re-dumped');
        assert.strictEqual(unbounded, 0, 'the unbounded full-table events SELECT is never issued');
    });
}

function streamIncrementalSnapshotFilterTests(){

    // skipLookups: a truncated/fast-chain replica syncs the append-only `.index`
    // lookup tables out of band (paged), so this response must OMIT them while
    // still streaming the block-scoped data the window needs.
    it('skipLookups omits the .index lookup tables but keeps block-scoped data', async function(){
        let db = createMockDb();
        db.getLastBlock.resolves(100);
        db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
        db.getFirstActionIndex.resolves(500);
        db.doQuery.callsFake(async (query) => {
            if(query.includes('information_schema'))
                return [{ table_name: 'index_addresses' }, { table_name: 'blocks' }];
            if(/SELECT \* FROM `index_addresses`/.test(query))
                return [{ id: 1, address: 'a1' }];
            if(/SELECT \* FROM `blocks`/.test(query))
                return [{ block_index: 90 }];
            return [];
        });

        let res = new PassThrough();
        let chunks = [];
        res.on('data', c => chunks.push(c));
        res.setHeader = sinon.stub();

        await new Promise((resolve) => {
            res.on('finish', resolve);
            builder.streamIncrementalSnapshot(db, 80, res, 'BTC', { skipLookups: true });
        });

        let parsed = JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
        assert.strictEqual(parsed.tables.index_addresses, undefined, 'lookup table omitted under skipLookups');
        assert.ok(parsed.tables.blocks, 'block-scoped table still streamed');
    });
}

function snapshotBuilderTests(){
    beforeEach(prepareSnapshotBuilder);
    afterEach(restoreSnapshotBuilder);
    describe('streamIncrementalSnapshot', streamIncrementalSnapshotStatusTests);
    describe('streamIncrementalSnapshot', streamIncrementalSnapshotDecoderTests);
    describe('streamIncrementalSnapshot', streamIncrementalSnapshotLookupTests);
    describe('streamIncrementalSnapshot', streamIncrementalSnapshotEventTests);
    describe('streamIncrementalSnapshot', streamIncrementalSnapshotFilterTests);
}

describe('SnapshotBuilder', snapshotBuilderTests);
