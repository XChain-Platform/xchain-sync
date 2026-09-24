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

// The indexer `pubkeys` table is replication:'snapshot', so streamTopology()
// puts it in no per-block bucket. Sending it through the action_index branch
// raises errno 1054 for the missing column and is swallowed as a schema gap:
// pubkeys rides NO incremental snapshot and freezes at bootstrap
// height on every incrementally-caught-up follower, invisibly (it is excluded
// from the /status count check and is not consensus-hashed).
//
// It is full-dumped instead, paged by its address_id PRIMARY KEY inside the one
// read view: a bare SELECT * would materialize the whole table per catch-up.
const PUBKEYS = [{ address_id: 7, pubkey: 'pk7' }, { address_id: 9, pubkey: 'pk9' }, { address_id: 12, pubkey: 'pk12' }];

// Serve pubkeys by address_id page, 1054 on any action-scoped read, and log every query.
function pubkeysDb(queries){
    let db = createMockDb();
    db.dbType = 'indexer';
    db.getLastBlock.resolves(100);
    db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
    // Non-null: the action_index fallback is reachable precisely here.
    db.getFirstActionIndex.resolves(500);
    db.doQuery.callsFake(async (query, args) => {
        if(query.includes('information_schema')) return [{ table_name: 'pubkeys' }];
        queries.push(query);
        if(/SELECT \* FROM `pubkeys` WHERE `address_id` > \? ORDER BY `address_id` ASC LIMIT \?/.test(query))
            return PUBKEYS.filter(r => r.address_id > args[0]).slice(0, args[1]);
        if(query.includes('`pubkeys`') && query.includes('action_index')){
            let e = new Error("Unknown column 'action_index' in 'where clause'");
            e.errno = 1054;
            throw e;
        }
        return [];
    });
    return db;
}

// Stream one incremental snapshot and return its parsed body.
async function streamParsed(db, opts){
    let res = new PassThrough();
    let chunks = [];
    res.on('data', c => chunks.push(c));
    res.setHeader = sinon.stub();
    await new Promise((resolve) => { res.on('finish', resolve); builder.streamIncrementalSnapshot(db, 80, res, undefined, opts); });
    return JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
}

function streamIncrementalSnapshotReplicationTests(){
    it('full-dumps indexer pubkeys instead of action-scoping it into errno 1054', async function(){
        let queries = [];
        let parsed = await streamParsed(pubkeysDb(queries));
        assert.deepStrictEqual(parsed.tables.pubkeys.map(r => r.address_id), [7, 9, 12], 'pubkeys rides the incremental payload');
        assert.ok(!queries.some(q => q.includes('`pubkeys`') && q.includes('action_index')),
            'pubkeys is never action-scoped (that query 1054s and is swallowed)');
    });

    it('pages indexer pubkeys by address_id rather than one unbounded SELECT *', async function(){
        builder.pageSize = 2;
        let queries = [];
        let parsed = await streamParsed(pubkeysDb(queries));
        assert.deepStrictEqual(parsed.tables.pubkeys.map(r => r.address_id), [7, 9, 12], 'every page lands, none twice');
        assert.ok(!queries.some(q => /SELECT \* FROM `pubkeys`\s*$/.test(q.trim())), 'no unbounded read of pubkeys');
        assert.strictEqual(queries.filter(q => q.includes('`address_id` >')).length, 2, 'two pages of at most 2 rows');
    });

    it('still streams indexer pubkeys under skip_lookups, which has no out-of-band route for it', async function(){
        let parsed = await streamParsed(pubkeysDb([]), { skipLookups: true });
        assert.deepStrictEqual(parsed.tables.pubkeys.map(r => r.address_id), [7, 9, 12]);
    });
}

function snapshotBuilderTests(){
    beforeEach(prepareSnapshotBuilder);
    afterEach(restoreSnapshotBuilder);
    describe('streamIncrementalSnapshot snapshot-replication tables', streamIncrementalSnapshotReplicationTests);
}

describe('SnapshotBuilder', snapshotBuilderTests);
