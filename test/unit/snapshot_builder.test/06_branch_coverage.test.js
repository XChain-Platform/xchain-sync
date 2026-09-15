// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const { assert, sinon, PassThrough, Readable, zlib, SnapshotBuilder, Utility, createMockDb, createMockRes } = require('./helpers/support');
let builder;
function prepareSnapshotBuilder(){
    builder = new SnapshotBuilder(new Utility());
    sinon.stub(console, 'error');
}
function restoreSnapshotBuilder(){ sinon.restore(); }
// gzip.pipe(res) needs a REAL writable stream (removeListener etc.), so use a
// genuine PassThrough with a setHeader stub + chunk collection; the plain
// createMockRes() object only works on the early-return (404) paths.
function streamRes(){
    let res = new PassThrough();
    let headers = {};
    res.setHeader = (k, v) => { headers[k] = v; };
    res.status = sinon.stub().returnsThis();
    res.json = sinon.stub();
    res._headers = headers;
    let chunks = [];
    res.on('data', c => chunks.push(c));
    res.getCollectedData = () => Buffer.concat(chunks);
    return res;
}

// Attach the finish listener BEFORE starting the stream, then resolve once
// gzip.end() flushes through the PassThrough (mirrors the pattern above).
function run(start, res){ return new Promise(r => { res.on('finish', r); start(); }); }

function branchGetOrderedTablesTests(){
    it('drops operator-local tables and tolerates the uppercase TABLE_NAME variant', async function(){
        let db = createMockDb();
        db.doQuery.resolves([
            { TABLE_NAME: 'icons' },             // operator-local → dropped
            { table_name: 'price_snapshots' },   // operator-local → dropped
            { table_name: 'pending_hub_pushes' },// operator-local → dropped
            { TABLE_NAME: 'middle_upper' },      // uppercase fallback, middle
            { table_name: 'blocks' }             // priority
        ]);
        let ordered = await builder.getOrderedTables(db);
        assert.ok(!ordered.includes('icons'));
        assert.ok(!ordered.includes('price_snapshots'));
        assert.ok(!ordered.includes('pending_hub_pushes'));
        assert.ok(ordered.includes('middle_upper'));
        assert.strictEqual(ordered[0], 'blocks');
    });
}

function branchFullSnapshotPayloadTests(){
    it('writes empty hash headers when the hashRow lacks fields, comma-joins tables/rows, and serializes BigInt', async function(){
        let db = createMockDb();
        db.getLastBlock.resolves(100);
        db.getBlockHashRow.resolves({}); // present but no ledger/actions/contract fields → '' fallbacks
        db.doQuery.resolves([{ table_name: 'blocks' }, { table_name: 'actions' }]);
        db.getTableCount.resolves(2);
        // Two rows (exercise the inter-row comma) incl. a BigInt (exercise bigIntReplacer).
        db.streamTableRows.callsFake(() => Readable.from([{ id: 1n, n: 'a' }, { id: 2n, n: 'b' }]));
        let res = streamRes();
        await run(() => builder.streamFullSnapshot(db, res), res);
        assert.strictEqual(res._headers['X-Ledger-Hash'], '');
        assert.strictEqual(res._headers['X-Actions-Hash'], '');
        assert.strictEqual(res._headers['X-Contract-Hash'], '');
        assert.ok(db.commitReadSnapshot.calledOnce);
        let out = JSON.parse(zlib.gunzipSync(res.getCollectedData()).toString());
        assert.deepStrictEqual(Object.keys(out.tables), ['blocks', 'actions']);
        assert.strictEqual(out.tables.blocks[0].id, '1'); // BigInt → string
    });

    it('skips zero-count tables without failing the snapshot', async function(){
        let db = createMockDb();
        db.getLastBlock.resolves(10);
        db.getBlockHashRow.resolves(null);
        db.doQuery.resolves([{ table_name: 'blocks' }, { table_name: 'actions' }]);
        db.getTableCount.withArgs('blocks').resolves(0);   // legitimately empty → omitted
        db.getTableCount.withArgs('actions').resolves(1);
        db.streamTableRows.callsFake(() => Readable.from([{ id: 1 }]));
        let res = streamRes();
        await run(() => builder.streamFullSnapshot(db, res), res);
        let out = JSON.parse(zlib.gunzipSync(res.getCollectedData()).toString());
        assert.deepStrictEqual(Object.keys(out.tables), ['actions'],
            'a zero-row table is still omitted; only real read errors abort');
        assert.ok(db.commitReadSnapshot.calledOnce);
    });
}

function branchFullSnapshotFailureTests(){

    // Catching and logging 'Error reading table ...' while continuing lets a
    // COUNT(*) lock-wait/timeout publish syntactically valid JSON
    // with that table simply absent while still advertising block_height at the tip.
    // ClientApplier.applyFullSnapshot DELETEs every snapshot-eligible local table and
    // re-inserts only what the payload carries, so a populated table reached the
    // replica EMPTY and the replica advanced to the tip; a single-source deployment
    // runs no post-apply content check to catch it. A partial snapshot must never be
    // published, so the whole read fails.
    it('aborts the whole snapshot on a per-table read error rather than omitting the table', async function(){
        let db = createMockDb();
        db.getLastBlock.resolves(10);
        db.getBlockHashRow.resolves(null);
        db.doQuery.resolves([{ table_name: 'blocks' }, { table_name: 'actions' }]);
        db.getTableCount.withArgs('blocks').resolves(1);
        db.getTableCount.withArgs('actions').rejects(new Error('Lock wait timeout exceeded'));
        db.streamTableRows.callsFake(() => Readable.from([{ id: 1 }]));
        let res = streamRes();
        await assert.rejects(() => builder.streamFullSnapshot(db, res),
            { message: 'Lock wait timeout exceeded' });
        assert.strictEqual(db.commitReadSnapshot.called, false,
            'a partial snapshot must never be committed/closed');
        assert.ok(db.rollbackReadSnapshot.calledOnce, 'the read view is released by rollback');
        // The closing '}}' is never emitted, so the client's JSON.parse of the
        // truncated download throws and the bootstrap retries instead of committing.
        assert.ok(!/\}\}\s*$/.test(res.getCollectedData().toString('binary')));
    });

    it('still returns quietly on a client-disconnect abort (not treated as a read error)', async function(){
        let db = createMockDb();
        db.getLastBlock.resolves(10);
        db.getBlockHashRow.resolves(null);
        db.doQuery.resolves([{ table_name: 'blocks' }]);
        db.getTableCount.withArgs('blocks').rejects(Object.assign(new Error('gone'), { aborted: true }));
        let res = streamRes();
        await builder.streamFullSnapshot(db, res); // returns, does not throw
        assert.strictEqual(db.commitReadSnapshot.called, false);
        assert.ok(db.rollbackReadSnapshot.calledOnce);
    });

    it('rolls back and rethrows when the snapshot read throws', async function(){
        let db = createMockDb();
        db.getLastBlock.rejects(new Error('read fail'));
        let res = createMockRes();
        await assert.rejects(() => builder.streamFullSnapshot(db, res), { message: 'read fail' });
        assert.ok(db.rollbackReadSnapshot.calledOnce);
    });
}

function branchIncrementalSnapshotScopeTests(){
    it('decoder: emits X-Block-Hash and scopes skip/block/tx/full-dump tables correctly', async function(){
        let db = createMockDb();
        db.dbType = 'decoder';
        db.getLastBlock.resolves(100);
        db.getBlockHashRow.resolves({ block_hash: 'BH' });
        db.doQuery.callsFake(async (sql) => {
            if(/information_schema/.test(sql))
                return [
                    { table_name: 'mempool_transactions' }, // decoderSkip
                    { table_name: 'blocks' },               // decoderBlockScoped
                    { table_name: 'transaction_outputs' },  // decoderTxScoped
                    { table_name: 'pubkeys' },              // decoderFullDump
                    { table_name: 'random_other' }          // else → continue
                ];
            if(/`blocks`/.test(sql)) return [{ block_index: 5 }];
            if(/`transaction_outputs`/.test(sql)) return [{ tx_index: 1 }];
            if(/`pubkeys`/.test(sql)) return [{ address_id: 1 }];
            return [];
        });
        let res = streamRes();
        await run(() => builder.streamIncrementalSnapshot(db, 3, res), res);
        assert.strictEqual(res._headers['X-Block-Hash'], 'BH');
        let out = JSON.parse(zlib.gunzipSync(res.getCollectedData()).toString());
        assert.deepStrictEqual(Object.keys(out.tables).sort(), ['blocks', 'pubkeys', 'transaction_outputs']);
        assert.ok(!('mempool_transactions' in out.tables));
        assert.ok(!('random_other' in out.tables));
    });

    it('indexer: emits empty hash headers, dumps full + action-scoped tables, and comma-joins them', async function(){
        let db = createMockDb();
        db.dbType = 'indexer';
        db.getLastBlock.resolves(100);
        db.getBlockHashRow.resolves({}); // missing fields → '' fallbacks
        db.getFirstActionIndex.resolves(500);
        db.doQuery.callsFake(async (sql) => {
            if(/information_schema/.test(sql))
                return [{ table_name: 'blocks' }, { table_name: 'index_actions' },
                        { table_name: 'sends' }, { table_name: 'no_action_col' }];
            if(/`blocks`/.test(sql)) return [{ block_index: 7 }];          // block-scoped
            if(/`index_actions`/.test(sql)) return [{ id: 1 }];            // full dump
            if(/`sends`/.test(sql) && /action_index/.test(sql)) return [{ action_index: 501 }]; // action-scoped
            // A table reached via the action_index branch that has no such column:
            // the inner try/catch swallows the genuine schema gap (errno 1054) and
            // skips the table (a transient error would instead re-throw / fail closed).
            if(/`no_action_col`/.test(sql)){ let e = new Error('Unknown column action_index'); e.errno = 1054; throw e; }
            return [];
        });
        let res = streamRes();
        await run(() => builder.streamIncrementalSnapshot(db, 3, res), res);
        assert.strictEqual(res._headers['X-Ledger-Hash'], '');
        assert.strictEqual(res._headers['X-Actions-Hash'], '');
        assert.strictEqual(res._headers['X-Contract-Hash'], '');
        let out = JSON.parse(zlib.gunzipSync(res.getCollectedData()).toString());
        assert.ok('blocks' in out.tables && 'index_actions' in out.tables && 'sends' in out.tables);
        assert.ok(!('no_action_col' in out.tables), 'action_index-less table is skipped');
    });
}

function branchIncrementalSnapshotEmissionTests(){

    it('indexer: skips middle tables when there is no firstActionIndex (no actions since the cursor)', async function(){
        let db = createMockDb();
        db.dbType = 'indexer';
        db.getLastBlock.resolves(100);
        db.getBlockHashRow.resolves(null);
        db.getFirstActionIndex.resolves(null); // no actions at/after sinceBlock → else-continue
        db.doQuery.callsFake(async (sql) => {
            if(/information_schema/.test(sql))
                return [{ table_name: 'blocks' }, { table_name: 'sends' }];
            if(/`blocks`/.test(sql)) return [{ block_index: 7 }]; // block-scoped still emitted
            return [];
        });
        let res = streamRes();
        await run(() => builder.streamIncrementalSnapshot(db, 3, res), res);
        let out = JSON.parse(zlib.gunzipSync(res.getCollectedData()).toString());
        assert.ok('blocks' in out.tables);
        assert.ok(!('sends' in out.tables), 'action-scoped table skipped when firstActionIndex is null');
    });

    // contract_emissions.action_index is NULL for INTERNAL emissions (SLASH), so
    // the generic `action_index >= ?` cursor drops them from a catch-up window
    // while the consensus contract_hash counts them via the execution_index
    // chain. The follower then carries a short table and only an advisory parity
    // log says so. Reach them the way the live stream does, by block.
    it('indexer: scopes contract_emissions by block through the execution_index chain, not the action_index cursor @regression', async function(){
        let db = createMockDb();
        db.dbType = 'indexer';
        db.getLastBlock.resolves(100);
        db.getBlockHashRow.resolves(null);
        db.getFirstActionIndex.resolves(500);
        let emissionSql = null, emissionArgs = null;
        db.doQuery.callsFake(async (sql, args) => {
            if(/information_schema/.test(sql)) return [{ table_name: 'contract_emissions' }];
            if(/contract_emissions/.test(sql)){
                emissionSql = sql; emissionArgs = args;
                // The internal emission the action_index cursor would have dropped.
                return [{ execution_index: 900, emitted_action: 'SLASH', action_index: null, position: 0 }];
            }
            return [];
        });
        let res = streamRes();
        await run(() => builder.streamIncrementalSnapshot(db, 3, res), res);
        let out = JSON.parse(zlib.gunzipSync(res.getCollectedData()).toString());
        assert.deepStrictEqual(out.tables.contract_emissions,
            [{ execution_index: 900, emitted_action: 'SLASH', action_index: null, position: 0 }],
            'the NULL-action internal emission must ride the catch-up payload');
        assert.ok(/contract_executions ce ON \(ce\.action_index = em\.execution_index\)/.test(emissionSql),
            'must reach the rows through the execution_index chain');
        assert.ok(/a\.block_index >= \?/.test(emissionSql), 'must be block-scoped');
        assert.ok(!/WHERE action_index >= /.test(emissionSql),
            'must not use the generic action_index cursor, which drops NULL-action rows');
        assert.deepStrictEqual(emissionArgs, [3], 'bound to sinceBlock, not firstActionIndex');
        // The AUTO_INCREMENT id is local to each node (the live stream never
        // carries it), so shipping the source's would collide on a plain INSERT.
        assert.ok(!/em\.\*/.test(emissionSql) && !/em\.id/.test(emissionSql),
            'must name the four protocol columns, never em.* (which carries the local id)');
    });
}

function branchIncrementalSnapshotGapTests(){

    // Same branch with a quiet window: block scoping makes it independent of
    // firstActionIndex, which the generic cursor branch is not.
    it('indexer: still ships internal emissions when firstActionIndex is null @regression', async function(){
        let db = createMockDb();
        db.dbType = 'indexer';
        db.getLastBlock.resolves(100);
        db.getBlockHashRow.resolves(null);
        db.getFirstActionIndex.resolves(null);
        db.doQuery.callsFake(async (sql) => {
            if(/information_schema/.test(sql)) return [{ table_name: 'contract_emissions' }];
            if(/contract_emissions/.test(sql))
                return [{ execution_index: 900, emitted_action: 'SLASH', action_index: null, position: 0 }];
            return [];
        });
        let res = streamRes();
        await run(() => builder.streamIncrementalSnapshot(db, 3, res), res);
        let out = JSON.parse(zlib.gunzipSync(res.getCollectedData()).toString());
        assert.ok(out.tables.contract_emissions && out.tables.contract_emissions.length === 1,
            'a quiet action window must not silence the emission branch');
    });

    it('swallows a per-table SCHEMA-GAP read error (errno 1146) during incremental', async function(){
        let db = createMockDb();
        db.dbType = 'indexer';
        db.getLastBlock.resolves(100);
        db.getBlockHashRow.resolves(null);
        db.getFirstActionIndex.resolves(500);
        db.doQuery.callsFake(async (sql) => {
            if(/information_schema/.test(sql)) return [{ table_name: 'blocks' }];
            let e = new Error('table missing'); e.errno = 1146; throw e;
        });
        let res = streamRes();
        await run(() => builder.streamIncrementalSnapshot(db, 3, res), res);
        assert.ok(console.error.getCalls().some(c => /Error reading table blocks for incremental/.test(c.args[0])));
        assert.ok(db.commitReadSnapshot.calledOnce);
    });
}

function branchIncrementalSnapshotFailureTests(){

    // Finding 1323: a transient/operational error (deadlock 1213, lock-wait 1205,
    // connection drop) must NOT be swallowed. Rows are fully fetched before any byte
    // is written, so swallowing it would ship a structurally-valid but silently
    // INCOMPLETE catch-up (the table's window vanishes yet the payload still parses).
    // Only genuine schema gaps (1146/1054) are tolerated; everything else fails closed.
    it('fails closed (rejects + rolls back) on a transient per-table read error during incremental @regression', async function(){
        let db = createMockDb();
        db.dbType = 'indexer';
        db.getLastBlock.resolves(100);
        db.getBlockHashRow.resolves(null);
        db.getFirstActionIndex.resolves(500);
        db.doQuery.callsFake(async (sql) => {
            if(/information_schema/.test(sql)) return [{ table_name: 'blocks' }];
            let e = new Error('Deadlock found'); e.errno = 1213; throw e;
        });
        let res = streamRes();
        await assert.rejects(() => builder.streamIncrementalSnapshot(db, 3, res), /Deadlock found/);
        assert.ok(db.rollbackReadSnapshot.calledOnce, 'read snapshot rolled back on fail-closed abort');
    });

    it('fails closed on a connection-drop (no errno) per-table read error during incremental @regression', async function(){
        let db = createMockDb();
        db.dbType = 'indexer';
        db.getLastBlock.resolves(100);
        db.getBlockHashRow.resolves(null);
        db.getFirstActionIndex.resolves(500);
        db.doQuery.callsFake(async (sql) => {
            if(/information_schema/.test(sql)) return [{ table_name: 'blocks' }];
            throw new Error('Connection lost: The server closed the connection');
        });
        let res = streamRes();
        await assert.rejects(() => builder.streamIncrementalSnapshot(db, 3, res), /Connection lost/);
        assert.ok(db.rollbackReadSnapshot.calledOnce);
    });
}

function branchIncrementalSnapshotRecoveryTests(){

    // Finding 1322: a catch-up window with zero actions (getFirstActionIndex null)
    // that contains only a legacy-era cooldown maturity mints NO actions row, so the
    // credits action-scoped base query is empty. The matured-cooldown merge keys off
    // the maturity block, not action_index, and must still run so the backdated refund
    // credit ships; otherwise the follower gets the updated_rows status flip but not
    // the credit and its balances silently diverge.
    it('ships matured cooldown refund credits when firstActionIndex is null (quiet window) @regression', async function(){
        let db = createMockDb();
        db.dbType = 'indexer';
        db.getLastBlock.resolves(100);
        db.getBlockHashRow.resolves(null);
        db.getFirstActionIndex.resolves(null); // quiet window: zero actions since cursor
        db.getStatusId = sinon.stub().resolves(3); // 'completed'
        db.doQuery.callsFake(async (sql) => {
            if(/information_schema/.test(sql)) return [{ table_name: 'credits' }];
            // Capability maturity refund join → one backdated credit maturing in-window.
            if(/JOIN unstakes u/.test(sql))
                return [{ action_index: 42, address_id: 7, tick_id: 1, amount: '100' }];
            // Contract maturity refund join → none.
            if(/JOIN contract_unstakes cu/.test(sql)) return [];
            return [];
        });
        let res = streamRes();
        await run(() => builder.streamIncrementalSnapshot(db, 3, res), res);
        let out = JSON.parse(zlib.gunzipSync(res.getCollectedData()).toString());
        assert.ok('credits' in out.tables, 'credits table present even with null firstActionIndex');
        assert.strictEqual(out.tables.credits.length, 1, 'the backdated refund credit is shipped');
        assert.strictEqual(out.tables.credits[0].action_index, 42);
    });

    it('rolls back and rethrows when the incremental read throws before streaming', async function(){
        let db = createMockDb();
        db.getLastBlock.rejects(new Error('inc read fail'));
        let res = createMockRes();
        await assert.rejects(() => builder.streamIncrementalSnapshot(db, 3, res), { message: 'inc read fail' });
        assert.ok(db.rollbackReadSnapshot.calledOnce);
    });
}

function branchCoverageTests(){
    describe('getOrderedTables', branchGetOrderedTablesTests);
    describe('streamFullSnapshot', branchFullSnapshotPayloadTests);
    describe('streamFullSnapshot', branchFullSnapshotFailureTests);
    describe('streamIncrementalSnapshot', branchIncrementalSnapshotScopeTests);
    describe('streamIncrementalSnapshot', branchIncrementalSnapshotEmissionTests);
    describe('streamIncrementalSnapshot', branchIncrementalSnapshotGapTests);
    describe('streamIncrementalSnapshot', branchIncrementalSnapshotFailureTests);
    describe('streamIncrementalSnapshot', branchIncrementalSnapshotRecoveryTests);
}

function snapshotBuilderTests(){
    beforeEach(prepareSnapshotBuilder);
    afterEach(restoreSnapshotBuilder);
    describe('branch coverage', branchCoverageTests);
}

describe('SnapshotBuilder', snapshotBuilderTests);
