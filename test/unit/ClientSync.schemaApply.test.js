// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md.

// _fetchAndApplySchema multi-pass + fail-closed halt. The old single-pass catch
// swallowed every DDL error: an FK-ordering miss self-healed across bootstrap
// re-routes, but a genuine fault (permissions, disk, lock, malformed DDL) left
// the table uncreated and the next snapshot apply looped forever on errno
// 1146/1054 with halted:false (no signal). The fix runs an ordering fixpoint
// (so ordering misses resolve in one bootstrap) and then records a DURABLE halt
// for any table still failing.

const assert = require('assert');
const sinon  = require('sinon');
const axios  = require('axios');
const ClientSync = require('../../src/ClientSync');
const Utility = require('../../src/utility');
const HashVerifier = require('../../src/HashVerifier');

function createMockDb(){
    return {
        dbName: 'test_db',
        dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        addMissingColumns: sinon.stub().resolves(),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        getActiveHalt: sinon.stub().resolves(null),
        clearHalt: sinon.stub().resolves(1)
    };
}

function makeSync(db){
    const config = { SYNC_SOURCES: 'http://a:3006', VERIFY_HASHES: true };
    const applier = { applyBlock: sinon.stub().resolves() };
    return new ClientSync('bitcoin', 'mainnet', db, applier,
        { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
}

// A doQuery fake that models real table creation: information_schema lookups
// report whether a table has been created yet; CREATE statements succeed or
// throw per the supplied policy and, on success, mark the table created.
function installDoQuery(db, policy){
    const created = new Set();
    db.doQuery = sinon.stub().callsFake(async (sql, params) => {
        if(/information_schema\.tables/.test(sql)){
            return created.has(params[1]) ? [{ ok: 1 }] : [];
        }
        const m = sql.match(/CREATE TABLE `([A-Za-z0-9_]+)`/);
        if(m){
            const table = m[1];
            const verdict = policy(table, created); // 'ok' | Error
            if(verdict === 'ok'){ created.add(table); return []; }
            throw verdict;
        }
        return [];
    });
    return created;
}

describe('ClientSync._fetchAndApplySchema multi-pass + fail-closed halt', function(){
    let db, sync;
    beforeEach(function(){
        db = createMockDb();
        sync = makeSync(db);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('resolves FK ordering across passes without halting', async function(){
        // B is listed before A but depends on A, so B fails until A exists.
        sinon.stub(axios, 'get').resolves({ data: { tables: {
            B: 'CREATE TABLE `B` (id INT)',
            A: 'CREATE TABLE `A` (id INT)'
        }}});
        const created = installDoQuery(db, (table, have) => {
            if(table === 'B' && !have.has('A')){ const e = new Error('FK'); e.errno = 1215; return e; }
            return 'ok';
        });

        await sync._fetchAndApplySchema('http://a:3006');

        assert.ok(created.has('A') && created.has('B'), 'both tables created');
        assert.strictEqual(sync.isHalted(), false, 'ordering miss must not halt');
        assert.ok(db.recordHalt.notCalled, 'no durable halt for an ordering miss');
    });

    it('records a durable halt when a genuine DDL fault survives the fixpoint', async function(){
        sinon.stub(axios, 'get').resolves({ data: { tables: {
            C: 'CREATE TABLE `C` (id INT)'
        }}});
        installDoQuery(db, () => { const e = new Error('access denied'); e.errno = 1142; return e; });

        await sync._fetchAndApplySchema('http://a:3006');

        assert.strictEqual(sync.isHalted(), true, 'genuine fault must halt');
        const info = sync.getHaltInfo();
        assert.strictEqual(info.reason, 'schema-apply-failed');
        assert.deepStrictEqual(info.mismatches, [{ table: 'C', errno: 1142, message: 'access denied' }]);
        assert.ok(db.recordHalt.calledOnce, 'halt persisted via recordHalt');
        assert.strictEqual(db.recordHalt.firstCall.args[2], 'schema-apply-failed');
    });

    it('does NOT halt on a schema-fetch transport failure (not a DDL fault)', async function(){
        sinon.stub(axios, 'get').rejects(new Error('ECONNREFUSED'));
        await sync._fetchAndApplySchema('http://a:3006');
        assert.strictEqual(sync.isHalted(), false, 'a fetch failure is left to the retry loop');
        assert.ok(db.recordHalt.notCalled);
    });

    it('aborts the bootstrap round once a schema halt is recorded', async function(){
        sinon.stub(axios, 'get').resolves({ data: { tables: {
            C: 'CREATE TABLE `C` (id INT)'
        }}});
        installDoQuery(db, () => { const e = new Error('disk full'); e.errno = 1021; return e; });
        sync.sources = ['http://a:3006'];

        const ok = await sync._bootstrapRotateSources();
        assert.strictEqual(ok, false, 'round returns false (does not proceed to snapshot download)');
        assert.strictEqual(sync.isHalted(), true);
    });
});
