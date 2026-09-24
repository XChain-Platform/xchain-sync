// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// An upsert full-dump (markets) that keeps failing on the same duplicate key must
// end in a durable, visible halt rather than a catch-up that re-fails forever while
// the replica reports halted:false.

const {
    assert, sinon, axios, registerClientSyncHooks
} = require('./support');

let sync, db, applier;

function assignState(state){
    ({ sync, db, applier } = state);
}

function dupKey(entry, table){
    let msg = "Duplicate entry '" + entry + "' for key 'PRIMARY'";
    return Object.assign(new Error(msg), { errno: 1062, sqlMessage: msg, upsertTable: table || 'markets' });
}

function serveSnapshot(){
    db.getLastBlock.resolves(5);
    db.recordHalt = sinon.stub().resolves({ block_index: 5 });
    sinon.stub(console, 'warn');
    let gz = require('zlib').gzipSync(JSON.stringify({ schema_version: 'x', block_height: 9, tables: {} }));
    sinon.stub(axios, 'get').resolves({ data: gz });
}

function registerHaltCases(){
    it('halts durably when the same upsert duplicate key fails twice in a row', async function(){
        serveSnapshot();
        applier.applyIncrementalSnapshot.rejects(dupKey('42'));

        await sync.runIncrementalCatchUp();
        assert.strictEqual(sync.isHalted(), false, 'the first hit retries');
        await sync.runIncrementalCatchUp();

        assert.strictEqual(sync.isHalted(), true);
        let info = sync.getHaltInfo();
        assert.strictEqual(info.reason, 'apply-duplicate-key');
        assert.deepStrictEqual(info.mismatches, [{ table: 'markets', entry: '42', key: 'PRIMARY' }]);
        assert.strictEqual(db.recordHalt.callCount, 1);
        assert.strictEqual(db.recordHalt.firstCall.args[2], 'apply-duplicate-key');
    });

    it('refuses further catch-ups once halted', async function(){
        serveSnapshot();
        applier.applyIncrementalSnapshot.rejects(dupKey('42'));
        await sync.runIncrementalCatchUp();
        await sync.runIncrementalCatchUp();

        await sync.incrementalCatchUp(6);
        assert.strictEqual(applier.applyIncrementalSnapshot.callCount, 2);
    });

    it('stays halted in memory when persisting the halt fails', async function(){
        serveSnapshot();
        db.recordHalt.rejects(new Error('db down'));
        applier.applyIncrementalSnapshot.rejects(dupKey('42'));
        await sync.runIncrementalCatchUp();
        await sync.runIncrementalCatchUp();
        assert.strictEqual(sync.getHaltInfo().reason, 'apply-duplicate-key');
    });
}

function registerNoHaltCases(){
    it('does not halt on a single duplicate key', async function(){
        serveSnapshot();
        applier.applyIncrementalSnapshot.rejects(dupKey('42'));
        await sync.runIncrementalCatchUp();
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(db.recordHalt.called, false);
    });

    it('does not halt when two failures name different keys', async function(){
        serveSnapshot();
        applier.applyIncrementalSnapshot
            .onFirstCall().rejects(dupKey('42'))
            .onSecondCall().rejects(dupKey('43'));
        await sync.runIncrementalCatchUp();
        await sync.runIncrementalCatchUp();
        assert.strictEqual(sync.isHalted(), false);
    });

    it('resets on a successful apply between two identical failures', async function(){
        serveSnapshot();
        applier.applyIncrementalSnapshot
            .onFirstCall().rejects(dupKey('42'))
            .onSecondCall().resolves()
            .onThirdCall().rejects(dupKey('42'));
        await sync.runIncrementalCatchUp();
        await sync.runIncrementalCatchUp();
        await sync.runIncrementalCatchUp();
        assert.strictEqual(sync.isHalted(), false);
    });

    it('does not halt on a repeated duplicate key from a non-upsert insert', async function(){
        serveSnapshot();
        let plain = Object.assign(new Error("Duplicate entry '7' for key 'PRIMARY'"), { errno: 1062 });
        applier.applyIncrementalSnapshot.rejects(plain);
        await sync.runIncrementalCatchUp();
        await sync.runIncrementalCatchUp();
        assert.strictEqual(sync.isHalted(), false);
    });

    it('still routes a schema gap to the heal path', async function(){
        serveSnapshot();
        applier.applyIncrementalSnapshot
            .onFirstCall().rejects(Object.assign(new Error('no table'), { errno: 1146 }))
            .onSecondCall().resolves();
        let heal = sinon.stub(sync, 'fetchAndApplySchema').resolves();
        await sync.runIncrementalCatchUp();
        assert.strictEqual(heal.calledOnce, true);
        assert.strictEqual(sync.isHalted(), false);
    });
}

describe('ClientSync', function(){
    registerClientSyncHooks(assignState);
    describe('runIncrementalCatchUp upsert duplicate-key halt', function(){
        registerHaltCases();
        registerNoHaltCases();
    });
});
