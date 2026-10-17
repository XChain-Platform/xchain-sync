// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// A binary column crosses the wire as a base64 sentinel object, and the INSERT
// path decodes it back to a Buffer. These pin the natural-key lookups built from
// the same rows decoding it too, so a binary key never binds the sentinel object.

const assert = require('assert');
const sinon  = require('sinon');
const ClientApplier = require('../../../src/client/applier');
const Utility = require('../../../src/util');
const { withDbMixins } = require('../../helpers/db_mixins.js');

const KEY_BYTES = Buffer.from([0xde, 0xad, 0xbe, 0xef]);

// The wire form of KEY_BYTES, as the snapshot builder encodes a Buffer column.
function wireKey(){
    return { __xbin__: KEY_BYTES.toString('base64') };
}

// Assert a bound key value is the decoded Buffer, not the sentinel object.
function assertDecodedKey(v){
    assert.ok(Buffer.isBuffer(v), 'binds a Buffer, not the wire sentinel: ' + JSON.stringify(v));
    assert.ok(v.equals(KEY_BYTES), 'binds the original key bytes');
}

function createApplier(){
    const db = withDbMixins({
        doQuery: sinon.stub().resolves([]),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves(),
    });
    db.findRowIdsByKeyColumns = sinon.stub().resolves([]);
    db.findRowIdById = sinon.stub().resolves([]);
    db.deleteRowById = sinon.stub().resolves();
    return { db, applier: new ClientApplier(db, new Utility()) };
}

function naturalKeyDeleteTests(){
    it('decodes a binary natural key and keeps one value per distinct key', function(){
        const { applier } = createApplier();
        applier.localSurrogateIdTables.set('zz_binary_keyed', 'k');
        const rows = [{ id: 1, k: wireKey(), x: 'a' }, { id: 2, k: wireKey(), x: 'b' }];
        const prepared = applier.prepareNaturalKeyRows('zz_binary_keyed', rows, ['id', 'k', 'x']);
        assert.strictEqual(prepared.naturalKey, 'k');
        assert.strictEqual(prepared.keyValues.length, 1, 'two rows with the same key bytes dedupe to one');
        assertDecodedKey(prepared.keyValues[0]);
    });

    it('passes a plain natural key through unchanged', function(){
        const { applier } = createApplier();
        const rows = [{ id: 1, block_index: 7 }, { id: 2, block_index: 7 }, { id: 3, block_index: 8 }];
        const prepared = applier.prepareNaturalKeyRows('blocks', rows, ['id', 'block_index']);
        assert.deepStrictEqual(prepared.keyValues, [7, 8]);
    });

    it('still refuses a row whose natural key is missing', function(){
        const { applier } = createApplier();
        assert.throws(() => applier.prepareNaturalKeyRows('blocks', [{ id: 1 }], ['id', 'x']),
            /missing its natural key block_index/);
    });
}

function lookupRepairTests(){
    it('decodes a binary natural key before the lookup repair probe', async function(){
        const { db, applier } = createApplier();
        db.findRowIdsByKeyColumns.resolves([{ id: 9 }]);
        await applier.reconcileLookupRows('index_statuses', [{ id: 9, k: wireKey() }], ['k']);
        assert.strictEqual(db.findRowIdsByKeyColumns.callCount, 1);
        assertDecodedKey(db.findRowIdsByKeyColumns.firstCall.args[2][0]);
        assert.strictEqual(db.deleteRowById.callCount, 0, 'an already-converged pair writes nothing');
    });

    it('passes a plain lookup key through unchanged', async function(){
        const { db, applier } = createApplier();
        db.findRowIdsByKeyColumns.resolves([{ id: 9 }]);
        await applier.reconcileLookupRows('index_statuses', [{ id: 9, status: 'open' }], ['status']);
        assert.deepStrictEqual(db.findRowIdsByKeyColumns.firstCall.args[2], ['open']);
    });
}

function staleGenerationTests(){
    const warning = [{ Code: 1062, Message: "Duplicate entry 'x' for key 'uk_k'" }];
    const pageRows = [{ id: 3 }, { id: 5 }, { id: 6 }];

    it('decodes a binary unique key and retires the stale holder inside the page window', async function(){
        const { db, applier } = createApplier();
        sinon.stub(applier, 'uniqueKeyColumns').resolves(['k']);
        db.findRowIdsByKeyColumns.resolves([{ id: 4 }]);
        const retired = await applier.retireStaleNaturalKeyRows('index_statuses',
            [{ id: 5, k: wireKey() }], pageRows, warning);
        assertDecodedKey(db.findRowIdsByKeyColumns.firstCall.args[2][0]);
        assert.deepStrictEqual(retired, [4]);
        assert.ok(db.deleteRowById.calledOnceWith('index_statuses', 4));
    });

    it('passes a plain unique key through unchanged', async function(){
        const { db, applier } = createApplier();
        sinon.stub(applier, 'uniqueKeyColumns').resolves(['status']);
        db.findRowIdsByKeyColumns.resolves([{ id: 4 }]);
        const retired = await applier.retireStaleNaturalKeyRows('index_statuses',
            [{ id: 5, status: 'open' }], pageRows, warning);
        assert.deepStrictEqual(db.findRowIdsByKeyColumns.firstCall.args[2], ['open']);
        assert.deepStrictEqual(retired, [4]);
    });
}

describe('ClientApplier binary natural keys', function(){
    afterEach(function(){ sinon.restore(); });
    describe('natural-key delete set', naturalKeyDeleteTests);
    describe('lookup repair probe', lookupRepairTests);
    describe('stale lookup generation retire', staleGenerationTests);
});
