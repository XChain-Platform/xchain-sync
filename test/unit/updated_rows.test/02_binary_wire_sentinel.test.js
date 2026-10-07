// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.
//
// Binary columns on the in-place updated-rows channel, through every apply entry point.

'use strict';

const assert = require('assert');
const sinon  = require('sinon');
const ClientApplier = require('../../../src/client/applier');
const { SCHEMA_VERSION } = require('../../../src/schema/version');
const Utility = require('../../../src/util');
const { encodeTables } = require('../../../src/util/wire_codec');
const { fakeDb } = require('./helpers/fake_db.js');

// A binary column on the updated_rows channel arrives as the __xbin__ sentinel and
// must reach the write as the source's bytes, never as an object ("[object Object]").
describe('ClientApplier in-place updated-rows apply: binary columns round-trip the wire sentinel', function(){
    let db, applier;
    beforeEach(function(){
        db = fakeDb([]);
        applier = new ClientApplier(db, new Utility());
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(() => sinon.restore());

    const SOURCE_BYTES = Buffer.from([0x00, 0xff, 0x10, 0x80, 0x7f]);
    const wireRows = () => JSON.parse(JSON.stringify(encodeTables({
        attests: [{ action_index: 7, request_status: 'pending', payload: SOURCE_BYTES }]
    }))).attests;

    // Find the attests upsert and read each argument by its column position.
    function upsertArgs(){
        let q = db.calls.find(c => c.sql.indexOf('ON DUPLICATE KEY UPDATE') !== -1 && c.sql.indexOf('`attests`') !== -1);
        assert.ok(q, 'expected an attests upsert');
        let cols = q.sql.slice(q.sql.indexOf('(') + 1, q.sql.indexOf(')')).split(',').map(s => s.trim().replace(/`/g, ''));
        let byCol = {};
        cols.forEach((c, i) => { byCol[c] = q.args[i]; });
        return { byCol, args: q.args };
    }
    function assertSourceBytes(){
        let { byCol, args } = upsertArgs();
        assert.ok(Buffer.isBuffer(byCol.payload), 'payload must be decoded to a Buffer');
        assert.ok(byCol.payload.equals(SOURCE_BYTES), 'payload bytes must equal the source bytes');
        assert.strictEqual(byCol.action_index, 7);
        assert.strictEqual(byCol.request_status, 'pending');
        assert.ok(!args.some(a => a && typeof a === 'object' && !Buffer.isBuffer(a) && '__xbin__' in a),
            'no argument may still carry the wire sentinel');
    }

    it('upsertRows decodes the sentinel to the source bytes', async function(){
        await applier.upsertRows('attests', wireRows());
        assertSourceBytes();
    });

    it('applyBlock decodes the sentinel in payload.updated_rows', async function(){
        await applier.applyBlock({
            schema_version: SCHEMA_VERSION.indexer,
            block_index: 9,
            data: { blocks: [{ block_index: 9 }] },
            updated_rows: { attests: wireRows() }
        });
        assertSourceBytes();
        assert.strictEqual(db.commitTransaction.calledOnce, true);
    });

    it('applyIncrementalSnapshot decodes the sentinel in its updated_rows', async function(){
        await applier.applyIncrementalSnapshot({
            schema_version: SCHEMA_VERSION.indexer,
            since_block: 5,
            tables: { blocks: [{ block_index: 9 }] },
            updated_rows: { attests: wireRows() }
        });
        assertSourceBytes();
    });
});
