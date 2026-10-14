// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Every route that writes snapshot rows must encode a Buffer column as the base64
// sentinel. A Buffer that skips the codec serializes as {"type":"Buffer",...} and
// lands on the replica as corrupt bytes, so each writer gets a binary fixture here.

const {
    assert,
    sinon,
    PassThrough,
    zlib,
    SnapshotBuilder,
    Utility,
    createMockDb
} = require('./helpers/support');
const { decodeValue, BINARY_TAG } = require('../../../src/util/wire_codec');

// Bytes that are not valid UTF-8, so a lossy text round trip cannot pass as equal.
const SRC = Buffer.from([0x00, 0xff, 0x10, 0x80, 0x7f]);

let builder;

// Assert the wire text carries SRC as the sentinel and decodes back to its bytes.
function assertBinaryWire(body, wireValue){
    assert.ok(body.includes('"' + BINARY_TAG + '"'), 'the wire carries the binary sentinel');
    assert.ok(!body.includes('"type":"Buffer"'), 'no Buffer skipped the codec');
    assert.deepStrictEqual(wireValue, { [BINARY_TAG]: SRC.toString('base64') });
    let decoded = decodeValue(wireValue);
    assert.ok(Buffer.isBuffer(decoded) && decoded.equals(SRC), 'the sentinel decodes to the source bytes');
}

// Collect what a SnapshotStreamWriter would receive, as one string.
function createFakeWriter(){
    return { chunks: [], async write(s){ this.chunks.push(s); }, text(){ return this.chunks.join(''); } };
}

// Run one gzip-streaming route into a PassThrough and return the gunzipped body text.
async function streamBody(run){
    let res = new PassThrough();
    let chunks = [];
    res.on('data', c => chunks.push(c));
    res.setHeader = sinon.stub();
    await new Promise((resolve) => {
        res.on('finish', resolve);
        run(res);
    });
    return zlib.gunzipSync(Buffer.concat(chunks)).toString();
}

function httpRouteTests(){
    it('encodes a Buffer column on the id-cursor lookup page route', async function(){
        let db = createMockDb();
        db.doQuery.resolves([{ id: 11, raw: SRC }]);
        let body = await streamBody(res => builder.streamTableRowsById(db, 'index_transactions', 10, 50000, res));
        assertBinaryWire(body, JSON.parse(body).rows[0].raw);
    });

    it('encodes a Buffer column on the decoder dispensers route', async function(){
        let db = createMockDb();
        db.dbType = 'decoder';
        db.doQueryStrict.resolves([{ tx_index: 5, address_id: 9, raw: SRC }]);
        let body = await streamBody(res => builder.streamDispensers(db, NaN, NaN, res));
        assertBinaryWire(body, JSON.parse(body).rows[0].raw);
    });
}

function snapshotBodyWriterTests(){
    it('encodes a Buffer column in the paged lookup body writer', async function(){
        let db = createMockDb();
        db.findLookupPageAfter = sinon.stub();
        db.findLookupPageAfter.onFirstCall().resolves([{ id: 1, address: 'a1', raw: SRC }]);
        db.findLookupPageAfter.resolves([]);
        builder.pageSize = 2;
        let writer = createFakeWriter();
        await builder.streamLookupPaged(writer, db, 'index_addresses', null, true);
        let body = '{' + writer.text() + '}';
        assertBinaryWire(body, JSON.parse(body).index_addresses[0].raw);
    });

    it('encodes a Buffer column in the bundled incremental table writer', async function(){
        let writer = createFakeWriter();
        await builder.writeIncrementalTableRows(writer, 'gated_files', [{ id: 1, raw_data: SRC }], true);
        let body = '{' + writer.text() + '}';
        assertBinaryWire(body, JSON.parse(body).gated_files[0].raw_data);
    });
}

function snapshotBuilderTests(){
    beforeEach(function(){
        builder = new SnapshotBuilder(new Utility());
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });
    describe('binary wire sentinel on the HTTP row routes', httpRouteTests);
    describe('binary wire sentinel in the snapshot body writers', snapshotBodyWriterTests);
}

describe('SnapshotBuilder', snapshotBuilderTests);
