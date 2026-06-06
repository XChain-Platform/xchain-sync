// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available —
// contact legal@dankest.llc.

const assert    = require('assert');
const zlib      = require('zlib');
const { PassThrough } = require('stream');
const WebSocket = require('ws');

const setup    = require('./helpers/setup');
const testDb   = require('./helpers/testDb');
const fixtures = require('./helpers/fixtures');

const SnapshotBuilder  = require('../../src/SnapshotBuilder');
const ClientApplier    = require('../../src/ClientApplier');
const BlockBroadcaster = require('../../src/BlockBroadcaster');
const { BINARY_TAG }   = require('../../src/wireCodec');

// Binary payload chosen to break a naive toString() round-trip: contains a NUL,
// 0xFF, and bytes that are not valid UTF-8.
const RAW_HEX = '00ff8042deadbeef13fe7f';
const RAW_BUF = Buffer.from(RAW_HEX, 'hex');

// gated_files (indexer, MEDIUMBLOB raw_data) is the live-observed corruption
// site. Its raw_data exercises the exact same encode/decode path as the
// decoder's transactions.raw_data, so covering it here covers both columns.
async function seedGatedFile(db, actionIndex){
    await db.doQuery(
        "INSERT INTO gated_files (action_index, gate_ticker, encryption_method, key_hash, status_id, raw_data) " +
        "VALUES (?, ?, ?, ?, ?, UNHEX(?))",
        [actionIndex, 'GATETOK', 1, 'ab'.repeat(32), null, RAW_HEX]
    );
}

async function readRawData(db, actionIndex){
    let rows = await db.doQuery("SELECT raw_data FROM gated_files WHERE action_index = ?", [actionIndex]);
    return rows.length > 0 ? rows[0].raw_data : null;
}

// Capture a full snapshot stream into a parsed JSON object (gunzip the gzip body
// SnapshotBuilder pipes to its res).
async function captureFullSnapshot(builder, db){
    let chunks = [];
    let res = new PassThrough();
    res.setHeader = () => {};
    res.status    = () => ({ json: () => {} });
    res.on('data', c => chunks.push(c));
    let ended = new Promise(resolve => res.on('end', resolve));
    await builder.streamFullSnapshot(db, res);
    await ended;
    let body = zlib.gunzipSync(Buffer.concat(chunks)).toString('utf8');
    return { json: JSON.parse(body), body };
}

describe('Integration: binary column replication (F2)', function(){

    let sourceDb, replicaDb;

    before(async function(){
        await setup.globalSetup();
    });

    after(async function(){
        await setup.globalTeardown();
    });

    beforeEach(async function(){
        await setup.resetDatabases();
        sourceDb  = setup.getSourceDb();
        replicaDb = setup.getReplicaDb();
    });

    it('source stores true binary (sanity — seed not corrupted by the helper)', async function(){
        await fixtures.seedBlocks(sourceDb, 1, 1);
        await seedGatedFile(sourceDb, 100);
        let src = await readRawData(sourceDb, 100);
        assert.ok(Buffer.isBuffer(src), 'source raw_data should be a Buffer');
        assert.ok(src.equals(RAW_BUF), 'source bytes must match the seed');
    });

    it('full snapshot round-trips a blob byte-for-byte', async function(){
        await fixtures.seedBlocks(sourceDb, 1, 1);
        await seedGatedFile(sourceDb, 100);

        let builder = new SnapshotBuilder(testDb.util);
        let { json, body } = await captureFullSnapshot(builder, sourceDb);

        // Wire format: base64 sentinel, NOT the mangled {"type":"Buffer"} shape.
        assert.ok(body.includes(BINARY_TAG), 'wire must carry the binary sentinel');
        assert.ok(!body.includes('"type":"Buffer"'), 'wire must not contain a mangled Buffer object');

        let applier = new ClientApplier(replicaDb, testDb.util);
        await applier.applyFullSnapshot(json);

        let dst = await readRawData(replicaDb, 100);
        assert.ok(Buffer.isBuffer(dst), 'replica raw_data should be a Buffer');
        assert.ok(dst.equals(RAW_BUF), 'replica bytes must equal source bytes');
    });

    it('live block broadcast round-trips a blob byte-for-byte', async function(){
        // Build the live block payload exactly as ServerPoller would, then push it
        // through the real BlockBroadcaster to capture the serialized wire message.
        let event = {
            type: 'block', chain: 'bitcoin', network: 'mainnet', dbType: 'indexer',
            block_index: 1, block_time: 100,
            data: {
                gated_files: [
                    { action_index: 200, gate_ticker: 'GATETOK', encryption_method: 1,
                      key_hash: 'cd'.repeat(32), status_id: null, raw_data: Buffer.from(RAW_HEX, 'hex') }
                ]
            }
        };

        let captured = [];
        let broadcaster = new BlockBroadcaster({ WS_MAX_PER_IP: 100, WS_BACKPRESSURE_LIMIT: 1000, TRUST_PROXY: false });
        let fakeWs = { readyState: WebSocket.OPEN, bufferedAmount: 0, send: (d) => captured.push(d) };
        broadcaster.subscribers.set('bitcoin:mainnet:indexer', new Set([fakeWs]));

        broadcaster.broadcast('bitcoin', 'mainnet', event, new Set());

        assert.strictEqual(captured.length, 1, 'broadcaster should have sent one message');
        assert.ok(captured[0].includes(BINARY_TAG), 'wire must carry the binary sentinel');
        assert.ok(!captured[0].includes('"type":"Buffer"'), 'wire must not contain a mangled Buffer object');

        let payload = JSON.parse(captured[0]);
        let applier = new ClientApplier(replicaDb, testDb.util);
        await applier.applyBlock(payload);

        let dst = await readRawData(replicaDb, 200);
        assert.ok(Buffer.isBuffer(dst), 'replica raw_data should be a Buffer');
        assert.ok(dst.equals(RAW_BUF), 'replica bytes must equal source bytes');
    });
});
