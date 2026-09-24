// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert    = require('assert');
const WebSocket = require('ws');

const testDb   = require('./helpers/testDb');
const fixtures = require('./helpers/fixtures');

const SnapshotBuilder  = require('../../src/server/snapshot_builder');
const ClientApplier    = require('../../src/client/applier');
const BlockBroadcaster = require('../../src/server/block_broadcaster');
const { BINARY_TAG }   = require('../../src/util/wire_codec');
const {
    RAW_HEX, RAW_BUF, seedGatedFile, readRawData, captureFullSnapshot,
    makeBroadcastEvent, registerBinaryReplicationHooks
} = require('./binary_replication.test/helpers/binary_replication_suite');

describe('Integration: binary column replication (F2)', function(){

    const suite = registerBinaryReplicationHooks();

    it('source stores true binary (sanity check: seed not corrupted by the helper)', async function(){
        await fixtures.seedBlocks(suite.sourceDb, 1, 1);
        await seedGatedFile(suite.sourceDb, 100);
        let src = await readRawData(suite.sourceDb, 100);
        assert.ok(Buffer.isBuffer(src), 'source raw_data should be a Buffer');
        assert.ok(src.equals(RAW_BUF), 'source bytes must match the seed');
    });

    it('full snapshot round-trips a blob byte-for-byte', async function(){
        await fixtures.seedBlocks(suite.sourceDb, 1, 1);
        await seedGatedFile(suite.sourceDb, 100);

        let builder = new SnapshotBuilder(testDb.util);
        let { json, body } = await captureFullSnapshot(builder, suite.sourceDb);

        // Wire format: base64 sentinel, NOT the mangled {"type":"Buffer"} shape.
        assert.ok(body.includes(BINARY_TAG), 'wire must carry the binary sentinel');
        assert.ok(!body.includes('"type":"Buffer"'), 'wire must not contain a mangled Buffer object');

        let applier = new ClientApplier(suite.replicaDb, testDb.util);
        await applier.applyFullSnapshot(json);

        let dst = await readRawData(suite.replicaDb, 100);
        assert.ok(Buffer.isBuffer(dst), 'replica raw_data should be a Buffer');
        assert.ok(dst.equals(RAW_BUF), 'replica bytes must equal source bytes');
    });

    it('live block broadcast round-trips a blob byte-for-byte', async function(){
        // Build the live block payload exactly as ServerPoller would, then push it
        // through the real BlockBroadcaster to capture the serialized wire message.
        let event = makeBroadcastEvent();

        let captured = [];
        let broadcaster = new BlockBroadcaster({ WS_MAX_PER_IP: 100, WS_BACKPRESSURE_LIMIT: 1000, TRUST_PROXY: false });
        let fakeWs = { readyState: WebSocket.OPEN, bufferedAmount: 0, send: (d) => captured.push(d) };
        broadcaster.subscribers.set('bitcoin:mainnet:indexer', new Set([fakeWs]));

        broadcaster.broadcast('bitcoin', 'mainnet', event, new Set());

        assert.strictEqual(captured.length, 1, 'broadcaster should have sent one message');
        assert.ok(captured[0].includes(BINARY_TAG), 'wire must carry the binary sentinel');
        assert.ok(!captured[0].includes('"type":"Buffer"'), 'wire must not contain a mangled Buffer object');

        let payload = JSON.parse(captured[0]);
        let applier = new ClientApplier(suite.replicaDb, testDb.util);
        await applier.applyBlock(payload);

        let dst = await readRawData(suite.replicaDb, 200);
        assert.ok(Buffer.isBuffer(dst), 'replica raw_data should be a Buffer');
        assert.ok(dst.equals(RAW_BUF), 'replica bytes must equal source bytes');
    });
});
