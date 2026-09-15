// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers shared binary replication fixtures. One part of binary_replication.test.js.
const zlib = require('zlib');
const { PassThrough } = require('stream');
const setup = require('../../helpers/setup');

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

function makeBroadcastEvent(){
    return {
        type: 'block', chain: 'bitcoin', network: 'mainnet', dbType: 'indexer',
        block_index: 1, block_time: 100,
        data: {
            gated_files: [
                { action_index: 200, gate_ticker: 'GATETOK', encryption_method: 1,
                  key_hash: 'cd'.repeat(32), status_id: null, raw_data: Buffer.from(RAW_HEX, 'hex') }
            ]
        }
    };
}

function registerBinaryReplicationHooks(){
    const suite = {};
    before(async function(){
        await setup.globalSetup();
    });
    after(async function(){
        await setup.globalTeardown();
    });
    beforeEach(async function(){
        await setup.resetDatabases();
        suite.sourceDb  = setup.getSourceDb();
        suite.replicaDb = setup.getReplicaDb();
    });
    return suite;
}

module.exports = {
    RAW_HEX, RAW_BUF, seedGatedFile, readRawData, captureFullSnapshot,
    makeBroadcastEvent, registerBinaryReplicationHooks
};
