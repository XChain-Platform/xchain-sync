// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const sinon  = require('sinon');
const { PassThrough, Readable } = require('stream');
const { EventEmitter } = require('events');
const zlib = require('zlib');
const SnapshotBuilder = require('../../../../src/server/snapshot_builder');
const { SnapshotStreamWriter } = require('../../../../src/server/snapshot_builder');
const Utility = require('../../../../src/util');
const poolSizing = require('../../../../src/db/pool_sizing');
const { withDbMixins } = require('../../../helpers/db_mixins.js');

function createMockDb(dbName){
    // Queries read through named Database methods. The real ones are installed for
    // any this fake does not stub, so they still reach doQuery below and every
    // doQuery call count these suites assert keeps counting them.
    return withDbMixins({
        dbName: dbName || 'test_db',
        doQuery: sinon.stub().resolves([]),
        // Always-throw twin of doQuery. The authoritative dump reads use it so a
        // query error can never ship as an empty-but-successful body.
        doQueryStrict: sinon.stub().resolves([]),
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getFirstActionIndex: sinon.stub().resolves(null),
        // Single-pass row stream per table (replaced ORDER BY 1 LIMIT/OFFSET
        // paging, which had no total order on keyless tables). Tests override
        // with Readable.from(rows) for populated tables.
        streamTableRows: sinon.stub().callsFake(() => Readable.from([])),
        getTableCount: sinon.stub().resolves(0),
        // beginReadSnapshot returns a dedicated connection handle the builder
        // threads into every read and ends via commit/rollbackReadSnapshot.
        beginReadSnapshot: sinon.stub().resolves({ _snapshotConn: true }),
        commitReadSnapshot: sinon.stub().resolves(true),
        rollbackReadSnapshot: sinon.stub().resolves()
    });
}

function createMockRes(){
    let chunks = [];
    let headers = {};
    let passthrough = new PassThrough();
    passthrough.on('data', (chunk) => chunks.push(chunk));

    return {
        _chunks: chunks,
        _headers: headers,
        _statusCode: 200,
        setHeader: sinon.stub().callsFake((k, v) => { headers[k] = v; }),
        status: sinon.stub().returnsThis(),
        json: sinon.stub(),
        write: passthrough.write.bind(passthrough),
        end: passthrough.end.bind(passthrough),
        pipe: passthrough.pipe.bind(passthrough),
        on: passthrough.on.bind(passthrough),
        once: passthrough.once.bind(passthrough),
        emit: passthrough.emit.bind(passthrough),
        // Make it a writable stream for gzip.pipe(res)
        _write: passthrough._write.bind(passthrough),
        _final: passthrough._final ? passthrough._final.bind(passthrough) : undefined,
        _passthrough: passthrough,
        getCollectedData: function(){
            return Buffer.concat(this._chunks);
        }
    };
}

module.exports = {
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
};
