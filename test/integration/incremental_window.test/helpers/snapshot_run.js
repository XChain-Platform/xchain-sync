// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Drive SnapshotBuilder.streamIncrementalSnapshot against a real database
// into an in-memory response, recording every array read and every streamed
// statement the snapshot connection issues while the tables are written.

const zlib = require('zlib');
const { Writable } = require('stream');
const { once } = require('events');

// An in-memory response. headersSent flips on the first body byte, as it does
// on a real response; while `hold` is a promise the reader stalls on it.
function makeSink() {
    const chunks = [];
    const sink = new Writable({
        write(chunk, _enc, cb) {
            chunks.push(chunk);
            sink.headersSent = true;
            if (sink.hold) sink.hold.then(() => cb());
            else cb();
        },
    });
    sink.headersSent = false;
    sink.hold = null;
    sink.headers = {};
    sink.statusCode = 200;
    sink.body = null;
    sink.setHeader = (k, v) => { sink.headers[k] = v; };
    sink.status = (code) => { sink.statusCode = code; return sink; };
    sink.json = (obj) => { sink.body = obj; sink.end(); return sink; };
    sink.bytes = () => Buffer.concat(chunks);
    return sink;
}

// Resolve `promise`, or reject once `ms` passes: a snapshot that deadlocks on
// its own connection must read as a failure, not as a hung suite.
function within(promise, ms, what) {
    let timer;
    const late = new Promise((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error(what + ' did not settle within ' + ms + ' ms')), ms);
    });
    return Promise.race([promise, late]).finally(() => clearTimeout(timer));
}

// Wrap the snapshot connection so reads issued while the tables phase runs
// are recorded; the wrappers are removed before the call returns.
function instrument(builder, db) {
    const record = { reads: [], streamed: [], phase: false, conns: [] };
    const begin = db.beginReadSnapshot;
    db.beginReadSnapshot = async function () {
        const conn = await begin.call(this);
        const query = conn.query;
        const queryStream = conn.queryStream;
        record.conns.push({ conn, query, queryStream });
        conn.query = async function (sql, args) {
            const rows = await query.call(conn, sql, args);
            if (record.phase && Array.isArray(rows)) record.reads.push({ sql: String(sql), n: rows.length });
            return rows;
        };
        conn.queryStream = function (sql, args) {
            if (record.phase) record.streamed.push(String(sql));
            return queryStream.call(conn, sql, args);
        };
        return conn;
    };
    const tables = builder.streamIncrementalTables;
    builder.streamIncrementalTables = async function (...args) {
        record.phase = true;
        try { return await tables.apply(this, args); } finally { record.phase = false; }
    };
    record.restore = () => {
        delete db.beginReadSnapshot;
        delete builder.streamIncrementalTables;
        for (const c of record.conns) { c.conn.query = c.query; c.conn.queryStream = c.queryStream; }
    };
    return record;
}

async function runIncremental(builder, db, since, ms = 20000) {
    const sink = makeSink();
    const record = instrument(builder, db);
    try {
        await within(builder.streamIncrementalSnapshot(db, since, sink), ms, 'incremental snapshot');
        if (!sink.writableFinished) await within(once(sink, 'finish'), ms, 'response flush');
    } finally {
        record.restore();
    }
    const text = sink.body ? null : zlib.gunzipSync(sink.bytes()).toString();
    return { sink, text, reads: record.reads, streamed: record.streamed };
}

module.exports = { makeSink, within, runIncremental };
