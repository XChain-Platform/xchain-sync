/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************
 *
 * XChain Sync - Wire Codec (binary-safe row serialization)
 *
 * MariaDB returns BLOB/binary columns as Node Buffers. JSON.stringify turns a
 * Buffer into {"type":"Buffer","data":[...]}, and the apply path's object->
 * toString() arg coercion (see src/db.js) then collapses that into the literal
 * string "[object Object]" on insert, so binary columns replicate corrupted on
 * both the snapshot and the live-broadcast paths.
 *
 * So each Buffer column value is tagged with a reserved single-key sentinel
 * { "__xbin__": "<base64>" } at serialize time and restored to a Buffer at
 * apply time. Encoding is driven by Buffer.isBuffer (the driver only returns a
 * Buffer for binary column types), so there are no encode-side false positives;
 * decoding only fires on the exact reserved-key shape. Row values are scalars,
 * strings, JSON text, or Buffers (never nested Buffers), so a shallow walk of
 * each row's own columns is sufficient.
 *
 * This is a wire-format change: SCHEMA_VERSION must be bumped alongside it so a
 * peer on the old format fails closed at bootstrap rather than re-introducing
 * the corruption.
 *
 * BIGINT columns travel in one of two forms, chosen per value by bigIntTypeCast
 * below, which the shared pool installs as its typeCast (src/db/index.js
 * poolOptions): a JSON Number while the value is a safe integer, and its exact
 * base-10 string once it passes Number.MAX_SAFE_INTEGER. The unsigned 64-bit
 * columns (the expiration, deadline and block-height fields the protocol accepts
 * up to 2^64-1) therefore replicate exactly, where the driver's own
 * bigIntAsNumber read rounded them on the source and could hand a follower a
 * value past the column's range. The client binds the string as an ordinary
 * parameter and MariaDB stores it exactly; it reads it back through the same
 * cast, so content parity compares the same form on both sides.
 * bigIntReplacer below is still the one replacer every server route serializes
 * with (the live broadcast and every snapshot, page and table stream), writing
 * any BigInt that does reach it as its exact base-10 string. The client applies
 * values without coercion, so two routes emitting different forms would
 * replicate the same column differently. Flipping bigIntAsNumber, dropping the
 * cast or changing either form is a wire-format change under the same
 * SCHEMA_VERSION rule.
 *
 ********************************************************************/

// Reserved single-key sentinel wrapping a base64-encoded binary column value.
const BINARY_TAG = '__xbin__';

// Shallow-encode one row's Buffer columns to the wire sentinel. Returns the
// original row object unchanged when it carries no Buffers (avoids needless
// allocation for the overwhelming majority of rows, which have none).
function encodeRow(row){
    if(!row || typeof row !== 'object') return row;
    let out = null;
    for(let k in row){
        if(Buffer.isBuffer(row[k])){
            if(!out) out = Object.assign({}, row);
            out[k] = { [BINARY_TAG]: row[k].toString('base64') };
        }
    }
    return out || row;
}

// Encode every row in a { table: [rows] } map (the live block payload's `data`).
function encodeTables(tables){
    if(!tables || typeof tables !== 'object') return tables;
    let out = {};
    for(let t in tables){
        let rows = tables[t];
        out[t] = Array.isArray(rows) ? rows.map(encodeRow) : rows;
    }
    return out;
}

// Decode one column value from the wire: a sentinel object -> Buffer, everything
// else passthrough. The strict shape check (plain object, exactly one key, the
// reserved key, string value) keeps a JSON-typed column from being misread as
// binary.
function decodeValue(v){
    if(v && typeof v === 'object' && !Array.isArray(v) && !Buffer.isBuffer(v)
        && typeof v[BINARY_TAG] === 'string'
        && Object.keys(v).length === 1){
        return Buffer.from(v[BINARY_TAG], 'base64');
    }
    return v;
}

// JSON.stringify replacer: BigInt -> base-10 string. Reads the RAW value via
// this[key] (so it must stay a regular function), so a global
// BigInt.prototype.toJSON patch cannot change the wire form, as in util jsonStringify.
function bigIntReplacer(key, value){
    const raw = this[key];
    return typeof raw === 'bigint' ? raw.toString() : value;
}

// Pool typeCast: read every BIGINT cell from its decimal text, so a value past
// Number.MAX_SAFE_INTEGER keeps every digit. A safe integer stays a Number (the
// form every BIGINT had before, so ids, heights and counts are unchanged); a
// larger one is returned as the exact string the server sent. The driver's
// next() cannot be used for these: under bigIntAsNumber it has already rounded.
// Exactly one of column.string() and next() runs per cell, because each consumes
// the cell's bytes. column.string() reads a length-encoded text cell, so this
// cast holds for the text protocol only (query and queryStream, which is all
// this service issues); a prepared-statement read would hand it 8 raw bytes.
function bigIntTypeCast(column, next){
    if(column.type !== 'BIGINT') return next();
    const text = column.string();
    if(text === null || text === undefined) return null;
    const approx = Number(text);
    return Number.isSafeInteger(approx) ? approx : text;
}

module.exports = { BINARY_TAG, encodeRow, encodeTables, decodeValue, bigIntReplacer, bigIntTypeCast };
