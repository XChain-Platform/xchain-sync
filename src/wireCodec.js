/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available —
 * contact legal@dankest.llc.
 *
 **********************************************************************
 *
 * XChain Sync - Wire Codec (binary-safe row serialization)
 *
 * MariaDB returns BLOB/binary columns as Node Buffers. JSON.stringify turns a
 * Buffer into {"type":"Buffer","data":[...]}, and the apply path's object→
 * toString() arg coercion (see src/db.js) then collapses that into the literal
 * string "[object Object]" on insert — so binary columns replicate corrupted on
 * both the snapshot and the live-broadcast paths.
 *
 * Fix: tag each Buffer column value with a reserved single-key sentinel
 * { "__xbin__": "<base64>" } at serialize time and restore it to a Buffer at
 * apply time. Encoding is driven by Buffer.isBuffer (the driver only returns a
 * Buffer for binary column types), so there are no encode-side false positives;
 * decoding only fires on the exact reserved-key shape. Row values are scalars,
 * strings, JSON text, or Buffers — never nested Buffers — so a shallow walk of
 * each row's own columns is sufficient.
 *
 * This is a wire-format change: SCHEMA_VERSION must be bumped alongside it so a
 * peer on the old format fails closed at bootstrap rather than re-introducing
 * the corruption.
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

// Decode one column value from the wire: a sentinel object → Buffer, everything
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

module.exports = { BINARY_TAG, encodeRow, encodeTables, decodeValue };
