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
const { BINARY_TAG, encodeRow, encodeTables, decodeValue, bigIntReplacer } = require('../../src/util/wire_codec');

// Simulate the wire trip: a row is encoded, JSON-serialized, parsed back, and
// each column value decoded, exactly what SnapshotBuilder/BlockBroadcaster do
// on serialize and ClientApplier does on apply.
function roundTripValue(row, col){
    let wire = JSON.parse(JSON.stringify(encodeRow(row)));
    return decodeValue(wire[col]);
}

describe('Unit: wireCodec (binary-safe row serialization)', function(){

    describe('encodeRow', function(){
        it('wraps a Buffer column in the base64 sentinel', function(){
            let buf = Buffer.from([0x00, 0xff, 0x80, 0x42]);
            let out = encodeRow({ id: 1, raw_data: buf });
            assert.strictEqual(typeof out.raw_data, 'object');
            assert.strictEqual(out.raw_data[BINARY_TAG], buf.toString('base64'));
            assert.strictEqual(Object.keys(out.raw_data).length, 1);
            // non-binary columns untouched
            assert.strictEqual(out.id, 1);
        });

        it('returns the same object reference when there are no Buffers', function(){
            let row = { id: 1, name: 'abc', n: null };
            assert.strictEqual(encodeRow(row), row);
        });

        it('does not mutate the input row', function(){
            let buf = Buffer.from([1, 2, 3]);
            let row = { raw_data: buf };
            encodeRow(row);
            assert.ok(Buffer.isBuffer(row.raw_data));
        });

        it('passes non-object inputs through', function(){
            assert.strictEqual(encodeRow(null), null);
            assert.strictEqual(encodeRow(undefined), undefined);
        });
    });
});

describe('Unit: wireCodec (binary-safe row serialization)', function(){

    describe('decodeValue', function(){
        it('restores a sentinel to the exact Buffer', function(){
            let buf = Buffer.from([0x00, 0xff, 0x80, 0x42, 0xde, 0xad, 0xbe, 0xef]);
            let decoded = decodeValue({ [BINARY_TAG]: buf.toString('base64') });
            assert.ok(Buffer.isBuffer(decoded));
            assert.ok(decoded.equals(buf));
        });

        it('passes scalars and null through unchanged', function(){
            assert.strictEqual(decodeValue(null), null);
            assert.strictEqual(decodeValue(42), 42);
            assert.strictEqual(decodeValue('hello'), 'hello');
        });

        it('leaves arrays untouched', function(){
            let arr = [1, 2, 3];
            assert.strictEqual(decodeValue(arr), arr);
        });

        it('leaves an ordinary JSON object untouched', function(){
            let obj = { foo: 'bar', n: 1 };
            assert.strictEqual(decodeValue(obj), obj);
        });

        it('does NOT decode a sentinel-shaped object with extra keys (collision guard)', function(){
            let obj = { [BINARY_TAG]: 'AAA=', other: 1 };
            assert.strictEqual(decodeValue(obj), obj);
        });

        it('does NOT decode when the tag value is not a string', function(){
            let obj = { [BINARY_TAG]: 123 };
            assert.strictEqual(decodeValue(obj), obj);
        });

        it('does NOT re-wrap an already-decoded Buffer', function(){
            let buf = Buffer.from([1, 2, 3]);
            assert.strictEqual(decodeValue(buf), buf);
        });
    });
});

describe('Unit: wireCodec (binary-safe row serialization)', function(){

    describe('round-trip (encode → JSON → parse → decode)', function(){
        it('preserves arbitrary binary bytes including 0x00 and 0xFF', function(){
            let buf = Buffer.from([0x00, 0xff, 0x80, 0x01, 0xfe, 0x7f, 0xde, 0xad, 0xbe, 0xef]);
            let out = roundTripValue({ raw_data: buf }, 'raw_data');
            assert.ok(Buffer.isBuffer(out));
            assert.ok(out.equals(buf), 'round-tripped bytes must equal source');
        });

        it('preserves an empty Buffer', function(){
            let buf = Buffer.alloc(0);
            let out = roundTripValue({ raw_data: buf }, 'raw_data');
            assert.ok(Buffer.isBuffer(out));
            assert.strictEqual(out.length, 0);
        });

        it('preserves a larger binary payload', function(){
            let buf = Buffer.alloc(4096);
            for(let i = 0; i < buf.length; i++) buf[i] = (i * 37) & 0xff;
            let out = roundTripValue({ raw_data: buf }, 'raw_data');
            assert.ok(out.equals(buf));
        });
    });
});

describe('Unit: wireCodec (binary-safe row serialization)', function(){

    describe('encodeTables', function(){
        it('encodes Buffer columns across every row of every table', function(){
            let buf = Buffer.from([0xde, 0xad]);
            let tables = {
                gated_files: [{ action_index: 1, raw_data: buf }],
                transactions: [{ tx_index: 2, raw_data: buf }, { tx_index: 3, raw_data: null }]
            };
            let out = encodeTables(tables);
            assert.strictEqual(out.gated_files[0].raw_data[BINARY_TAG], buf.toString('base64'));
            assert.strictEqual(out.transactions[0].raw_data[BINARY_TAG], buf.toString('base64'));
            assert.strictEqual(out.transactions[1].raw_data, null);
        });

        it('passes non-object input through', function(){
            assert.strictEqual(encodeTables(null), null);
        });
    });
});

// Assert every server serializer imports the one replacer and none defines its own.
function assertOneBigIntReplacer(){
    const fs = require('fs');
    const path = require('path');
    const SRC = path.join(__dirname, '../../src/server');
    const users = ['block_broadcaster/broadcast_and_heartbeats.js', 'snapshot_builder/full_snapshot.js',
        'snapshot_builder/incremental_snapshot.js', 'snapshot_builder/table_streaming.js'];
    for(const rel of users){
        const src = fs.readFileSync(path.join(SRC, rel), 'utf8');
        assert.match(src, /\{[^}]*\bbigIntReplacer\b[^}]*\}\s*=\s*require\('\.\.\/\.\.\/util\/wire_codec'\)/,
            rel + ' must take bigIntReplacer from src/util/wire_codec.js');
    }
    const walk = (dir) => fs.readdirSync(dir, { withFileTypes: true }).flatMap(e =>
        e.isDirectory() ? walk(path.join(dir, e.name)) : e.name.endsWith('.js') ? [path.join(dir, e.name)] : []);
    for(const file of walk(SRC)){
        const src = fs.readFileSync(file, 'utf8');
        assert.doesNotMatch(src, /bigIntReplacer\s*=|function\s+bigIntReplacer|typeof\s+\w+\s*===\s*'bigint'\s*\?/,
            path.relative(SRC, file) + ' defines its own BigInt replacer');
    }
}

// One BigInt wire form for every server route: the client applies values uncoerced.
describe('Unit: wireCodec bigIntReplacer', function(){
    describe('bigIntReplacer', function(){
        it('writes a BigInt above the safe-integer range as its exact decimal string', function(){
            assert.strictEqual(JSON.stringify({ id: 12345678901234567890n, n: 1, s: 'x' }, bigIntReplacer),
                '{"id":"12345678901234567890","n":1,"s":"x"}');
        });

        it('writes a top-level BigInt and one inside encodeTables output as strings', function(){
            assert.strictEqual(JSON.stringify(5n, bigIntReplacer), '"5"');
            let wire = JSON.parse(JSON.stringify(encodeTables({ t: [{ q: 9007199254740993n }] }), bigIntReplacer));
            assert.strictEqual(wire.t[0].q, '9007199254740993');
        });

        it('ignores a global BigInt.prototype.toJSON patch', function(){
            const prior = Object.getOwnPropertyDescriptor(BigInt.prototype, 'toJSON');
            Object.defineProperty(BigInt.prototype, 'toJSON',
                { value: function(){ return Number(this); }, configurable: true, writable: true });
            try {
                assert.strictEqual(JSON.stringify({ id: 12345678901234567890n }, bigIntReplacer),
                    '{"id":"12345678901234567890"}');
            } finally {
                if(prior) Object.defineProperty(BigInt.prototype, 'toJSON', prior);
                else delete BigInt.prototype.toJSON;
            }
        });

        it('is the only BigInt replacer: every server serializer imports it from wire_codec', function(){
            assertOneBigIntReplacer();
        });

        it('passes a Number BIGINT, the bigIntAsNumber driver form, through as a JSON Number', function(){
            assert.strictEqual(JSON.stringify({ id: 42, h: 9007199254740991, s: 'x' }, bigIntReplacer),
                '{"id":42,"h":9007199254740991,"s":"x"}');
        });

        it('the shared pool sets bigIntAsNumber: true, so BIGINT travels as a Number', function(){
            const src = require('fs').readFileSync(require('path').join(__dirname, '../../src/db/index.js'), 'utf8');
            assert.match(src, /bigIntAsNumber:\s*true/,
                'flipping bigIntAsNumber changes the BIGINT wire form, a SCHEMA_VERSION change');
            assert.doesNotMatch(src, /bigIntAsNumber:\s*(?:false|this\.|\()/);
        });
    });
});
