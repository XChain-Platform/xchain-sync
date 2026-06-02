const assert = require('assert');
const { BINARY_TAG, encodeRow, encodeTables, decodeValue } = require('../../src/wireCodec');

// Simulate the wire trip: a row is encoded, JSON-serialized, parsed back, and
// each column value decoded — exactly what SnapshotBuilder/BlockBroadcaster do
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
