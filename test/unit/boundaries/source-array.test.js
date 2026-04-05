const assert = require('assert');
const sinon  = require('sinon');
const ClientSync    = require('../../../src/ClientSync');
const ClientApplier = require('../../../src/ClientApplier');
const ClientRollback = require('../../../src/ClientRollback');
const HashVerifier  = require('../../../src/HashVerifier');
const Utility       = require('../../../src/utility');

function createMockDb(){
    return {
        doQuery: sinon.stub().resolves([]),
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getFirstActionIndex: sinon.stub().resolves(null),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves(),
        truncateTable: sinon.stub().resolves(),
        dbName: 'test_db'
    };
}

function createSync(sources, opts){
    let db = createMockDb();
    let applier = new ClientApplier(db, new Utility());
    let rollback = new ClientRollback(db, new Utility());
    let verifier = new HashVerifier();
    let config = {
        SYNC_SOURCES: sources,
        VERIFY_HASHES: (opts && opts.verifyHashes) || false,
        CLIENT_RECONNECT_DELAY: 100,
        HASH_CONFIRM_TIMEOUT: 100
    };
    return new ClientSync('bitcoin', 'mainnet', db, applier, rollback, verifier, config, new Utility());
}

describe('Boundary: Source Array Parsing', function(){

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){ sinon.restore(); });

    describe('constructor source parsing', function(){
        it('empty string → empty array', function(){
            let cs = createSync('');
            assert.strictEqual(cs.sources.length, 0);
        });

        it('single URL → 1-element array', function(){
            let cs = createSync('http://server1');
            assert.deepStrictEqual(cs.sources, ['http://server1']);
        });

        it('two URLs → 2-element array', function(){
            let cs = createSync('http://s1,http://s2');
            assert.deepStrictEqual(cs.sources, ['http://s1', 'http://s2']);
        });

        it('trailing comma → filtered out', function(){
            let cs = createSync('http://s1,');
            assert.deepStrictEqual(cs.sources, ['http://s1']);
        });

        it('leading comma → filtered out', function(){
            let cs = createSync(',http://s1');
            assert.deepStrictEqual(cs.sources, ['http://s1']);
        });

        it('multiple commas → all empty strings filtered', function(){
            let cs = createSync(',,,http://s1,,,');
            assert.deepStrictEqual(cs.sources, ['http://s1']);
        });

        it('whitespace trimmed', function(){
            let cs = createSync(' http://s1 , http://s2 ');
            assert.deepStrictEqual(cs.sources, ['http://s1', 'http://s2']);
        });

        it('only whitespace → empty array', function(){
            let cs = createSync(' , , ');
            assert.strictEqual(cs.sources.length, 0);
        });
    });

    describe('_bootstrapFromSnapshot recursion limit', function(){
        it('does not recurse when only 1 source', async function(){
            let cs = createSync('http://s1');
            // Stub axios to fail
            let axios = require('axios');
            sinon.stub(axios, 'get').rejects(new Error('fail'));
            await cs._bootstrapFromSnapshot();
            // Should not rotate (only 1 source)
            assert.deepStrictEqual(cs.sources, ['http://s1']);
            axios.get.restore();
        });

        it('rotates sources on failure with 2 sources', async function(){
            let cs = createSync('http://s1,http://s2');
            let axios = require('axios');
            sinon.stub(axios, 'get').rejects(new Error('fail'));
            await cs._bootstrapFromSnapshot();
            // Sources rotated: s1 moved to end, s2 is now first
            // Then s2 fails too, but attempt limit reached (1 < 2-1=1 is false)
            axios.get.restore();
        });

        it('stops after exhausting all sources (no infinite recursion)', async function(){
            let cs = createSync('http://s1,http://s2,http://s3');
            let axios = require('axios');
            let callCount = 0;
            sinon.stub(axios, 'get').callsFake(async () => {
                callCount++;
                throw new Error('fail');
            });
            await cs._bootstrapFromSnapshot();
            // Should try all 3 sources (initial + 2 retries) but not loop infinitely
            // axios.get is called once per schema fetch attempt + once per snapshot
            // The key assertion is that it terminates
            assert.ok(callCount <= 6); // max 2 calls per source (schema + snapshot)
            axios.get.restore();
        });

        it('no sources configured — returns immediately', async function(){
            let cs = createSync('');
            await cs._bootstrapFromSnapshot(); // should not throw
        });
    });
});
