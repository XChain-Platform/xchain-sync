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
        HASH_CONFIRM_TIMEOUT: 100,
        // Default to no in-process retry rounds so direct _bootstrapFromSnapshot calls
        // fail fast in tests; individual tests raise this to exercise the backoff loop.
        BOOTSTRAP_MAX_RETRIES: (opts && opts.maxRetries !== undefined) ? opts.maxRetries : 0,
        BOOTSTRAP_RETRY_BASE_MS: 1,
        BOOTSTRAP_RETRY_MAX_MS: 5
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

    describe('_bootstrapRotateSources rotation (one round, returns boolean)', function(){
        it('does not recurse when only 1 source', async function(){
            let cs = createSync('http://s1');
            let axios = require('axios');
            sinon.stub(axios, 'get').rejects(new Error('fail'));
            let ok = await cs._bootstrapRotateSources();
            assert.strictEqual(ok, false);
            assert.deepStrictEqual(cs.sources, ['http://s1']);
            axios.get.restore();
        });

        it('rotates sources on failure with 2 sources', async function(){
            let cs = createSync('http://s1,http://s2');
            let axios = require('axios');
            sinon.stub(axios, 'get').rejects(new Error('fail'));
            let ok = await cs._bootstrapRotateSources();
            // Both sources fail this round → false; sources rotated during the pass.
            assert.strictEqual(ok, false);
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
            let ok = await cs._bootstrapRotateSources();
            assert.strictEqual(ok, false);
            // Should try all 3 sources (initial + 2 retries) but not loop infinitely
            // axios.get is called once per schema fetch attempt + once per snapshot
            assert.ok(callCount <= 6); // max 2 calls per source (schema + snapshot)
            axios.get.restore();
        });

        it('no sources configured: returns false', async function(){
            let cs = createSync('');
            assert.strictEqual(await cs._bootstrapRotateSources(), false);
        });
    });

    describe('_bootstrapFromSnapshot failure propagation', function(){
        // The core single-source defect: a bootstrap that exhausts its sources must
        // signal failure to start() (so it never live-follows an empty replica),
        // not return normally.
        it('throws when no sources are configured', async function(){
            let cs = createSync('');
            await assert.rejects(() => cs._bootstrapFromSnapshot(), /no sync sources configured/);
        });

        it('throws (does not silently return) when all sources are exhausted', async function(){
            let cs = createSync('http://s1', { maxRetries: 0 });
            let axios = require('axios');
            sinon.stub(axios, 'get').rejects(new Error('fail'));
            await assert.rejects(() => cs._bootstrapFromSnapshot(), /all sync sources exhausted/);
            assert.strictEqual(cs.lastAppliedBlock, null);
            axios.get.restore();
        });

        it('retries with backoff, then succeeds on a later round', async function(){
            let cs = createSync('http://s1', { maxRetries: 3 });
            let sleep = sinon.stub(cs.util, 'sleep').resolves();   // instant backoff
            sinon.stub(cs, '_fetchAndApplySchema').resolves();
            sinon.stub(cs.applier, 'applyFullSnapshot').resolves();
            let axios = require('axios');
            let payload = Buffer.from(JSON.stringify({ block_height: 7, tables: {} }));
            let get = sinon.stub(axios, 'get');
            get.onCall(0).rejects(new Error('transient'));
            get.onCall(1).resolves({ data: payload });

            await cs._bootstrapFromSnapshot();

            assert.strictEqual(cs.lastAppliedBlock, 7);
            assert.ok(sleep.callCount >= 1, 'backed off before the successful retry');
            axios.get.restore();
        });
    });
});
