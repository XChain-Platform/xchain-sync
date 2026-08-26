'use strict';

// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Boot-time consensus-pin verification for the sync service, mirroring the hub,
// indexer, decoder and utxo-tracker. startApi() must call
// coins.verifyConsensusPin() for every network BEFORE it builds the express app,
// so a coin bundle that drifted on the running host halts with the pin-mismatch
// error rather than recomputing state off a divergent registry and surfacing as
// an opaque local-recompute divergence. CI hashes the checkout; only this check
// sees the artifact actually running.

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');
const sinon  = require('sinon');

const coins  = require('../../src/coins');

const API_PATH = path.join(__dirname, '..', '..', 'src', 'api.js');

describe('sync boot consensus-pin verification', function(){

    afterEach(() => sinon.restore());

    it('the pin check runs before the express app is built', function(){
        // Source-order guard: the call must sit inside startApi() ahead of
        // express(), so it cannot be reordered behind the HTTP surface or
        // silently dropped. Scoped to startApi's body because textual order over
        // the whole file is not execution order.
        const src   = fs.readFileSync(API_PATH, 'utf8');
        const body  = src.slice(src.indexOf('async function startApi()'));
        const pinAt = body.indexOf('coins.verifyConsensusPin(net)');
        const appAt = body.indexOf('const app = express()');
        assert.ok(pinAt > -1, 'startApi() does not call coins.verifyConsensusPin');
        assert.ok(appAt > -1, 'startApi() no longer builds the express app the guard is ordered against');
        assert.ok(pinAt < appAt, 'the pin check must precede express()');
    });

    it('passes on the vendored bundle for every network', function(){
        for(const net of coins.NETWORKS) coins.verifyConsensusPin(net);
    });

    it('skips on the (currently null) mainnet pin', function(){
        assert.deepStrictEqual(coins.verifyConsensusPin('mainnet'), { ok: true, skipped: true });
    });

    it('halts startApi fail-closed on a pin mismatch, before any express app', async function(){
        // Preconditioned on the source guard above so a REMOVED check fails here
        // instead of booting a real server (and a real port) inside the suite.
        const body = fs.readFileSync(API_PATH, 'utf8');
        assert.ok(body.indexOf('coins.verifyConsensusPin(net)') > -1,
            'startApi() does not call coins.verifyConsensusPin; refusing to boot it');

        const stub = sinon.stub(coins, 'verifyConsensusPin')
            .throws(new Error('CONSENSUS CONFIG PIN MISMATCH (test)'));
        const api  = require('../../src/api.js');
        await assert.rejects(() => api.startApi(), /CONSENSUS CONFIG PIN MISMATCH/);
        assert.strictEqual(stub.callCount, 1, 'the first network must throw before any further work');
        assert.strictEqual(stub.firstCall.args[0], coins.NETWORKS[0]);
    });
});
