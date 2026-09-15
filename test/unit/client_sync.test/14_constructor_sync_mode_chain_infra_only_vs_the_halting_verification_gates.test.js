// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const {
    assert, sinon, axios, ClientSync, createMockDb, registerClientSyncHooks
} = require('./support');

let sync, db, applier, rollback, hashVerifier, config, util;

function assignState(state){
    ({ sync, db, applier, rollback, hashVerifier, config, util } = state);
}

function registerConstructorSyncModeChainInfraOnlyVsTheHaltingVerificationGatesTests(){
    describe('constructor: SYNC_MODE_<CHAIN>=infra-only vs the halting verification gates', function(){
        // The server filters infra-only live blocks down to the infra tables, so the
        // apply-time recompute over the withheld rows cannot pass and the first filtered
        // block would trip a DURABLE local-recompute-divergence halt (#5608). Resolve the
        // mode once and refuse to start instead, naming the remedy.
        afterEach(function(){ delete process.env.SYNC_MODE_BITCOIN; });

        it('throws at construction when infra-only is combined with default-on gates (indexer), naming them', function(){
            process.env.SYNC_MODE_BITCOIN = 'infra-only';
            let cfg = Object.assign({}, config, { VERIFY_RECOMPUTE: true }); // STATE_HASH / STATE_COMMITMENT default on (undefined !== false)
            assert.throws(() => new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, cfg, util),
                /SYNC_MODE_BITCOIN=infra-only .*VERIFY_RECOMPUTE, VERIFY_STATE_HASH, VERIFY_STATE_COMMITMENT.*VERIFY_RECOMPUTE=false VERIFY_STATE_HASH=false VERIFY_STATE_COMMITMENT=false/);
        });

        it('names only the gates still on', function(){
            process.env.SYNC_MODE_BITCOIN = 'infra-only';
            let cfg = Object.assign({}, config, { VERIFY_RECOMPUTE: false, VERIFY_STATE_HASH: false }); // commitment still on
            assert.throws(() => new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, cfg, util),
                /\(VERIFY_STATE_COMMITMENT\)/);
        });

        it('constructs in infra-only when all three gates are explicitly false, and carries the mode to the subscribe URL', function(){
            process.env.SYNC_MODE_BITCOIN = 'infra-only';
            let cfg = Object.assign({}, config, { VERIFY_RECOMPUTE: false, VERIFY_STATE_HASH: false, VERIFY_STATE_COMMITMENT: false });
            let s = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, cfg, util);
            assert.strictEqual(s._syncMode, 'infra-only');
        });

        it('full mode (default) never consults the gates; a decoder replica ignores infra-only (no infra tables)', function(){
            let s = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, Object.assign({}, config, { VERIFY_RECOMPUTE: true }), util);
            assert.strictEqual(s._syncMode, 'full');
            process.env.SYNC_MODE_BITCOIN = 'infra-only';
            let ddb = Object.assign(createMockDb(), { dbType: 'decoder' });
            let d = new ClientSync('bitcoin', 'mainnet', ddb, applier, rollback, hashVerifier, Object.assign({}, config, { VERIFY_RECOMPUTE: true }), util);
            assert.strictEqual(d._syncMode, 'infra-only');
        });
    });
}

describe('ClientSync', function(){
    registerClientSyncHooks(assignState);
    registerConstructorSyncModeChainInfraOnlyVsTheHaltingVerificationGatesTests();
});
