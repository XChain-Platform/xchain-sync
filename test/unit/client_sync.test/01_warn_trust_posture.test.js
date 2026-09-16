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

function registerWarnTrustPostureTests(){
    describe('warnTrustPosture', function(){
        it('warns when running single-source (no cross-source rejection)', function(){
            config.SYNC_SOURCES = 'http://only-source:3006';
            let s = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            let warn = sinon.stub(console, 'warn');
            s.warnTrustPosture();
            assert.ok(warn.getCalls().some(c => /SINGLE-SOURCE/.test(c.args[0])));
        });

        it('does not warn about single-source with 2+ sources', function(){
            // default config has two sources
            let warn = sinon.stub(console, 'warn');
            sync.warnTrustPosture();
            assert.ok(!warn.getCalls().some(c => /SINGLE-SOURCE/.test(c.args[0])));
        });

        it('warns that the decoder path has no hash rejection', function(){
            let decoderDb = createMockDb(); decoderDb.dbType = 'decoder';
            let s = new ClientSync('bitcoin', 'mainnet', decoderDb, applier, rollback, hashVerifier, config, util);
            let warn = sinon.stub(console, 'warn');
            s.warnTrustPosture();
            assert.ok(warn.getCalls().some(c => /decoder replication has no hash-based rejection/.test(c.args[0])));
        });

        it('indexer with 2+ sources emits no SINGLE-SOURCE warning, but DOES warn the checkpoint anchor is off', function(){
            let warn = sinon.stub(console, 'warn');
            sync.warnTrustPosture();
            // Cross-source quorum alone only outvotes a Byzantine minority; a
            // consensus-relevant replica with no active checkpoint anchor is warned
            // that all-sources-collude is undefended.
            assert.ok(!warn.getCalls().some(c => /SINGLE-SOURCE/.test(c.args[0])),
                'no single-source warning with 2+ sources');
            assert.ok(warn.getCalls().some(c => /NO active checkpoint-quorum anchor/.test(c.args[0])),
                'warns the checkpoint anchor is inactive');
        });

        it('indexer with the checkpoint anchor active and a pinned set emits no trust warnings', function(){
            const ENVKEY = 'CHECKPOINT_VALIDATORS_BITCOIN_MAINNET';
            process.env[ENVKEY] = JSON.stringify([{ pubkey: 'ab'.repeat(32), weight: '100', source: 'ab'.repeat(32) }]);
            let cfg = { ...config, VERIFY_CHECKPOINT_QUORUM: true };
            let s = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, cfg, util);
            let warn = sinon.stub(console, 'warn');
            s.warnTrustPosture();
            delete process.env[ENVKEY];
            assert.strictEqual(warn.callCount, 0, 'no warnings once the anchor is active with a pinned set');
        });
    });
}

describe('ClientSync', function(){
    registerClientSyncHooks(assignState);
    registerWarnTrustPostureTests();
});
