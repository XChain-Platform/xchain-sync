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
 * ClientSync: durable consensus-divergence HALT.
 *
 * On a CONFIRMED cross-source hash divergence (two honest sources committed
 * different consensus hashes for the same block; one is on a forked/Byzantine
 * chain. The client must HALT: stop applying, persist the halt durably (so it
 * survives restart), and require an explicit operator clear. It must never
 * silently pick one source and replicate onto a contested chain.
 ********************************************************************/

const assert = require('assert');
const sinon  = require('sinon');
const ClientSync = require('../../../src/client/sync');
const Utility = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');

describe('ClientSync: VERIFY_RECOMPUTE=false is declared unsafe @regression', function(){
    // Operator decision 2026-06-12: the recompute is the only verification of
    // the catch-up JOIN block, so disabling it lets a reorg that crosses a
    // disconnect silently fork the replica. The constructor must warn loudly.
    afterEach(function(){ sinon.restore(); });

    function build(config){
        const db = { dbType: 'indexer', doQuery: sinon.stub().resolves([]) };
        const applier = { applyBlock: sinon.stub().resolves() };
        return new ClientSync('bitcoin', 'mainnet', db, applier,
            { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
    }

    it('warns UNSAFE at construction when explicitly disabled', function(){
        const err = sinon.stub(console, 'error');
        build({ SYNC_SOURCES: 'http://a:3006', VERIFY_RECOMPUTE: false });
        assert.ok(err.getCalls().some(c => /UNSAFE/.test(String(c.args[0]))),
            'constructor must emit the UNSAFE warning when VERIFY_RECOMPUTE is false');
    });

    it('stays quiet when recompute is enabled', function(){
        const err = sinon.stub(console, 'error');
        build({ SYNC_SOURCES: 'http://a:3006', VERIFY_RECOMPUTE: true });
        assert.ok(!err.getCalls().some(c => /UNSAFE/.test(String(c.args[0]))),
            'no UNSAFE warning when recompute is on');
    });
});
