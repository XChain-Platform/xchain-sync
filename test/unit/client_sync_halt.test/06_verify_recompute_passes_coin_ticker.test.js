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
 * ClientSync.verifyRecompute: state_key collation gate keyed by the TICKER
 *
 * SyncService builds ClientSync from the hub's full coin name ('bitcoin'),
 * while STATE_KEY_COLLATION_ACTIVATION is keyed '<TICKER>:<network>' with no
 * bare mainnet key. Forwarding the full name reads the gate OFF, so the live
 * recompute keeps the folding collation past the height the source switched
 * to utf8_bin and false-halts on any block whose state keys sort differently.
 ********************************************************************/

'use strict';

const assert = require('assert');
const sinon  = require('sinon');
const ClientSync   = require('../../../src/client/sync');
const Utility      = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');
const gateRegistry = require('../../../src/consensus/gate_registry');
const { withDbMixins } = require('../../helpers/db_mixins.js');

const STATE_KEY_COLLATION_KEY = 'state_key_collation_activation.STATE_KEY_COLLATION_ACTIVATION';
const COMMITTED = { ledger_hash: 'x', actions_hash: 'x', contract_hash: 'x' };

// Build an indexer ClientSync from the full coin name, capturing every SQL string.
function captureSync(chain, network){
    const calls = [];
    const capture = async (sql) => { calls.push(sql); return []; };
    const db = withDbMixins({
        dbName: 'test_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getActiveHalt: sinon.stub().resolves(null),
        doQuery: capture, doQueryStrict: capture
    });
    const config = { SYNC_SOURCES: 'http://a:3006', VERIFY_RECOMPUTE: true, MAX_ROLLBACK_DEPTH: 10 };
    const sync = new ClientSync(chain, network, db, {}, {}, new HashVerifier(), config, new Utility());
    return { sync, calls };
}

// Return the contract_state gather statement from a captured SQL list.
function stateQueryOf(calls){
    const hit = calls.find(q => /FROM contract_state cs/.test(q));
    assert.ok(hit, 'contract_state gather statement was not issued');
    return hit.replace(/\s+/g, ' ');
}

describe('ClientSync.verifyRecompute passes the coin ticker to the collation gate @regression', function(){
    const height = gateRegistry.get(STATE_KEY_COLLATION_KEY)['BTC:mainnet'];

    it('pins COLLATE utf8_bin at the BTC:mainnet activation height for a "bitcoin" replica', async function(){
        const { sync, calls } = captureSync('bitcoin', 'mainnet');
        await sync.verifyRecompute({ block_index: height }, COMMITTED);
        assert.match(stateQueryOf(calls), /GROUP BY contract_index, state_key COLLATE utf8_bin/);
    });

    it('keeps the legacy folding collation one block below the activation height', async function(){
        const { sync, calls } = captureSync('bitcoin', 'mainnet');
        await sync.verifyRecompute({ block_index: height - 1 }, COMMITTED);
        assert.doesNotMatch(stateQueryOf(calls), /COLLATE utf8_bin/);
    });
});
