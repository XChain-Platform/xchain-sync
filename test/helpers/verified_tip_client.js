/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************
 *
 * A ClientSync over a stub database whose committed block hashes are
 * 'L<h>' / 'A<h>' / 'C<h>' for every height up to `tip`, with a recompute
 * that reproduces them unless the height is listed in `bad`. sync_state is a
 * Map, so tests read the verified-tip marker and the order of writes.
 *
 ********************************************************************/

const sinon = require('sinon');
const ClientSync = require('../../src/client/sync');
const Utility = require('../../src/util');
const HashVerifier = require('../../src/client/hash_verifier');

const KEY = 'verified_tip:indexer';

function hashesAt(h){ return { ledger_hash: 'L' + h, actions_hash: 'A' + h, contract_hash: 'C' + h }; }

function buildClient(opts){
    let o = Object.assign({ tip: 200, bad: [], config: {} }, opts);
    let state = new Map();
    let writes = [];
    let db = {
        state, writes, dbName: 'test_db', dbType: 'indexer',
        tip: o.tip,
        getLastBlock: sinon.stub().callsFake(async () => db.tip),
        getBlockHashRow: sinon.stub().callsFake(async h => (h <= db.tip ? Object.assign({ block_index: h }, hashesAt(h)) : null)),
        doQuery: sinon.stub().resolves([]),
        getSyncState: sinon.stub().callsFake(async k => (state.has(k) ? state.get(k) : null)),
        setSyncState: sinon.stub().callsFake(async (k, v) => { writes.push([k, v]); state.set(k, v); return true; }),
        deleteSyncState: sinon.stub().callsFake(async k => { state.delete(k); return true; }),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        getActiveHalt: sinon.stub().resolves(null),
        clearHalt: sinon.stub().resolves(1)
    };
    let applier = { applyBlock: sinon.stub().resolves(), applyIncrementalSnapshot: sinon.stub().resolves(), _lastComputedRoots: null };
    let rollback = { rollback: sinon.stub().resolves() };
    let config = Object.assign({ SYNC_SOURCES: 'http://a:3006', VERIFY_HASHES: true, HASH_CONFIRM_TIMEOUT: 5000,
        HALT_ON_DIVERGENCE: true, VERIFY_RECOMPUTE: true, VERIFY_STATE_HASH: false, VERIFY_STATE_COMMITMENT: false }, o.config);
    let util = new Utility();
    util.sleep = async () => {};
    let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, new HashVerifier(), config, util);
    let bad = new Set(o.bad);
    sync.blockHasher.computeBlockHashes = sinon.stub().callsFake(async h => (bad.has(h) ? hashesAt('x' + h) : hashesAt(h)));
    return { sync, db, applier, rollback, state, writes, bad };
}

module.exports = { buildClient, hashesAt, KEY };
