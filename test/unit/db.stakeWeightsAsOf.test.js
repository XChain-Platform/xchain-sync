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
 *********************************************************************/

'use strict';

// Unit coverage for Database.getStakeWeightsByCapabilityAsOf (review #4927): the
// sync-only historical stake-weight reconstruction used by the SPV checkpoint
// forward-follow. These are mock-based (no live MariaDB): they lock the query
// SHAPE, the placeholder/arg-order invariant, the result mapping, and the
// ClientSync callsite swap. The end-to-end SEMANTIC proof (a slash at B>S restores
// the pre-slash weight) needs a real DB and runs at the integration venue.

const assert     = require('assert');
const sinon      = require('sinon');
const Database   = require('../../src/db');

function makeUtil(){
    return {
        isNull:     (v) => v === null || v === undefined,
        throwError: (m) => { throw new Error(m); },
        sleep:      sinon.stub().resolves(),
        logError:   sinon.stub()
    };
}
function makeDb(){
    return new Database('localhost', 3306, 'replica_db', 'u', 'p', makeUtil(), 'indexer');
}

describe('Database.getStakeWeightsByCapabilityAsOf (#4927)', function(){
    let db, captured;
    beforeEach(function(){
        sinon.stub(console, 'warn');
        db = makeDb();
        sinon.stub(db, 'getStatusId').resolves(1);          // valid_id = 1
        captured = null;
        sinon.stub(db, 'doQuery').callsFake(async (query, args) => {
            captured = { query, args };
            return [];
        });
    });
    afterEach(function(){ sinon.restore(); });

    it('returns [] when the valid status id cannot be resolved (no query run)', async function(){
        db.getStatusId.resolves(null);
        let out = await db.getStakeWeightsByCapabilityAsOf('oracle_publish', 106, '500', 1000);
        assert.deepStrictEqual(out, []);
        assert.strictEqual(db.doQuery.called, false);
    });

    it('adds back post-snapshot stakes slash debits in the weight subquery', async function(){
        await db.getStakeWeightsByCapabilityAsOf('oracle_publish', 106, '500', 1000);
        let q = captured.query;
        assert.match(q, /LEFT JOIN\s*\(/, 'reconstruction LEFT JOIN present');
        assert.match(q, /FROM capability_slash_debits csd/, 'joins the slash-debit ledger');
        assert.match(q, /csd\.target_table = 'stakes'/, "only stakes debits (not unstakes) are added back");
        assert.match(q, /csd\.block_index > \?/, 'strictly post-snapshot slashes only');
        assert.match(q, /COALESCE\(CAST\(addback\.amt AS DECIMAL\(30,8\)\), 0\)/, 'add-back folded into the SUM');
        assert.match(q, /addback\.stake_action_index = s\.action_index/, 'add-back keyed to the stakes row');
    });

    it('binds the add-back snapshot block FIRST, then the base arg sequence, then LIMIT', async function(){
        await db.getStakeWeightsByCapabilityAsOf('oracle_publish', 106, '500', 1000);
        // addback block, then [valid, S, S, minStake, valid, S, S, valid, S, S, valid, S, S, S], then LIMIT
        assert.deepStrictEqual(captured.args, [
            106,
            1, 106, 106, '500',
            1, 106, 106, 1, 106, 106,
            1, 106, 106, 106,
            1000
        ]);
        assert.strictEqual(captured.args[0], 106, 'add-back block_index bind comes first');
        assert.strictEqual(captured.args[captured.args.length - 1], 1000, 'LIMIT is last');
    });

    it('placeholder count equals the bound-arg count (arg-order drift guard)', async function(){
        await db.getStakeWeightsByCapabilityAsOf('oracle_publish', 106, '500', 1000);
        let placeholders = (captured.query.match(/\?/g) || []).length;
        assert.strictEqual(placeholders, captured.args.length,
            'every ? must have exactly one bound arg, in order');
    });

    it('passes minStake through to the HAVING floor as a string', async function(){
        await db.getStakeWeightsByCapabilityAsOf('oracle_publish', 106, 500, 1000);  // numeric in
        assert.match(captured.query, /HAVING total >= CAST\(\? AS DECIMAL\(30,8\)\)/);
        assert.strictEqual(captured.args[4], '500', 'minStake coerced to string for the bind');
    });

    it('maps rows to {pubkey, source, weight}', async function(){
        db.doQuery.callsFake(async () => ([
            { pubkey: 'AA', source: 'addr1', weight: '6000.00000000' },
            { pubkey: 'BB', source: 'addr2', weight: '0' }
        ]));
        let out = await db.getStakeWeightsByCapabilityAsOf('oracle_publish', 106, '500', 1000);
        assert.deepStrictEqual(out, [
            { pubkey: 'AA', source: 'addr1', weight: '6000.00000000' },
            { pubkey: 'BB', source: 'addr2', weight: '0' }
        ]);
    });

    it('REFUSES a null weight rather than defaulting it to "0" ', async function(){
        // The old default was the defect: a coerced zero keeps the source in the
        // quorum's dedupe map with no stake, so the denominator S shrinks and a
        // smaller real stake clears the two-thirds bar. stakes.amount is NOT NULL
        // and the aggregate is HAVING-filtered, so this can only be a corrupt read.
        db.doQuery.callsFake(async () => ([
            { pubkey: 'AA', source: 'addr1', weight: '6000.00000000' },
            { pubkey: 'BB', source: 'addr2', weight: null }
        ]));
        await assert.rejects(() => db.getStakeWeightsByCapabilityAsOf('oracle_publish', 106, '500', 1000),
            /denominator S/);
    });

    it('does not reference _stakeWeightsSql (keeps the drift-guarded twin untouched)', async function(){
        // The reconstruction is self-contained; _stakeWeightsSql must remain the
        // byte-identical consensus twin used only by the live, in-order callers.
        let spy = sinon.spy(db, '_stakeWeightsSql');
        await db.getStakeWeightsByCapabilityAsOf('oracle_publish', 106, '500', 1000);
        assert.strictEqual(spy.called, false);
    });
});

describe('ClientSync._oraclePublishSetAt uses the as-of reconstruction (#4927)', function(){
    const ClientSync   = require('../../src/ClientSync');
    const Utility      = require('../../src/utility');
    const HashVerifier = require('../../src/HashVerifier');

    let sync, db;
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        db = {
            dbName: 'replica_db', dbType: 'indexer',
            getLastBlock: sinon.stub().resolves(null),
            getStakeWeightsByCapabilityAsOf: sinon.stub().resolves(
                [{ pubkey: 'AA', source: 'addr1', weight: '6000.00000000' }]),
            getStakeWeightsByCapability: sinon.stub().resolves(
                [{ pubkey: 'AA', source: 'addr1', weight: '0' }])   // the buggy live result
        };
        const config = { SYNC_SOURCES: 'http://a:3006', VERIFY_RECOMPUTE: true };
        sync = new ClientSync('BTC', 'regtest', db, { applyBlock: sinon.stub().resolves() },
            { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
    });
    afterEach(function(){ sinon.restore(); });

    it('calls getStakeWeightsByCapabilityAsOf, not the live getStakeWeightsByCapability', async function(){
        let set = await sync._oraclePublishSetAt(106);
        assert.ok(db.getStakeWeightsByCapabilityAsOf.calledOnce, 'forward-follow uses the as-of reconstruction');
        assert.strictEqual(db.getStakeWeightsByCapability.called, false, 'must NOT use the slash-mutated live query');
        assert.strictEqual(db.getStakeWeightsByCapabilityAsOf.firstCall.args[1], 106, 'queried at the snapshot block');
        assert.deepStrictEqual(set, [{ pubkey: 'AA', weight: '6000.00000000', source: 'addr1' }]);
    });
});
