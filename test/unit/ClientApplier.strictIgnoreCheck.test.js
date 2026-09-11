// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// a from-zero lookup repair on a short index_* table (observed on the
// production RDOGE replica: index_statuses stayed short by one row, hourly,
// forever) needs INSERT IGNORE's silent per-row failures to surface when they
// are NOT the table's own expected PRIMARY-key re-send. These pin
// _insertRows({ strictIgnoreCheck: true }) doing exactly that, and pin that the
// ordinary (unflagged) apply path stays silent and cheap - it must, since every
// block re-sends these tables' rows by design.

const assert = require('assert');
const sinon  = require('sinon');
const ClientApplier = require('../../src/ClientApplier');
const Utility = require('../../src/utility');

function createMockDb(){
    return {
        doQuery: sinon.stub().resolves([]),
        getBlockHashRow: sinon.stub().resolves(null),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves(),
        truncateTable: sinon.stub().resolves()
    };
}

describe('ClientApplier strictIgnoreCheck', function(){
    let applier, db, util;

    beforeEach(function(){
        db = createMockDb();
        util = new Utility();
        applier = new ClientApplier(db, util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){ sinon.restore(); });

    it('is a no-op by default: no SHOW WARNINGS round trip on the ordinary apply path', async function(){
        await applier._insertRows('index_statuses', [{ id: 1, status: 'open' }]);
        assert.strictEqual(db.doQuery.callCount, 1, 'only the INSERT itself, no follow-up SHOW WARNINGS');
    });

    it('reads SHOW WARNINGS when strictIgnoreCheck is set and passes clean on no warnings', async function(){
        await applier._insertRows('index_statuses', [{ id: 1, status: 'open' }], { strictIgnoreCheck: true });
        assert.strictEqual(db.doQuery.callCount, 2);
        assert.strictEqual(db.doQuery.secondCall.args[0], 'SHOW WARNINGS');
    });

    it('stays silent on the table\'s own expected re-send: a duplicate on PRIMARY', async function(){
        db.doQuery.onSecondCall().resolves([
            { Code: 1062, Message: "Duplicate entry '3' for key 'PRIMARY'" }
        ]);
        await assert.doesNotReject(() =>
            applier._insertRows('index_statuses', [{ id: 3, status: 'completed' }], { strictIgnoreCheck: true }));
    });

    it('throws loud on a collision against a DIFFERENT unique key (index_statuses.status)', async function(){
        // This is the exact shape of the production RDOGE defect: id 2 ('closed')
        // could not land because some OTHER row already held status='closed', and
        // plain INSERT IGNORE swallowed that conflict every pass with no signal.
        db.doQuery.onSecondCall().resolves([
            { Code: 1062, Message: "Duplicate entry 'closed' for key 'status'" }
        ]);
        await assert.rejects(
            () => applier._insertRows('index_statuses', [{ id: 2, status: 'closed' }], { strictIgnoreCheck: true }),
            /silently dropped a row applying to `index_statuses`/);
    });

    it('throws loud on a non-duplicate warning (e.g. truncation) even under strictIgnoreCheck', async function(){
        db.doQuery.onSecondCall().resolves([
            { Code: 1265, Message: "Data truncated for column 'status' at row 1" }
        ]);
        await assert.rejects(
            () => applier._insertRows('index_statuses', [{ id: 2, status: 'closed' }], { strictIgnoreCheck: true }),
            /silently dropped a row applying to `index_statuses`/);
    });

    it('leaves validator_rewards alone under strictIgnoreCheck: its expected re-send collides on a SECONDARY key', async function(){
        // validator_rewards' surrogate `id` plays no part in de-duplication (its real
        // identity is the reward_unique composite key), so a re-send warning there is
        // the NORMAL case and must never throw, flagged or not.
        db.doQuery.onSecondCall().resolves([
            { Code: 1062, Message: "Duplicate entry '1-2-oracle_round-3-0' for key 'reward_unique'" }
        ]);
        await assert.doesNotReject(() => applier._insertRows('validator_rewards',
            [{ id: 99, source_id: 1, signing_pubkey_id: 2, reward_type: 'oracle_round', round_reference: 3, round_qualifier: 0 }],
            { strictIgnoreCheck: true }));
    });
});
