// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

'use strict';

const assert = require('assert');
const sinon = require('sinon');
const ClientRollback = require('../../../src/client/rollback');
const Utility = require('../../../src/util');

const UNVERIFIED_STATUS_ID = 7;
const ARCHIVE_RESET = /^UPDATE anchor_actions p /;
const RESTORE_AND_CLEAR = /SET p\.status_id = COALESCE\(p\.pre_archive_status_id, us\.id\), p\.pre_archive_status_id = NULL /;

function createMockDb(head){
    return {
        doQuery: sinon.stub().callsFake(async (sql) => {
            if(ARCHIVE_RESET.test(sql)){
                assert.match(sql, RESTORE_AND_CLEAR);
                head.status_id = head.pre_archive_status_id === null
                    ? UNVERIFIED_STATUS_ID
                    : head.pre_archive_status_id;
                head.pre_archive_status_id = null;
            }
            return [];
        }),
        getFirstActionIndex: sinon.stub().resolves(500),
        getStatusId: sinon.stub().resolves(null),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves()
    };
}

async function rollbackHead(head){
    const db = createMockDb(head);
    await new ClientRollback(db, new Utility(), undefined, 'regtest').rollback(100);
    const reset = db.doQuery.getCalls().filter(call => ARCHIVE_RESET.test(call.args[0]));
    assert.strictEqual(reset.length, 1);
    assert.deepStrictEqual(reset[0].args[1], [500, 500]);
    assert.strictEqual(db.commitTransaction.calledOnce, true);
}

describe('ClientRollback archive stamp journal restore @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    it('restores a journaled head status verbatim and clears the journal', async function(){
        const head = { status_id: 99, pre_archive_status_id: 23 };
        await rollbackHead(head);
        assert.deepStrictEqual(head, { status_id: 23, pre_archive_status_id: null });
    });

    it('resets an unjournaled head to unverified and leaves the journal clear', async function(){
        const head = { status_id: 99, pre_archive_status_id: null };
        await rollbackHead(head);
        assert.deepStrictEqual(head, { status_id: UNVERIFIED_STATUS_ID, pre_archive_status_id: null });
    });
});
