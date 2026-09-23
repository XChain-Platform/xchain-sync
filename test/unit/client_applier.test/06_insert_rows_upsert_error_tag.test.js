// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// insertRows names the upsert full-dump table on a failing statement's error, which
// is what lets ClientSync halt on a repeating markets duplicate key and on nothing else.

const assert = require('assert');
const sinon  = require('sinon');
const ClientApplier = require('../../../src/client/applier');
const Utility = require('../../../src/util');
const { withDbMixins } = require('../../helpers/db_mixins.js');

function dupKeyError(){
    return Object.assign(new Error("Duplicate entry '42' for key 'PRIMARY'"), { errno: 1062 });
}

describe('ClientApplier.insertRows upsert error tag', function(){
    let applier, db;
    beforeEach(function(){
        db = withDbMixins({ doQuery: sinon.stub().rejects(dupKeyError()) });
        applier = new ClientApplier(db, new Utility());
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('tags a failing markets upsert with its table and keeps the source id', async function(){
        let err = await applier.insertRows('markets', [{ id: 42, tick1_id: 1, tick2_id: 2 }]).then(() => null, e => e);
        assert.ok(err, 'the duplicate key propagates');
        assert.strictEqual(err.errno, 1062);
        assert.strictEqual(err.upsertTable, 'markets');
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('ON DUPLICATE KEY UPDATE'), 'markets upserts');
        assert.ok(sql.includes('`id` = VALUES(`id`)'), 'markets carries the source id');
    });

    it('leaves a plain INSERT table untagged', async function(){
        let err = await applier.insertRows('credits', [{ id: 1, block_index: 5 }]).then(() => null, e => e);
        assert.ok(err);
        assert.strictEqual(err.upsertTable, undefined);
    });
});
