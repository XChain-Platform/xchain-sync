// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const sinon  = require('sinon');
const ClientApplier = require('../../../src/client/applier');
const Utility = require('../../../src/util');
const { SCHEMA_VERSION } = require('../../../src/schema/version');
const balanceHelpers = require('../../../src/db/balance_helpers');
const { withDbMixins } = require('../../helpers/db_mixins.js');

function createMockDb(){
    return withDbMixins({
        doQuery: sinon.stub().resolves([]),
        getBlockHashRow: sinon.stub().resolves(null),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves(),
        truncateTable: sinon.stub().resolves()
    });
}

let applier, db, util;

function registerHooks(){
    beforeEach(function(){
        db = createMockDb();
        util = new Utility();
        applier = new ClientApplier(db, util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });
}

function registerInsertCases1(){
    it('does nothing for empty rows', async function(){
        await applier.insertRows('blocks', []);
        assert.strictEqual(db.doQuery.called, false);
    });
    it('does nothing for null rows', async function(){
        await applier.insertRows('blocks', null);
        assert.strictEqual(db.doQuery.called, false);
    });
    it('uses INSERT IGNORE for index tables', async function(){
        await applier.insertRows('index_actions', [{ id: 1, name: 'test' }]);
        let query = db.doQuery.firstCall.args[0];
        assert.ok(query.startsWith('INSERT IGNORE'));
    });
    it('uses INSERT for non-index tables', async function(){
        await applier.insertRows('blocks', [{ block_index: 1 }]);
        let query = db.doQuery.firstCall.args[0];
        assert.ok(query.startsWith('INSERT INTO'));
        assert.ok(!query.includes('IGNORE'));
    });
    // #4771: c793db4 added merkle_epochs (INSERT IGNORE, append-only) and the
    // mutable-aggregate full-dump tables markets / attest_validator_stats
    // (INSERT ... ON DUPLICATE KEY UPDATE so a re-dump refreshes stale values
    // instead of skipping them). Pin both so a future edit can't silently drop
    // either mode (the F-2 divergence class) and pass CI.
    it('uses INSERT IGNORE for append-only merkle_epochs', async function(){
        await applier.insertRows('merkle_epochs', [{ epoch: 1, root: 'aa' }]);
        let query = db.doQuery.firstCall.args[0];
        assert.ok(query.startsWith('INSERT IGNORE'), 'merkle_epochs must be INSERT IGNORE');
        assert.ok(!query.includes('ON DUPLICATE KEY UPDATE'));
    });
    // The two close_block-keyed roll-call tables ride the bootstrap full dump AND
    // stream per block, so an overlapping window re-delivers a row already applied.
    // Each is pinned at its close and never re-derived, so the repeat is identical:
    // IGNORE is a no-op, while a plain INSERT aborts the whole apply transaction.
    for(const table of ['rollcalls', 'rollcall_absences']){
        it('uses INSERT IGNORE for the re-deliverable ' + table, async function(){
            await applier.insertRows(table, [{ epoch_height: 1000, close_block: 1100 }]);
            let query = db.doQuery.firstCall.args[0];
            assert.ok(query.startsWith('INSERT IGNORE'), table + ' must be INSERT IGNORE');
            assert.ok(!query.includes('ON DUPLICATE KEY UPDATE'),
                table + ' is pinned at close and must never be overwritten by a re-delivery');
        });
    }
    for(const table of ['markets', 'attest_validator_stats']){
        it('upserts ' + table + ' with ON DUPLICATE KEY UPDATE covering every carried column', async function(){
            await applier.insertRows(table, [{ id: 1, a: 'x', b: 'y' }]);
            let query = db.doQuery.firstCall.args[0];
            assert.ok(query.startsWith('INSERT INTO'), table + ' upsert starts as INSERT (not IGNORE)');
            assert.ok(!query.startsWith('INSERT IGNORE'), table + ' must not be INSERT IGNORE');
            assert.ok(query.includes('ON DUPLICATE KEY UPDATE'), table + ' must upsert');
            for(const col of ['a', 'b']){
                assert.ok(query.includes('`' + col + '` = VALUES(`' + col + '`)'),
                    table + ' upsert must refresh column ' + col);
            }
        });
    }
}

function registerInsertCases2(){
    // The id half of that contract is NOT shared, and the split is the point.
    // markets keeps its source-assigned id (a replica never mints one for it, and its
    // uq_markets_pair natural key arrives from a migration an aged replica may lack,
    // so the id is the only key a re-dump can safely collide on). attest_validator_stats
    // mints its own id locally, so replicating the source's rewrites the replica's
    // PRIMARY KEY onto a number another surviving row holds.
    it('keeps refreshing markets.id, whose id space is source-assigned end to end', async function(){
        await applier.insertRows('markets', [{ id: 1, a: 'x' }]);
        let query = db.doQuery.firstCall.args[0];
        assert.ok(query.includes('`id`'), 'markets must still carry the source id');
        assert.ok(query.includes('`id` = VALUES(`id`)'), 'markets must still refresh id');
    });
    // These three cover generic column/batch handling and used `blocks` only as an
    // arbitrary plain-INSERT table, with synthetic {id} rows a real blocks row never
    // has (it always carries block_index). `blocks` is now a localSurrogateIdTables
    // member whose id is stripped, so it is no longer a neutral stand-in; retargeted
    // to `actions`, which takes the plain-INSERT path. blocks' own behaviour is
    // pinned separately below.
    it('batches inserts in groups of 100', async function(){
        let rows = [];
        for(let i = 0; i < 250; i++) rows.push({ id: i });
        await applier.insertRows('actions', rows);
        assert.strictEqual(db.doQuery.callCount, 3); // 100 + 100 + 50
    });
    it('handles null column values', async function(){
        await applier.insertRows('actions', [{ id: 1, name: null }]);
        let args = db.doQuery.firstCall.args[1];
        assert.strictEqual(args[1], null);
    });
    it('handles undefined column values as null', async function(){
        await applier.insertRows('actions', [{ id: 1, name: undefined }]);
        let args = db.doQuery.firstCall.args[1];
        assert.strictEqual(args[1], null);
    });
}

// Item 808: litecoin/mainnet/indexer froze for days, reporting halted:false,
// because the source streamed a `blocks` row carrying its own surrogate id that
// the replica had long ago assigned to a different block, so every apply died on
// ER_DUP_ENTRY and rolled back the whole transaction. The replica must not
// inherit that id at all.
function registerInsertCases3(){
    describe('blocks surrogate id (item 808)', function(){
        it('strips the source id so the replica assigns its own', async function(){
            await applier.insertRows('blocks', [{ id: 27681, block_index: 3147670, block_time: 5 }]);
            let insert = db.doQuery.getCalls().map(c => c.args[0]).find(q => /^INSERT/.test(q));
            assert.ok(!insert.includes('`id`'), 'the source surrogate id must not be replicated');
            assert.ok(insert.includes('`block_index`') && insert.includes('`block_time`'),
                'every other column must still be written');
        });

        it('deletes the existing row for that block_index first, so a re-send is idempotent', async function(){
            await applier.insertRows('blocks', [{ id: 27681, block_index: 3147670 }]);
            let calls = db.doQuery.getCalls().map(c => c.args[0]);
            let delIdx = calls.findIndex(q => /^DELETE FROM `blocks`/.test(q));
            let insIdx = calls.findIndex(q => /^INSERT/.test(q));
            assert.ok(delIdx !== -1, 'must clear the natural key before inserting');
            assert.ok(delIdx < insIdx, 'the DELETE must precede the INSERT');
            assert.ok(calls[delIdx].includes('`block_index` IN'), 'the DELETE must be scoped to block_index');
            assert.deepStrictEqual(db.doQuery.getCall(delIdx).args[1], [3147670],
                'the DELETE must be scoped to exactly the block_index being applied');
        });

        it('scopes the delete to the applied blocks only, never the whole table', async function(){
            await applier.insertRows('blocks', [
                { id: 1, block_index: 10 },
                { id: 2, block_index: 11 }
            ]);
            let del = db.doQuery.getCalls().find(c => /^DELETE FROM `blocks`/.test(c.args[0]));
            assert.deepStrictEqual(del.args[1], [10, 11]);
            assert.ok(!/DELETE FROM `blocks`\s*$/.test(del.args[0]), 'must never be an unscoped wipe');
        });

        it('does not use IGNORE or UPSERT, which would drop or overwrite a block', async function(){
            await applier.insertRows('blocks', [{ id: 1, block_index: 10 }]);
            let insert = db.doQuery.getCalls().map(c => c.args[0]).find(q => /^INSERT/.test(q));
            assert.ok(!insert.startsWith('INSERT IGNORE'), 'IGNORE would silently skip the block');
            assert.ok(!insert.includes('ON DUPLICATE KEY UPDATE'),
                'UPSERT would overwrite whichever unrelated block holds that id');
        });

        it('fails closed on a row with no block_index rather than appending a duplicate', async function(){
            // block_index is a plain INDEX, not UNIQUE, so an unscoped insert cannot
            // be de-duplicated afterwards; refuse instead of corrupting the table.
            await assert.rejects(
                () => applier.insertRows('blocks', [{ id: 1, block_time: 5 }]),
                /missing its natural key block_index/);
        });

        it('leaves a legacy row that carries no id untouched', async function(){
            await applier.insertRows('blocks', [{ block_index: 10, block_time: 5 }]);
            let calls = db.doQuery.getCalls().map(c => c.args[0]);
            assert.ok(!calls.some(q => /^DELETE/.test(q)), 'no id to strip means no delete is needed');
            assert.ok(calls.some(q => /^INSERT INTO `blocks`/.test(q)));
        });
    });
}

function registerInsertCases4(){
    // attest_validator_stats gained a node-local AUTO_INCREMENT id (indexer migration
    // 2026-08-19-attest-validator-stats-surrogate-id) while still riding the
    // upsertFullDumpTables path, so the applier emitted `id` = VALUES(`id`) against
    // the replica's PRIMARY KEY. A replica assigns that id itself (sync runs no
    // migrations; db.addMissingColumns backfills the sequence in local row order), so
    // the source's value lands on a number another surviving row holds: ER_DUP_ENTRY
    // 1062, outside the {1146, 1054} schema-heal set, aborting the apply transaction
    // and re-failing forever. Strip the id; the composite UNIQUE
    // (validator_pubkey, provider_id) is what identifies the row.
    describe('attest_validator_stats surrogate id (strip-only class)', function(){
        it('strips the source id so the replica keeps its own', async function(){
            await applier.insertRows('attest_validator_stats',
                [{ id: 42, validator_pubkey: 'aa', provider_id: 'http_get', fulfilled_count: 3 }]);
            let insert = db.doQuery.getCalls().map(c => c.args[0]).find(q => /^INSERT/.test(q));
            assert.ok(!insert.includes('`id`'),
                'the source surrogate id must reach neither the column list nor the update suffix');
            for(const col of ['validator_pubkey', 'provider_id', 'fulfilled_count'])
                assert.ok(insert.includes('`' + col + '`'), col + ' must still be written');
        });

        it('still upserts on the natural key, so a re-dump refreshes the counters', async function(){
            await applier.insertRows('attest_validator_stats',
                [{ id: 42, validator_pubkey: 'aa', provider_id: 'http_get', fulfilled_count: 3 }]);
            let insert = db.doQuery.getCalls().map(c => c.args[0]).find(q => /^INSERT/.test(q));
            assert.ok(insert.includes('ON DUPLICATE KEY UPDATE'), 'the full-dump upsert must survive the strip');
            assert.ok(insert.includes('`fulfilled_count` = VALUES(`fulfilled_count`)'),
                'the running counters must still be overwritten with the source values');
        });

        it('issues no DELETE: the natural key is composite and a scoped delete would drop siblings', async function(){
            await applier.insertRows('attest_validator_stats', [
                { id: 1, validator_pubkey: 'aa', provider_id: 'http_get' },
                { id: 2, validator_pubkey: 'aa', provider_id: 'other' }
            ]);
            let calls = db.doQuery.getCalls().map(c => c.args[0]);
            assert.ok(!calls.some(q => /^DELETE/.test(q)),
                'deleting by validator_pubkey alone would remove that validator other-provider rows');
        });

        it('leaves a row that carries no id alone', async function(){
            await applier.insertRows('attest_validator_stats',
                [{ validator_pubkey: 'aa', provider_id: 'http_get' }]);
            let insert = db.doQuery.getCalls().map(c => c.args[0]).find(q => /^INSERT/.test(q));
            assert.ok(insert.includes('`validator_pubkey`') && insert.includes('`provider_id`'));
        });

        it('refuses a row that carries only the stripped id rather than inserting nothing', async function(){
            await assert.rejects(
                () => applier.insertRows('attest_validator_stats', [{ id: 7 }]),
                /carries only the stripped surrogate id/);
        });
    });
}

function registerInsertCases5(){
    it('backtick-wraps column names', async function(){
        await applier.insertRows('blocks', [{ 'block_index': 1 }]);
        let query = db.doQuery.firstCall.args[0];
        assert.ok(query.includes('`block_index`'));
    });
    it('throws on an invalid table name without querying (fail closed)', async function(){
        await assert.rejects(() => applier.insertRows('bad;name', [{ id: 1 }]), /Rejected table name/);
        assert.strictEqual(db.doQuery.called, false);
    });
    it('throws on an invalid column name without querying (fail closed)', async function(){
        await assert.rejects(() => applier.insertRows('blocks', [{ 'bad-col': 1 }]), /Rejected column name/);
        assert.strictEqual(db.doQuery.called, false);
    });
}
describe('ClientApplier', function(){
    registerHooks();

    describe('insertRows', function(){
        registerInsertCases1();
        registerInsertCases2();
        registerInsertCases3();
        registerInsertCases4();
        registerInsertCases5();
    });
});
