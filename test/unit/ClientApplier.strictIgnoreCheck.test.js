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

    it('names the local row as unretirable when the source still serves its id', async function(){
        db.doQuery.onSecondCall().resolves([
            { Code: 1062, Message: "Duplicate entry 'closed' for key 'status'" }
        ]);
        await assert.rejects(
            () => applier._insertRows('index_statuses', [{ id: 2, status: 'closed' }], { strictIgnoreCheck: true }),
            /needs a human to reconcile it/);
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

// The DLTC replica held `completed` at an id the source no longer serves (the lookup
// ids are node-local AUTO_INCREMENT surrogates interned in first-seen order, and the
// table is never rolled back), so the source's own row could not land by id and the
// repair pass measured the same short count at every start. Convergence retires the
// superseded row; a row the source still serves is never touched.
describe('ClientApplier: natural-key collision on an upsert-only lookup table', function(){
    let applier, util;

    // A small emulator of one lookup table: the INSERT IGNORE, the warning it raises, the
    // probes the convergence path makes, and the DELETE it issues.
    function lookupDb(initialRows){
        let rows = new Map(initialRows.map(r => [Number(r.id), r.status]));
        let warnings = [];
        let log = [];
        let db = {
            dbName: 'replica_db',
            getBlockHashRow: sinon.stub().resolves(null),
            beginTransaction: sinon.stub().resolves(),
            commitTransaction: sinon.stub().resolves(),
            rollbackTransaction: sinon.stub().resolves(),
            truncateTable: sinon.stub().resolves(),
            rows,
            log,
            doQuery: async (sql, args) => {
                log.push({ sql, args });
                if(/^INSERT IGNORE INTO `index_statuses`/.test(sql)){
                    warnings = [];
                    let cols = /\(([^)]*)\) VALUES/.exec(sql)[1].split(',').map(s => s.trim().replace(/`/g, ''));
                    for(let i = 0; i < args.length; i += cols.length){
                        let row = {};
                        cols.forEach((c, j) => { row[c] = args[i + j]; });
                        let id = Number(row.id);
                        if(rows.has(id)){
                            warnings.push({ Code: 1062, Message: "Duplicate entry '" + id + "' for key 'PRIMARY'" });
                            continue;
                        }
                        let holder = [...rows.entries()].find(([, s]) => s === row.status);
                        if(holder){
                            warnings.push({ Code: 1062, Message: "Duplicate entry '" + row.status + "' for key 'status'" });
                            continue;
                        }
                        rows.set(id, row.status);
                    }
                    return [];
                }
                if(sql === 'SHOW WARNINGS') return warnings;
                if(/information_schema\.statistics/.test(sql)) return [{ column_name: 'status' }];
                if(/SELECT id FROM `index_statuses` WHERE id = \?/.test(sql))
                    return rows.has(Number(args[0])) ? [{ id: Number(args[0]) }] : [];
                if(/SELECT id FROM `index_statuses` WHERE `status` = \?/.test(sql)){
                    let hit = [...rows.entries()].filter(([, s]) => s === args[0]);
                    return hit.map(([id]) => ({ id }));
                }
                if(/DELETE FROM `index_statuses` WHERE id = \?/.test(sql)){
                    rows.delete(Number(args[0]));
                    return [];
                }
                return [];
            }
        };
        return db;
    }

    // The source's three rows, and a replica holding `completed` at a retired id.
    let sourcePage = [{ id: 1, status: 'open' }, { id: 3, status: 'completed' }, { id: 4, status: 'valid' }];

    beforeEach(function(){
        util = new Utility();
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
    });
    afterEach(function(){ sinon.restore(); });

    it('converges the stale generation: rows match the source by id and by status', async function(){
        let db = lookupDb([{ id: 1, status: 'open' }, { id: 2, status: 'completed' }]);
        applier = new ClientApplier(db, util);
        await applier._insertRows('index_statuses', sourcePage, { strictIgnoreCheck: true });
        assert.deepStrictEqual([...db.rows.entries()].sort((a, b) => a[0] - b[0]),
            [[1, 'open'], [3, 'completed'], [4, 'valid']]);
        assert.ok(db.log.some(q => /DELETE FROM `index_statuses`/.test(q.sql) && Number(q.args[0]) === 2),
            'the superseded row must be retired by id');
    });

    it('logs one structured line naming the table, key, retired id and landed id', async function(){
        let db = lookupDb([{ id: 1, status: 'open' }, { id: 2, status: 'completed' }]);
        applier = new ClientApplier(db, util);
        await applier._insertRows('index_statuses', sourcePage, { strictIgnoreCheck: true });
        let line = console.warn.getCalls().map(c => String(c.args[0] || ''))
            .find(m => /STALE_LOOKUP_GENERATION_RETIRED/.test(m));
        assert.ok(line, 'the retirement must leave a journal line');
        assert.ok(/table=index_statuses/.test(line) && /key=status/.test(line));
        assert.ok(/retired_id=2/.test(line) && /landed_id=3/.test(line));
    });

    it('still throws on a genuine conflict: the colliding local id is one the source serves', async function(){
        // Local id 2 holds `completed` AND the source serves id 2 (as `closed`), so the
        // local row is live: retiring it would destroy a row the source still has.
        let db = lookupDb([{ id: 1, status: 'open' }, { id: 2, status: 'completed' }]);
        applier = new ClientApplier(db, util);
        await assert.rejects(() => applier._insertRows('index_statuses',
            [{ id: 1, status: 'open' }, { id: 2, status: 'closed' }, { id: 3, status: 'completed' }],
            { strictIgnoreCheck: true }),
            /needs a human to reconcile it/);
        assert.ok(!db.log.some(q => /DELETE FROM `index_statuses`/.test(q.sql)), 'nothing may be retired here');
    });

    it('leaves a collision outside the page id window for the throw', async function(){
        // The holder's id sits above this page's window, so another page may still carry
        // it: the page proves nothing about it.
        let db = lookupDb([{ id: 1, status: 'open' }, { id: 90, status: 'completed' }]);
        applier = new ClientApplier(db, util);
        await assert.rejects(() => applier._insertRows('index_statuses', sourcePage, { strictIgnoreCheck: true }),
            /needs a human to reconcile it/);
        assert.ok(!db.log.some(q => /DELETE FROM `index_statuses`/.test(q.sql)));
    });

    it('the repair pass completes clean on the converged table', async function(){
        let db = lookupDb([{ id: 1, status: 'open' }, { id: 2, status: 'completed' }]);
        applier = new ClientApplier(db, util);
        await applier._insertRows('index_statuses', sourcePage, { strictIgnoreCheck: true });
        db.log.length = 0;
        await assert.doesNotReject(() =>
            applier._insertRows('index_statuses', sourcePage, { strictIgnoreCheck: true }));
        assert.ok(!db.log.some(q => /DELETE FROM `index_statuses`/.test(q.sql)),
            'a converged table must retire nothing on the next pass');
        assert.deepStrictEqual([...db.rows.entries()].sort((a, b) => a[0] - b[0]),
            [[1, 'open'], [3, 'completed'], [4, 'valid']]);
    });
});
