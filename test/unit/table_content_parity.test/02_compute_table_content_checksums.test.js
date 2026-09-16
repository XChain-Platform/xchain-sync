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

const BlockHasher = require('../../../src/client/block_hasher');
const Utility     = require('../../../src/util');

// A fake Database serving canned per-table windows. `tables` maps a table name to
// its rows; anything absent is simply an empty window (omitted from the result).
function fakeDb(tables, opts){
    let o = opts || {};
    return {
        dbType: o.dbType || 'indexer',
        calls: [],
        async listExistingTables(){
            if(o.listThrows) throw new Error('information_schema unavailable');
            return new Set(Object.keys(tables));
        },
        async getContentWindowRows(table, bound, fromBlock, toBlock){
            this.calls.push({ table, bound, fromBlock, toBlock });
            if(o.throwFor && o.throwFor.has(table)) throw Object.assign(new Error('no such table'), { errno: 1146 });
            return (tables[table] || []).slice();
        },
        async getMaxRowId(table){
            this.calls.push({ table, maxId: true });
            let rows = tables[table] || [];
            return rows.length ? Math.max(...rows.map(r => Number(r.id))) : null;
        },
        async getContentIdWindowRows(table, fromId, toId){
            this.calls.push({ table, fromId, toId });
            return (tables[table] || []).filter(r => Number(r.id) > fromId && Number(r.id) <= toId);
        }
    };
}

const hasherFor = (tables, opts) => new BlockHasher(fakeDb(tables, opts), new Utility());

describe('Advisory table-content parity', function(){

    describe('computeTableContentChecksums', function(){

        it('publishes the window, the block and a sparse per-table map', async function(){
            let h = hasherFor({ sends: [{ action_index: 1, amount: '5' }], blocks: [] });
            let out = await h.computeTableContentChecksums(500, { window: 25 });
            assert.strictEqual(out.window, 25);
            assert.strictEqual(out.block, 500);
            assert.deepStrictEqual(Object.keys(out.tables), ['sends'],
                'a table with no rows in the window is omitted, not carried as an empty digest');
            assert.strictEqual(out.tables.sends.n, 1);
            assert.strictEqual(out.tables.sends.h.length, 64);
        });

        it('windows blocks as [upto - window + 1, upto] and never below zero', async function(){
            let db = fakeDb({ sends: [{ action_index: 1 }] });
            await new BlockHasher(db, new Utility()).computeTableContentChecksums(500, { window: 100 });
            let call = db.calls.find(c => c.table === 'sends');
            assert.strictEqual(call.fromBlock, 401);
            assert.strictEqual(call.toBlock, 500);

            let db2 = fakeDb({ sends: [{ action_index: 1 }] });
            await new BlockHasher(db2, new Utility()).computeTableContentChecksums(3, { window: 100 });
            assert.strictEqual(db2.calls.find(c => c.table === 'sends').fromBlock, 0);
        });

        it('an invalid or absent window falls back to the default rather than disabling the check', async function(){
            for(let bad of [undefined, 0, -5, 'abc', null]){
                let out = await hasherFor({ sends: [{ action_index: 1 }] })
                    .computeTableContentChecksums(10, bad === undefined ? undefined : { window: bad });
                assert.strictEqual(out.window, 100, 'window ' + String(bad) + ' must not silently empty the window');
            }
        });

        it('a source publishes its id ceiling and a follower reuses it verbatim', async function(){
            // Without the published ceiling each side would hash its own tail, and a
            // follower one row behind would "diverge" every poll.
            let rows = [{ id: 1, status: 'valid' }, { id: 2, status: 'invalid' }, { id: 3, status: 'pending' }];
            let src = await hasherFor({ index_statuses: rows }).computeTableContentChecksums(10, { window: 2 });
            assert.strictEqual(src.tables.index_statuses.id_max, '3');
            assert.strictEqual(src.tables.index_statuses.n, 2, 'id window (id_max - window, id_max]');

            // The follower has a 4th row the source has not published yet; using the
            // source's ceiling keeps it out of the comparison.
            let follower = await hasherFor({ index_statuses: rows.concat([{ id: 4, status: 'later' }]) })
                .computeTableContentChecksums(10, { window: 2, idBounds: { index_statuses: '3' } });
            assert.strictEqual(follower.tables.index_statuses.h, src.tables.index_statuses.h);
        });
    });
});

describe('Advisory table-content parity', function(){

    describe('computeTableContentChecksums', function(){

        it('skips tables the local schema does not have (an older replica must not spam or fail)', async function(){
            let h = hasherFor({ sends: [{ action_index: 1 }] });   // listExistingTables reports only `sends`
            let out = await h.computeTableContentChecksums(10, { window: 5 });
            assert.deepStrictEqual(Object.keys(out.tables), ['sends']);
        });

        it('case 7: one unreadable table does not sink the other tables\' check', async function(){
            let h = hasherFor(
                { sends: [{ action_index: 1 }], issues: [{ action_index: 2 }] },
                { throwFor: new Set(['issues']) });
            let out = await h.computeTableContentChecksums(10, { window: 5 });
            assert.deepStrictEqual(Object.keys(out.tables).sort(), ['sends']);
        });

        it('a failed table listing degrades to probing, not to an empty result', async function(){
            let h = hasherFor({ sends: [{ action_index: 1 }] }, { listThrows: true });
            let out = await h.computeTableContentChecksums(10, { window: 5 });
            assert.strictEqual(out.tables.sends.n, 1);
        });

        it('reads the decoder plan on a decoder handle (no actions table to join)', async function(){
            let db = fakeDb({ blocks: [{ block_index: 9 }], transaction_outputs: [{ tx_index: 3 }] }, { dbType: 'decoder' });
            await new BlockHasher(db, new Utility()).computeTableContentChecksums(9, { window: 5 });
            assert.strictEqual(db.calls.find(c => c.table === 'transaction_outputs').bound, 'tx');
            assert.ok(!db.calls.some(c => c.bound === 'action'), 'the decoder DB has no actions table to join');
        });
    });
});
