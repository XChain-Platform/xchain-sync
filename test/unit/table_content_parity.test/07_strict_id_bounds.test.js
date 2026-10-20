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
const HashVerifier = require('../../../src/client/hash_verifier');
const Utility     = require('../../../src/util');

// A fake Database serving one id-bounded lookup; getMaxRowId reports the live tail.
function fakeDb(rows){
    return {
        dbType: 'indexer',
        maxCalls: 0,
        async listExistingTables(){ return new Set(['index_statuses']); },
        async getContentWindowRows(){ return []; },
        async getMaxRowId(){
            this.maxCalls++;
            return rows.length ? Math.max(...rows.map(r => Number(r.id))) : null;
        },
        async getContentIdWindowRows(table, fromId, toId){
            return rows.filter(r => Number(r.id) > fromId && Number(r.id) <= toId);
        }
    };
}

const ROWS = [{ id: 1, status: 'valid' }, { id: 2, status: 'invalid' }, { id: 3, status: 'pending' }];

describe('Advisory table-content parity', function(){

    describe('strictIdBounds', function(){

        it('omits a lookup with no supplied ceiling instead of reading the live MAX(id)', async function(){
            let db = fakeDb(ROWS);
            let out = await new BlockHasher(db, new Utility())
                .computeTableContentChecksums(10, { window: 5, idBounds: {}, strictIdBounds: true });
            assert.ok(!('index_statuses' in out.tables));
            assert.strictEqual(db.maxCalls, 0);
        });

        it('uses a supplied ceiling and publishes it as id_max', async function(){
            let db = fakeDb(ROWS);
            let out = await new BlockHasher(db, new Utility())
                .computeTableContentChecksums(10, { window: 5, idBounds: { index_statuses: 2 }, strictIdBounds: true });
            assert.strictEqual(out.tables.index_statuses.id_max, '2');
            assert.strictEqual(out.tables.index_statuses.n, 2);
            assert.strictEqual(db.maxCalls, 0);
        });

        it('keeps the live fallback when the option is absent', async function(){
            let db = fakeDb(ROWS);
            let out = await new BlockHasher(db, new Utility()).computeTableContentChecksums(10, { window: 5 });
            assert.strictEqual(out.tables.index_statuses.id_max, '3');
            assert.strictEqual(db.maxCalls, 1);
        });

        it('lets a follower one interned row behind compare, where the live ceiling skipped it', async function(){
            // The source DB holds id 3, interned by a block its poller has not broadcast;
            // the follower at the broadcast height holds ids 1 and 2.
            let follower = fakeDb(ROWS.slice(0, 2));
            let hv = new HashVerifier();

            let live = await new BlockHasher(fakeDb(ROWS), new Utility()).computeTableContentChecksums(10, { window: 5 });
            let liveLocal = await new BlockHasher(follower, new Utility()).computeTableContentChecksums(10,
                { window: 5, idBounds: { index_statuses: live.tables.index_statuses.id_max } });
            let liveRes = hv.compareTableContent(10, liveLocal, live);
            assert.strictEqual(liveRes.compared, 0, 'control: the live ceiling files the table as row-count-differs');
            assert.strictEqual(liveRes.skipped[0].reason, 'row-count-differs');

            let pinned = await new BlockHasher(fakeDb(ROWS), new Utility()).computeTableContentChecksums(10,
                { window: 5, idBounds: { index_statuses: 2 }, strictIdBounds: true });
            let pinnedLocal = await new BlockHasher(follower, new Utility()).computeTableContentChecksums(10,
                { window: 5, idBounds: { index_statuses: pinned.tables.index_statuses.id_max } });
            let pinnedRes = hv.compareTableContent(10, pinnedLocal, pinned);
            assert.strictEqual(pinnedRes.compared, 1);
            assert.strictEqual(pinnedRes.match, true);
        });
    });
});
