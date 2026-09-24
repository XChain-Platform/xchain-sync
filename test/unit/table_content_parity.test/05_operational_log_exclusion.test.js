// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.
//
// The operational-log boundary: a table the row-count check declares unconvergent
// must not be content-compared either, or an id-offset replica reports it as
// "content diverged at equal row count".

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');

const BlockHasher      = require('../../../src/client/block_hasher');
const HashVerifier     = require('../../../src/client/hash_verifier');
const Utility          = require('../../../src/util');
const replicatedTables = require('../../../src/schema/replicated_tables');

// Serve canned decoder rows by id window; every listed table exists.
function decoderDb(tables){
    return {
        dbType: 'decoder',
        async listExistingTables(){ return new Set(Object.keys(tables)); },
        async getContentWindowRows(table){ return (tables[table] || []).slice(); },
        async getMaxRowId(table){
            let rows = tables[table] || [];
            return rows.length ? Math.max(...rows.map(r => Number(r.id))) : null;
        },
        async getContentIdWindowRows(table, fromId, toId){
            return (tables[table] || []).filter(r => Number(r.id) > fromId && Number(r.id) <= toId);
        }
    };
}

// Return the source and follower checksum maps, the follower reusing the source ceilings.
async function sourceAndFollower(sourceTables, followerTables){
    let src = await new BlockHasher(decoderDb(sourceTables), new Utility()).computeTableContentChecksums(9, { window: 5 });
    let idBounds = {};
    for(let [table, entry] of Object.entries(src.tables)) if(entry.id_max !== undefined) idBounds[table] = entry.id_max;
    let local = await new BlockHasher(decoderDb(followerTables), new Utility())
        .computeTableContentChecksums(9, { window: 5, idBounds: idBounds });
    return { src, local };
}

describe('Advisory table-content parity', function(){

    describe('operational-log exclusion', function(){

        it('no content-parity plan compares a table the count check declares unconvergent', function(){
            assert.ok(replicatedTables.OPERATIONAL_LOG_TABLES.includes('events'));
            for(const dbType of ['indexer', 'decoder']){
                let planned  = new Set(replicatedTables.contentParityPlan(dbType).map(p => p.table));
                let excluded = replicatedTables.contentParityExclusions(dbType);
                let replicated = new Set(replicatedTables.getReplicatedTables(dbType));
                for(const table of replicatedTables.OPERATIONAL_LOG_TABLES){
                    assert.ok(!planned.has(table), dbType + ' plan must not content-compare ' + table);
                    if(replicated.has(table))
                        assert.match(excluded[table] || '', /^operational log: /, dbType + ' ' + table + ' needs a declared reason');
                }
            }
        });

        it('stays scoped to the operational logs: the decoder lookups are still compared', function(){
            let decoder = {};
            for(let p of replicatedTables.contentParityPlan('decoder')) decoder[p.table] = p.bound;
            for(const table of ['index_addresses', 'index_transactions', 'pubkeys'])
                assert.strictEqual(decoder[table], 'id', table + ' must stay in the decoder plan');
        });

        it('the count check reads the same declaration rather than a hand list', function(){
            let src = fs.readFileSync(path.join(__dirname, '../../../src/client/sync.js'), 'utf8');
            assert.ok(/new Set\(replicatedTables\.OPERATIONAL_LOG_TABLES\)/.test(src),
                'ClientSync must build its count exclusion from replicated_tables.js');
            assert.ok(!/new Set\(\['events'\]\)/.test(src), 'a second hand-listed copy can drift from the plan');
        });

        it('an id-offset replica with equal window counts raises no content mismatch', async function(){
            let lookups = [{ id: 1, address: 'a' }, { id: 2, address: 'b' }];
            let { src, local } = await sourceAndFollower(
                { index_addresses: lookups, events: [{ id: 7, code: 'REORG', data: '[9]' }] },
                { index_addresses: lookups, events: [{ id: 7, code: 'LOCAL', data: 'own row' }] });
            assert.ok(!('events' in src.tables) && !('events' in local.tables), 'events must not be published or recomputed');

            let res = new HashVerifier().compareTableContent(9, local, src);
            assert.deepStrictEqual(res.mismatches, []);
            assert.strictEqual(res.compared, 1, 'the lookup beside it is still compared');
        });

        it('a real lookup divergence on the same replica is still reported', async function(){
            let { src, local } = await sourceAndFollower(
                { index_addresses: [{ id: 1, address: 'a' }] },
                { index_addresses: [{ id: 1, address: 'forged' }] });
            let res = new HashVerifier().compareTableContent(9, local, src);
            assert.deepStrictEqual(res.mismatches.map(m => m.table), ['index_addresses']);
        });
    });
});
