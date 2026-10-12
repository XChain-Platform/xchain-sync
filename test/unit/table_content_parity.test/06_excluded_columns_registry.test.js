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
// The excluded-column registry must cover every column the follower is built NOT to
// share with the source: the `id` the applier strips or re-mints, and every
// database-generated column it never names. A hashed table missing one of them reads
// as a permanent false content-parity alarm while every other test stays green.

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');

const ClientApplier       = require('../../../src/client/applier');
const Utility              = require('../../../src/util');
const lifecycle            = require('../../../src/table_lifecycle');
const replicatedTables     = require('../../../src/schema/replicated_tables');
const { GENERATED_COLUMNS } = require('../../../src/schema/generated_columns');

// Return every { table, column, source } a hashed table needs excluded but is not,
// plus every pair the check examined (so a caller can prove it checked something).
function missingExclusions({ planned, surrogateIdTables, generated, excludedFor }){
    let gaps = [], checked = [];
    let need = (table, column, source) => {
        if(!planned.has(table)) return;
        checked.push(table + '.' + column);
        if(!excludedFor(table).includes(column)) gaps.push({ table, column, source });
    };
    for(let table of surrogateIdTables) need(table, 'id', 'applier node-local id');
    for(let table of Object.keys(generated))
        for(let column of generated[table]) need(table, column, 'GENERATED column');
    return { gaps, checked };
}

// The tables content parity actually hashes, on either database.
function plannedTables(){
    return new Set(replicatedTables.contentParityPlan('indexer')
        .concat(replicatedTables.contentParityPlan('decoder')).map(p => p.table));
}

// Every table the updated-rows carry re-sends after an in-place edit: the literal add() calls
// plus every table named by the exported spec lists (strings or { table } entries).
function updatedRowsCarriedTables(){
    let dir = path.join(__dirname, '../../../src/server');
    let files = [path.join(dir, 'updated_rows.js')].concat(fs.readdirSync(path.join(dir, 'updated_rows'))
        .filter(f => f.endsWith('.js')).map(f => path.join(dir, 'updated_rows', f)));
    let tables = new Set();
    for(let file of files)
        for(let m of fs.readFileSync(file, 'utf8').matchAll(/add\(acc,\s*'(\w+)'/g)) tables.add(m[1]);
    for(let list of Object.values(require('../../../src/server/updated_rows/table_specs')))
        if(Array.isArray(list)) for(let s of list) tables.add(typeof s === 'string' ? s : s.table);
    return tables;
}

// Carried tables that stay in the parity window with no declared in-place column.
function undeclaredInPlaceTables(carried, planned, inPlace){
    return [...carried].filter(t => planned.has(t) && !(inPlace[t] && inPlace[t].length)).sort();
}

// The applier's two node-local id sets, read off a real instance (both are set in the constructor).
function applierSurrogateTables(){
    let applier = new ClientApplier({}, new Utility());
    return [...applier.localSurrogateIdTables.keys(), ...applier.localSurrogateIdOnlyTables];
}

describe('Advisory table-content parity', function(){
    describe('excluded-column registry @regression', function(){
        const excludedFor = lifecycle.contentParityExcludedColumns;

        it('excludes every applier node-local id and every generated column of a hashed table', function(){
            let { gaps } = missingExclusions({ planned: plannedTables(), surrogateIdTables: applierSurrogateTables(),
                generated: GENERATED_COLUMNS, excludedFor });
            assert.deepStrictEqual(gaps, [], 'add each column to CONTENT_PARITY_EXCLUDED_COLUMNS in src/table_lifecycle.js ' +
                'AND its xchain-indexer twin, or content parity raises a permanent false alarm: ' + JSON.stringify(gaps));
        });

        it('checks a non-empty plan and reaches both known exclusions', function(){
            let planned = plannedTables();
            assert.ok(planned.size > 0, 'the content-parity plan is empty, so the registry check examines nothing');
            let { checked } = missingExclusions({ planned, surrogateIdTables: applierSurrogateTables(),
                generated: GENERATED_COLUMNS, excludedFor });
            assert.ok(checked.includes('blocks.id'), 'blocks.id was not examined: ' + JSON.stringify(checked));
            assert.ok(checked.includes('contract_state.state_key_bin'), 'contract_state.state_key_bin was not examined: ' + JSON.stringify(checked));
        });

        it('reports a generated column the registry does not exclude', function(){
            let { gaps } = missingExclusions({ planned: plannedTables(), surrogateIdTables: [],
                generated: { contract_state: ['state_key_bin', 'phantom_generated'] }, excludedFor });
            assert.deepStrictEqual(gaps, [{ table: 'contract_state', column: 'phantom_generated', source: 'GENERATED column' }]);
        });

        it('reports a hashed node-local-id table with no id exclusion, and ignores one that is not hashed', function(){
            let planned = plannedTables();
            let bare = [...planned].find(t => excludedFor(t).length === 0);
            assert.ok(bare, 'expected at least one hashed table with no exclusions');
            let { gaps } = missingExclusions({ planned, surrogateIdTables: [bare, 'not_a_hashed_table'],
                generated: {}, excludedFor });
            assert.deepStrictEqual(gaps, [{ table: bare, column: 'id', source: 'applier node-local id' }]);
        });

        it('declares the edited columns of every in-place carried table still in the parity window', function(){
            let carried = updatedRowsCarriedTables();
            assert.ok(carried.has('order_matches') && carried.has('tokens'), 'the carried-table scan found ' +
                JSON.stringify([...carried]) + '; it no longer reads the updated-rows sources');
            assert.deepStrictEqual(undeclaredInPlaceTables(carried, plannedTables(), lifecycle.CONTENT_PARITY_IN_PLACE_COLUMNS), [],
                'a later block edits these rows in place, so a source one block ahead false-alarms; declare the ' +
                'state_hash class or a CONTENT_PARITY_IN_PLACE_COLUMNS entry in both registry twins');
            assert.ok(lifecycle.contentParityExcludedColumns('order_matches').includes('status_id'));
        });

        it('reports an in-place carried table with no declared column, and ignores one outside the window', function(){
            let planned = plannedTables();
            assert.deepStrictEqual(undeclaredInPlaceTables(new Set(['order_matches', 'tokens']), planned, {}), ['order_matches']);
        });
    });
});
