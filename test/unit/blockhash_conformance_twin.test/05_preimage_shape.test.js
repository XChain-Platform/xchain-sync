/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************/

// test/unit/blockhash_conformance_twin.test/05_preimage_shape.test.js
//
// Static drift-lock for item 9 of ../blockhash_conformance_twin.test.js: the key names, key
// order, gatherer and bind of the ledger and contract preimage objects. The SQL case cannot
// see them, yet JSON.stringify hashes the keys in declared order.

'use strict';

const assert = require('assert');
const fs     = require('fs');
const { stripComments, extractFunction, syncFile, loadPair } = require('./helpers/twin_sources.js');
const { LEDGER_GATHERERS, CONTRACT_GATHERERS, make } = require('./helpers/preimage_shape.js');

const shape = make({ assert, stripComments, extractFunction });

// Prove each follower fill reads the query named after its own key (state is spliced from three parts).
function assertQueriesByKey(filled, from){
    for(const [key, args] of filled){
        const query = key === 'state' ? 'queries.statePrefix + ' : 'queries.' + key + ', ';
        assert.ok(args.startsWith(query), from + ' fills `' + key + '` from the wrong query: ' + args);
    }
}

// getBlockLeafRows hands its SQL to the helpers as `key: <literal>` pairs, and the key picks the
// preimage slot a result lands in. Each entry is [key, the table and alias its literal selects
// FROM, or null for a fragment spliced into the contract_state query].
const LEAF_QUERY_TABLES = [
    ['credits', 'credits c'], ['debits', 'debits d'], ['escrows', 'escrows e'],
    ['contracts', 'contracts c'], ['statePrefix', 'contract_state cs'], ['stateSuffix', null],
    ['stateOrder', null], ['executions', 'contract_executions ce'], ['emissions', 'contract_emissions em'],
    ['deposits', 'deposits d'], ['withdrawals', 'withdrawals w']
];

// Assert every key in getBlockLeafRows' two queries objects holds the literal for its own table.
function assertLeafQueryKeysHoldTheirTables(dbSrc){
    const fn = stripComments(extractFunction(dbSrc,
        /async getBlockLeafRows\(block_index, conn, network, coin\)\{/, 'db/actions.js'));
    const pairs = [...fn.matchAll(/\b(\w+): `([^`]*)`/g)].map(m => [m[1], m[2]]);
    assert.deepStrictEqual(pairs.map(p => p[0]), LEAF_QUERY_TABLES.map(e => e[0]),
        'getBlockLeafRows queries keys changed; update LEAF_QUERY_TABLES with the preimage slot each one fills');
    pairs.forEach(([key, literal], i) => {
        const from = literal.match(/\bFROM (\w+) (\w+)/);
        const want = LEAF_QUERY_TABLES[i][1];
        assert.strictEqual(from ? from[1] + ' ' + from[2] : null, want,
            'getBlockLeafRows files the wrong SQL under `' + key + '`: it must select FROM ' + want);
    });
}

describe('consensus block-hash conformance twins (static drift-lock) @regression', function(){
    it('getBlockLeafRows files each SQL literal under the key of its own table', function(){
        assertLeafQueryKeysHoldTheirTables(fs.readFileSync(syncFile('src/db/actions.js'), 'utf8'));
    });

    it('the key-to-table check fails on swapped labels that keep the literal order', function(){
        const dbSrc = fs.readFileSync(syncFile('src/db/actions.js'), 'utf8');
        const swapped = dbSrc.replace('deposits: `', 'TMPKEY: `').replace('withdrawals: `', 'deposits: `')
            .replace('TMPKEY: `', 'withdrawals: `');
        assert.notStrictEqual(swapped, dbSrc, 'the label swap must change the source');
        assert.throws(() => assertLeafQueryKeysHoldTheirTables(swapped), /queries keys changed/);
    });

    it('the key-to-table check fails on a literal that selects another key\'s table', function(){
        const dbSrc = fs.readFileSync(syncFile('src/db/actions.js'), 'utf8');
        const wrong = dbSrc.replace('FROM deposits d INNER', 'FROM withdrawals w INNER');
        assert.notStrictEqual(wrong, dbSrc, 'the table swap must change the source');
        assert.throws(() => assertLeafQueryKeysHoldTheirTables(wrong), /wrong SQL under `deposits`/);
    });

    // Reads this repo's own files, so it never skips when the indexer checkout is absent.
    it('BlockHasher and the leaf-row gatherers build the pinned preimage keys, in order, at the hashed block', function(){
        shape.assertBlockHasherPreimage(fs.readFileSync(syncFile('src/client/block_hasher.js'), 'utf8'));
        const dbSrc = fs.readFileSync(syncFile('src/db/actions.js'), 'utf8');
        const bound = /, \[block_index\], conn$/;
        const ledgerFn = shape.collapsed(dbSrc,
            /async function getLedgerLeafRows\(db, block_index, conn, queries\)\{/, 'db/actions.js');
        const contractFn = shape.collapsed(dbSrc,
            /async function getContractLeafRows\(db, block_index, conn, stateKeyCollate, queries\)\{/, 'db/actions.js');
        assertQueriesByKey(shape.assertFollowerObject(ledgerFn, 'ledger', 'db', LEDGER_GATHERERS, bound,
            'db.getLedgerLeafRows'), 'db.getLedgerLeafRows');
        assertQueriesByKey(shape.assertFollowerObject(contractFn, 'contracts', 'db', CONTRACT_GATHERERS, bound,
            'db.getContractLeafRows'), 'db.getContractLeafRows');
    });

    it('db.getBlockHashLedgerRows and getBlockHashContractData build the same preimage keys as BlockHasher', function(){
        const pair = loadPair(this, 'src/client/block_hasher.js', 'src/db/actions.js');
        if(!pair) return;
        const indexer = shape.assertIndexerPreimage(pair.indexer);
        const sync    = shape.assertBlockHasherPreimage(pair.sync);
        assert.deepStrictEqual(indexer.ledger.map(([key]) => key), sync.ledger,
            'the ledger preimage keys drifted between db.getBlockHashLedgerRows and BlockHasher');
        assert.deepStrictEqual(indexer.contracts.map(([key]) => key), sync.contracts,
            'the contract preimage keys drifted between db.getBlockHashContractData and BlockHasher');
    });
});
