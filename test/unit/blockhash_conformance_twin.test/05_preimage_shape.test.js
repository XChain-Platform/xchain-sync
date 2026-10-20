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

describe('consensus block-hash conformance twins (static drift-lock) @regression', function(){
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
