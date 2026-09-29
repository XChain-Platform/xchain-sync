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

// test/unit/blockhash_conformance_twin.test/01_state_key_collation_gate.test.js
//
// Static drift-lock for the state_key collation flag-day gate, item 7 of the
// compared pieces in ../blockhash_conformance_twin.test.js.

'use strict';

const assert = require('assert');
const fs     = require('fs');
const { stripComments, extractFunction, syncFile, loadPair } = require('./helpers/twin_sources.js');

// ---- state_key collation flag-day gate --------------------------------------
//
// sqlLiterals() reads only backtick literals, so the pairwise SQL cases never
// see the single-quoted ' COLLATE utf8_bin' splice value, the ternary around it,
// the gate key or the activeAt arguments. Any one-sided edit to those forks
// contract_hash and block_merkle_root at the activation height. These cases pin
// the gate shape and the splice composition in all three copies, and every
// extraction asserts it matched, so a reworded copy fails by name, never by
// silently matching nothing.

// Comment-stripped, whitespace-collapsed, keeping `+` (the splice is the subject).
function collapse(src){
    return stripComments(src).replace(/\s+/g, ' ').trim();
}

// The one file-level STATE_KEY_COLLATION_KEY value in a source file.
function collationKeyValue(src, from){
    const all = collapse(src).match(/const STATE_KEY_COLLATION_KEY = [^;]+;/g) || [];
    assert.strictEqual(all.length, 1,
        'expected exactly one STATE_KEY_COLLATION_KEY declaration in ' + from + ', found ' + all.length);
    return all[0].replace(/^const STATE_KEY_COLLATION_KEY = /, '').replace(/;$/, '');
}

// The stateKeyCollate right-hand side in a function, with a local stateKeyBin inlined.
const COLLATION_GATE_RE = /^gateRegistry\.activeAt\((\w+), (.+?), (.+?), (\w+), (\w+)\) \? ('[^']*') : ('[^']*')$/;
function collationGate(fnSrc, from){
    const body = collapse(fnSrc);
    const collates = body.match(/let stateKeyCollate = [^;]+;/g) || [];
    assert.strictEqual(collates.length, 1,
        'expected exactly one `let stateKeyCollate = ...;` in ' + from + ', found ' + collates.length);
    let expr = collates[0].replace(/^let stateKeyCollate = /, '').replace(/;$/, '');
    if(/\bstateKeyBin\b/.test(expr)){
        const bins = body.match(/let stateKeyBin = [^;]+;/g) || [];
        assert.strictEqual(bins.length, 1,
            'stateKeyCollate in ' + from + ' reads stateKeyBin, but `let stateKeyBin = ...;` matched ' + bins.length + ' times');
        assert.ok(/^stateKeyBin \? /.test(expr),
            'stateKeyCollate in ' + from + ' must be exactly `stateKeyBin ? ... : ...`, got: ' + expr);
        expr = expr.replace(/^stateKeyBin/, bins[0].replace(/^let stateKeyBin = /, '').replace(/;$/, ''));
    }
    const m = expr.match(COLLATION_GATE_RE);
    assert.ok(m, 'state_key collation gate in ' + from + ' no longer has the shape ' +
        'gateRegistry.activeAt(KEY, NETWORK, COIN, HEIGHT, TIME) ? ON : OFF; got: ' + expr);
    return { key: m[1], network: m[2], coin: m[3], height: m[4], time: m[5], on: m[6], off: m[7] };
}

// Pin one copy's gate: identical everywhere except the declared per-host network/coin form.
function assertCollationGate(gate, from, network, coin){
    const want = { key: 'STATE_KEY_COLLATION_KEY', network: network, coin: coin, height: 'block_index',
        time: 'null', on: "' COLLATE utf8_bin'", off: "''" };
    for(const field of Object.keys(want)){
        assert.strictEqual(gate[field], want[field],
            'state_key collation gate ' + field + ' drifted in ' + from + ' (expected ' + want[field] +
            ', got ' + gate[field] + '); the gate decides the contract_state collation, a hash preimage input, ' +
            'and a one-sided edit forks contract_hash / block_merkle_root at the activation height');
    }
}

// The contract_state query assignment with every literal replaced by `_`.
function contractStateSplice(fnSrc, from){
    const src = stripComments(fnSrc);
    const head = '`SELECT cs.contract_index, cs.state_key, cs.state_value';
    const at = src.indexOf(head);
    assert.ok(at !== -1 && src.indexOf(head, at + 1) === -1,
        'expected exactly one contract_state gather query in ' + from);
    const shape = src.slice(at).replace(/`[^`]*`/g, '`_`');
    return shape.slice(0, shape.indexOf(';')).replace(/\s+/g, ' ').trim();
}

const CONTRACT_STATE_SPLICE = '`_` + stateKeyCollate + `_` + stateKeyCollate + `_`';
const LEAF_ROWS_SPLICE = 'queries.statePrefix + stateKeyCollate + queries.stateSuffix + stateKeyCollate + queries.stateOrder';

describe('consensus block-hash conformance twins (static drift-lock) @regression', function(){
    // Both copies are in THIS repo, so this case is never gated on the sibling checkout.
    it('state_key collation gate and splice match between BlockHasher and getBlockLeafRows', function(){
        const bhSrc = fs.readFileSync(syncFile('src/client/block_hasher.js'), 'utf8');
        const dbSrc = fs.readFileSync(syncFile('src/db/actions.js'), 'utf8');
        const bhFrom = 'xchain-sync/src/client/block_hasher.js', dbFrom = 'xchain-sync/src/db/actions.js';

        assert.strictEqual(collationKeyValue(dbSrc, dbFrom), collationKeyValue(bhSrc, bhFrom),
            'STATE_KEY_COLLATION_KEY drifted between ' + bhFrom + ' and ' + dbFrom);

        const hashFn = extractFunction(bhSrc, /async computeBlockHashes\(block_index, network, coin\)\{/, bhFrom);
        const leafFn = extractFunction(dbSrc, /async getBlockLeafRows\(block_index, conn, network, coin\)\{/, dbFrom);
        assertCollationGate(collationGate(hashFn, bhFrom + ' computeBlockHashes'), bhFrom, 'network', 'coin');
        assertCollationGate(collationGate(leafFn, dbFrom + ' getBlockLeafRows'), dbFrom, 'network', 'coin');

        assert.strictEqual(contractStateSplice(hashFn, bhFrom), CONTRACT_STATE_SPLICE,
            'the contract_state query in ' + bhFrom + ' must splice stateKeyCollate after GROUP BY and ORDER BY state_key');
        // getBlockLeafRows hands stateKeyCollate to getContractLeafRows, which does the splicing.
        const helper = collapse(extractFunction(dbSrc,
            /async function getContractLeafRows\(db, block_index, conn, stateKeyCollate, queries\)\{/, dbFrom));
        assert.strictEqual(helper.split(LEAF_ROWS_SPLICE).length - 1, 1,
            'getContractLeafRows in ' + dbFrom + ' must compose the contract_state query exactly as ' + LEAF_ROWS_SPLICE);
        assert.strictEqual(collapse(leafFn).split('getContractLeafRows(this, block_index, conn, stateKeyCollate, {').length - 1, 1,
            'getBlockLeafRows in ' + dbFrom + ' must pass its gated stateKeyCollate to getContractLeafRows exactly once');
    });

    it('state_key collation gate and splice match across xchain-sync and xchain-indexer', function(){
        const pair = loadPair(this, 'src/client/block_hasher.js', 'src/db/actions.js');
        if(!pair) return;
        const bhFrom = 'xchain-sync/src/client/block_hasher.js', idxFrom = 'xchain-indexer/src/db/actions.js';

        assert.strictEqual(collationKeyValue(pair.indexer, idxFrom), collationKeyValue(pair.sync, bhFrom),
            'STATE_KEY_COLLATION_KEY drifted between ' + bhFrom + ' and ' + idxFrom);

        const hashFn = extractFunction(pair.sync, /async computeBlockHashes\(block_index, network, coin\)\{/, bhFrom);
        const idxFn  = extractFunction(pair.indexer, /async getBlockHashContractStateRows\(block_index\)\{/, idxFrom);
        assertCollationGate(collationGate(hashFn, bhFrom + ' computeBlockHashes'), bhFrom, 'network', 'coin');
        // The indexer reads its own chain from config; that host difference is declared, not drift.
        assertCollationGate(collationGate(idxFn, idxFrom + ' getBlockHashContractStateRows'), idxFrom,
            "this.config['NETWORK']", "this.config['COIN']");

        assert.strictEqual(contractStateSplice(idxFn, idxFrom), CONTRACT_STATE_SPLICE,
            'the contract_state query in ' + idxFrom + ' must splice stateKeyCollate after GROUP BY and ORDER BY state_key');
        assert.strictEqual(contractStateSplice(hashFn, bhFrom), CONTRACT_STATE_SPLICE,
            'the contract_state query in ' + bhFrom + ' must splice stateKeyCollate after GROUP BY and ORDER BY state_key');
    });
});
