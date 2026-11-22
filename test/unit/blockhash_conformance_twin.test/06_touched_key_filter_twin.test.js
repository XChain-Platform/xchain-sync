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
 **********************************************************************
 * test/unit/blockhash_conformance_twin.test/06_touched_key_filter_twin.test.js
 *
 * Static drift-lock for the touched (address, tick) filter that picks the
 * balances_root leaves a block updates. The follower collector
 * (ClientApplier.collectSmtTouchedKeys) and its consumer
 * (computeFollowerBalancesRoot) must keep exactly the pairs the indexer's
 * ledgerKeysForBlock keeps, since that set is what the indexer's touch guard
 * checks its own touched set against. Each skip condition is negated clause by
 * clause and compared as a set with the indexer's keep condition.
 *
 * Skips when the xchain-indexer sibling is absent and fails where
 * XCHAIN_REQUIRE_SIBLINGS=1.
 */

'use strict';

const assert = require('assert');
const fs     = require('fs');
const { extractFunction, stripComments, syncFile, loadPair } = require('./helpers/twin_sources.js');

const LEDGER_READS = 'xchain-indexer/src/db/state_commitment/ledger_reads.js';

function clauseSet(cond, joiner){
    return new Set(cond.split(joiner).map(c => c.replace(/\s+/g, ' ').trim()));
}

// The indexer keeps a row when every clause of its condition holds.
function indexerKeepClauses(fnSrc){
    const m = stripComments(fnSrc).match(/if\((r\.address[^\n]*)\)\s*keys\.add\(/);
    assert.ok(m, 'ledgerKeysForBlock keep condition not found in ' + LEDGER_READS);
    return clauseSet(m[1].replace(/\br\./g, ''), '&&');
}

// The follower skips a pair when any clause holds; negating each clause gives the keep set.
function followerKeepClauses(fnSrc, from){
    const m = stripComments(fnSrc).match(/if\((address == null[^\n]*)\) continue;/);
    assert.ok(m, 'touched-pair skip condition not found in ' + from);
    let negated = Array.from(clauseSet(m[1], '||'))
        .map(c => c.replace(' === ', ' !== ').replace(' == ', ' != '));
    return new Set(negated);
}

function assertSameKeepRule(followerSet, indexerSet, from){
    assert.deepStrictEqual(Array.from(followerSet).sort(), Array.from(indexerSet).sort(),
        'the touched-pair filter in ' + from + ' no longer keeps the pairs ' + LEDGER_READS +
        ' ledgerKeysForBlock keeps. Change xchain-sync/src/client/applier.js collectSmtTouchedKeys, ' +
        'xchain-sync/src/state_commitment/index.js computeFollowerBalancesRoot and the indexer ' +
        'touched-set rules together.');
}

const COLLECTOR_SIG = /async collectSmtTouchedKeys\(data\)\{/;
const CONSUMER_SIG  = /async function computeFollowerBalancesRoot\(db, smt, chain, network, blockIndex, touchedKeys,/;
const LEDGER_SIG    = /async function ledgerKeysForBlock\(db, blockIndex\)\{/;

describe('touched-key filter twins @regression', function(){
    it('the follower collector keeps the pairs indexer ledgerKeysForBlock keeps', function(){
        const pair = loadPair(this, 'src/client/applier.js', 'src/db/state_commitment/ledger_reads.js');
        if(!pair) return;
        const indexerSet = indexerKeepClauses(extractFunction(pair.indexer, LEDGER_SIG, LEDGER_READS));
        const from = 'xchain-sync/src/client/applier.js';
        assertSameKeepRule(followerKeepClauses(extractFunction(pair.sync, COLLECTOR_SIG, from), from),
            indexerSet, from);
    });

    it('the follower balances_root consumer keeps the pairs indexer ledgerKeysForBlock keeps', function(){
        const pair = loadPair(this, 'src/state_commitment/index.js', 'src/db/state_commitment/ledger_reads.js');
        if(!pair) return;
        const indexerSet = indexerKeepClauses(extractFunction(pair.indexer, LEDGER_SIG, LEDGER_READS));
        const from = 'xchain-sync/src/state_commitment/index.js';
        assertSameKeepRule(followerKeepClauses(extractFunction(pair.sync, CONSUMER_SIG, from), from),
            indexerSet, from);
    });

    it('the comparison fails when one side gains an extra clause', function(){
        const src = fs.readFileSync(syncFile('src/client/applier.js'), 'utf8');
        const from = 'xchain-sync/src/client/applier.js';
        const real = followerKeepClauses(extractFunction(src, COLLECTOR_SIG, from), from);
        const mutated = src.replace("if(address == null || tick == null || tick === '') continue;",
            "if(address == null || address === '' || tick == null || tick === '') continue;");
        assert.notStrictEqual(mutated, src, 'mutation did not apply to the collector');
        const widened = followerKeepClauses(extractFunction(mutated, COLLECTOR_SIG, from), from);
        assert.throws(() => assertSameKeepRule(widened, real, from), /no longer keeps the pairs/);
        const indexerShaped = indexerKeepClauses(
            "async function ledgerKeysForBlock(db, blockIndex){ if(r.address != null && r.tick != null && r.tick !== '')\n keys.add(1); }");
        assert.throws(() => assertSameKeepRule(widened, indexerShaped, from), /no longer keeps the pairs/);
        assertSameKeepRule(real, indexerShaped, from);
    });
});
