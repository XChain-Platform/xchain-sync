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
 * test/unit/blockhash_conformance_twin.test/03_state_root_assembly_twin.test.js
 *
 * Static drift-lock for the state-root assembly functions the header of
 * src/state_commitment/index.js lists. The identical functions compare
 * code-for-code; each root-neutral shape difference is applied to the INDEXER's
 * code (every substitution asserts it fired) before the comparison, so a
 * one-sided edit anywhere else fails and points back at that header.
 *
 * Skips when the xchain-indexer sibling is absent and fails where
 * XCHAIN_REQUIRE_SIBLINGS=1.
 */

'use strict';

const assert = require('assert');

const { normalize, extractFunction, loadPair, sqlLiterals } = require('./helpers/twin_sources.js');

const HEADER = 'xchain-sync/src/state_commitment/index.js';

// Apply [find, replace] pairs to an indexer slice, asserting each one matched.
function applyDeclared(src, pairs, from){
    let out = src;
    for(const [find, replace] of pairs){
        assert.ok(out.includes(find),
            'the indexer code no longer has the shape this guard expects, in ' + from +
            '. Missing: ' + find + '\nRe-derive this case and the root-neutral differences ' +
            'listed in the ' + HEADER + ' header before trusting this guard again.');
        out = out.replace(find, replace);
    }
    return out;
}

describe('state-root assembly twins: identical functions @regression', function(){
    it('assembleStateRoot and extraSubRootColumn are identical to indexer state_root.js', function(){
        const pair = loadPair(this, 'src/state_commitment/index.js', 'src/state_commitment/state_root.js');
        if(!pair) return;
        for(const sig of [/function assembleStateRoot\(balancesRootHex, stakesRootHex, extraSubRoots\)\{/,
                          /function extraSubRootColumn\(extraSubRoots, slotName\)\{/]){
            assert.strictEqual(normalize(extractFunction(pair.sync, sig, HEADER)),
                normalize(extractFunction(pair.indexer, sig, 'xchain-indexer/src/state_commitment/state_root.js')),
                sig + ' differs between xchain-sync and xchain-indexer. Port the change, or move it ' +
                'out of the identical list in the ' + HEADER + ' header');
        }
    });

    it('reservedSubRootCandidates is identical to indexer state_commitment/index.js', function(){
        const pair = loadPair(this, 'src/state_commitment/index.js', 'src/state_commitment/index.js');
        if(!pair) return;
        const sig = /async function reservedSubRootCandidates\(db, chain, network, blockIndex\)\{/;
        assert.strictEqual(normalize(extractFunction(pair.sync, sig, HEADER)),
            normalize(extractFunction(pair.indexer, sig, 'xchain-indexer/src/state_commitment/index.js')),
            'reservedSubRootCandidates differs between xchain-sync and xchain-indexer. Port the ' +
            'change, or move it out of the identical list in the ' + HEADER + ' header');
    });

    it('shadowSubRoots is identical to indexer state_commitment/index.js', function(){
        const pair = loadPair(this, 'src/state_commitment/index.js', 'src/state_commitment/index.js');
        if(!pair) return;
        const sig = /async function shadowSubRoots\(db, chain, network, blockIndex\)\{/;
        assert.strictEqual(normalize(extractFunction(pair.sync, sig, HEADER)),
            normalize(extractFunction(pair.indexer, sig, 'xchain-indexer/src/state_commitment/index.js')),
            'shadowSubRoots differs between xchain-sync and xchain-indexer. Port the ' +
            'change, or move it out of the identical list in the ' + HEADER + ' header');
    });
});

describe('state-root assembly twins: computeBlockMerkleRoot @regression', function(){
    it('differs only by the collation arguments (declared)', function(){
        const pair = loadPair(this, 'src/state_commitment/index.js', 'src/state_commitment/state_root.js');
        if(!pair) return;
        const from = 'xchain-indexer/src/state_commitment/state_root.js';
        const idx = applyDeclared(
            normalize(extractFunction(pair.indexer, /async function computeBlockMerkleRoot\(db, blockIndex\)\{/, from)),
            [['async function computeBlockMerkleRoot(db, blockIndex){',
              'async function computeBlockMerkleRoot(db, blockIndex, network, coin){'],
             ['db.getBlockLeafRows(blockIndex)', 'db.getBlockLeafRows(blockIndex, undefined, network, coin)']],
            from);
        assert.strictEqual(normalize(extractFunction(pair.sync,
            /async function computeBlockMerkleRoot\(db, blockIndex, network, coin\)\{/, HEADER)), idx,
            'computeBlockMerkleRoot differs by more than the declared collation arguments. Port the ' +
            'change, or extend the root-neutral differences in the ' + HEADER + ' header');
    });
});

describe('state-root assembly twins: buildFullBalancesRoot @regression', function(){
    it('differs only by the inlined ledger read (declared)', function(){
        const pair = loadPair(this, 'src/state_commitment/index.js', 'src/state_commitment/full_balances_root.js');
        if(!pair) return;
        const ledger = loadPair(this, 'src/state_commitment/index.js', 'src/db/state_commitment/ledger_reads.js');
        const from = 'xchain-indexer/src/state_commitment/full_balances_root.js';
        const read = normalize(extractFunction(ledger.indexer, /async function getNonzeroNetBalances\(db\)\{/,
            'xchain-indexer/src/db/state_commitment/ledger_reads.js'));
        const m = read.match(/^async function getNonzeroNetBalances\(db\)\{ return (db\.doQueryStrict\(.*\)); \}$/);
        assert.ok(m, 'getNonzeroNetBalances is no longer a single doQueryStrict read; re-derive this case');
        const idx = applyDeclared(
            normalize(extractFunction(pair.indexer,
                /async function buildFullBalancesRootWith\(smt, db, chain, network, blockIndex, opts\)\{/, from)),
            [['async function buildFullBalancesRootWith(smt, db, chain, network, blockIndex, opts){',
              'async function buildFullBalancesRoot(db, chain, network, blockIndex, opts){ ' +
              'const smt = new PersistentSMT(new DbNodeStore(db));'],
             ['LEDGER.getNonzeroNetBalances(db)', m[1]],
             ['leafOrNull(r.net)', '_leafOrNull(r.net)']],
            from);
        assert.strictEqual(normalize(extractFunction(pair.sync,
            /async function buildFullBalancesRoot\(db, chain, network, blockIndex, opts\)\{/, HEADER)), idx,
            'buildFullBalancesRoot differs by more than the declared inlined ledger read. Port the ' +
            'change, or extend the root-neutral differences in the ' + HEADER + ' header');
    });
});

describe('state-root assembly twins: leaf encoders @regression', function(){
    it('differ only by name (declared)', function(){
        const pair = loadPair(this, 'src/state_commitment/index.js', 'src/state_commitment/leaf_values.js');
        if(!pair) return;
        const from = 'xchain-indexer/src/state_commitment/leaf_values.js';
        const zero = 'const ZERO_CANON = M.canonicalAmount(\'0\');';
        assert.ok(pair.sync.includes(zero) && pair.indexer.includes(zero), 'ZERO_CANON is no longer ' + zero);
        for(const [idxSig, idxName, syncSig, syncName] of [
            [/function canonicalAmountOf\(amountStr\)\{/, 'canonicalAmountOf', /function _nz\(amountStr\)\{/, '_nz'],
            [/function leafOrNull\(amountStr\)\{/, 'leafOrNull', /function _leafOrNull\(amountStr\)\{/, '_leafOrNull']
        ]){
            const renames = [['function ' + idxName + '(', 'function ' + syncName + '(']];
            if(idxName === 'leafOrNull') renames.push(['canonicalAmountOf(amountStr)', '_nz(amountStr)']);
            const idx = applyDeclared(normalize(extractFunction(pair.indexer, idxSig, from)), renames, from);
            assert.strictEqual(normalize(extractFunction(pair.sync, syncSig, HEADER)), idx,
                syncName + ' differs from the indexer ' + idxName + ' by more than its name. Port the ' +
                'change, or extend the root-neutral differences in the ' + HEADER + ' header');
        }
    });
});

const BATCH_FROM = 'xchain-indexer/src/db/state_commitment/ledger_reads.js';

// The indexer block path reads leaves through getNetBalances; pin it against the single-key read.
function assertBatchReadTwin(ctx, pair, m, rerun){
    const from = BATCH_FROM;
    const batchFn = extractFunction(pair.indexer,
        /async function getNetBalances\(db, keys\)\{/, from);
    const batchSql = sqlLiterals(batchFn);
    assert.strictEqual(batchSql.length, 1,
        'the indexer getNetBalances is no longer one SQL literal per chunk' + rerun);
    const batchMatch = batchSql[0].match(/^SELECT k\.i AS i, \((SELECT .+ FROM credits c .+)\) AS cr, \((SELECT .+ FROM debits d .+)\) AS dr FROM \(\$\{inputs\}\) k$/);
    assert.ok(batchMatch, 'the indexer getNetBalances SQL shape changed' + rerun);
    for(let i = 1; i <= 2; i++){
        const singleKey = batchMatch[i].replaceAll('a.address=k.address AND t.tick=k.tick',
            'a.address=? AND t.tick=?');
        assert.strictEqual(singleKey, m[i],
            'getNetBalances sum #' + i + ' differs from getNetBalance beyond its derived-key lookup' + rerun);
    }
    const batchNorm = normalize(batchFn);
    assert.ok(batchNorm.includes("const cr = r ? String(r.cr) : '0'; const dr = r ? String(r.dr) : '0';"),
        'getNetBalances no longer defaults a missing key to zero' + rerun);
    assert.ok(batchNorm.includes('db.util.bcstr(db.util.bcsub(cr, dr, 18))'),
        'getNetBalances no longer uses the pinned balance-leaf render' + rerun);

    const route = loadPair(ctx, 'src/state_commitment/index.js', 'src/state_commitment/index.js');
    if(!route) return;
    const thread = extractFunction(route.indexer,
        /async function threadBalancesRoot\(/, 'xchain-indexer/src/state_commitment/index.js');
    assert.ok(thread.includes('await getNetBalances(') && !thread.includes('await getNetBalance('),
        'the follower guards must bind whichever net-balance read threadBalancesRoot calls; ' +
        'retarget this case and the e2e suite if the indexer block path changes');
}

// The balances_root leaf value. The indexer selects the two sums and subtracts in mathjs;
// this repo subtracts in SQL and renders through minimalDecimal. The e2e conformance suite
// proves the renders agree on a database, so this tier pins the code that suite exercises.
describe('state-root assembly twins: getNetBalance @regression', function(){
    const sig = /async function getNetBalance\(db, address, tick\)\{/;
    const from = 'xchain-indexer/src/db/state_commitment/ledger_reads.js';
    const rerun = '. Re-run test/e2e/state_commitment_conformance.test.js against a database ' +
        'before updating this case, and re-derive the getNetBalance difference in the ' + HEADER + ' header';

    it('runs the same credit and debit sums on both sides (declared render difference)', function(){
        const pair = loadPair(this, 'src/state_commitment/index.js', 'src/db/state_commitment/ledger_reads.js');
        if(!pair) return;
        const idxFn = extractFunction(pair.indexer, sig, from);
        const syncFn = extractFunction(pair.sync, sig, HEADER);
        const idxSql = sqlLiterals(idxFn);
        assert.strictEqual(idxSql.length, 1, 'the indexer getNetBalance is no longer one SQL literal' + rerun);
        const m = idxSql[0].match(/^SELECT \((SELECT .+ FROM credits c .+)\) AS cr, \((SELECT .+ FROM debits d .+)\) AS dr$/);
        assert.ok(m, 'the indexer getNetBalance no longer selects (credits) AS cr, (debits) AS dr' + rerun);
        for(const sum of [m[1], m[2]])
            assert.ok(sum.includes('SUM(CAST(') && sum.includes(' AS DECIMAL(60,18))'),
                'an indexer getNetBalance sum no longer casts each amount to DECIMAL(60,18): ' + sum + rerun);
        const call = syncFn.match(/\$\{minimalDecimal\(([\s\S]*?)\)\} AS net`/);
        assert.ok(call, 'the ' + HEADER + ' getNetBalance no longer renders its net through minimalDecimal' + rerun);
        assert.strictEqual(call[1].replace(/'[^']*'/g, '').replace(/[\s+]/g, ''), '',
            'the minimalDecimal argument in ' + HEADER + ' getNetBalance is no longer plain string pieces' + rerun);
        const syncExpr = [...call[1].matchAll(/'([^']*)'/g)].map(p => p[1]).join('').replace(/\s+/g, ' ').trim();
        assert.strictEqual(syncExpr, '( (' + m[1] + ') - (' + m[2] + ') )',
            'getNetBalance sums differ between xchain-sync and xchain-indexer' + rerun);
        const params = '[address, tick, address, tick]);';
        assert.ok(normalize(idxFn).endsWith(params + " const cr = rows.length ? String(rows[0].cr) : '0'; " +
            "const dr = rows.length ? String(rows[0].dr) : '0'; return db.util.bcstr(db.util.bcsub(cr, dr, 18)); }"),
            'the indexer getNetBalance params or bcstr(bcsub(cr, dr, 18)) tail changed' + rerun);
        assert.ok(normalize(syncFn).endsWith(params + " return rows.length ? String(rows[0].net) : '0'; }"),
            'the ' + HEADER + ' getNetBalance params or net tail changed' + rerun);

        assertBatchReadTwin(this, pair, m, rerun);
    });

    it('pins the two renders the leaf value goes through', function(){
        const pair = loadPair(this, 'src/db/balance_helpers.js', 'src/utility/bcmath.js');
        if(!pair) return;
        const bcmath = 'xchain-indexer/src/utility/bcmath.js';
        for(const [src, re, at, expected] of [
            [pair.indexer, /bcnum\(num\)\{/, bcmath,
             "bcnum(num){ let str = String(num).trim(); if(str === 'NaN' || str === 'Infinity' || " +
             "str === '-Infinity' || !this.isNumeric(num)) return mathjs.bignumber(0); return mathjs.bignumber(str); }"],
            [pair.indexer, /bcstr\(num\)\{/, bcmath, 'bcstr(num){ return this.bcnum(num).toFixed(); }'],
            [pair.indexer, /bcsub\(numA, numB, decimals\)\{/, bcmath,
             'bcsub(numA, numB, decimals){ let a = (!this.isNull(numA)) ? numA : 0; let b = (!this.isNull(numB)) ? ' +
             'numB : 0; let d = (!this.isNull(decimals)) ? parseInt(decimals) : 0; return this.bcnum(mathjs.format(' +
             "mathjs.subtract(mathjs.bignumber(a),mathjs.bignumber(b)),{notation: 'fixed', precision: d})); }"],
            [pair.sync, /function minimalDecimal\(sumExpr\) \{/, 'xchain-sync/src/db/balance_helpers.js',
             "function minimalDecimal(sumExpr) { return \"TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM CAST(\" " +
             "sumExpr ' AS CHAR)))'; }"],
        ]) assert.strictEqual(normalize(extractFunction(src, re, at)), expected, 'the leaf render in ' + at + ' changed' + rerun);
    });
});

// The balances_root full-build gate. Both twins pick a full rebuild over incremental
// threading on the activation block, on the escrow-leaf ARMING block, and when the prior
// block has no roots row; one side disagreeing forks balances_root at that height. The
// shapes differ (the indexer hoists armingBlock out of writeEscrowJournalForBlock and
// reads a row list, this side reads one row or null), so each part is pinned on its own
// and the gate disjuncts are compared as sets after the declared !prior.length mapping.
const IDX = 'xchain-indexer/src/state_commitment/index.js';
const rerun = '. Re-derive this case and the balances full-build gate difference in the ' + HEADER + ' header';
const fnOf = (src, name, from) => normalize(extractFunction(src, new RegExp('(?:async )?function ' + name + '\\('), from));
const loadGate = ctx => loadPair(ctx, 'src/state_commitment/index.js', 'src/state_commitment/index.js');

// The condition of the first if() in a balances function, which must be the full-build
// gate: a flat || list whose branch calls buildFullBalancesRoot and returns a null shadow.
function gateOf(fn, from){
    const m = fn.match(/\bif\(([^()]*)\)\{/);
    assert.ok(m, 'no flat if() gate in ' + from + rerun);
    const disjuncts = m[1].split(' || ');
    for(const d of disjuncts)
        assert.ok(/^!?[A-Za-z_][A-Za-z_.]*$/.test(d), 'the gate in ' + from + ' is no longer a flat || list of names: ' + m[1] + rerun);
    const rest = fn.slice(m.index);
    const ret = rest.match(/return \{ ([^}]*)\}/);
    assert.ok(ret, 'no return after the gate in ' + from + rerun);
    assert.ok(rest.slice(0, ret.index).includes('await buildFullBalancesRoot(db, chain, network, blockIndex)'),
        'the gate branch in ' + from + ' no longer calls buildFullBalancesRoot(db, chain, network, blockIndex)' + rerun);
    assert.ok(/balancesRoot,? /.test(ret[1]) && ret[1].includes('shadowBalanceUpdates: null'),
        'the gate branch in ' + from + ' no longer returns the full-built root with a null shadow' + rerun);
    return disjuncts;
}

describe('state-root assembly twins: balances_root gate inputs @regression', function(){
    it('computes armingBlock from the same two height-gate calls', function(){
        const pair = loadGate(this);
        if(!pair) return;
        const idxFn = fnOf(pair.indexer, 'writeEscrowJournalForBlock', IDX);
        const escArmed = idxFn.match(/const escArmed = ([^;]+);/);
        const idxArming = idxFn.match(/const armingBlock = ([^;]+);/);
        assert.ok(escArmed && idxArming, 'the indexer no longer declares escArmed and armingBlock' + rerun);
        const idx = applyDeclared(idxArming[1], [['escArmed && ', escArmed[1] + ' && ']], IDX);
        const syncArming = fnOf(pair.sync, 'computeFollowerBalancesRoot', HEADER).match(/const armingBlock = ([^;]+);/);
        assert.ok(syncArming, 'the follower no longer declares armingBlock' + rerun);
        assert.strictEqual(syncArming[1], idx, 'armingBlock differs between xchain-sync and xchain-indexer' + rerun);
        assert.ok(idx.includes('!SUB.isEscrowLockedLeafActive(blockIndex - 1, network, chain)'),
            'armingBlock no longer compares against the previous block' + rerun);
    });

    it('hands the indexer armingBlock to balancesRootForBlock in the position its signature names', function(){
        const pair = loadGate(this);
        if(!pair) return;
        const write = fnOf(pair.indexer, 'writeEscrowJournalForBlock', IDX);
        assert.ok(write.includes('return { escShadow, armingBlock }'), 'writeEscrowJournalForBlock no longer returns armingBlock' + rerun);
        const store = fnOf(pair.indexer, 'computeAndStoreRoots', IDX);
        assert.ok(store.includes('const { escShadow, armingBlock } = await writeEscrowJournalForBlock(db, chain, network, blockIndex);'),
            'computeAndStoreRoots no longer takes armingBlock from writeEscrowJournalForBlock' + rerun);
        const params = normalize(pair.indexer).match(/async function balancesRootForBlock\(([^)]*)\)/);
        const call = store.match(/await balancesRootForBlock\(([^)]*)\)/);
        assert.ok(params && call, 'balancesRootForBlock signature or call not found' + rerun);
        assert.strictEqual(call[1].trim(), params[1].trim(), 'the balancesRootForBlock call arguments no longer line up with its parameters' + rerun);
        assert.ok(params[1].trim().split(', ').includes('armingBlock'), 'balancesRootForBlock no longer takes armingBlock' + rerun);
    });

    it('reads the prior root of blockIndex - 1 on both sides', function(){
        const pair = loadGate(this);
        const roots = loadPair(this, 'src/db/state_tree_roots.js', 'src/db/state_commitment/roots.js');
        if(!pair || !roots) return;
        const bal = fnOf(pair.indexer, 'balancesRootForBlock', IDX);
        assert.ok(bal.includes('const prior = isActivationBlock ? [] : await getPriorBalancesRoot(db, chain, network, blockIndex - 1);'),
            'the indexer prior lookup is no longer getPriorBalancesRoot(..., blockIndex - 1)' + rerun);
        const idxRead = fnOf(roots.indexer, 'getPriorBalancesRoot', 'xchain-indexer/src/db/state_commitment/roots.js');
        assert.ok(idxRead.includes("'SELECT balances_root FROM state_tree_roots WHERE chain=? AND network=? AND block_index=? LIMIT 1', " +
            '[chain, network, priorBlockIndex]'), 'the indexer prior-root read changed its key columns' + rerun);
        assert.strictEqual(fnOf(pair.sync, 'lookupFollowerPriorRoot', HEADER),
            'function lookupFollowerPriorRoot(db, chain, network, blockIndex){ return db.getStateRootsRow(chain, network, blockIndex - 1); }',
            'the follower prior lookup is no longer getStateRootsRow(..., blockIndex - 1)' + rerun);
        const follower = fnOf(pair.sync, 'computeFollowerBalancesRoot', HEADER);
        assert.ok(follower.includes('const prior = isActivationBlock ? null : await lookupFollowerPriorRoot(db, chain, network, blockIndex);') &&
            follower.includes('const noPriorRoot = !(prior && prior.balances_root);'), 'the follower prior or noPriorRoot declaration changed' + rerun);
        const syncRead = normalize(extractFunction(roots.sync, /async getStateRootsRow\(/, 'xchain-sync/src/db/state_tree_roots.js'));
        assert.ok(syncRead.includes('FROM state_tree_roots WHERE chain=? AND network=? AND block_index=? LIMIT 1`, [chain, network, block_index], conn);') &&
            syncRead.endsWith('return rows.length ? rows[0] : null; }'), 'the follower roots-row read changed its key columns or its null answer' + rerun);
    });

});

describe('state-root assembly twins: balances_root full-build gate @regression', function(){
    it('takes the same full-build branch conditions on both sides (declared !prior.length mapping)', function(){
        const pair = loadGate(this);
        if(!pair) return;
        const idx = gateOf(fnOf(pair.indexer, 'balancesRootForBlock', IDX), IDX).map(d => d === '!prior.length' ? 'noPriorRoot' : d);
        const sync = gateOf(fnOf(pair.sync, 'computeFollowerBalancesRoot', HEADER), HEADER);
        assert.deepStrictEqual([...sync].sort(), [...idx].sort(),
            'the full-build gate disjuncts differ between xchain-sync and xchain-indexer' + rerun);
        assert.ok(sync.includes('armingBlock') && sync.includes('isActivationBlock') && sync.includes('noPriorRoot'),
            'the full-build gate lost one of its three triggers' + rerun);
    });

    it('keeps balances_root NOT NULL, the only reason !prior.length and noPriorRoot select the same blocks', function(){
        const pair = loadPair(this, 'src/sql/state_tree_roots.sql', 'src/sql/state_tree_roots.sql');
        if(!pair) return;
        for(const [side, src] of [['xchain-sync', pair.sync], ['xchain-indexer', pair.indexer]])
            assert.ok(/\bbalances_root\s+CHAR\(64\)\s+NOT NULL\b/.test(src),
                side + ' state_tree_roots.sql no longer declares balances_root CHAR(64) NOT NULL; a prior row with a ' +
                'NULL root would full-build here and thread there' + rerun);
    });
});
