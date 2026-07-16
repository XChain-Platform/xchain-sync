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
 * test/unit/blockhash-conformance-twin.test.js
 *
 * Static drift-lock for the consensus block-hash CONFORMANCE PAIR .
 *
 * xchain-sync/src/BlockHasher.js computeBlockHashes() is a hand-ported twin of
 * xchain-indexer/src/db.js getBlockHashes(): same consensus SELECTs, same
 * special-address canonicalization, same chaining/version fold, hashed through
 * the same getDataHash/jsonStringify pair. Unlike the whole-file twins locked
 * in rollback-coverage.test.js (stateHash.js, merkle.js, ...), the two live
 * inside DIFFERENT host structures (a db.js method vs a class here), so
 * whole-file byte-identity cannot apply. This test extracts the
 * consensus-bearing pieces from BOTH repos' sources and asserts them equal
 * after stripping comments and collapsing whitespace:
 *
 *   1. BLOCK_HASH_VERSION
 *   2. every consensus SQL literal, in gathering order (credits, debits,
 *      escrows, actions, contracts, contract_state, executions, emissions,
 *      deposits, withdrawals, previous-block hashes)
 *   3. the BURN/GAS/DONATE/REWARD canonicalization loops
 *   4. the hash-assembly tail (block_index / previous_hash / hash_version fold)
 *   5. utility jsonStringify + getDataHash (the shared preimage serializer)
 *   6. stateCommitment.js reportOrphanStats (documented byte-identical twin;
 *      compared RAW, header comment included, unlike the normalized checks)
 *
 * A one-sided edit to any of these forks every sync validator's recomputed
 * hash on the next real block (durable divergence halt fleet-wide). The
 * fixture-driven unit goldens only lock each side against ITSELF; the live
 * e2e recompute scenario (xchain-e2e-test consensusHashConformance) only runs
 * on a hand-launched regtest stack. This is the CI-time gate. The reciprocal
 * copy in xchain-indexer/test/unit/ fails indexer CI at the point of change.
 */

'use strict';

const assert  = require('assert');
const fs      = require('fs');
const path    = require('path');

// Sibling resolution + hard-fail policy: same conventions as
// rollback-coverage.test.js (see the comments there). Skip when the sibling
// checkout is absent, throw where XCHAIN_REQUIRE_SIBLINGS=1 makes
// green-by-skip impossible (bin/ci-all.sh and the sibling-checkout CI job).
const SYNC_ROOT    = path.resolve(__dirname, '..', '..');
const INDEXER_ROOT = process.env.XCHAIN_INDEXER_SQL_PATH
    ? path.resolve(process.env.XCHAIN_INDEXER_SQL_PATH, '..', '..')
    : path.resolve(__dirname, '..', '..', '..', 'xchain-indexer');
const SIBLING_REQUIRED = process.env.XCHAIN_REQUIRE_SIBLINGS === '1';
function requireSibling(ctx, absPath){
    if(fs.existsSync(absPath)) return true;
    if(SIBLING_REQUIRED)
        throw new Error('consensus drift guard cannot run: sibling missing at ' + absPath +
            ' (check out xchain-indexer or set XCHAIN_INDEXER_SQL_PATH)');
    ctx.skip();
    return false;
}

// ---- extraction helpers -----------------------------------------------------

// Cut unquoted // comments (tracking ' " ` quote state per line) so the two
// sides compare on code, not on their independently-worded comments.
function stripComments(src){
    return src.split('\n').map(line => {
        let q = null;
        for(let i = 0; i < line.length; i++){
            const ch = line[i];
            if(q){ if(ch === q && line[i-1] !== '\\') q = null; continue; }
            if(ch === "'" || ch === '"' || ch === '`'){ q = ch; continue; }
            if(ch === '/' && line[i+1] === '/') return line.slice(0, i);
        }
        return line;
    }).join('\n');
}

// Comment-stripped, string-concat-joined, whitespace-collapsed form. The `+`
// collapse keeps a template literal split by concatenation (the flag-day
// stateKeyCollate splice) comparable across formatting choices.
function normalize(src){
    return stripComments(src).replace(/\s+\+\s+/g, ' ').replace(/\s+/g, ' ').trim();
}

// Slice a balanced-brace function/method starting at the first match of sigRe.
// Tracks quote state so braces inside string/template literals don't count.
function extractFunction(src, sigRe, from){
    const m = src.match(sigRe);
    assert.ok(m, 'signature not found in ' + from + ': ' + sigRe);
    let depth = 0, q = null;
    for(let j = src.indexOf('{', m.index); j < src.length; j++){
        const ch = src[j];
        if(q){
            if(ch === '\\'){ j++; continue; }
            if(ch === q) q = null;
            continue;
        }
        if(ch === "'" || ch === '"' || ch === '`'){ q = ch; continue; }
        if(ch === '/' && src[j+1] === '/'){ j = src.indexOf('\n', j); continue; }
        if(ch === '{') depth++;
        if(ch === '}'){ depth--; if(depth === 0) return src.slice(m.index, j + 1); }
    }
    assert.fail('unbalanced braces extracting ' + sigRe + ' from ' + from);
}

// Ordered whitespace-collapsed template-literal list inside a function slice.
// A query spliced by concatenation yields one fragment per literal piece; both
// sides splice identically, so the fragment lists still compare pairwise.
function sqlLiterals(fnSrc){
    const out = [];
    const re = /`([^`]*)`/g;
    let m;
    while((m = re.exec(fnSrc)) !== null) out.push(m[1].replace(/\s+/g, ' ').trim());
    return out;
}

function syncFile(rel){ return path.join(SYNC_ROOT, rel); }
function indexerFile(rel){ return path.join(INDEXER_ROOT, rel); }

describe('consensus block-hash conformance twins (static drift-lock, ) @regression', function(){

    function loadPair(ctx, syncRel, indexerRel){
        if(!requireSibling(ctx, indexerFile(indexerRel))) return null;
        return {
            sync:    fs.readFileSync(syncFile(syncRel), 'utf8'),
            indexer: fs.readFileSync(indexerFile(indexerRel), 'utf8')
        };
    }

    it('BLOCK_HASH_VERSION is identical across BlockHasher.js and indexer db.js', function(){
        const pair = loadPair(this, 'src/BlockHasher.js', 'src/db.js');
        if(!pair) return;
        const vSync    = pair.sync.match(/const BLOCK_HASH_VERSION = (\d+)/);
        const vIndexer = pair.indexer.match(/const BLOCK_HASH_VERSION = (\d+)/);
        assert.ok(vSync && vIndexer, 'BLOCK_HASH_VERSION constant missing on one side');
        assert.strictEqual(vSync[1], vIndexer[1],
            'BLOCK_HASH_VERSION drifted between xchain-sync/src/BlockHasher.js and ' +
            'xchain-indexer/src/db.js; a version bump is a consensus break and MUST land on both sides');
    });

    it('every consensus SQL literal matches, in gathering order', function(){
        const pair = loadPair(this, 'src/BlockHasher.js', 'src/db.js');
        if(!pair) return;
        const syncFn    = stripComments(extractFunction(pair.sync,
            /async computeBlockHashes\(block_index, network, coin\)\{/, 'BlockHasher.js'));
        const indexerFn = stripComments(extractFunction(pair.indexer,
            /async getBlockHashes\(block_index\)\{/, 'db.js'));
        const syncSql    = sqlLiterals(syncFn);
        const indexerSql = sqlLiterals(indexerFn);
        assert.ok(syncSql.length >= 11,
            'expected the 11 consensus gathering queries in computeBlockHashes, found ' + syncSql.length +
            ' template literals; if the gathering set changed, mirror it on both sides and update this count');
        assert.strictEqual(syncSql.length, indexerSql.length,
            'consensus query count drifted between BlockHasher.computeBlockHashes (' + syncSql.length +
            ') and db.getBlockHashes (' + indexerSql.length + '); a query added/removed on one side forks the hash');
        for(let i = 0; i < syncSql.length; i++){
            assert.strictEqual(syncSql[i], indexerSql[i],
                'consensus SQL #' + (i + 1) + ' drifted between BlockHasher.computeBlockHashes and ' +
                'db.getBlockHashes; the SELECT column set / JOINs / ORDER BY are hash preimage inputs ' +
                'and MUST stay byte-identical (modulo whitespace)');
        }
    });

    it('special-address canonicalization covers credits, debits and escrows on both sides', function(){
        const pair = loadPair(this, 'src/BlockHasher.js', 'src/db.js');
        if(!pair) return;
        const loopRe = /for \(const row of ledger\.(credits|debits|escrows)\)\s+row\.address = canonicalizeHashAddress\(row\.address\);/g;
        for(const [name, src] of [['BlockHasher.js', pair.sync], ['db.js', pair.indexer]]){
            const seen = new Set();
            let m;
            loopRe.lastIndex = 0;
            while((m = loopRe.exec(src)) !== null) seen.add(m[1]);
            assert.deepStrictEqual([...seen].sort(), ['credits', 'debits', 'escrows'],
                name + ' must canonicalize BURN/GAS/DONATE/REWARD addresses on all three ledger row sets ' +
                'before hashing; a missing loop leaks the per-chain address encoding into the hash on one side only');
        }
    });

    it('the hash-assembly tail (chaining + hash_version fold) is identical', function(){
        const pair = loadPair(this, 'src/BlockHasher.js', 'src/db.js');
        if(!pair) return;
        const tailRe = /let tables = \[[^]*?tables\.forEach\(table => \{[^]*?\}\);/;
        const tSync    = pair.sync.match(tailRe);
        const tIndexer = pair.indexer.match(tailRe);
        assert.ok(tSync && tIndexer, 'hash-assembly tail (tables.forEach) not found on one side');
        assert.strictEqual(normalize(tSync[0]), normalize(tIndexer[0]),
            'hash-assembly tail drifted between BlockHasher.computeBlockHashes and db.getBlockHashes; ' +
            'the block_index / previous_hash / hash_version fold order is part of the preimage');
    });

    it('utility jsonStringify + getDataHash (shared preimage serializer) are identical', function(){
        const pair = loadPair(this, 'src/utility.js', 'src/utility.js');
        if(!pair) return;
        for(const sig of [/jsonStringify\(obj\)\{/, /getDataHash\(data\)\{/]){
            assert.strictEqual(
                normalize(extractFunction(pair.sync, sig, 'xchain-sync/src/utility.js')),
                normalize(extractFunction(pair.indexer, sig, 'xchain-indexer/src/utility.js')),
                sig + ' drifted between xchain-sync and xchain-indexer utility.js; it serializes every ' +
                'consensus hash preimage and MUST stay identical (bigint coercion included)');
        }
    });

    it('stateCommitment reportOrphanStats block is BYTE-identical (documented twin, comments included)', function(){
        const pair = loadPair(this, 'src/stateCommitment.js', 'src/stateCommitment.js');
        if(!pair) return;
        // The twin contract covers the whole block: the "---- Orphan-node
        // observability" header comment THROUGH the end of reportOrphanStats.
        // Raw byte comparison, no comment-stripping or whitespace-normalizing:
        // the header comment itself carries the twin contract.
        const sig = /async function reportOrphanStats\(query, chain, network, opts\)\{/;
        function extractTwinBlock(src, from){
            const marker = '// ---- Orphan-node observability';
            const i = src.indexOf(marker);
            assert.ok(i !== -1, 'orphan-observability marker not found in ' + from);
            const tail = src.slice(i);
            const fn = extractFunction(tail, sig, from);
            return tail.slice(0, tail.indexOf(fn) + fn.length);
        }
        assert.strictEqual(
            extractTwinBlock(pair.sync, 'xchain-sync/src/stateCommitment.js'),
            extractTwinBlock(pair.indexer, 'xchain-indexer/src/stateCommitment.js'),
            'reportOrphanStats block drifted between xchain-sync and xchain-indexer stateCommitment.js; ' +
            'the header comment declares it a keep-BYTE-IDENTICAL twin (comments included)');
    });
});
