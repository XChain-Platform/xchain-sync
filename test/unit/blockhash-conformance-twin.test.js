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
 * Static drift-lock for the consensus block-hash CONFORMANCE PAIR.
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

describe('consensus block-hash conformance twins (static drift-lock) @regression', function(){

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

    // THIRD copy of the same gathering set, and the one nothing pinned. The indexer
    // needs no third copy (its getBlockLeafRows replays the getBlockHashes stash), but
    // this repo hand-maintains one in db.getBlockLeafRows to rebuild block_merkle_root
    // on the follower. A one-sided edit to a column set, JOIN, COLLATE or ORDER BY
    // there forks the recomputed root at the next block with the pairwise check above
    // still green, and the follower's root is served by api.js and trusted by the
    // published light client.
    //
    // Deliberately NOT routed through loadPair()/requireSibling(): both files are in
    // THIS repo, and gating on the xchain-indexer checkout would let the case skip
    // green exactly where the gap it closes lives. computeBlockHashes carries one
    // EXTRA trailing literal, the previous-block-hash chaining query, which has no
    // counterpart in the leaf-row gathering; everything before it must match pairwise.
    it('getBlockLeafRows gathers the same consensus SQL as BlockHasher, in order', function(){
        const dbSrc = fs.readFileSync(syncFile('src/db.js'), 'utf8');
        const bhSrc = fs.readFileSync(syncFile('src/BlockHasher.js'), 'utf8');
        const leafSql = sqlLiterals(stripComments(extractFunction(dbSrc,
            /async getBlockLeafRows\(block_index, conn, network, coin\)\{/, 'db.js')));
        const hashSql = sqlLiterals(stripComments(extractFunction(bhSrc,
            /async computeBlockHashes\(block_index, network, coin\)\{/, 'BlockHasher.js')));
        assert.ok(leafSql.length >= 12,
            'expected the 12 gathering fragments in getBlockLeafRows (the contract_state query is ' +
            'spliced into three by the stateKeyCollate flag-day concatenation), found ' + leafSql.length +
            '; if the gathering set changed, mirror it in BlockHasher and update this count');
        assert.strictEqual(hashSql.length, leafSql.length + 1,
            'computeBlockHashes must carry exactly the getBlockLeafRows fragments plus the trailing ' +
            'previous-block-hash chaining query; counts are ' + hashSql.length + ' vs ' + leafSql.length +
            ', so a query was added or removed on one side and the recomputed roots have diverged');
        for(let i = 0; i < leafSql.length; i++){
            assert.strictEqual(leafSql[i], hashSql[i],
                'consensus SQL #' + (i + 1) + ' drifted between db.getBlockLeafRows and ' +
                'BlockHasher.computeBlockHashes; these SELECTs are block_merkle_root preimage inputs ' +
                'and MUST stay byte-identical (modulo whitespace)');
        }
        assert.ok(/FROM blocks b/.test(hashSql[hashSql.length - 1]),
            'the extra computeBlockHashes literal must be the previous-block-hash chaining query; ' +
            'if it is a gathering SELECT instead, getBlockLeafRows is missing one');
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

    // ---- SMT engine twin ----------------------------------------------------
    //
    // The follower header calls the SMT engine byte-identical to the indexer's,
    // and until these cases nothing checked it: the whole-file loop in
    // rollback-coverage.test.js never listed stateCommitment.js, and the case
    // above pins only reportOrphanStats. The engine HAD drifted - the indexer
    // grew a read-through node cache the follower has not - and the stale claim
    // is why nobody had to look. Both halves are pinned now: what is still
    // shared, and the shape of the one divergence the header declares.

    it('DbNodeStore and MemoryNodeStore are BYTE-identical (node-store twin)', function(){
        const pair = loadPair(this, 'src/stateCommitment.js', 'src/stateCommitment.js');
        if(!pair) return;
        // Raw, comments included: these two stores define what a node row IS on
        // both sides of the recompute, so a one-sided edit is a fork risk even
        // when it reads as a refactor.
        function stores(src, from){
            const i = src.indexOf('class DbNodeStore');
            const j = src.indexOf('// ---- Persistent SMT engine');
            assert.ok(i !== -1 && j > i, 'node-store block markers not found in ' + from);
            return src.slice(i, j);
        }
        assert.strictEqual(
            stores(pair.sync, 'xchain-sync/src/stateCommitment.js'),
            stores(pair.indexer, 'xchain-indexer/src/stateCommitment.js'),
            'the DbNodeStore / MemoryNodeStore block drifted between xchain-sync and ' +
            'xchain-indexer stateCommitment.js; the follower header declares it byte-identical');
    });

    it('PersistentSMT update / buildFull / prove are BYTE-identical (root-bearing twin)', function(){
        const pair = loadPair(this, 'src/stateCommitment.js', 'src/stateCommitment.js');
        if(!pair) return;
        // These three are the whole root-producing surface of the engine. The
        // declared cache divergence lives entirely in _descend's read and
        // _putBatch's write, so these must stay equal to the byte: a difference
        // here IS a state_root fork, and the follower halts the fleet on it.
        for(const sig of [/async update\(rootHex, keyBuf, newLeafHexOrNull\)\{/,
                          /async buildFull\(entries\)\{/,
                          /async prove\(rootHex, keyBuf\)\{/]){
            assert.strictEqual(
                extractFunction(pair.sync, sig, 'xchain-sync/src/stateCommitment.js'),
                extractFunction(pair.indexer, sig, 'xchain-indexer/src/stateCommitment.js'),
                sig + ' drifted between xchain-sync and xchain-indexer PersistentSMT; ' +
                'this is the root-producing surface and it must stay byte-identical');
        }
    });

    it('PersistentSMT divergence is exactly the indexer node cache (declared, not drift)', function(){
        const pair = loadPair(this, 'src/stateCommitment.js', 'src/stateCommitment.js');
        if(!pair) return;
        // The follower header names ONE divergence and argues why it is
        // root-neutral. This case is what makes that paragraph binding: it fails
        // if the indexer drops the cache, if the follower gains one, or if the
        // divergence spreads past the two methods it is allowed to touch.
        // Scoped to the engine block: the follower's file header NAMES the cache
        // fields in the paragraph that declares the divergence, and a whole-file
        // search would read that prose as the cache itself.
        function engine(src, from){
            const i = src.indexOf('// ---- Persistent SMT engine');
            const j = src.indexOf('// Every EMPTY[h] constant, hex.');
            assert.ok(i !== -1 && j > i, 'SMT engine block markers not found in ' + from);
            return src.slice(i, j);
        }
        const syncEngine    = engine(pair.sync, 'xchain-sync/src/stateCommitment.js');
        const indexerEngine = engine(pair.indexer, 'xchain-indexer/src/stateCommitment.js');
        for(const marker of ['SMT_NODE_CACHE_MAX', '_nodeCache', '_cacheGet(', '_cachePut(']){
            assert.ok(indexerEngine.includes(marker),
                'xchain-indexer stateCommitment.js no longer has ' + marker + '. If the node ' +
                'cache was removed the engines are byte-identical again: port the change and ' +
                'rewrite the DECLARED DIVERGENCE paragraph in xchain-sync/src/stateCommitment.js');
            assert.ok(!syncEngine.includes(marker),
                'xchain-sync stateCommitment.js now has ' + marker + '. If the cache was ' +
                'deliberately ported, rewrite the DECLARED DIVERGENCE paragraph in that file ' +
                'and replace this case with a full byte comparison of the engine block');
        }

        // Nothing else may differ. Subtract the cache from the INDEXER's two
        // remaining methods and they must equal the follower's, code-for-code
        // (comments stripped: independently-worded prose is not a fork, and one
        // word of it had already drifted - "pre-batch" vs "pre-batching").
        // Every substitution asserts it FIRED, so a reworded cache cannot pass
        // this case by silently matching nothing.
        function subtract(src, from, pairs){
            let out = src;
            for(const [find, replace] of pairs){
                assert.ok(out.includes(find),
                    'the indexer node cache no longer has the shape this guard subtracts, in ' +
                    from + '. Missing: ' + find + '\nRe-derive the subtraction against ' +
                    'xchain-indexer/src/stateCommitment.js before trusting this guard again.');
                out = out.replace(find, replace);
            }
            return out;
        }
        const descendSig  = /async _descend\(rootHex, keyBuf\)\{/;
        const putBatchSig = /async _putBatch\(nodes\)\{/;

        const idxDescend = subtract(
            normalize(extractFunction(pair.indexer, descendSig, 'xchain-indexer _descend')),
            '_descend',
            [['let row = this._cacheGet(cur); if(row === undefined){ row = await this.store.get(cur); ' +
              'if(row) this._cachePut(cur, row.left_hash, row.right_hash); }',
              'const row = await this.store.get(cur);']]);
        assert.strictEqual(normalize(extractFunction(pair.sync, descendSig, 'xchain-sync _descend')),
            idxDescend,
            '_descend differs between xchain-sync and xchain-indexer by more than the declared ' +
            'node-cache read. Port the change, or extend the DECLARED DIVERGENCE paragraph in ' +
            'xchain-sync/src/stateCommitment.js to say what else may differ');

        const idxPutBatch = subtract(
            normalize(extractFunction(pair.indexer, putBatchSig, 'xchain-indexer _putBatch')),
            '_putBatch',
            [['} else { for(const n of nodes) await this.store.put(n.hash, n.left, n.right); } ' +
              'for(const n of nodes) this._cachePut(n.hash, n.left, n.right); }',
              'return; } for(const n of nodes) await this.store.put(n.hash, n.left, n.right); }']]);
        assert.strictEqual(normalize(extractFunction(pair.sync, putBatchSig, 'xchain-sync _putBatch')),
            idxPutBatch,
            '_putBatch differs between xchain-sync and xchain-indexer by more than the declared ' +
            'node-cache seeding. Port the change, or extend the DECLARED DIVERGENCE paragraph in ' +
            'xchain-sync/src/stateCommitment.js to say what else may differ');
    });
});
