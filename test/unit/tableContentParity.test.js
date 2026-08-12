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
// Advisory table-content parity (TABLE_CONTENT_PARITY_CHECK).
//
// The gap this closes: getReplicatedTables('indexer') returns 107 per-block
// replicated tables, while the block hashes commit the ledger/actions/contract
// projections, state_hash commits the in-place mutation classes, the state
// commitment covers balances + stakes, computeIndexMapChecksum covers
// index_addresses, and _verifyTableCounts compares CARDINALITY only (and only
// flags remote > local). An equal-count content substitution in any other
// replicated table therefore passed every check a follower ran.
//
// Acceptance cases, modelled on test/integration/index-map-parity.test.js:
//   1. every replicated table is committed by something: covered here, or
//      knowingly excluded by one of the two declared classes;
//   2. a faithful replica matches the source, digest-for-digest;
//   3. equal row count + substituted content DIVERGES (the case counts miss);
//   4. row ORDER is not content: a reordered table still matches;
//   5. a by-design column difference (the stripped blocks.id surrogate) does
//      NOT raise a false alarm;
//   6. a row-count difference is SKIPPED, not reported as divergence;
//   7. the check never halts, and one unreadable table cannot sink the pass.

const assert = require('assert');

const lifecycle       = require('../../src/tableLifecycle');
const replicatedTables = require('../../src/replicatedTables');
const BlockHasher     = require('../../src/BlockHasher');
const HashVerifier    = require('../../src/HashVerifier');
const Utility         = require('../../src/utility');

// A fake Database serving canned per-table windows. `tables` maps a table name to
// its rows; anything absent is simply an empty window (omitted from the result).
function fakeDb(tables, opts){
    let o = opts || {};
    return {
        dbType: o.dbType || 'indexer',
        calls: [],
        async listExistingTables(){
            if(o.listThrows) throw new Error('information_schema unavailable');
            return new Set(Object.keys(tables));
        },
        async getContentWindowRows(table, bound, fromBlock, toBlock){
            this.calls.push({ table, bound, fromBlock, toBlock });
            if(o.throwFor && o.throwFor.has(table)) throw Object.assign(new Error('no such table'), { errno: 1146 });
            return (tables[table] || []).slice();
        },
        async getMaxRowId(table){
            this.calls.push({ table, maxId: true });
            let rows = tables[table] || [];
            return rows.length ? Math.max(...rows.map(r => Number(r.id))) : null;
        },
        async getContentIdWindowRows(table, fromId, toId){
            this.calls.push({ table, fromId, toId });
            return (tables[table] || []).filter(r => Number(r.id) > fromId && Number(r.id) <= toId);
        }
    };
}

const hasherFor = (tables, opts) => new BlockHasher(fakeDb(tables, opts), new Utility());

describe('Advisory table-content parity', function(){

    describe('coverage contract', function(){

        // The finding in one assertion: no replicated table may be silently
        // uncommitted. Every table in the per-block replicated set is either in the
        // content-parity plan or carries a declared reason for being out.
        for(const dbType of ['indexer', 'decoder']){
            it('every replicated ' + dbType + ' table is either covered or declared excluded', function(){
                let replicated = replicatedTables.getReplicatedTables(dbType);
                let covered    = new Set(replicatedTables.contentParityPlan(dbType).map(p => p.table));
                let excluded   = replicatedTables.contentParityExclusions(dbType);

                let orphans = replicated.filter(t => !covered.has(t) && excluded[t] === undefined);
                assert.deepStrictEqual(orphans, [],
                    'replicated tables with NO content commitment and NO declared exclusion: ' + orphans.join(', ') +
                    '. Add the table to the content-parity plan or declare why it cannot be bounded ' +
                    '(src/tableLifecycle.js CONTENT_PARITY_*).');

                // ...and nothing is claimed twice, which would hide an exclusion behind
                // a check that never runs (or vice versa).
                let both = replicated.filter(t => covered.has(t) && excluded[t] !== undefined);
                assert.deepStrictEqual(both, [], 'tables both covered and excluded: ' + both.join(', '));
            });
        }

        it('the operator carve-outs are exactly markets (indexer) and dispensers (decoder)', function(){
            // Pinned to the 2026-08-11 ruling. Widening this set is an operator
            // decision, not a code change: each carve-out is a replicated table left
            // with no content commitment at all.
            assert.deepStrictEqual(
                lifecycle.CONTENT_PARITY_CARVE_OUTS.map(c => c.dbType + ':' + c.table).sort(),
                ['decoder:dispensers', 'indexer:markets']);
            for(let c of lifecycle.CONTENT_PARITY_CARVE_OUTS)
                assert.ok(c.reason && c.reason.length > 20, c.table + ' carve-out must carry its reason');
        });

        it('the indexer dispensers table is covered even though the decoder one is carved out', function(){
            // Same table name, different schema and different lifecycle: the carve-out
            // is dbType-scoped, and treating it as a bare name would silently drop 1 of
            // the 93 covered indexer tables.
            assert.strictEqual(lifecycle.contentParityCarveOut('dispensers', 'decoder') !== null, true);
            assert.strictEqual(lifecycle.contentParityCarveOut('dispensers', 'indexer'), null);
            assert.ok(replicatedTables.contentParityPlan('indexer').some(p => p.table === 'dispensers'));
            assert.ok(!replicatedTables.contentParityPlan('decoder').some(p => p.table === 'dispensers'));
        });

        it('the second exclusion class is exactly the state_hash (in-place mutated) tables', function(){
            // These are excluded because they are ALREADY committed by the enforced
            // state_hash, and because an in-place edit in a later block moves content
            // inside an already-published window. Derived from the hash declarations,
            // so a new mutation class cannot join one without the other.
            let mutable  = new Set(lifecycle.contentParityMutableTables());
            let excluded = replicatedTables.contentParityExclusions('indexer');
            let carved   = new Set(lifecycle.CONTENT_PARITY_CARVE_OUTS.map(c => c.table));
            for(let table of Object.keys(excluded)){
                if(carved.has(table)) continue;
                assert.ok(mutable.has(table),
                    table + ' is excluded from content parity but declares no state_hash class; ' +
                    'an uncommitted table must not be silently dropped');
            }
            for(let table of ['stakes', 'bets', 'tokens', 'credits'])
                assert.ok(excluded[table], table + ' mutates in place and must ride state_hash, not the window checksum');
        });

        it('bounds match the scope each table actually replicates through', function(){
            let byTable = {};
            for(let p of replicatedTables.contentParityPlan('indexer')) byTable[p.table] = p.bound;
            assert.strictEqual(byTable['blocks'], 'block');
            assert.strictEqual(byTable['sends'], 'action');
            assert.strictEqual(byTable['sync_meta'], 'block');
            // contract_emissions carries a NULL action_index for internal emissions, so
            // it must NOT ride the generic action join (which would drop them).
            assert.strictEqual(byTable['contract_emissions'], 'emission');
            // The two reorg-scoped lookups carry a block stamp; the inert ones do not.
            assert.strictEqual(byTable['index_addresses'], 'block');
            assert.strictEqual(byTable['index_tickers'], 'block');
            assert.strictEqual(byTable['index_statuses'], 'id');

            let decoder = {};
            for(let p of replicatedTables.contentParityPlan('decoder')) decoder[p.table] = p.bound;
            assert.strictEqual(decoder['transaction_outputs'], 'tx');
            // The decoder schema stamps no block on its lookups, so index_addresses is
            // id-bounded there even though the indexer's copy is block-bounded.
            assert.strictEqual(decoder['index_addresses'], 'id');
        });

        it('the block-bounded read excludes NULL-block rows, as the index-map checksum does', function(){
            // Rows assigned outside a consensus block tx (recovery pre-seed, API
            // read-path createAddress) are benign source-local drift. Hashing them
            // would make an honest replica fail forever.
            const src = require('fs').readFileSync(require('path').join(__dirname, '../../src/db.js'), 'utf8');
            const body = src.slice(src.indexOf('async getContentWindowRows('), src.indexOf('async getMaxRowId('));
            assert.ok(/block_index IS NOT NULL AND block_index BETWEEN/.test(body),
                'getContentWindowRows lost its NULL-block exclusion; benign out-of-band rows would false-alarm');
            assert.ok(/doQueryStrict/.test(body),
                'getContentWindowRows must read strictly: a fail-soft [] would read as "table has no rows" on both sides');
        });
    });

    describe('content digest', function(){

        const ROWS = [
            { action_index: 10, address: 'bc1qalice', amount: '100' },
            { action_index: 11, address: 'bc1qbob',   amount: '250' },
            { action_index: 12, address: 'bc1qcarol', amount: null  }
        ];

        it('case 2: a faithful replica reproduces the source digest', function(){
            let src = hasherFor({}).contentDigest('sends', ROWS.map(r => Object.assign({}, r)));
            let rep = hasherFor({}).contentDigest('sends', ROWS.map(r => Object.assign({}, r)));
            assert.strictEqual(typeof src, 'string');
            assert.strictEqual(src.length, 64, 'sha256 hex');
            assert.strictEqual(src, rep);
        });

        it('case 3: equal row count + substituted content diverges (row counts miss this)', function(){
            let tampered = ROWS.map(r => Object.assign({}, r));
            tampered[1] = { action_index: 11, address: 'bc1qmallory', amount: '250' };
            let src = hasherFor({}).contentDigest('sends', ROWS);
            let rep = hasherFor({}).contentDigest('sends', tampered);
            assert.strictEqual(tampered.length, ROWS.length, 'the substitution keeps the row COUNT equal');
            assert.notStrictEqual(src, rep);
        });

        it('a single altered amount diverges (the smallest possible tamper)', function(){
            let tampered = ROWS.map(r => Object.assign({}, r));
            tampered[0].amount = '101';
            assert.notStrictEqual(hasherFor({}).contentDigest('sends', ROWS),
                                  hasherFor({}).contentDigest('sends', tampered));
        });

        it('case 4: row order is not content (unlike the consensus hashes)', function(){
            let shuffled = [ROWS[2], ROWS[0], ROWS[1]].map(r => Object.assign({}, r));
            assert.strictEqual(hasherFor({}).contentDigest('sends', ROWS),
                               hasherFor({}).contentDigest('sends', shuffled));
        });

        it('column ORDER and driver value typing do not change the digest', function(){
            // A BIGINT arriving as Number on one side and BigInt on the other, and a
            // differently-ordered SELECT *, must not read as divergence.
            let reshaped = ROWS.map(r => ({ amount: r.amount, address: r.address, action_index: BigInt(r.action_index) }));
            assert.strictEqual(hasherFor({}).contentDigest('sends', ROWS),
                               hasherFor({}).contentDigest('sends', reshaped));
        });

        it('case 5: the stripped blocks.id surrogate is excluded, so it cannot false-alarm', function(){
            // ClientApplier drops the source's blocks.id and lets the replica assign
            // its own (localSurrogateIdTables), so the two sides legitimately disagree.
            let source  = [{ id: 7,  block_index: 100, block_hash: 'aa' }];
            let replica = [{ id: 41, block_index: 100, block_hash: 'aa' }];
            assert.strictEqual(hasherFor({}).contentDigest('blocks', source),
                               hasherFor({}).contentDigest('blocks', replica));
            // ...but the same difference in a column that IS replicated must diverge.
            assert.notStrictEqual(hasherFor({}).contentDigest('blocks', source),
                                  hasherFor({}).contentDigest('blocks', [{ id: 41, block_index: 100, block_hash: 'bb' }]));
        });

        it('the generated contract_state.state_key_bin column is excluded', function(){
            // The applier never names a generated column; the database computes it.
            let a = [{ block_index: 5, state_key: 'k', state_key_bin: 'k',     value: '1' }];
            let b = [{ block_index: 5, state_key: 'k', state_key_bin: 'other', value: '1' }];
            assert.strictEqual(hasherFor({}).contentDigest('contract_state', a),
                               hasherFor({}).contentDigest('contract_state', b));
        });

        it('the digest is table-scoped: identical rows under different table names differ', function(){
            assert.notStrictEqual(hasherFor({}).contentDigest('sends', ROWS),
                                  hasherFor({}).contentDigest('issues', ROWS));
        });
    });

    describe('computeTableContentChecksums', function(){

        it('publishes the window, the block and a sparse per-table map', async function(){
            let h = hasherFor({ sends: [{ action_index: 1, amount: '5' }], blocks: [] });
            let out = await h.computeTableContentChecksums(500, { window: 25 });
            assert.strictEqual(out.window, 25);
            assert.strictEqual(out.block, 500);
            assert.deepStrictEqual(Object.keys(out.tables), ['sends'],
                'a table with no rows in the window is omitted, not carried as an empty digest');
            assert.strictEqual(out.tables.sends.n, 1);
            assert.strictEqual(out.tables.sends.h.length, 64);
        });

        it('windows blocks as [upto - window + 1, upto] and never below zero', async function(){
            let db = fakeDb({ sends: [{ action_index: 1 }] });
            await new BlockHasher(db, new Utility()).computeTableContentChecksums(500, { window: 100 });
            let call = db.calls.find(c => c.table === 'sends');
            assert.strictEqual(call.fromBlock, 401);
            assert.strictEqual(call.toBlock, 500);

            let db2 = fakeDb({ sends: [{ action_index: 1 }] });
            await new BlockHasher(db2, new Utility()).computeTableContentChecksums(3, { window: 100 });
            assert.strictEqual(db2.calls.find(c => c.table === 'sends').fromBlock, 0);
        });

        it('an invalid or absent window falls back to the default rather than disabling the check', async function(){
            for(let bad of [undefined, 0, -5, 'abc', null]){
                let out = await hasherFor({ sends: [{ action_index: 1 }] })
                    .computeTableContentChecksums(10, bad === undefined ? undefined : { window: bad });
                assert.strictEqual(out.window, 100, 'window ' + String(bad) + ' must not silently empty the window');
            }
        });

        it('a source publishes its id ceiling and a follower reuses it verbatim', async function(){
            // Without the published ceiling each side would hash its own tail, and a
            // follower one row behind would "diverge" every poll.
            let rows = [{ id: 1, status: 'valid' }, { id: 2, status: 'invalid' }, { id: 3, status: 'pending' }];
            let src = await hasherFor({ index_statuses: rows }).computeTableContentChecksums(10, { window: 2 });
            assert.strictEqual(src.tables.index_statuses.id_max, '3');
            assert.strictEqual(src.tables.index_statuses.n, 2, 'id window (id_max - window, id_max]');

            // The follower has a 4th row the source has not published yet; using the
            // source's ceiling keeps it out of the comparison.
            let follower = await hasherFor({ index_statuses: rows.concat([{ id: 4, status: 'later' }]) })
                .computeTableContentChecksums(10, { window: 2, idBounds: { index_statuses: '3' } });
            assert.strictEqual(follower.tables.index_statuses.h, src.tables.index_statuses.h);
        });

        it('skips tables the local schema does not have (an older replica must not spam or fail)', async function(){
            let h = hasherFor({ sends: [{ action_index: 1 }] });   // listExistingTables reports only `sends`
            let out = await h.computeTableContentChecksums(10, { window: 5 });
            assert.deepStrictEqual(Object.keys(out.tables), ['sends']);
        });

        it('case 7: one unreadable table does not sink the other tables\' check', async function(){
            let h = hasherFor(
                { sends: [{ action_index: 1 }], issues: [{ action_index: 2 }] },
                { throwFor: new Set(['issues']) });
            let out = await h.computeTableContentChecksums(10, { window: 5 });
            assert.deepStrictEqual(Object.keys(out.tables).sort(), ['sends']);
        });

        it('a failed table listing degrades to probing, not to an empty result', async function(){
            let h = hasherFor({ sends: [{ action_index: 1 }] }, { listThrows: true });
            let out = await h.computeTableContentChecksums(10, { window: 5 });
            assert.strictEqual(out.tables.sends.n, 1);
        });

        it('reads the decoder plan on a decoder handle (no actions table to join)', async function(){
            let db = fakeDb({ blocks: [{ block_index: 9 }], transaction_outputs: [{ tx_index: 3 }] }, { dbType: 'decoder' });
            await new BlockHasher(db, new Utility()).computeTableContentChecksums(9, { window: 5 });
            assert.strictEqual(db.calls.find(c => c.table === 'transaction_outputs').bound, 'tx');
            assert.ok(!db.calls.some(c => c.bound === 'action'), 'the decoder DB has no actions table to join');
        });
    });

    describe('compareTableContent', function(){

        const hv = new HashVerifier();

        it('case 2: identical maps match', function(){
            let m = { window: 10, block: 5, tables: { sends: { n: 2, h: 'aaa' }, issues: { n: 1, h: 'bbb' } } };
            let res = hv.compareTableContent(5, m, m);
            assert.strictEqual(res.match, true);
            assert.strictEqual(res.compared, 2);
            assert.deepStrictEqual(res.mismatches, []);
        });

        it('case 3: equal count + different digest is the reported mismatch', function(){
            let res = hv.compareTableContent(5,
                { tables: { sends: { n: 2, h: 'local' } } },
                { tables: { sends: { n: 2, h: 'remote' } } });
            assert.strictEqual(res.match, false);
            assert.deepStrictEqual(res.mismatches, [{ table: 'sends', rows: 2, local: 'local', remote: 'remote' }]);
        });

        it('case 6: a row-count difference is skipped, never reported as divergence', function(){
            // That is completeness (the /status count check owns it) or a source that
            // has simply advanced a block inside the window. Either way, not a fork.
            let res = hv.compareTableContent(5,
                { tables: { sends: { n: 1, h: 'x' } } },
                { tables: { sends: { n: 2, h: 'y' } } });
            assert.strictEqual(res.match, true);
            assert.strictEqual(res.compared, 0);
            assert.deepStrictEqual(res.skipped, [{ table: 'sends', reason: 'row-count-differs', local: 1, remote: 2 }]);
        });

        it('a table present on one side only is skipped with its own reason', function(){
            let res = hv.compareTableContent(5,
                { tables: { sends: { n: 1, h: 'x' } } },
                { tables: { issues: { n: 1, h: 'y' } } });
            assert.strictEqual(res.match, true);
            assert.deepStrictEqual(res.skipped.map(s => s.table + ':' + s.reason).sort(),
                ['issues:absent-locally', 'sends:absent-on-source']);
        });

        it('tolerates a missing or empty payload on either side', function(){
            assert.strictEqual(hv.compareTableContent(5, null, null).match, true);
            assert.strictEqual(hv.compareTableContent(5, {}, { tables: {} }).match, true);
        });

        it('compares numeric counts across the JSON round-trip (string n must not read as a difference)', function(){
            let res = hv.compareTableContent(5,
                { tables: { sends: { n: 2, h: 'same' } } },
                { tables: { sends: { n: '2', h: 'same' } } });
            assert.strictEqual(res.compared, 1);
            assert.strictEqual(res.match, true);
        });
    });

    describe('wiring', function(){

        const src = require('fs').readFileSync(require('path').join(__dirname, '../../src/ClientSync.js'), 'utf8');
        const body = src.slice(src.indexOf('async _verifyTableContentParity('),
                               src.indexOf('async _recordTableContentMismatch('));

        it('case 7: the advisory check never halts', function(){
            assert.ok(body.length > 200, '_verifyTableContentParity body not found; update this guard');
            assert.ok(!/_haltOnDivergence|this\.halted\s*=/.test(body),
                'table-content parity is advisory: it must never halt a follower');
        });

        it('runs only at the source\'s published height, behind the flag', function(){
            assert.ok(/TABLE_CONTENT_PARITY_CHECK/.test(body), 'the check must stay behind its flag');
            assert.ok(/Number\(remoteStatus\.block_height\) !== Number\(blockHeight\)/.test(body),
                'comparing across heights would compare different windows');
        });

        it('feeds the source\'s window and id ceilings back into the local recompute', function(){
            assert.ok(/window:\s*remoteParity\.window/.test(body), 'both sides must use the SOURCE window');
            assert.ok(/idBounds/.test(body), 'both sides must use the SOURCE id ceilings');
        });

        it('is wired into the decoder path too, which has no hashes at all', function(){
            let start = src.indexOf('async _verifyDecoderCompleteness(');
            assert.ok(start !== -1, '_verifyDecoderCompleteness not found; update this guard');
            const decoderBody = src.slice(start, src.indexOf('\n    async ', start + 10));
            assert.ok(/_verifyTableContentParity\(/.test(decoderBody),
                'the decoder DB has no ledger/actions/contract hash, so this is its only content signal');
        });

        it('the /status producer publishes the payload for both dbTypes, default null', function(){
            const api = require('fs').readFileSync(require('path').join(__dirname, '../../src/api.js'), 'utf8');
            assert.ok(/row\.table_content_parity = null;/.test(api), 'default null so a follower skips');
            assert.ok(/cfg\['TABLE_CONTENT_PARITY_CHECK'\]/.test(api), 'publishing stays behind the flag');
            // It must sit OUTSIDE the indexer-only branch that owns index_map_checksum.
            let idx = api.indexOf('row.table_content_parity = null;');
            let mapIdx = api.indexOf('row.index_map_checksum = null;');
            assert.ok(mapIdx !== -1 && idx > mapIdx, 'unexpected api.js layout; update this guard');
            assert.ok(!/dbType === 'decoder'[\s\S]{0,200}table_content_parity/.test(api));
        });
    });
});
