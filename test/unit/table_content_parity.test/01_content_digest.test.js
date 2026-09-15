// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');

const BlockHasher = require('../../../src/client/block_hasher');
const Utility     = require('../../../src/util');

const hasherFor = () => new BlockHasher(null, new Utility());
const ROWS = [
    { action_index: 10, address: 'bc1qalice', amount: '100' },
    { action_index: 11, address: 'bc1qbob',   amount: '250' },
    { action_index: 12, address: 'bc1qcarol', amount: null  }
];

describe('Advisory table-content parity', function(){

    describe('content digest', function(){

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
    });
});

describe('Advisory table-content parity', function(){

    describe('content digest', function(){

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

        it('the node-local sync_meta.id/logged_at columns are excluded, so they cannot false-alarm', function(){
            // ServerPoller builds the streamed sync_meta row by hand from the block
            // hashes and omits id/logged_at, so the follower auto-assigns its own id
            // and stamps its own insert wall-clock on EVERY live-applied block. Left
            // in the preimage those two columns guarantee a mismatch at equal counts.
            let source  = [{ id: 7,  block_index: 100, block_time: 1700, ledger_hash: 'aa', actions_hash: 'bb', contract_hash: 'cc', logged_at: '2026-08-16T00:00:00Z' }];
            let replica = [{ id: 41, block_index: 100, block_time: 1700, ledger_hash: 'aa', actions_hash: 'bb', contract_hash: 'cc', logged_at: '2026-08-16T09:31:02Z' }];
            assert.strictEqual(hasherFor({}).contentDigest('sync_meta', source),
                               hasherFor({}).contentDigest('sync_meta', replica));
            // ...but the replicated hash columns still have to match.
            let forged = [Object.assign({}, replica[0], { ledger_hash: 'zz' })];
            assert.notStrictEqual(hasherFor({}).contentDigest('sync_meta', source),
                                  hasherFor({}).contentDigest('sync_meta', forged));
        });

        it('the node-local contract_emissions.id column is excluded, so it cannot false-alarm', function(){
            // db.getEmissionRowsForBlock streams only the four protocol columns
            // ("not em.*, which would carry the AUTO_INCREMENT id"), so the follower
            // assigns its own id on every live-applied block while the parity read is
            // a SELECT em.*. Left in the preimage that guarantees a mismatch at equal
            // counts on exactly the table the check is meant to police.
            let source  = [{ id: 7,  execution_index: 900, emitted_action: 'SLASH', action_index: null, position: 0 }];
            let replica = [{ id: 41, execution_index: 900, emitted_action: 'SLASH', action_index: null, position: 0 }];
            assert.strictEqual(hasherFor({}).contentDigest('contract_emissions', source),
                               hasherFor({}).contentDigest('contract_emissions', replica));
            // ...but a difference in any replicated column still has to diverge.
            for(let col of ['execution_index', 'emitted_action', 'action_index', 'position']){
                let forged = [Object.assign({}, replica[0], { [col]: 'forged' })];
                assert.notStrictEqual(hasherFor({}).contentDigest('contract_emissions', source),
                                      hasherFor({}).contentDigest('contract_emissions', forged),
                                      col + ' must stay in the preimage');
            }
        });
    });
});

describe('Advisory table-content parity', function(){

    describe('content digest', function(){

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
});
