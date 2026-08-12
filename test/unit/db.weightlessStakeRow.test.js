/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 *********************************************************************/

'use strict';

// : a weightless row must never leave the follower's stake-weight
// producer either. stake_weighted_quorum fails closed on a missing weight, but
// it only ever sees a weight the producer already resolved: coercing the missing
// one to '0' leaves the source in the quorum's dedupe map carrying no stake, so
// the denominator S shrinks while a signer keeps the full numerator and a
// smaller real stake clears 3*tally > 2*S. Both of this follower's source-keyed
// weight paths (live and the AsOf historical reconstruction) route through the
// same guard as the indexer twin, so the two sides of a stakes_root comparison
// cannot disagree about what a weightless row means.
//
// stakes.amount is NOT NULL and the source aggregate is HAVING-filtered, so the
// throw is unreachable on honest data; a 2026-08-12 sweep of the BTC, LTC and
// DOGE regtest ledgers found zero weightless rows.

const assert   = require('assert');
const sinon    = require('sinon');
const Database = require('../../src/db');

function makeUtil(){
    return {
        isNull:     (v) => v === null || v === undefined,
        throwError: (m) => { throw new Error(m); },
        sleep:      sinon.stub().resolves(),
        logError:   sinon.stub()
    };
}
function makeDb(rows){
    const db = new Database('localhost', 3306, 'replica_db', 'u', 'p', makeUtil(), 'indexer');
    sinon.stub(db, 'getStatusId').resolves(1);
    sinon.stub(db, 'doQuery').resolves(rows || []);
    return db;
}
function rowsWith(weight){
    return [
        { pubkey: 'aa', source: 'src1', weight: '50000' },
        { pubkey: 'bb', source: 'src2', weight: weight }
    ];
}

const MISSING = [
    { label: 'null',             weight: null },
    { label: 'undefined',        weight: undefined },
    { label: 'empty string',     weight: '' },
    { label: 'blank string',     weight: '   ' },
    { label: 'nonnumeric',       weight: 'lots' },
    { label: 'the literal null', weight: 'null' }
];

describe('weightless stake-weight rows fail closed ', function(){

    beforeEach(function(){ sinon.stub(console, 'warn'); });
    afterEach(function(){ sinon.restore(); });

    describe('Database.requireStakeWeight', function(){

        for(const bad of MISSING){
            it('throws on a weight that is ' + bad.label, function(){
                assert.throws(() => Database.requireStakeWeight(bad.weight, 'probe'), /denominator S/);
            });
        }

        it('accepts a LEGITIMATE zero', function(){
            assert.strictEqual(Database.requireStakeWeight('0', 'probe'), '0');
        });

        it('preserves the value byte-for-byte (it feeds hashed stakes_root leaves)', function(){
            assert.strictEqual(Database.requireStakeWeight('12345.67890000', 'probe'), '12345.67890000');
        });

        it('names the producer in the error', function(){
            assert.throws(() => Database.requireStakeWeight(null, 'getStakeWeightsByCapabilityAsOf(oracle_publish)'),
                /getStakeWeightsByCapabilityAsOf\(oracle_publish\)/);
        });
    });

    describe('getStakeWeightsByCapability (live)', function(){

        for(const bad of MISSING){
            it('throws when a row weight is ' + bad.label, async function(){
                const db = makeDb(rowsWith(bad.weight));
                await assert.rejects(
                    () => db.getStakeWeightsByCapability('oracle_publish', 106, '500', 1000, 'BTC', 'regtest'),
                    /denominator S/);
            });
        }

        it('passes a fully-weighted set through unchanged', async function(){
            const db = makeDb(rowsWith('25000'));
            const out = await db.getStakeWeightsByCapability('oracle_publish', 106, '500', 1000, 'BTC', 'regtest');
            assert.deepStrictEqual(out.map(r => r.weight), ['50000', '25000']);
        });

        it('does not reject a legitimate zero weight', async function(){
            const db = makeDb(rowsWith('0'));
            const out = await db.getStakeWeightsByCapability('oracle_publish', 106, '0', 1000, 'BTC', 'regtest');
            assert.deepStrictEqual(out.map(r => r.weight), ['50000', '0']);
        });
    });

    describe('getStakeWeightsByCapabilityAsOf (historical reconstruction)', function(){

        it('throws when the reconstructed set carries a weightless row', async function(){
            const db = makeDb(rowsWith(null));
            await assert.rejects(
                () => db.getStakeWeightsByCapabilityAsOf('oracle_publish', 106, '500', 1000, 'BTC', 'regtest'),
                /denominator S/);
        });

        it('passes a fully-weighted reconstruction through', async function(){
            const db = makeDb(rowsWith('600'));
            const out = await db.getStakeWeightsByCapabilityAsOf('oracle_publish', 106, '500', 1000, 'BTC', 'regtest');
            assert.deepStrictEqual(out.map(r => r.weight), ['50000', '600']);
        });
    });

    describe('twin parity with the indexer guard', function(){

        // The two producers are read against each other whenever a follower's
        // stakes_root is compared with the indexer's, so their verdict on a given
        // weight has to be the same verdict. This pins the sync side's table; the
        // indexer side pins the identical table in its own suite.
        const VERDICTS = [
            { weight: '0',      accepted: true  },
            { weight: '1',      accepted: true  },
            { weight: '0.00000001', accepted: true },
            { weight: '-5',     accepted: true  },   // the predicate itself rejects negatives
            { weight: '',       accepted: false },
            { weight: '  ',     accepted: false },
            { weight: null,     accepted: false },
            { weight: 'null',   accepted: false },
            { weight: '1e5',    accepted: false },   // exponent notation is not a canonical amount
            { weight: '0x10',   accepted: false }
        ];

        for(const v of VERDICTS){
            it((v.accepted ? 'accepts ' : 'rejects ') + JSON.stringify(v.weight), function(){
                if(v.accepted) assert.strictEqual(Database.requireStakeWeight(v.weight, 'twin'), String(v.weight));
                else assert.throws(() => Database.requireStakeWeight(v.weight, 'twin'));
            });
        }
    });
});
