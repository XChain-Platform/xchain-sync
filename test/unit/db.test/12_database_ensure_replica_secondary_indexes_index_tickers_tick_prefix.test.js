// Copyright © 2025–2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

const { assert, sinon, makeDb, silenceConsole } = require('./support/helpers');

function stubSchema(db, tickRows, error){
    const calls = [];
    const fake = async (sql, params) => {
        calls.push({ sql, params });
        if(error && /index_name = 'tick'/.test(sql)) throw error;
        if(/information_schema\.tables/.test(sql)){
            const table = (params && params[1]) || (/index_tickers/.test(sql) ? 'index_tickers' : '');
            return table === 'index_tickers' ? [{ table_name: table }] : [];
        }
        if(/information_schema\.statistics/.test(sql) && params && params[2] === 'block_index')
            return [{ index_name: 'block_index' }];
        if(/index_name = 'tick'/.test(sql)) return tickRows;
        return [];
    };
    sinon.stub(db, 'doQuery').callsFake(fake);
    sinon.stub(db, 'doQueryStrict').callsFake(fake);
    return calls;
}

describe('Database replica index_tickers tick prefix repair', function(){
    let db;
    beforeEach(function(){ silenceConsole(); db = makeDb('indexer'); });
    afterEach(async function(){ sinon.restore(); await db.close(); });

    it('widens the stale unique tick(200) prefix to tick(250)', async function(){
        const calls = stubSchema(db, [{ SUB_PART: 200, NON_UNIQUE: 0 }]);
        await db.ensureReplicaSecondaryIndexes();
        const alters = calls.filter(c => /ALTER TABLE `index_tickers` DROP INDEX IF EXISTS `tick`/.test(c.sql));
        assert.strictEqual(alters.length, 1);
        assert.match(alters[0].sql, /ADD UNIQUE INDEX `tick` \(tick\(250\)\)/);
    });

    it('leaves full, wider, and missing tick prefixes unchanged', async function(){
        for(const row of [
            { SUB_PART: 250, NON_UNIQUE: 0 },
            { SUB_PART: 300, NON_UNIQUE: 0 },
            { SUB_PART: null, NON_UNIQUE: 0 }
        ]){
            const isolated = makeDb('indexer');
            const calls = stubSchema(isolated, [row]);
            await isolated.ensureReplicaSecondaryIndexes();
            assert.ok(!calls.some(c => /DROP INDEX IF EXISTS `tick`/.test(c.sql)));
            await isolated.close();
            sinon.restore();
        }
    });

    it('skips absent tables and missing-table probe errors', async function(){
        db.doQuery = async () => [];
        db.doQueryStrict = async () => [];
        await db.ensureReplicaTickersTickKey();
        const missing = Object.assign(new Error('missing'), { errno: 1146 });
        sinon.stub(db, 'doQueryStrict').rejects(missing);
        await db.ensureReplicaTickersTickKey();
    });

    it('logs and contains unexpected probe errors', async function(){
        const fault = Object.assign(new Error('denied'), { errno: 1142 });
        stubSchema(db, [], fault);
        await db.ensureReplicaTickersTickKey();
        assert.strictEqual(console.error.called, true);
    });
});
