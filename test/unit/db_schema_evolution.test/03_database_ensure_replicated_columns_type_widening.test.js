// Copyright © 2025–2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

const assert = require('assert');
const sinon = require('sinon');
const { makeTestDatabase } = require('../support/fake_db');

function makeDb(dbType = 'indexer'){
    return makeTestDatabase('replica_db', 'u', 'p',
        { isNull: (v) => v === null || v === undefined }, dbType);
}

async function runWithShape(type, nullable){
    const db = makeDb();
    const alters = [];
    sinon.stub(db, 'doQuery').callsFake(async (sql) => {
        if(/information_schema\.columns/i.test(sql)) return type === null ? [] : [
            { COLUMN_TYPE: type, IS_NULLABLE: nullable }
        ];
        if(/ALTER TABLE/i.test(sql)) alters.push(sql);
        return [];
    });
    await db.widenReplicatedColumnTypes();
    await db.close();
    sinon.restore();
    return alters;
}

describe('Database replica column type widening', function(){
    afterEach(function(){ sinon.restore(); });

    it('widens stale type or nullability shapes to BIGINT UNSIGNED NULL', async function(){
        const expected = 'ALTER TABLE `file_manifests` MODIFY COLUMN `total_bytes` BIGINT UNSIGNED NULL';
        for(const shape of [
            ['int(10) unsigned', 'NO'],
            ['bigint(20) unsigned', 'NO'],
            ['int(10) unsigned', 'YES']
        ]){
            assert.deepStrictEqual(await runWithShape(shape[0], shape[1]), [expected]);
        }
    });

    it('leaves current and absent column shapes unchanged', async function(){
        assert.deepStrictEqual(await runWithShape('bigint(20) unsigned', 'YES'), []);
        assert.deepStrictEqual(await runWithShape('bigint unsigned', 'YES'), []);
        assert.deepStrictEqual(await runWithShape(null, null), []);
    });

    it('does no type probe on decoder replicas', async function(){
        const db = makeDb('decoder');
        const query = sinon.stub(db, 'doQuery').resolves([]);
        await db.ensureReplicatedColumns();
        assert.strictEqual(query.called, false);
        await db.close();
    });

    it('propagates a probe fault instead of reading it as an absent column', async function(){
        const db = makeDb();
        sinon.stub(db, 'doQuery').rejects(new Error('probe failed'));
        await assert.rejects(() => db.widenReplicatedColumnTypes(), /probe failed/);
        await db.close();
    });
});
