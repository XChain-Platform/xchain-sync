// Copyright © 2025-2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

const assert = require('assert');
const sinon = require('sinon');
const { makeTestDatabase } = require('../support/fake_db');

function makeDb(){
    return makeTestDatabase('replica_db', 'u', 'p',
        { isNull: (v) => v === null || v === undefined }, 'indexer');
}

async function runShapes(shapes){
    const db = makeDb(), alters = [];
    sinon.stub(console, 'log');
    sinon.stub(console, 'warn');
    sinon.stub(console, 'error');
    sinon.stub(db, 'doQuery').callsFake(async (sql, args) => {
        if(/information_schema\.columns/i.test(sql)){
            const shape = shapes[args[1]];
            return shape ? [shape] : [];
        }
        if(/^SELECT COALESCE\(MAX\(id\)/i.test(sql)) return [{ base: 0 }];
        if(/^ALTER TABLE/i.test(sql)) alters.push(sql);
        return [];
    });
    try {
        await db.repairReplicatedAutoIncrement();
        return alters;
    } finally {
        sinon.restore();
        await db.close();
    }
}

describe('Database replica AUTO_INCREMENT signedness repair', function(){
    it('leaves all current and absent cursor columns unchanged', async function(){
        const alters = await runShapes({
            price_snapshots: { EXTRA: 'auto_increment', COLUMN_TYPE: 'bigint' },
            cross_chain_matches: { EXTRA: 'auto_increment', COLUMN_TYPE: 'bigint unsigned' },
            capability_snapshots: { extra: 'auto_increment', column_type: 'bigint(20) unsigned' }
        });
        assert.deepStrictEqual(alters, []);
    });

    it('retypes already-repaired signed cursors whose declarations are unsigned', async function(){
        const shape = { EXTRA: 'auto_increment', COLUMN_TYPE: 'bigint' };
        const alters = await runShapes({
            cross_chain_matches: shape,
            capability_snapshots: shape,
            state_checkpoints: shape
        });
        assert.deepStrictEqual(alters, [
            'ALTER TABLE `cross_chain_matches` MODIFY id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT',
            'ALTER TABLE `capability_snapshots` MODIFY id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT',
            'ALTER TABLE `state_checkpoints` MODIFY id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT'
        ]);
    });

    it('restores missing attributes with each table declaration signedness', async function(){
        const shape = { EXTRA: '', COLUMN_TYPE: 'bigint' };
        const alters = await runShapes({
            price_snapshots: shape,
            cross_chain_matches: shape,
            capability_snapshots: shape,
            state_checkpoints: shape
        });
        assert.deepStrictEqual(alters, [
            'ALTER TABLE `price_snapshots` MODIFY id BIGINT NOT NULL AUTO_INCREMENT',
            'ALTER TABLE `cross_chain_matches` MODIFY id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT',
            'ALTER TABLE `capability_snapshots` MODIFY id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT',
            'ALTER TABLE `state_checkpoints` MODIFY id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT'
        ]);
    });
});
