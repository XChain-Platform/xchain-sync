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
// Advisory index-map parity drill (NON-consensus). Exercises the SHIPPED compute
// + compare + counter paths against REAL MariaDB source and replica schemas, the
// gap the fake-db sanity check could not cover (real BIGINT id ordering, NULL
// block_index exclusion, driver id typing, real sync_state persistence).
//
// Acceptance cases:
//   1. faithful replica -> checksums match (and a NULL-block row on the source,
//      a benign recovery/API pre-seed, does NOT break parity);
//   2. equal row count + swapped identity (the INSERT IGNORE divergence) ->
//      checksums DIFFER and compareIndexMap reports a mismatch;
//   3. a NULL-block row the source lacks does NOT raise a false alarm;
//   4. the advisory mismatch counter persists + increments via real setSyncState.

const assert      = require('assert');
const setup       = require('./helpers/setup');
const testDb      = require('./helpers/testDb');
const Database    = require('../../src/db');
const BlockHasher = require('../../src/BlockHasher');
const HashVerifier= require('../../src/HashVerifier');

// Byte-identical copy of ClientSync._recordIndexMapMismatch's body, exercised here
// against a real Database so the counter logic + real sync_state persistence are
// validated together (the method itself lives on ClientSync, which is too heavy to
// stand up for a storage check).
async function recordIndexMapMismatch(db, dbType, blockIndex){
    if(!db || typeof db.setSyncState !== 'function') return;
    let countKey = 'index_map_mismatch_count:' + dbType;
    let cur = (typeof db.getSyncState === 'function') ? await db.getSyncState(countKey) : null;
    let n = (cur != null && Number.isFinite(Number(cur))) ? Number(cur) + 1 : 1;
    await db.setSyncState(countKey, String(n));
    await db.setSyncState('index_map_mismatch_last_block:' + dbType, String(blockIndex));
}

describe('Integration: index-map parity (advisory, NON-consensus)', function() {
    // Generous: the before hook seeds the full indexer schema into TWO fresh DBs.
    this.timeout(180000);

    let sourceDb, replicaDb, util, hv;
    let srcHasher, repHasher;
    let realReplicaDb; // src/db.js Database for the real setSyncState path

    // Seed index_addresses verbatim (explicit id + block_index), mirroring how the
    // indexer assigns dense ids in-block (block_index set) vs benign out-of-band
    // rows (block_index NULL).
    async function seed(db, rows){
        await db.doQuery("DELETE FROM index_addresses");
        for(let r of rows){
            await db.doQuery(
                "INSERT INTO index_addresses (`id`, `address`, `block_index`) VALUES (?, ?, ?)",
                [r.id, r.address, r.block_index]
            );
        }
    }

    before(async function() {
        await setup.globalSetup();
        sourceDb  = setup.getSourceDb();
        replicaDb = setup.getReplicaDb();
        util      = testDb.util;
        hv        = new HashVerifier();
        srcHasher = new BlockHasher(sourceDb, util);
        repHasher = new BlockHasher(replicaDb, util);
        realReplicaDb = new Database(
            testDb.TEST_DB_HOST, testDb.TEST_DB_PORT, testDb.REPLICA_DB_NAME,
            testDb.TEST_DB_USER, testDb.TEST_DB_PASS, util, 'indexer'
        );
    });

    after(async function() {
        if(realReplicaDb && realReplicaDb.close) { try { await realReplicaDb.close(); } catch(e){} }
        await setup.globalTeardown();
    });

    const H = 12;
    // Deterministic in-block subset (block_index set) shared by source + replica.
    const DETERMINISTIC = [
        { id: 1, address: 'bc1qsource00000000000000000000000000000sss', block_index: 10 },
        { id: 2, address: 'bc1qalice000000000000000000000000000000aaa', block_index: 11 },
        { id: 3, address: 'bc1qbob0000000000000000000000000000000bbbb', block_index: 12 },
    ];

    it('case 1: faithful replica matches the source (NULL-block source row excluded)', async function() {
        // Source carries an extra NULL-block row (recovery reward-source pre-seed).
        await seed(sourceDb, DETERMINISTIC.concat([
            { id: 50, address: 'bc1qrecovery00000000000000000000000000rrr', block_index: null }
        ]));
        await seed(replicaDb, DETERMINISTIC.slice());

        let src = await srcHasher.computeIndexMapChecksum(H);
        let rep = await repHasher.computeIndexMapChecksum(H);
        assert.strictEqual(typeof src, 'string');
        assert.strictEqual(src.length, 64, 'sha256 hex');
        assert.strictEqual(src, rep, 'faithful replica must match source (NULL-block row excluded)');
        assert.strictEqual(hv.compareIndexMap(H, rep, src).match, true);
    });

    it('case 2: equal row count + swapped identity diverges (the INSERT IGNORE bug)', async function() {
        await seed(sourceDb, DETERMINISTIC.slice());
        // Replica locally pre-created id=3 -> mallory; the source INSERT IGNORE of
        // id=3 -> bob was dropped. SAME deterministic row count, different content.
        let diverged = DETERMINISTIC.slice();
        diverged[2] = { id: 3, address: 'bc1qmallory0000000000000000000000000mmmm', block_index: 12 };
        await seed(replicaDb, diverged);

        let src = await srcHasher.computeIndexMapChecksum(H);
        let rep = await repHasher.computeIndexMapChecksum(H);

        // Row counts agree, so the existing table-count check would NOT flag this.
        let srcCount = (await sourceDb.doQuery("SELECT COUNT(*) c FROM index_addresses WHERE block_index IS NOT NULL"))[0].c;
        let repCount = (await replicaDb.doQuery("SELECT COUNT(*) c FROM index_addresses WHERE block_index IS NOT NULL"))[0].c;
        assert.strictEqual(Number(srcCount), Number(repCount), 'equal deterministic row count');

        assert.notStrictEqual(src, rep, 'swapped identity must diverge the checksum');
        assert.strictEqual(hv.compareIndexMap(H, rep, src).match, false);
    });

    it('case 3: a NULL-block replica row the source lacks raises no false alarm', async function() {
        await seed(sourceDb, DETERMINISTIC.slice());
        // Replica has its own benign out-of-band row (API read-path createAddress).
        await seed(replicaDb, DETERMINISTIC.concat([
            { id: 77, address: 'bc1qapiseed00000000000000000000000000aaa1', block_index: null }
        ]));

        let src = await srcHasher.computeIndexMapChecksum(H);
        let rep = await repHasher.computeIndexMapChecksum(H);
        assert.strictEqual(src, rep, 'NULL-block rows are excluded; benign drift must not alarm');
        assert.strictEqual(hv.compareIndexMap(H, rep, src).match, true);
    });

    it('case 4: advisory mismatch counter persists + increments (real sync_state)', async function() {
        let dbType = 'indexer';
        // Fresh count baseline (table self-heals on first setSyncState).
        await recordIndexMapMismatch(realReplicaDb, dbType, 4111);
        let c1 = await realReplicaDb.getSyncState('index_map_mismatch_count:' + dbType);
        let lb1 = await realReplicaDb.getSyncState('index_map_mismatch_last_block:' + dbType);
        assert.strictEqual(c1, '1', 'first mismatch -> count 1');
        assert.strictEqual(lb1, '4111', 'last divergent block recorded');

        await recordIndexMapMismatch(realReplicaDb, dbType, 4222);
        let c2 = await realReplicaDb.getSyncState('index_map_mismatch_count:' + dbType);
        let lb2 = await realReplicaDb.getSyncState('index_map_mismatch_last_block:' + dbType);
        assert.strictEqual(c2, '2', 'second mismatch -> count 2');
        assert.strictEqual(lb2, '4222', 'last block updated');
    });

    it('case 5: the bound is honored (rows above uptoBlock are excluded)', async function() {
        // A row assigned in a block ABOVE the checksum bound must not be hashed, so
        // a replica that has not yet applied block 13 still matches a source bounded
        // to block 12.
        await seed(sourceDb, DETERMINISTIC.concat([
            { id: 4, address: 'bc1qfuture0000000000000000000000000000fff', block_index: 13 }
        ]));
        await seed(replicaDb, DETERMINISTIC.slice());
        let src = await srcHasher.computeIndexMapChecksum(H);   // bound = 12
        let rep = await repHasher.computeIndexMapChecksum(H);
        assert.strictEqual(src, rep, 'rows above the bound are excluded');
    });
});
