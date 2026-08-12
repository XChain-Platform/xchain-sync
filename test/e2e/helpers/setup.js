// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const testDb = require('./testDb');

let sourceDb  = null;
let replicaDb = null;
let source2Db = null;

async function globalSetup(opts = {}) {
    console.log('    [e2e setup] Creating test databases...');

    sourceDb = await testDb.createDatabase(testDb.SOURCE_DB_NAME);
    await testDb.seedSchema(sourceDb);
    console.log('    [e2e setup] Source database ready: ' + testDb.SOURCE_DB_NAME);

    replicaDb = await testDb.createDatabase(
        testDb.REPLICA_DB_NAME,
        testDb.REPLICA_DB_HOST,
        testDb.REPLICA_DB_PORT,
        testDb.REPLICA_DB_USER,
        testDb.REPLICA_DB_PASS
    );
    await testDb.seedSchema(replicaDb);
    console.log('    [e2e setup] Replica database ready: ' + testDb.REPLICA_DB_NAME);

    // Optionally create second source database (for cross-source tests)
    if (opts.source2) {
        source2Db = await testDb.createDatabase(
            testDb.SOURCE2_DB_NAME,
            testDb.SOURCE2_DB_HOST,
            testDb.SOURCE2_DB_PORT,
            testDb.SOURCE2_DB_USER,
            testDb.SOURCE2_DB_PASS
        );
        await testDb.seedSchema(source2Db);
        console.log('    [e2e setup] Source2 database ready: ' + testDb.SOURCE2_DB_NAME);
    }
}

async function globalTeardown() {
    console.log('    [e2e teardown] Dropping test databases...');
    if (sourceDb)  await sourceDb.close();
    if (replicaDb) await replicaDb.close();
    if (source2Db) await source2Db.close();
    await testDb.dropDatabase(testDb.SOURCE_DB_NAME);
    await testDb.dropDatabase(
        testDb.REPLICA_DB_NAME,
        testDb.REPLICA_DB_HOST,
        testDb.REPLICA_DB_PORT,
        testDb.REPLICA_DB_USER,
        testDb.REPLICA_DB_PASS
    );
    await testDb.dropDatabase(
        testDb.SOURCE2_DB_NAME,
        testDb.SOURCE2_DB_HOST,
        testDb.SOURCE2_DB_PORT,
        testDb.SOURCE2_DB_USER,
        testDb.SOURCE2_DB_PASS
    );
    console.log('    [e2e teardown] Cleanup complete');
}

function getSourceDb()  { return sourceDb; }
function getReplicaDb() { return replicaDb; }
function getSource2Db() { return source2Db; }

async function resetDatabases() {
    if (sourceDb)  await testDb.truncateAll(sourceDb);
    if (replicaDb) await testDb.truncateAll(replicaDb);
    if (source2Db) await testDb.truncateAll(source2Db);
}

module.exports = {
    globalSetup,
    globalTeardown,
    getSourceDb,
    getReplicaDb,
    getSource2Db,
    resetDatabases
};
