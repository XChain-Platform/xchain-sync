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

// Global setup — create databases and seed schema
async function globalSetup() {
    console.log('    [setup] Creating test databases...');

    // Create source database with full indexer schema
    sourceDb = await testDb.createDatabase(testDb.SOURCE_DB_NAME);
    await testDb.seedSchema(sourceDb);
    console.log('    [setup] Source database ready: ' + testDb.SOURCE_DB_NAME);

    // Create replica database (empty — schema replicated by sync service)
    replicaDb = await testDb.createDatabase(testDb.REPLICA_DB_NAME);
    await testDb.seedSchema(replicaDb);
    console.log('    [setup] Replica database ready: ' + testDb.REPLICA_DB_NAME);
}

// Global teardown — drop test databases
async function globalTeardown() {
    console.log('    [teardown] Dropping test databases...');
    if (sourceDb)  await sourceDb.close();
    if (replicaDb) await replicaDb.close();
    await testDb.dropDatabase(testDb.SOURCE_DB_NAME);
    await testDb.dropDatabase(testDb.REPLICA_DB_NAME);
    console.log('    [teardown] Cleanup complete');
}

// Get references to the test database instances
function getSourceDb() {
    return sourceDb;
}

function getReplicaDb() {
    return replicaDb;
}

// Reset both databases (truncate all tables)
async function resetDatabases() {
    if (sourceDb)  await testDb.truncateAll(sourceDb);
    if (replicaDb) await testDb.truncateAll(replicaDb);
}

module.exports = {
    globalSetup,
    globalTeardown,
    getSourceDb,
    getReplicaDb,
    resetDatabases
};
