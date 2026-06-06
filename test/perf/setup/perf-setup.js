'use strict';

// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available —
// contact legal@dankest.llc.

const setup         = require('../../e2e/helpers/setup');
const testDb        = require('../../e2e/helpers/testDb');
const ServerProcess = require('../../e2e/helpers/serverProcess');
const ClientProcess = require('../../e2e/helpers/clientProcess');
const DataGenerator = require('./data-generator');

const SERVER_PORT = 29700;

async function bootEnvironment(opts) {
    await setup.globalSetup(opts);
    return {
        sourceDb:  setup.getSourceDb(),
        replicaDb: setup.getReplicaDb(),
        source2Db: setup.getSource2Db()
    };
}

async function teardownEnvironment() {
    await setup.globalTeardown();
}

async function resetAll() {
    await setup.resetDatabases();
}

function createServer(sourceDb, port, chain, network) {
    return new ServerProcess(sourceDb, port || SERVER_PORT, chain, network);
}

function createClient(replicaDb, serverUrl, chain, network, opts) {
    return new ClientProcess(replicaDb, serverUrl, chain, network, opts);
}

function createGenerator(db) {
    return new DataGenerator(db);
}

module.exports = {
    SERVER_PORT,
    bootEnvironment,
    teardownEnvironment,
    resetAll,
    createServer,
    createClient,
    createGenerator,
    testDb
};
