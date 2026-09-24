// Copyright © 2025–2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

const assert = require('assert');
const net = require('node:net');
const sinon = require('sinon');
const proxyquire = require('proxyquire').noCallThru();
const Database = require('../../src/db');

const RETRY_ENV_KEYS = [
    'DB_CONNECT_RETRY_MAX_ATTEMPTS',
    'DB_CONNECT_RETRY_TIMEOUT_MS'
];

function sleep(ms){
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function makeDatabase(DatabaseClass, port){
    return new DatabaseClass('127.0.0.1', port, 'retry_bound', 'user', 'pass', {
        sleep,
        isNull: (value) => value === null || value === undefined,
        throwError: (message) => { throw new Error(message); },
        logError: () => {}
    });
}

async function listen(server){
    await new Promise((resolve, reject) => {
        server.once('error', reject);
        server.listen(0, '127.0.0.1', resolve);
    });
    return server.address().port;
}

async function closeServer(server){
    await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
}

async function closedPort(){
    let server = net.createServer();
    let port = await listen(server);
    await closeServer(server);
    return port;
}

function addEnvironmentHooks(){
    let savedEnv;
    beforeEach(function(){
        savedEnv = {};
        for(let key of RETRY_ENV_KEYS) savedEnv[key] = process.env[key];
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
    });

    afterEach(function(){
        for(let key of RETRY_ENV_KEYS){
            if(savedEnv[key] === undefined) delete process.env[key];
            else process.env[key] = savedEnv[key];
        }
        sinon.restore();
    });
}

function addClosedPortTests(){
    for(let method of ['verifyDatabase', 'createDatabase']){
        it(method + ' stops retrying a closed port within its configured bound', async function(){
            process.env.DB_CONNECT_RETRY_MAX_ATTEMPTS = '2';
            process.env.DB_CONNECT_RETRY_TIMEOUT_MS = '250';
            let db = makeDatabase(Database, await closedPort());
            let started = Date.now();
            try {
                await assert.rejects(() => db[method]());
                assert.ok(Date.now() - started < 700, method + ' exceeded the retry bound');
            } finally {
                await db.close();
            }
        });
    }
}

function addSilentServerTests(){
    for(let method of ['verifyDatabase', 'createDatabase']){
        it(method + ' bounds a connection that accepts but never answers', async function(){
            process.env.DB_CONNECT_RETRY_MAX_ATTEMPTS = '5';
            process.env.DB_CONNECT_RETRY_TIMEOUT_MS = '500';
            let sockets = new Set();
            let server = net.createServer((socket) => {
                sockets.add(socket);
                socket.on('close', () => sockets.delete(socket));
            });
            let port = await listen(server);
            let db = makeDatabase(Database, port);
            let started = Date.now();
            try {
                await assert.rejects(() => db[method]());
                assert.ok(Date.now() - started < 900, method + ' exceeded the hanging connection tolerance');
            } finally {
                await db.close();
                for(let socket of sockets) socket.destroy();
                await closeServer(server);
            }
        });
    }
}

function addAccessDeniedTests(){
    for(let method of ['verifyDatabase', 'createDatabase']){
        it(method + ' does not retry rejected credentials', async function(){
            let denied = Object.assign(new Error('Access denied'), { code: 'ER_ACCESS_DENIED_ERROR' });
            let createConnection = sinon.stub().rejects(denied);
            let FakeDatabase = proxyquire('../../src/db', {
                mariadb: {
                    createPool: () => ({ end: sinon.stub().resolves() }),
                    createConnection,
                    '@noCallThru': true
                }
            });
            let db = makeDatabase(FakeDatabase, 3306);
            db.util.sleep = sinon.stub().resolves();
            try {
                await assert.rejects(() => db[method](), (error) => error === denied);
                assert.strictEqual(createConnection.callCount, 1);
            } finally {
                await db.close();
            }
        });
    }
}

describe('Database connection retry bounds', function(){
    addEnvironmentHooks();
    addClosedPortTests();
    addSilentServerTests();
    addAccessDeniedTests();
});
