// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert        = require('assert');
const sinon         = require('sinon');
const ServerProcess = require('./helpers/serverProcess');

const SERVER_PORT = 29951;
const FORWARDED_IP = '198.51.100.7';

describe('E2E: ServerProcess proxy trust', function() {

    let server;
    const sourceDb = {
        dbType: 'indexer',
        getLastBlock: async () => null,
        getReplicaStatus: async () => ({ isReplica: false })
    };

    before(function() {
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    after(async function() {
        sinon.restore();
        if (server) await server.stop();
    });

    afterEach(async function() {
        if (server) {
            await server.stop();
            server = null;
        }
    });

    async function requestContext(trustProxy) {
        server = new ServerProcess(sourceDb, SERVER_PORT);
        if (trustProxy === undefined) delete server.config.TRUST_PROXY;
        else server.config.TRUST_PROXY = trustProxy;
        await server.start();

        server.app.get('/request-context', (req, res) => {
            res.json({ ip: req.ip, protocol: req.protocol });
        });

        let response = await fetch(server.getUrl() + '/request-context', {
            headers: {
                'x-forwarded-for': FORWARDED_IP,
                'x-forwarded-proto': 'https'
            }
        });
        assert.strictEqual(response.status, 200);
        return response.json();
    }

    it('honours forwarded client and protocol when TRUST_PROXY is true', async function() {
        this.timeout(30000);
        let context = await requestContext(true);

        assert.strictEqual(context.ip, FORWARDED_IP);
        assert.strictEqual(context.protocol, 'https');
    });

    it('ignores forwarded client and protocol when TRUST_PROXY is unset', async function() {
        this.timeout(30000);
        let context = await requestContext(undefined);

        assert.notStrictEqual(context.ip, FORWARDED_IP);
        assert.strictEqual(context.protocol, 'http');
    });
});
