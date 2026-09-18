'use strict';

// Copyright © 2025-2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC - https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// carrier_logic_digest on /health (C7) is READ from the committed pin by
// src/health/carrier_logic.js, while bin/lib/carrier_logic_pin.js is what
// writes and guards that pin. Two formulas for one published value are only
// safe while a test holds them equal, so this suite does that on the tree,
// then drives fresh copies of the module at a pin and at no pin (the image
// case P2 found), and finally reads the field off the real /health route.

const assert = require('assert');
const fs     = require('fs');
const os     = require('os');
const path   = require('path');
const { spawnSync } = require('child_process');

const ROOT = path.join(__dirname, '../../..');
const MODULE = path.join(ROOT, 'src/health/carrier_logic.js');
const health = require(MODULE);
const logicPin = require(path.join(ROOT, 'bin/lib/carrier_logic_pin.js'));
const { computeArmedMapFingerprintV2 } = require(path.join(ROOT, 'src/consensus/armed_map/fingerprint'));

const HEX64 = /^[0-9a-f]{64}$/;

// A fresh copy of the module at <dir>/src/health, so its PIN_PATH resolves to
// <dir>/bin/pins/carrier-logic.json: present or absent as the case needs.
function freshCopy(withPin) {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sync-carrier-logic-'));
    fs.mkdirSync(path.join(dir, 'src/health'), { recursive: true });
    fs.copyFileSync(MODULE, path.join(dir, 'src/health/carrier_logic.js'));
    if (withPin) {
        fs.mkdirSync(path.join(dir, 'bin/pins'), { recursive: true });
        fs.copyFileSync(health.PIN_PATH, path.join(dir, 'bin/pins/carrier-logic.json'));
    }
    return require(path.join(dir, 'src/health/carrier_logic.js'));
}

// Boots startApi() with the service, the coin-pin check and the listener
// stubbed (the shape armed_map/fingerprint.test.js uses) and prints the
// 503 body, which is the consensusIdentityFields() spread the route serves.
const HEALTH_DRIVE = `
const http = require('http');
const proxyquire = require(process.argv[1]);
let server = null;
class SyncService {
    isReady() { return false; }
    getHubConfigAgeSeconds() { return null; }
    getChains() { return []; }
    getDatabase() { return null; }
    start() { return Promise.resolve(); }
}
const api = proxyquire(process.argv[2], {
    './SyncService': SyncService,
    './coins': { verifyConsensusPin: () => {} },
    http: { createServer: (app) => (server = http.createServer(app)) },
});
(async () => {
    await api.startApi();
    while (!server.listening) await new Promise((r) => setTimeout(r, 10));
    http.get({ port: server.address().port, path: '/health' }, (res) => {
        let body = '';
        res.on('data', (d) => { body += d; });
        res.on('end', () => { process.stdout.write('\\n' + JSON.stringify({ status: res.statusCode, body: JSON.parse(body) })); process.exit(0); });
    }).on('error', (e) => { process.stderr.write(String(e)); process.exit(1); });
})().catch((e) => { process.stderr.write(String(e && e.stack)); process.exit(1); });
`;

describe('health/carrier_logic: the published carrier logic digest', function () {

    it('equals the pin module digest of the committed pin, with no second formula in effect', function () {
        const expected = logicPin.digest(logicPin.readPin(ROOT));
        assert.match(expected, HEX64);
        assert.strictEqual(health.carrierLogicDigest(), expected);
        assert.strictEqual(health.digestOf(logicPin.readPin(ROOT)), expected);
        assert.strictEqual(health.PIN_PATH, path.join(ROOT, 'bin/pins/carrier-logic.json'));
    });

    it('is memoised per process', function () {
        assert.strictEqual(health.carrierLogicDigest(), health.carrierLogicDigest());
    });

    it('reads the pin two directories up from src/health, so a copy with the pin publishes the hex', function () {
        assert.strictEqual(freshCopy(true).carrierLogicDigest(), logicPin.digest(logicPin.readPin(ROOT)));
    });

    it('publishes the literal UNREADABLE when the pin is absent, never a hex and never a throw', function () {
        const copy = freshCopy(false);
        assert.strictEqual(copy.carrierLogicDigest(), health.UNREADABLE);
        assert.strictEqual(copy.UNREADABLE, 'UNREADABLE');
        assert.ok(!HEX64.test(copy.carrierLogicDigest()));
    });

    it('never requires the pin module or the tokenizer, so a src-only image can load it', function () {
        const src = fs.readFileSync(MODULE, 'utf8');
        const required = [...src.matchAll(/require\(['"]([^'"]+)['"]\)/g)].map((m) => m[1]);
        assert.deepStrictEqual(required.sort(), ['crypto', 'fs', 'path']);
        const api = fs.readFileSync(path.join(ROOT, 'src/api.js'), 'utf8');
        assert.ok(!/require\(['"]\.\.\/bin/.test(api), 'src/api.js requires under bin/');
    });

    it('/health carries the digest as its own field beside v2 and version 2, with no _v2 alias', function () {
        this.timeout(30000);
        const res = spawnSync(process.execPath, ['-e', HEALTH_DRIVE, require.resolve('proxyquire'), path.join(ROOT, 'src/api.js')], {
            cwd: ROOT, encoding: 'utf8',
            env: { ...process.env, SYNC_API_PORT: '0', SYNC_MODE: 'client', XCHAIN_LOG_PATCH: '0' },
        });
        assert.strictEqual(res.status, 0, res.stderr);
        // startApi() logs its listening line to stdout ahead of the reading.
        const { status, body } = JSON.parse(res.stdout.slice(res.stdout.lastIndexOf('\n') + 1));
        assert.strictEqual(status, 503);
        assert.strictEqual(body.carrier_logic_digest, logicPin.digest(logicPin.readPin(ROOT)));
        assert.match(body.carrier_logic_digest, HEX64);
        assert.strictEqual(body.armed_map_fingerprint_version, 2);
        assert.strictEqual(body.armed_map_fingerprint, computeArmedMapFingerprintV2().hex);
        assert.ok(!Object.prototype.hasOwnProperty.call(body, 'armed_map_fingerprint_v2'), 'the W1 to W4 alias is gone at W5');
    });
});
