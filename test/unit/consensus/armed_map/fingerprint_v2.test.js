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

// v2 is published as the armed-map identity on /health and in the identity pin. The
// value is only useful if every surface carries the SAME computation, so this
// suite checks the module against the canonicaliser directly, then boots the
// real /health route in a child process and reads both bodies it can return.

const assert = require('assert');
const crypto = require('crypto');
const fs     = require('fs');
const path   = require('path');
const { spawnSync } = require('child_process');

const ROOT = path.join(__dirname, '../../../..');
const { computeArmedMapFingerprintV2 } = require(path.join(ROOT, 'src/consensus/armed_map/fingerprint_v2'));
const { ENTRIES, collectRows } = require(path.join(ROOT, 'src/consensus/armed_map/manifest'));
const { canonicalValue, fingerprint } = require(path.join(ROOT, 'src/consensus/armed_map/canonical'));
const { buildPin, compare } = require(path.join(ROOT, 'bin/pin-identity.js'));
const logicPin = require(path.join(ROOT, 'bin/lib/carrier_logic_pin.js'));

// Boots startApi() with the service, the coin-pin check and the listener
// stubbed, so no database, hub or fixed port is involved, then reads /health
// once while the service is still starting (503) and once when ready (200).
const HEALTH_DRIVE = `
const http = require('http');
const proxyquire = require(process.argv[1]);
let ready = false;
let server = null;
class SyncService {
    isReady() { return ready; }
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
const get = (port) => new Promise((resolve, reject) => {
    http.get({ port, path: '/health' }, (res) => {
        let body = '';
        res.on('data', (d) => { body += d; });
        res.on('end', () => resolve({ status: res.statusCode, keys: Object.keys(JSON.parse(body)), body: JSON.parse(body) }));
    }).on('error', reject);
});
(async () => {
    await api.startApi();
    while (!server.listening) await new Promise((r) => setTimeout(r, 10));
    const port = server.address().port;
    const starting = await get(port);
    ready = true;
    const healthy = await get(port);
    process.stdout.write(JSON.stringify({ starting, healthy }));
    process.exit(0);
})().catch((e) => { process.stderr.write(String(e && e.stack)); process.exit(1); });
`;

// Declared once per section under one title, so each callback stays under the 60-line
// function limit and the pinned suite titles do not move.
describe('armed map v2: fingerprint module and publication', function () {

    it('publishes a 64-hex fingerprint over every manifest row', function () {
        const v2 = computeArmedMapFingerprintV2();
        assert.match(v2.hex, /^[0-9a-f]{64}$/, v2.reason);
        assert.strictEqual(v2.count, ENTRIES.length);
        assert.strictEqual(Object.keys(v2.rows).length, ENTRIES.length);
    });

    it('is the canonical fingerprint of the manifest rows, with no second computation path', function () {
        const collected = collectRows();
        assert.strictEqual(collected.ok, true, collected.reason);
        assert.strictEqual(computeArmedMapFingerprintV2().hex, fingerprint(collected.rows).hex);
    });

    it('names each row by the sha256 of its exported value, so a mismatch points at the row', function () {
        const { STATE_COMMITMENT_ACTIVATION } = require(path.join(ROOT, 'src/state_commitment_activation'));
        const expected = crypto.createHash('sha256').update(canonicalValue(STATE_COMMITMENT_ACTIVATION), 'utf8').digest('hex');
        assert.strictEqual(computeArmedMapFingerprintV2().rows['state_commitment_activation.STATE_COMMITMENT_ACTIVATION'], expected);
    });

    it('is memoised per process, like v1', function () {
        assert.strictEqual(computeArmedMapFingerprintV2(), computeArmedMapFingerprintV2());
    });

    it('never lists a directory, so the value cannot depend on the file layout', function () {
        for (const rel of ['canonical.js', 'manifest.js', 'fingerprint_v2.js']) {
            const src = fs.readFileSync(path.join(ROOT, 'src/consensus/armed_map', rel), 'utf8');
            assert.ok(!/readdirSync|readdir\(/.test(src), rel + ' reads a directory');
        }
    });

    it('uses v2 for the legacy field and records version 2 plus the logic digest', function () {
        const pin = buildPin();
        assert.strictEqual(pin.armed_map_fingerprint, computeArmedMapFingerprintV2().hex);
        assert.strictEqual(pin.armed_map_fingerprint_v2, pin.armed_map_fingerprint);
        assert.strictEqual(pin.armed_map_fingerprint_version, 2);
        assert.strictEqual(pin.carrier_logic_digest, logicPin.digest(logicPin.readPin(ROOT)));
    });
});

describe('armed map v2: fingerprint module and publication', function () {

    it('both /health bodies carry v2, version 2 and the carrier logic digest', function () {
        this.timeout(30000);
        const res = spawnSync(process.execPath, ['-e', HEALTH_DRIVE, require.resolve('proxyquire'), path.join(ROOT, 'src/api.js')], {
            cwd: ROOT, encoding: 'utf8',
            env: { ...process.env, SYNC_API_PORT: '0', SYNC_MODE: 'client', XCHAIN_LOG_PATCH: '0' },
        });
        assert.strictEqual(res.status, 0, res.stderr);
        // startApi() logs its listening line to stdout ahead of the readings.
        const { starting, healthy } = JSON.parse(res.stdout.slice(res.stdout.indexOf('{"starting"')));
        const hex = computeArmedMapFingerprintV2().hex;
        const logicDigest = logicPin.digest(logicPin.readPin(ROOT));
        assert.strictEqual(starting.status, 503);
        assert.strictEqual(starting.body.status, 'starting');
        assert.strictEqual(healthy.status, 200);
        for (const reading of [starting, healthy]) {
            assert.strictEqual(reading.body.armed_map_fingerprint, hex);
            assert.strictEqual(reading.body.armed_map_fingerprint_v2, hex);
            assert.strictEqual(reading.body.armed_map_fingerprint_version, 2);
            assert.strictEqual(reading.body.carrier_logic_digest, logicDigest);
            assert.strictEqual(reading.keys.indexOf('armed_map_fingerprint_v2'), reading.keys.indexOf('armed_map_fingerprint') + 1);
        }
    });

    describe('identity pin', function () {
        it('records v2, its row hashes and count with no v1 fields', function () {
            const pin = buildPin();
            assert.strictEqual(pin.armed_map_fingerprint, computeArmedMapFingerprintV2().hex);
            assert.strictEqual(pin.armed_map_fingerprint_v2, computeArmedMapFingerprintV2().hex);
            assert.strictEqual(pin.armed_map_rows, ENTRIES.length);
            assert.deepStrictEqual(pin.armedMapRows, computeArmedMapFingerprintV2().rows);
            assert.ok(!Object.prototype.hasOwnProperty.call(pin, 'armedMapFingerprint'));
            assert.ok(!Object.prototype.hasOwnProperty.call(pin, 'armedMapFiles'));
        });

        it('reports a moved v2 and a changed row count, and nothing for an identical tree', function () {
            const fresh = buildPin();
            assert.deepStrictEqual(compare(fresh, fresh), []);
            const movedV2 = compare({ ...fresh, armed_map_fingerprint_v2: '0'.repeat(64) }, fresh);
            assert.strictEqual(movedV2.length, 1);
            assert.ok(movedV2[0].includes(fresh.armed_map_fingerprint_v2), movedV2[0]);
            assert.strictEqual(compare({ ...fresh, armed_map_rows: fresh.armed_map_rows - 1 }, fresh).length, 1);
            assert.strictEqual(compare({ ...fresh, armed_map_fingerprint_v2: undefined }, fresh).length, 1,
                'a pin taken before v2 existed must not read as holding');
        });
    });
});
