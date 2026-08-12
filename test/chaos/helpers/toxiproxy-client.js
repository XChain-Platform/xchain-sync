/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************
 * Toxiproxy client: manages two named proxies (source DB + replica DB)
 * and an optional WebSocket proxy for chaos engineering experiments.
 *
 * Two-proxy model: source and replica are faulted independently via named
 * proxy instances. Each exports a `faults` object with the full fault API.
 *
 * Toxiproxy API docs: https://github.com/Shopify/toxiproxy#http-api
 *
 * Requires: toxiproxy running on localhost:8474 (see docker-compose.chaos.yml)
 */

'use strict';

const http = require('http');

const TOXIPROXY_HOST = process.env.TOXIPROXY_HOST || '127.0.0.1';
const TOXIPROXY_PORT = parseInt(process.env.TOXIPROXY_PORT || '8474', 10);

const SOURCE_PROXY = {
    name:     'source_db_chaos',
    upstream: 'source-db-chaos:3306',
    listen:   '0.0.0.0:33060'
};

const REPLICA_PROXY = {
    name:     'replica_db_chaos',
    upstream: 'replica-db-chaos:3306',
    listen:   '0.0.0.0:33061'
};

// WS proxy is created on-demand by CE-NET tests (upstream varies per test port)
const WS_PROXY_LISTEN = '0.0.0.0:33062';
const WS_PROXY_NAME   = 'ws_chaos';

// Raw http module; no axios dependency for this helper.
function request(method, path, body) {
    return new Promise((resolve, reject) => {
        const payload = body ? JSON.stringify(body) : null;
        const opts = {
            hostname: TOXIPROXY_HOST,
            port:     TOXIPROXY_PORT,
            path,
            method,
            headers:  { 'Content-Type': 'application/json' }
        };
        if (payload) opts.headers['Content-Length'] = Buffer.byteLength(payload);

        const req = http.request(opts, (res) => {
            let data = '';
            res.on('data', (chunk) => { data += chunk; });
            res.on('end', () => {
                const parsed = data ? JSON.parse(data) : null;
                if (res.statusCode >= 400) {
                    const err = new Error(`Toxiproxy ${method} ${path} → ${res.statusCode}: ${data}`);
                    err.statusCode = res.statusCode;
                    err.body = parsed;
                    return reject(err);
                }
                resolve(parsed);
            });
        });
        req.on('error', reject);
        if (payload) req.write(payload);
        req.end();
    });
}

async function createProxy(def) {
    try {
        await request('POST', '/proxies', {
            name:     def.name,
            listen:   def.listen,
            upstream: def.upstream,
            enabled:  true
        });
    } catch (e) {
        // 409 = proxy already exists; that's fine
        if (e.statusCode !== 409) throw e;
    }
}

async function deleteProxy(name) {
    try {
        await request('DELETE', `/proxies/${name}`);
    } catch (e) {
        if (e.statusCode !== 404) throw e;
    }
}

async function enableProxy(name) {
    await request('POST', `/proxies/${name}`, { enabled: true });
}

async function disableProxy(name) {
    await request('POST', `/proxies/${name}`, { enabled: false });
}

async function resetProxy(name) {
    await removeAllToxics(name);
    await enableProxy(name);
}

async function resetAllProxies() {
    await resetProxy(SOURCE_PROXY.name);
    await resetProxy(REPLICA_PROXY.name);
}

async function addToxic(proxyName, toxic) {
    return request('POST', `/proxies/${proxyName}/toxics`, {
        name:       toxic.name || `${toxic.type}_${toxic.stream || 'downstream'}`,
        type:       toxic.type,
        stream:     toxic.stream || 'downstream',
        toxicity:   toxic.toxicity != null ? toxic.toxicity : 1.0,
        attributes: toxic.attributes || {}
    });
}

async function removeToxic(proxyName, toxicName) {
    try {
        await request('DELETE', `/proxies/${proxyName}/toxics/${toxicName}`);
    } catch (e) {
        if (e.statusCode !== 404) throw e;
    }
}

async function listToxics(proxyName) {
    return request('GET', `/proxies/${proxyName}/toxics`);
}

async function removeAllToxics(proxyName) {
    const toxics = await listToxics(proxyName);
    if (Array.isArray(toxics)) {
        for (const t of toxics) {
            await removeToxic(proxyName, t.name);
        }
    }
}

function createProxyFaults(proxyName) {
    return {
        async dbDown() {
            await disableProxy(proxyName);
        },

        async dbUp() {
            await enableProxy(proxyName);
        },

        async addLatency(ms, jitter = 0) {
            return addToxic(proxyName, {
                type: 'latency',
                stream: 'downstream',
                name: 'chaos_latency',
                attributes: { latency: ms, jitter }
            });
        },

        async limitBandwidth(bytesPerSec) {
            return addToxic(proxyName, {
                type: 'bandwidth',
                stream: 'downstream',
                name: 'chaos_bandwidth',
                attributes: { rate: bytesPerSec }
            });
        },

        async resetConnections(toxicity = 0.3) {
            return addToxic(proxyName, {
                type: 'reset_peer',
                stream: 'downstream',
                name: 'chaos_reset',
                toxicity,
                attributes: { timeout: 0 }
            });
        },

        async timeout(ms) {
            return addToxic(proxyName, {
                type: 'timeout',
                stream: 'downstream',
                name: 'chaos_timeout',
                attributes: { timeout: ms }
            });
        },

        async limitData(bytes) {
            return addToxic(proxyName, {
                type: 'limit_data',
                stream: 'downstream',
                name: 'chaos_limit_data',
                attributes: { bytes }
            });
        },

        async sliceData(avgBytes = 10, delay = 100) {
            return addToxic(proxyName, {
                type: 'slicer',
                stream: 'downstream',
                name: 'chaos_slicer',
                attributes: { average_size: avgBytes, size_variation: 5, delay }
            });
        },

        async reset() {
            await resetProxy(proxyName);
        }
    };
}

const sourceFaults  = createProxyFaults(SOURCE_PROXY.name);
const replicaFaults = createProxyFaults(REPLICA_PROXY.name);

async function createWsProxy(serverPort) {
    const hostIp = process.env.CHAOS_HOST_IP || '172.17.0.1';
    const def = {
        name:     WS_PROXY_NAME,
        upstream: `${hostIp}:${serverPort}`,
        listen:   WS_PROXY_LISTEN
    };
    await createProxy(def);
    return def;
}

async function deleteWsProxy() {
    await deleteProxy(WS_PROXY_NAME);
}

const wsFaults = createProxyFaults(WS_PROXY_NAME);

async function resetBoth() {
    await resetProxy(SOURCE_PROXY.name);
    await resetProxy(REPLICA_PROXY.name);
}

async function waitForToxiproxy(timeoutMs = 15000) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        try {
            await request('GET', '/version');
            return true;
        } catch {
            await new Promise(r => setTimeout(r, 500));
        }
    }
    throw new Error(`Toxiproxy not reachable at ${TOXIPROXY_HOST}:${TOXIPROXY_PORT} after ${timeoutMs}ms`);
}

module.exports = {
    request,
    waitForToxiproxy,
    createProxy,
    deleteProxy,
    enableProxy,
    disableProxy,
    resetProxy,
    resetAllProxies,
    addToxic,
    removeToxic,
    listToxics,
    removeAllToxics,
    createProxyFaults,
    SOURCE_PROXY,
    REPLICA_PROXY,
    WS_PROXY_NAME,
    sourceFaults,
    replicaFaults,
    wsFaults,
    createWsProxy,
    deleteWsProxy,
    resetBoth
};
