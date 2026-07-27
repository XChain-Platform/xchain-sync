#!/usr/bin/env node
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
 *
 *  - per-block fan-out load test against a REAL MariaDB.
 *
 * ServerPoller assembles each indexer block payload from ~113 sequential
 * queries; every one takes a pooled connection. This runner replays that
 * fan-out at a range of connection limits and reports wall time per block,
 * so a pool size can be chosen from measurement instead of from a guess.
 * It also runs a "poller under snapshot load" leg: N connections pinned the
 * way a bootstrap cohort pins them, then a poller-shaped acquire timed
 * against the remainder.
 *
 * Read-only: it issues SELECTs against the target schema and never writes.
 * Point it at a REGTEST database, never mainnet.
 *
 * Credentials come from the environment only (never arguments, never
 * printed). Accepted names, first match wins:
 *   INDEXER_DB_HOST/PORT/USER/PASS/NAME   (xchain-node container env)
 *   DECODER_DB_*                          (with DBTYPE=decoder)
 *   DB_HOST/DB_PORT/DB_USER/DB_PASS/DB_NAME
 *
 * Usage (inside a container that already holds the creds):
 *   node pool-fanout-load.js
 *   BLOCKS=20 POOL_SIZES=5,12,20 node pool-fanout-load.js
 *
 ********************************************************************/

'use strict';

const mariadb = require('mariadb');

const DBTYPE  = (process.env.DBTYPE || 'indexer').toLowerCase();
const PREFIX  = DBTYPE === 'decoder' ? 'DECODER_DB_' : 'INDEXER_DB_';

function env(suffix, fallback){
    return process.env[PREFIX + suffix] || process.env['DB_' + suffix] || fallback;
}

const CONF = {
    host: env('HOST', '127.0.0.1'),
    port: parseInt(env('PORT', '3306'), 10),
    user: env('USER'),
    pass: env('PASS'),
    name: env('NAME')
};

// Queries per block payload: the measured ServerPoller fan-out (block-scoped
// rows over 8+ tables, action-scoped rows over 80+ tables, index-id pooling).
const QUERIES_PER_BLOCK = parseInt(process.env.QUERIES_PER_BLOCK || '113', 10);
const BLOCKS            = parseInt(process.env.BLOCKS || '10', 10);
const POOL_SIZES        = (process.env.POOL_SIZES || '3,5,12,20')
                            .split(',').map(s => parseInt(s.trim(), 10)).filter(Boolean);

// Per-query server-side cost, in ms. A regtest schema answers the real
// per-block queries in microseconds, which makes every pool size look alike;
// a populated mainnet indexer answers the same joins in single-digit ms, and
// THAT is the regime the pool has to absorb. QUERY_COST_MS models it (0 for a
// raw round-trip-only run).
const QUERY_COST_MS = parseFloat(process.env.QUERY_COST_MS || '5');

// One fan-out query: schema-agnostic so the run measures pool scheduling
// rather than one deployment's row counts, while still crossing the wire and
// occupying a real server thread for QUERY_COST_MS.
const FANOUT_SQL = 'SELECT ? AS n, SLEEP(?) AS cost';

async function runFanOut(pool, queries){
    let work = [];
    let cost = QUERY_COST_MS / 1000;
    for(let i = 0; i < queries; i++){
        work.push((async () => {
            let conn = await pool.getConnection();
            try { await conn.query(FANOUT_SQL, [i, cost]); }
            finally { await conn.release(); }
        })());
    }
    await Promise.all(work);
}

async function measure(limit){
    let pool = mariadb.createPool({
        host: CONF.host, port: CONF.port, user: CONF.user,
        password: CONF.pass, database: CONF.name,
        connectionLimit: limit, acquireTimeout: 30000, queryTimeout: 30000
    });
    try {
        // Warm the pool so connection setup is not billed to block 1.
        await runFanOut(pool, limit);

        let perBlock = [];
        for(let b = 0; b < BLOCKS; b++){
            let t0 = process.hrtime.bigint();
            await runFanOut(pool, QUERIES_PER_BLOCK);
            perBlock.push(Number(process.hrtime.bigint() - t0) / 1e6);
        }

        // Poller-under-snapshot-load leg: pin (limit - 2) connections the way
        // concurrent /snapshot streams do, then time a poller-shaped acquire
        // plus query against what is left.
        let pinned = [];
        let pinCount = Math.max(0, limit - 2);
        for(let i = 0; i < pinCount; i++) pinned.push(await pool.getConnection());
        let t1 = process.hrtime.bigint();
        let pollerMs = null;
        try {
            let conn = await pool.getConnection();
            await conn.query(FANOUT_SQL, [0, 0]);
            await conn.release();
            pollerMs = Number(process.hrtime.bigint() - t1) / 1e6;
        } catch (e){
            pollerMs = 'STARVED (' + e.code + ')';
        }
        for(let c of pinned) await c.release();

        perBlock.sort((a, b) => a - b);
        return {
            limit,
            medianMs: perBlock[Math.floor(perBlock.length / 2)],
            minMs:    perBlock[0],
            maxMs:    perBlock[perBlock.length - 1],
            pinned:   pinCount,
            pollerMs
        };
    } finally {
        await pool.end();
    }
}

function fmt(v){
    return typeof v === 'number' ? v.toFixed(1).padStart(9) : String(v).padStart(9);
}

(async () => {
    if(!CONF.user || !CONF.name){
        console.error('Missing DB env (' + PREFIX + 'USER / ' + PREFIX + 'NAME or DB_USER / DB_NAME).');
        process.exit(2);
    }
    console.log(' pool fan-out load: dbType=' + DBTYPE + ' schema=' + CONF.name +
                ' blocks=' + BLOCKS + ' queries/block=' + QUERIES_PER_BLOCK);
    console.log('    pool   median(ms)      min      max   pinned  pollerAcquire+query(ms)');
    let results = [];
    for(let limit of POOL_SIZES){
        let r = await measure(limit);
        results.push(r);
        console.log('  ' + String(r.limit).padStart(6) + fmt(r.medianMs) + fmt(r.minMs) +
                    fmt(r.maxMs) + String(r.pinned).padStart(9) + fmt(r.pollerMs));
    }
    let best  = results.reduce((a, b) => (a.medianMs <= b.medianMs ? a : b));
    let five  = results.find(r => r.limit === 5);
    if(five && best.limit !== 5)
        console.log('  -> pool ' + best.limit + ' is ' + (five.medianMs / best.medianMs).toFixed(2) +
                    'x faster per block than pool 5');
    process.exit(0);
})().catch(e => { console.error('load run failed:', e.message); process.exit(1); });
