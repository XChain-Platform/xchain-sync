#!/usr/bin/env node
/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md.
 *
 **********************************************************************
 *
 * Do the configured sync sources agree on markets.id for each traded pair?
 *
 * A replica applies the markets full-dump as an UPSERT that carries the
 * source's AUTO_INCREMENT id, so two sources that number the same pair
 * differently hand a replica two id spaces, and a replica that holds a row at
 * an id the other source gives a different pair fails that upsert with
 * ER_DUP_ENTRY (ClientSync halts on it as apply-duplicate-key). This measures
 * whether that skew exists.
 *
 * READ-ONLY. Three GET routes per source, the same ones a replica calls:
 *   /status/indexer/<chain>/<network>                      the source tip
 *   /snapshot/indexer/<chain>/<network>/since/<tip>?skip_lookups=1
 *                                                          the markets full-dump
 *   /snapshot-rows/indexer/<chain>/<network>/index_tickers  ticker names, paged
 * Pairs are compared by ticker NAME, because index_tickers ids are node-local
 * too; a native-coin side (tick id 0) is keyed by its coin id.
 *
 * USAGE
 *   SYNC_SOURCES=http://a:3006,http://b:3006 node bin/markets-id-parity.js --chain BTC --network mainnet
 *   node bin/markets-id-parity.js --chain BTC --network mainnet --sources http://a:3006,http://b:3006
 * SYNC_UPSTREAM_KEY is sent as the bearer token when set.
 *
 * EXIT  0 every source agrees, 1 ids differ or pairs are missing, 2 usage or fetch error.
 */

'use strict';

const zlib  = require('zlib');
const axios = require('axios');

const TICKER_PAGE = 50000;

// Parse argv and env into the run's options, or throw a usage error.
function parseArgs(argv, env){
    let opts = { chain: null, network: null, sources: null };
    for(let i = 0; i < argv.length; i++){
        let a = argv[i];
        if(a === '--chain') opts.chain = argv[++i];
        else if(a === '--network') opts.network = argv[++i];
        else if(a === '--sources') opts.sources = argv[++i];
        else throw new Error('unknown argument: ' + a);
    }
    let raw = opts.sources || env.SYNC_SOURCES || '';
    opts.sources = raw.split(',').map(s => s.trim().replace(/\/+$/, '')).filter(s => s);
    if(!opts.chain || !opts.network) throw new Error('--chain and --network are required');
    if(opts.sources.length < 2) throw new Error('need at least two sources (--sources or SYNC_SOURCES)');
    opts.headers = env.SYNC_UPSTREAM_KEY ? { Authorization: 'Bearer ' + env.SYNC_UPSTREAM_KEY } : {};
    return opts;
}

// GET a JSON body that may arrive gzipped.
async function getJson(url, headers){
    let res = await axios.get(url, { headers, responseType: 'arraybuffer', timeout: 600000, decompress: true });
    let body = res.data;
    if(body && typeof body === 'object' && !Buffer.isBuffer(body)) return body;
    if(Buffer.isBuffer(body)){
        try { body = zlib.gunzipSync(body); } catch(e){ /* already inflated */ }
    }
    return JSON.parse(body.toString());
}

// Ticker id to name for one source, paged by id.
async function fetchTickers(base, headers){
    let names = new Map();
    let after = 0;
    for(;;){
        let page = await getJson(base.rows + '/index_tickers?after_id=' + after + '&limit=' + TICKER_PAGE, headers);
        for(let r of page.rows || []) names.set(String(r.id), String(r.tick));
        if(!page.has_more) return names;
        after = page.max_id;
    }
}

// The markets rows and ticker names one source serves at its current tip.
async function fetchSource(source, opts){
    let path = '/indexer/' + opts.chain + '/' + opts.network;
    let status = await getJson(source + '/status' + path, opts.headers);
    // Refuse a null height: since/0 would pull the whole history, not one block.
    let tip = (status.block_height == null) ? NaN : Number(status.block_height);
    if(!Number.isInteger(tip) || tip < 1) throw new Error(source + ' reported no block_height');
    let snap = await getJson(source + '/snapshot' + path + '/since/' + tip + '?skip_lookups=1', opts.headers);
    let tickers = await fetchTickers({ rows: source + '/snapshot-rows' + path }, opts.headers);
    return { tip, markets: (snap.tables && snap.tables.markets) || [], tickers };
}

// Name one side of a pair by ticker, or by coin id when it is the native coin.
function sideName(tickId, coinId, tickers){
    let t = String(tickId == null ? '' : tickId);
    if(t === '0') return 'coin#' + String(coinId == null ? 0 : coinId);
    return tickers.has(t) ? tickers.get(t) : '?tick#' + t;
}

// Map each pair name to its markets.id for one source.
function idsByPair(markets, tickers){
    let out = new Map();
    for(let m of markets){
        let pair = sideName(m.tick1_id, m.coin1_id, tickers) + '/' + sideName(m.tick2_id, m.coin2_id, tickers);
        out.set(pair, String(m.id));
    }
    return out;
}

// Compare per-source pair maps: pairs missing somewhere, pairs whose id differs,
// and ids that name different pairs on different sources (the upsert collision shape).
function compareSources(maps){
    let names = Object.keys(maps);
    let allPairs = new Set();
    for(let n of names) for(let p of maps[n].keys()) allPairs.add(p);
    let missing = [], differing = [];
    for(let pair of [...allPairs].sort()){
        let ids = {};
        for(let n of names) ids[n] = maps[n].has(pair) ? maps[n].get(pair) : null;
        let present = Object.values(ids).filter(v => v !== null);
        if(present.length < names.length) missing.push({ pair, ids });
        if(new Set(present).size > 1) differing.push({ pair, ids });
    }
    return { pairs: allPairs.size, missing, differing, collisions: idCollisions(maps) };
}

// Ids that name one pair on one source and a different pair on another.
function idCollisions(maps){
    let byId = new Map();
    for(let [src, m] of Object.entries(maps)){
        for(let [pair, id] of m){
            if(!byId.has(id)) byId.set(id, {});
            byId.get(id)[src] = pair;
        }
    }
    let out = [];
    for(let [id, pairs] of byId)
        if(new Set(Object.values(pairs)).size > 1) out.push({ id, pairs });
    return out.sort((a, b) => Number(a.id) - Number(b.id));
}

// Human-readable report, ending with the verdict line.
function renderReport(opts, fetched, result){
    let lines = ['markets id parity: ' + opts.chain + '/' + opts.network];
    for(let s of opts.sources)
        lines.push('  ' + s + '  tip ' + fetched[s].tip + '  markets ' + fetched[s].markets.length);
    lines.push('pairs seen: ' + result.pairs);
    lines.push('pairs missing on some source: ' + result.missing.length);
    for(let d of result.missing.slice(0, 20)) lines.push('  ' + d.pair + '  ' + JSON.stringify(d.ids));
    lines.push('pairs whose id differs: ' + result.differing.length);
    for(let d of result.differing.slice(0, 20)) lines.push('  ' + d.pair + '  ' + JSON.stringify(d.ids));
    lines.push('ids naming different pairs across sources: ' + result.collisions.length);
    for(let c of result.collisions.slice(0, 20)) lines.push('  id ' + c.id + '  ' + JSON.stringify(c.pairs));
    let agree = !result.differing.length && !result.missing.length;
    lines.push(agree ? 'VERDICT: sources agree on markets.id for every pair'
                     : 'VERDICT: markets ids are NOT reproducible across these sources');
    return { text: lines.join('\n'), agree };
}

async function main(){
    let opts;
    try { opts = parseArgs(process.argv.slice(2), process.env); }
    catch(e){ console.error('usage error: ' + e.message); process.exitCode = 2; return; }
    let fetched = {};
    try {
        for(let s of opts.sources) fetched[s] = await fetchSource(s, opts);
    } catch(e){
        console.error('fetch failed: ' + (e.message || e));
        process.exitCode = 2;
        return;
    }
    let maps = {};
    for(let s of opts.sources) maps[s] = idsByPair(fetched[s].markets, fetched[s].tickers);
    let report = renderReport(opts, fetched, compareSources(maps));
    console.log(report.text);
    process.exitCode = report.agree ? 0 : 1;
}

if(require.main === module) main();

module.exports = { parseArgs, fetchSource, sideName, idsByPair, compareSources, idCollisions, renderReport };
