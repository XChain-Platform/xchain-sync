// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// A fixture-backed stand-in for the incremental snapshot's database reads. The
// real Database mixins build every SQL string; this fake answers them from
// fixture arrays, applying only the scope predicates the window logic depends
// on (the base `>= ?` cursor and the optional side-set `< ?` bound). It is a
// JavaScript emulation of those predicates, not MariaDB.
//
// Like the real driver, a query sent while a row stream on the same
// connection is still open cannot be answered (the paused stream holds the
// connection), so this fake rejects it instead of hanging. The connection is
// freed when the stream ends or when close() is called; destroy() alone does
// not free it, matching the driver.

const { Readable } = require('stream');
const { withDbMixins } = require('../../../helpers/db_mixins.js');

const SIDE_SETS = [
    [/FROM credits c .*JOIN unstakes u/, 'capCredits'],
    [/FROM credits c .*JOIN contract_unstakes cu/, 'conCredits'],
    [/FROM escrows e .*JOIN unstakes u/, 'capEscrows'],
    [/FROM escrows e .*JOIN contract_unstakes cu/, 'conEscrows'],
    [/recovery_pending_rewards/, 'redriven'],
    [/derive_block_index BETWEEN/, 'derived'],
];

const BASES = [
    [/FROM `credits` WHERE action_index >= \?/, 'credits', 'action_index'],
    [/FROM `escrows` WHERE action_index >= \?/, 'escrows', 'action_index'],
    [/FROM `validator_rewards` WHERE block_index >= \?/, 'validator_rewards', 'block_index'],
];

function sideRows(fx, sql, args){
    const hit = SIDE_SETS.find(([re]) => re.test(sql));
    let rows = hit ? (fx[hit[1]] || []) : [];
    const bound = args[args.length - 1];
    if(/ AND c\.action_index < \?$| AND e\.action_index < \?$/.test(sql))
        rows = rows.filter(r => r.action_index < bound);
    if(/ AND vr\.block_index < \?$/.test(sql))
        rows = rows.filter(r => r.block_index < bound);
    return rows.map(r => ({ ...r }));
}

function baseRows(fx, sql, args){
    const hit = BASES.find(([re]) => re.test(sql));
    if(!hit) return [];
    return (fx[hit[1]] || []).filter(r => r[hit[2]] >= args[0]).map(r => ({ ...r }));
}

function windowFakeDb(fx){
    const calls = [];
    let openStreams = 0;
    const conn = {
        queryStream(sql, args){
            calls.push({ kind: 'stream', sql, args });
            openStreams++;
            const stream = Readable.from(baseRows(fx, sql, args));
            let closed = false;
            const release = () => { if(!closed){ closed = true; openStreams--; } };
            stream.once('end', release);
            stream.close = release;
            return stream;
        },
        held: () => openStreams > 0,
    };
    const db = withDbMixins({
        doQuery: async (sql, args) => {
            if(openStreams > 0) throw new Error('query queued behind an open row stream');
            calls.push({ kind: 'query', sql, args });
            return sideRows(fx, sql, args);
        },
        getStatusId: async () => 1,
    });
    return { db, conn, calls };
}

function windowCtx(conn, sinceBlock, firstActionIndex){
    return {
        dbType: 'indexer', sinceBlock, lastBlock: sinceBlock + 50, conn,
        skipLookups: false, lookupSet: new Set(), firstActionIndex,
        indexerBlockScoped: new Set(['validator_rewards']), indexerFullDump: new Set(),
    };
}

module.exports = { windowFakeDb, windowCtx, baseRows };
