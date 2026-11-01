/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
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
 * In-process replica over an in-memory table store, used for both an origin
 * and a replica. The origin mints blocks and hashes them with the real
 * BlockHasher; the replica is the real ClientSync, ClientApplier,
 * HashVerifier and Utility. Origin transport is a sinon double on axios.get
 * and rollback deletes rows at or above the target height, so these tests
 * prove ClientSync sequencing (verification, resume, halts), not SQL.
 *
 ********************************************************************/
'use strict';
const sinon = require('sinon');
const axios = require('axios');
const mathjs = require('mathjs');
const ClientSync = require('../../src/client/sync');
const ClientApplier = require('../../src/client/applier');
const HashVerifier = require('../../src/client/hash_verifier');
const BlockHasher = require('../../src/client/block_hasher');
const Utility = require('../../src/util');
const { SCHEMA_VERSION } = require('../../src/schema/version');

const NUM_COLS = new Set(['id', 'block_index', 'block_time', 'ledger_hash_id', 'actions_hash_id',
    'contract_hash_id', 'state_hash_id', 'action_index', 'tx_index', 'tx_vout', 'action_id',
    'action_format', 'source_id', 'address_id', 'tick_id']);
const UNIQUE = {
    actions: [['action_index']], index_addresses: [['address']], index_tickers: [['tick']],
    index_transactions: [['hash']], balances: [['address_id', 'tick_id']]
};
const AUTO_ID = new Set(['blocks', 'index_addresses', 'index_tickers', 'index_actions', 'index_transactions', 'balances']);

// Mimic the pool typeCast: a BIGINT is a Number while safe, else its exact string.
function coerce(col, v){
    if(v === null || v === undefined) return null;
    if(NUM_COLS.has(col)){
        let n = Number(v);
        return Number.isSafeInteger(n) ? n : String(v);
    }
    return String(v);
}
function clone(x){ return JSON.parse(JSON.stringify(x)); }
function binCmp(a, b){ if(a === b) return 0; if(a === null) return -1; if(b === null) return 1; return a < b ? -1 : 1; }

class Store {
    constructor(name){
        this.dbName = name; this.dbType = 'indexer';
        this.tables = {}; this.autoInc = {}; this.halts = []; this.state = new Map();
        this._tx = null; this.unknown = []; this.faults = {}; this.insertCalls = 0; this.queryLog = [];
        this.transactionConnection = null;
    }
    rows(t){ return this.tables[t] || (this.tables[t] = []); }
    nextId(t){
        let m = this.autoInc[t] || 0;
        for(const r of this.rows(t)) if(typeof r.id === 'number' && r.id > m) m = r.id;
        this.autoInc[t] = m + 1; return m + 1;
    }
    conflictOf(t, row){
        let rows = this.rows(t);
        if(row.id !== undefined && row.id !== null){
            let hit = rows.find(r => r.id !== undefined && String(r.id) === String(row.id));
            if(hit) return { hit, key: 'PRIMARY', val: row.id };
        }
        for(const cols of (UNIQUE[t] || [])){
            let hit = rows.find(r => cols.every(c => String(r[c]) === String(row[c])));
            if(hit) return { hit, key: cols.join('_'), val: cols.map(c => row[c]).join('-') };
        }
        return null;
    }
    insertRow(t, row, ignore, upsert){
        if(AUTO_ID.has(t) && (row.id === undefined || row.id === null)) row.id = this.nextId(t);
        let c = this.conflictOf(t, row);
        if(c){
            if(upsert){ Object.assign(c.hit, row); return c.hit; }
            if(ignore) return null;
            let e = new Error("Duplicate entry '" + c.val + "' for key '" + c.key + "'");
            e.errno = 1062; e.code = 'ER_DUP_ENTRY'; throw e;
        }
        this.rows(t).push(row);
        if(typeof row.id === 'number') this.autoInc[t] = Math.max(this.autoInc[t] || 0, row.id);
        return row;
    }
    async insertRowValues(table, columns, n, args, ignore, upsert){
        this.insertCalls++;
        if(this.faults.insertThrowAt && this.insertCalls === this.faults.insertThrowAt){
            this.faults.insertThrowAt = null;
            throw new Error('injected crash at insert call ' + this.insertCalls);
        }
        for(let i = 0; i < n; i++){
            let row = {};
            columns.forEach((c, j) => { row[c] = coerce(c, args[i * columns.length + j]); });
            this.insertRow(table, row, ignore, upsert);
        }
        return { affectedRows: n };
    }
    async deleteRowsByKeyValues(table, key, values){
        let set = new Set(values.map(String));
        this.tables[table] = this.rows(table).filter(r => !set.has(String(r[key])));
    }
    async beginTransaction(){
        if(this._tx) throw new Error('nested transaction');
        this._tx = clone(this.tables); this.transactionConnection = {};
    }
    async commitTransaction(){
        if(this.faults.commitThrow){ this.faults.commitThrow = false; throw new Error('injected crash at commit'); }
        this._tx = null; this.transactionConnection = null;
    }
    async rollbackTransaction(){ if(this._tx){ this.tables = this._tx; } this._tx = null; this.transactionConnection = null; }
    lookup(t, col, id){ let r = this.rows(t).find(x => String(x.id) === String(id)); return r ? r[col] : null; }
    async getLastBlock(){
        let m = null;
        for(const b of this.rows('blocks')) if(m === null || b.block_index > m) m = b.block_index;
        return m;
    }
    async getBlockHashRow(idx){
        if(this.faults.hashRowThrow){ this.faults.hashRowThrow--; throw new Error('injected hash row read error'); }
        let b = this.rows('blocks').find(r => Number(r.block_index) === Number(idx));
        if(!b) return null;
        return { block_index: b.block_index, block_time: b.block_time,
            ledger_hash: this.lookup('index_transactions', 'hash', b.ledger_hash_id),
            actions_hash: this.lookup('index_transactions', 'hash', b.actions_hash_id),
            contract_hash: this.lookup('index_transactions', 'hash', b.contract_hash_id),
            state_hash: null };
    }
    actionsAt(h){ return this.rows('actions').filter(a => Number(a.block_index) === Number(h)); }
    async doQueryStrict(sql, params){
        if(this.faults.strictThrow){ this.faults.strictThrow--; throw new Error('injected recompute read error'); }
        let q = sql.replace(/\s+/g, ' ');
        let h = Number(params[0]);
        let m = q.match(/FROM (credits|debits|escrows) [cde] INNER JOIN actions/);
        if(m){
            let aidx = new Set(this.actionsAt(h).map(a => String(a.action_index)));
            let out = this.rows(m[1]).filter(r => aidx.has(String(r.action_index))).map(r => ({
                action_index: r.action_index,
                address: this.lookup('index_addresses', 'address', r.address_id),
                tick: this.lookup('index_tickers', 'tick', r.tick_id),
                amount: r.amount }));
            out.sort((a, b) => (Number(a.action_index) - Number(b.action_index)) || binCmp(a.address, b.address) ||
                binCmp(a.tick, b.tick) || binCmp(a.amount, b.amount));
            return out;
        }
        if(q.includes('FROM actions a LEFT JOIN index_actions')){
            return this.actionsAt(h).slice().sort((a, b) => Number(a.action_index) - Number(b.action_index))
                .map(a => ({ action_index: a.action_index, tx_index: a.tx_index,
                    action: this.lookup('index_actions', 'action', a.action_id) }));
        }
        if(q.includes('FROM blocks b LEFT JOIN index_transactions t1') && q.includes('as ledger')){
            return this.rows('blocks').filter(b => Number(b.block_index) === h).map(b => ({
                ledger: this.lookup('index_transactions', 'hash', b.ledger_hash_id),
                actions: this.lookup('index_transactions', 'hash', b.actions_hash_id),
                contracts: this.lookup('index_transactions', 'hash', b.contract_hash_id) }));
        }
        if(/FROM (contracts|contract_state|contract_executions|contract_emissions|deposits|withdrawals) /.test(q)) return [];
        throw new Error('harness: unhandled strict query ' + q.slice(0, 120));
    }
    async doQuery(sql, params, conn, opts){
        let q = String(sql).replace(/\s+/g, ' ').trim();
        this.queryLog.push(q.slice(0, 60));
        if(q === 'SHOW WARNINGS') return [];
        if(q.startsWith('DELETE FROM balances')){
            if(!params || !params.length){ this.tables.balances = []; return []; }
            let half = params.length / 2;
            let addrs = new Set(params.slice(0, half).map(String)), ticks = new Set(params.slice(half).map(String));
            this.tables.balances = this.rows('balances').filter(r => !(addrs.has(String(r.address_id)) && ticks.has(String(r.tick_id))));
            return [];
        }
        if(q.startsWith('INSERT INTO balances')){
            let scoped = q.includes('WHERE address_id IN');
            let half = scoped ? params.length / 4 : 0;
            let addrs = scoped ? new Set(params.slice(0, half).map(String)) : null;
            let ticks = scoped ? new Set(params.slice(half, 2 * half).map(String)) : null;
            let sums = new Map();
            for(const [t, sign] of [['credits', 1], ['debits', -1]]){
                for(const r of this.rows(t)){
                    if(scoped && !(addrs.has(String(r.address_id)) && ticks.has(String(r.tick_id)))) continue;
                    let k = r.address_id + '\t' + r.tick_id;
                    let v = mathjs.bignumber(r.amount); if(sign < 0) v = mathjs.multiply(v, -1);
                    sums.set(k, sums.has(k) ? mathjs.add(sums.get(k), v) : v);
                }
            }
            for(const [k, v] of sums){
                if(mathjs.equal(v, 0)) continue;
                let [a, t] = k.split('\t');
                this.insertRow('balances', { address_id: coerce('address_id', a), tick_id: coerce('tick_id', t), amount: v.toFixed() }, false, false);
            }
            return [];
        }
        this.unknown.push('doQuery:' + q.slice(0, 80));
        return [];
    }
    async findIndexAddressTextByIds(ids){ return this.rows('index_addresses').filter(r => ids.map(String).includes(String(r.id))); }
    async findIndexTickTextByIds(ids){ return this.rows('index_tickers').filter(r => ids.map(String).includes(String(r.id))); }
    async listExistingTables(){ return new Set(Object.keys(this.tables)); }
    async findStreamableTableNames(){ return Object.keys(this.tables).map(t => ({ table_name: t })); }
    async deleteAllRows(t){ this.tables[t] = []; }
    async deleteStateTreeRootsFromBlock(){ /* no state_tree_roots below the commitment gate */ }
    async recordHalt(dbType, blockIndex, reason, mismatches, sources){
        if(this.faults.recordHaltThrow){ this.faults.recordHaltThrow = false; throw new Error('injected crash in recordHalt'); }
        let row = { id: this.halts.length + 1, db_type: dbType, block_index: blockIndex, reason,
            mismatches: JSON.stringify(mismatches || []), sources: JSON.stringify(sources || []),
            detected_at: new Date().toISOString(), cleared_at: null };
        this.halts.push(row); return row;
    }
    async getActiveHalt(dbType){
        let a = this.halts.filter(h => h.db_type === dbType && h.cleared_at === null);
        return a.length ? a[a.length - 1] : null;
    }
    async clearHalt(dbType){
        let n = 0; for(const h of this.halts) if(h.db_type === dbType && h.cleared_at === null){ h.cleared_at = new Date().toISOString(); n++; }
        return n;
    }
    async getSyncState(k){ return this.state.has(k) ? this.state.get(k) : null; }
    async setSyncState(k, v){ this.state.set(k, v); }
    async deleteSyncState(k){ this.state.delete(k); }
}

// Report (once per name) every method ClientSync or ClientApplier asks for that the store
// lacks, so a silently undefined capability is visible rather than assumed.
const PASS = new Set(['then', 'toJSON', 'constructor', 'inspect', 'nodeType', 'asymmetricMatch', '$$typeof', '@@toStringTag']);
function proxied(store){
    return new Proxy(store, {
        get(t, p, r){
            if(typeof p === 'string' && !(p in t) && !PASS.has(p) && !t.unknown.includes('prop:' + p)) t.unknown.push('prop:' + p);
            return Reflect.get(t, p, r);
        }
    });
}

// Delete everything at or above height R, actions-owned ledger rows first.
function rollbackStore(store, R){
    let gone = new Set(store.rows('actions').filter(a => Number(a.block_index) >= R).map(a => String(a.action_index)));
    for(const t of ['credits', 'debits', 'escrows'])
        store.tables[t] = store.rows(t).filter(r => !gone.has(String(r.action_index)));
    store.tables.actions = store.rows('actions').filter(a => Number(a.block_index) < R);
    store.tables.blocks = store.rows('blocks').filter(b => Number(b.block_index) < R);
}

const LOOKUPS = ['index_actions', 'index_addresses', 'index_tickers', 'index_transactions'];
const ORDER = [...LOOKUPS, 'actions', 'credits', 'debits', 'blocks'];

class Origin {
    constructor(){
        this.db = new Store('origin'); this.util = new Utility(); this.hasher = new BlockHasher(this.db, this.util);
        this.events = new Map(); this.blockTime = 1700000000; this.fault = null;
    }
    nextAction(){ let m = 0; for(const a of this.db.rows('actions')) if(a.action_index > m) m = a.action_index; return m + 1; }
    ensure(t, col, val, h, data){
        let r = this.db.rows(t).find(x => x[col] === val);
        if(!r){
            r = { id: null, [col]: val };
            if(t === 'index_addresses' || t === 'index_tickers') r.block_index = h;
            r = this.db.insertRow(t, r, false, false);
            data[t].push(clone(r));
        }
        return r.id;
    }
    // spec: { actions: [{ action, tx_index, tx_vout, action_format, source }], credits: [{ a, address, tick, amount }], debits: [...] }
    async mint(h, spec){
        let data = {}; for(const t of ORDER) data[t] = [];
        let base = this.nextAction();
        (spec.actions || []).forEach((a, i) => {
            let row = { action_index: base + i, block_index: h, tx_index: a.tx_index, tx_vout: a.tx_vout || 0,
                action_id: this.ensure('index_actions', 'action', a.action, h, data),
                action_format: a.action_format || 0,
                source_id: a.source ? this.ensure('index_addresses', 'address', a.source, h, data) : null };
            data.actions.push(clone(this.db.insertRow('actions', row, false, false)));
        });
        for(const t of ['credits', 'debits']){
            for(const c of (spec[t] || [])){
                let row = { action_index: base + c.a, address_id: this.ensure('index_addresses', 'address', c.address, h, data),
                    tick_id: this.ensure('index_tickers', 'tick', c.tick, h, data), amount: String(c.amount) };
                data[t].push(clone(this.db.insertRow(t, row, false, false)));
            }
        }
        let hashes = await this.hasher.computeBlockHashes(h, 'mainnet', 'BTC');
        let ids = {};
        for(const f of ['ledger_hash', 'actions_hash', 'contract_hash'])
            ids[f] = this.ensure('index_transactions', 'hash', hashes[f], h, data);
        let bt = this.blockTime + h * 600;
        let brow = this.db.insertRow('blocks', { id: null, block_index: h, block_time: bt,
            ledger_hash_id: ids.ledger_hash, actions_hash_id: ids.actions_hash, contract_hash_id: ids.contract_hash,
            state_hash_id: null }, false, false);
        data.blocks.push(clone(brow));
        let ev = { type: 'block', block_index: h, block_time: bt, schema_version: SCHEMA_VERSION.indexer,
            ledger_hash: hashes.ledger_hash, actions_hash: hashes.actions_hash, contract_hash: hashes.contract_hash, data };
        this.events.set(h, ev);
        return clone(ev);
    }
    tip(){ let m = null; for(const k of this.events.keys()) if(m === null || k > m) m = k; return m; }
    event(h){ return clone(this.events.get(h)); }
    snapshotSince(S){
        let tip = this.tip();
        if(tip === null || S > tip){ let e = new Error('Request failed with status code 404'); e.response = { status: 404 }; throw e; }
        let tables = {}; for(const t of ORDER) tables[t] = [];
        for(let h = S; h <= tip; h++){
            let ev = this.events.get(h); if(!ev) continue;
            for(const t of ORDER) tables[t].push(...clone(ev.data[t] || []));
        }
        return { since_block: S, block_height: tip, schema_version: SCHEMA_VERSION.indexer, tables };
    }
    // Rewind the origin to R (as the origin indexer's own rollback does). Lookups stay.
    rollbackTo(R){ rollbackStore(this.db, R); for(const k of [...this.events.keys()]) if(k >= R) this.events.delete(k); }
    status(){ let t = this.tip(); let e = this.events.get(t); return { block_height: t, ledger_hash: e.ledger_hash, actions_hash: e.actions_hash, contract_hash: e.contract_hash }; }
}

// A standard block body; `salt` changes the hashed content deterministically.
function stdSpec(h, salt){
    let s = salt || 0;
    return {
        actions: [{ action: 'SEND', tx_index: h * 10 + 1, tx_vout: 1, action_format: 0, source: 'addrA' },
                  { action: 'ISSUE', tx_index: h * 10 + 2, tx_vout: 0, action_format: 1, source: 'addr' + h }],
        credits: [{ a: 0, address: 'addrB', tick: 'TOKEN', amount: String(100 + h + s) + '.5' },
                  { a: 1, address: 'addr' + h, tick: 'T' + h, amount: '123456789012345678901234567890.123456789012345678' }],
        debits:  [{ a: 0, address: 'addrA', tick: 'TOKEN', amount: String(100 + h + s) + '.5' }]
    };
}

const CONFIG = () => ({
    SYNC_SOURCES: 'http://origin:3006', VERIFY_HASHES: true, HALT_ON_DIVERGENCE: true, VERIFY_RECOMPUTE: true,
    VERIFY_STATE_HASH: false, VERIFY_STATE_COMMITMENT: false, HASH_CONFIRM_TIMEOUT: 5000,
    CLIENT_RECONNECT_DELAY: 5000, SNAPSHOT_MAX_CONTENT: 1e9, CLIENT_SOURCE_STALE_MS: 60000
});

function makeReplica(store){
    store = store || new Store('replica');
    let p = proxied(store);
    let util = new Utility(); util.sleep = async () => {};
    let applier = new ClientApplier(p, util, 'bitcoin', 'mainnet');
    applier.updateBlockStateCommitment = async function(){ this._lastComputedRoots = null; };
    let rollback = { calls: [], rollback: async (R) => { rollback.calls.push(R); await p.beginTransaction(); rollbackStore(store, R); await p.commitTransaction(); } };
    let sync = new ClientSync('bitcoin', 'mainnet', p, applier, rollback, new HashVerifier(), CONFIG(), util);
    sync.checkTrainActivation = async () => false;
    return { sync, store, applier, rollback };
}

// Install the axios double that serves the origin. `origin.fault` injects one transport fault.
function serveOrigin(origin){
    let calls = [];
    let stub = sinon.stub(axios, 'get').callsFake(async (url) => {
        calls.push(url);
        let m = url.match(/\/since\/(\d+)/);
        if(m){
            let f = origin.fault; origin.fault = null;
            if(f === '404' || f === 'restart'){ let e = new Error('origin restarting'); if(f === '404') e.response = { status: 404 }; else e.code = 'ECONNRESET'; throw e; }
            let snap = origin.snapshotSince(Number(m[1]));
            if(typeof f === 'function') snap = f(snap);
            let buf = Buffer.from(JSON.stringify(snap));
            if(f === 'truncated') buf = buf.subarray(0, Math.floor(buf.length / 2));
            return { data: buf };
        }
        if(url.includes('/status/')) return { data: origin.status() };
        if(url.includes('/schema/')) return { data: { tables: {} } };
        throw new Error('harness: unrouted GET ' + url);
    });
    return { stub, calls };
}

// Content view for equality: local surrogate ids (blocks, balances) dropped, rows sorted.
const COMPARE = ['actions', 'credits', 'debits', 'blocks', 'balances', ...LOOKUPS];
function view(store, opts){
    let o = opts || {}; let out = {};
    for(const t of COMPARE){
        if(o.skip && o.skip.includes(t)) continue;
        let rows = clone(store.rows(t));
        if(t === 'blocks' || t === 'balances') rows.forEach(r => { delete r.id; });
        if(t === 'blocks') rows = rows.map(r => {
            // blocks hash ids resolve to hashes; compare resolved hashes, not ids
            let x = Object.assign({}, r);
            for(const f of ['ledger_hash_id', 'actions_hash_id', 'contract_hash_id']) x[f] = store.lookup('index_transactions', 'hash', r[f]);
            return x;
        });
        out[t] = rows.map(r => JSON.stringify(Object.keys(r).sort().reduce((a, k) => (a[k] = r[k], a), {}))).sort();
    }
    return out;
}
function diffViews(a, b){
    let d = [];
    for(const t of Object.keys(a)){
        let A = new Set(a[t]), B = new Set(b[t] || []);
        let onlyA = a[t].filter(x => !B.has(x)), onlyB = (b[t] || []).filter(x => !A.has(x));
        if(onlyA.length || onlyB.length || a[t].length !== (b[t] || []).length) d.push({ table: t, onlyA: onlyA.slice(0, 3), onlyB: onlyB.slice(0, 3), nA: a[t].length, nB: (b[t] || []).length });
    }
    return d;
}

function captureLogs(){
    let lines = [];
    for(const k of ['log', 'error', 'warn', 'info']){
        sinon.stub(console, k).callsFake((...a) => { lines.push(k + ': ' + a.map(x => typeof x === 'string' ? x : (x && x.message) || JSON.stringify(x)).join(' ')); });
    }
    return lines;
}

// ---- scenarios shared by the resume and catch-up suites

// Origin minted 1..n, replica seeded through the real catch-up path.
async function seeded(n){
    let origin = new Origin();
    for(let h = 1; h <= n; h++) await origin.mint(h, stdSpec(h));
    let srv = serveOrigin(origin);
    let r = makeReplica();
    await r.sync.incrementalCatchUp(1);
    if(r.sync.isHalted()) throw new Error('seed halted: ' + JSON.stringify(r.sync.getHaltInfo()));
    if(r.sync.lastAppliedBlock !== n) throw new Error('seed stopped at ' + r.sync.lastAppliedBlock);
    r.sync.running = true;
    return Object.assign({ origin, srv }, r);
}

// Origin minted 1..8, replica holding 1..3; the caller drives 4..8.
async function seed3(){
    let origin = new Origin();
    for(let h = 1; h <= 3; h++) await origin.mint(h, stdSpec(h));
    let srv = serveOrigin(origin);
    let r = makeReplica();
    await r.sync.incrementalCatchUp(1);
    for(let h = 4; h <= 8; h++) await origin.mint(h, stdSpec(h));
    return Object.assign({ origin, srv }, r);
}

// Restart: a fresh ClientSync and ClientApplier over the same store, run through the
// start() steps that decide the resume point (halt read, guards, stored-replica sync).
async function restart(store){
    let r = makeReplica(store);
    r.sync.running = false;                 // waitForStartupHalt returns at once
    let action = await r.sync.resolveStartupHalt();
    if(action === 'stop') return Object.assign(r, { startup: 'halted' });
    await r.sync.loadBootstrapBase();
    await r.sync.synchronizeStoredReplica();
    if(r.sync.isHalted()) return Object.assign(r, { startup: 'halted' });
    r.sync.ensureLiveFollowTip();
    await r.sync.prepareLiveFollow();
    r.sync.running = true;
    return Object.assign(r, { startup: 'resumed' });
}

// Seeded to 10, then the origin re-derives 6..10 with a changed hashed amount at 6
// and no reorg event: a consensus hotfix the replica never saw.
async function hotfixed(){
    let s = await seeded(10);
    s.origin.rollbackTo(6);
    for(let h = 6; h <= 10; h++) await s.origin.mint(h, stdSpec(h, h === 6 ? 7 : 0));
    return s;
}

// Block 9 whose served rows do not reproduce its committed hash.
async function tamperedNine(origin){
    let ev = await origin.mint(9, stdSpec(9));
    ev.data.credits[0].amount = '999.5';
    return ev;
}

function originDiff(origin, store){
    return diffViews(view(origin.db, { skip: ['balances'] }), view(store, { skip: ['balances'] }));
}

function haltSummary(sync, store){
    let h = sync.getHaltInfo();
    return { halted: sync.isHalted(), reason: h && h.reason, blockIndex: h && h.blockIndex,
        durable: store.halts.filter(x => x.cleared_at === null).map(x => ({ block: x.block_index, reason: x.reason })) };
}

// A snapshot transform that changes one block's served rows inside a catch-up range.
function snapTamper(kind, h){
    return (snap) => {
        let aidx = new Set((snap.tables.actions || []).filter(a => a.block_index === h).map(a => a.action_index));
        let credits = (snap.tables.credits || []).filter(c => aidx.has(c.action_index));
        let actions = (snap.tables.actions || []).filter(a => a.block_index === h);
        if(kind === 'changed_row') credits[0].amount = '999.5';
        if(kind === 'missing_row') snap.tables.credits = snap.tables.credits.filter(c => c !== credits[1]);
        if(kind === 'extra_row') snap.tables.credits.push(Object.assign({}, credits[0], { amount: '1' }));
        if(kind === 'hashed_tx_index') actions[0].tx_index += 1;
        return snap;
    };
}

module.exports = { sinon, axios, Store, Origin, stdSpec, makeReplica, serveOrigin, view, diffViews, captureLogs,
    rollbackStore, clone, SCHEMA_VERSION, ClientSync, seeded, seed3, restart, hotfixed, tamperedNine,
    originDiff, haltSummary, snapTamper };
