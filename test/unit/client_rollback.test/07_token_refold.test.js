// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// The reverse leg of updated_rows class 7: a reorg that orphans an ISSUE edit must leave
// the replica's tokens row equal to the source's refold from the surviving issues. Each
// scenario runs ClientRollback against an in-memory replica, checks the row against
// hand-written expectations, and, when the xchain-indexer sibling is present, against
// the indexer's OWN getTokenInfo + createToken run over the same surviving issues.

'use strict';

const assert = require('assert');
const path   = require('path');
const fs     = require('fs');
const sinon  = require('sinon');
const ClientRollback = require('../../../src/client/rollback');
const BlockHasher    = require('../../../src/client/block_hasher');
const Utility        = require('../../../src/util');
const tokenRefold    = require('../../../src/db/token_refold');
const { siblingCheckout, skipOrFail } = require('../../helpers/sibling_checkout.js');

const ADDRESSES = { 1: 'addrAlice', 2: 'addrBob', 3: 'addrCarol' };
const TICKERS   = { 10: 'TOKA', 11: 'CBTICK' };
const TICK = 10;

function issue(action_index, fields){
    return Object.assign({
        tick_id: TICK, action_index, status: 'valid', source_addr_id: 1, transfer_addr_id: null,
        callback_tick_ref: null, max_supply: '', max_mint: '', decimals: '', description: '',
        lock_max_supply: '', lock_mint_supply: '', lock_mint: '', lock_max_mint: '',
        lock_description: '', lock_sleep: '', lock_callback: '', callback_block: '',
        callback_amount: '', mint_address_max: '', mint_start_block: '', mint_stop_block: '',
        allow_list: null, block_list: null, bridge_chains: '', min_depth: '', lock_bridge: '',
    }, fields);
}

const GENESIS = issue(100, { decimals: '8', max_supply: '1000', max_mint: '10', description: 'first',
    lock_max_supply: '0', lock_mint: '0', lock_description: '0', mint_start_block: '50',
    mint_stop_block: '900', bridge_chains: 'LTC', min_depth: '6', lock_bridge: '0' });
const SURVIVING_EDIT = issue(300, { description: 'second', callback_tick_ref: 11,
    callback_block: '700', callback_amount: '1.5', allow_list: 42 });

// The replica row as the orphaned edit left it (forward class 7 carried it).
function editedRow(over){
    return Object.assign({ tick_id: TICK, action_index: 100, supply: '5', bridged: 0, escrow_action_index: null,
        max_supply: '1000.00000000', max_mint: '10.00000000', decimals: 8, description: 'second',
        lock_max_supply: 0, lock_mint: 0, lock_mint_supply: 0, lock_max_mint: 0, lock_description: 0,
        lock_sleep: 0, lock_callback: 0, callback_block: '700', callback_tick_id: 11, callback_amount: '1.5',
        allow_list: 42, block_list: null, mint_address_max: '0.00000000', mint_start_block: '50',
        mint_stop_block: '900', bridge_chains: 'LTC', min_depth: 6, lock_bridge: 0, owner_id: 1,
        last_action_index: 500 }, over);
}

// What the source's refold writes after the reorg drops the edit at 500.
const EXPECTED = editedRow({ last_action_index: 300 });

// An in-memory replica answering exactly the statements the refold path issues.
function replica(issues, tokenRow, firstActionIndex){
    let world = { issues: issues.slice(), token: Object.assign({}, tokenRow), updates: 0 };
    let doQuery = sinon.stub().callsFake(async (sql, args) => {
        if(/^SELECT DISTINCT tick_id FROM issues WHERE action_index >= \?/.test(sql))
            return [...new Set(world.issues.filter(i => i.action_index >= args[0]).map(i => i.tick_id))].map(t => ({ tick_id: t }));
        if(sql === 'DELETE FROM `issues` WHERE action_index >= ?'){
            world.issues = world.issues.filter(i => i.action_index < args[0]);
            return { affectedRows: 1 };
        }
        if(/FROM issues i INNER JOIN actions a1/.test(sql))
            return world.issues.filter(i => i.status === 'valid' && args.map(Number).includes(i.tick_id))
                .sort((a, b) => (a.tick_id - b.tick_id) || (a.action_index - b.action_index));
        if(/^SELECT tick_id, action_index FROM tokens WHERE tick_id IN/.test(sql))
            return args.map(Number).includes(world.token.tick_id) ? [{ tick_id: world.token.tick_id, action_index: world.token.action_index }] : [];
        if(/^UPDATE tokens SET max_supply=\?/.test(sql)){
            world.updates++;
            if(Number(args[args.length - 1]) === world.token.tick_id)
                tokenRefold.FOLD_COLUMNS.forEach((c, k) => { world.token[c] = args[k]; });
            return { affectedRows: 1 };
        }
        return [];
    });
    let db = { doQuery, getFirstActionIndex: sinon.stub().resolves(firstActionIndex),
        getStatusId: sinon.stub().resolves(null), beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(), rollbackTransaction: sinon.stub().resolves() };
    return { db, world };
}

// Storage view of a bound value: every fold column is VARCHAR or an integer column, so
// String() is what the row holds, and undefined binds as NULL.
function stored(row){
    let o = {};
    for(let c of tokenRefold.FOLD_COLUMNS) o[c] = (row[c] === null || row[c] === undefined) ? null : String(row[c]);
    return o;
}

// ── The indexer's own fold, loaded from the sibling checkout ──────────────────
function indexerRoot(){
    if(process.env.XCHAIN_INDEXER_SQL_PATH) return { usable: true, path: path.resolve(process.env.XCHAIN_INDEXER_SQL_PATH, '..', '..'), reason: null };
    return siblingCheckout(__dirname, '../../../../xchain-indexer');
}

// Runs getTokenInfo + createToken exactly as updateTokenInfo does, over a stub whose
// doQuery answers the replay SELECT with the surviving issues in the source's row shape,
// and returns the UPDATE's bound values keyed by FOLD_COLUMNS.
async function sourceFold(root, issues){
    // Runtime path: the sibling root is resolved per run (env override or sibling checkout).
    let req = (rel) => require(path.join(root, rel));
    let config = {};
    req('src/config/wire_fields.js').applyWireFields(config);
    req('src/config/token_limits.js').applyTokenSupplyLimits(config);
    let util = Object.assign({ safeToString: (v) => (v === null || v === undefined) ? null : String(v) },
        req('src/utility/validation/value_checks.js'), req('src/utility/bcmath.js'));
    let byName = (map, v) => { let k = Object.keys(map).find(id => map[id] === v); return k === undefined ? null : Number(k); };
    let captured = null;
    let db = Object.assign({ util, config }, req('src/db/issues/token_info.js'),
        req('src/db/tokens/token_writer.js'), req('src/db/database/normalize.js'));
    db.createTicker = async (t) => util.isNull(t) ? null : byName(TICKERS, t);
    db.createAddress = async (a) => util.isNull(a) ? null : byName(ADDRESSES, a);
    db.getTokenSupply = async () => '5';
    db.doQuery = async (sql, args) => {
        if(/FROM\s+issues i/.test(sql)) sourceFold.replaySql = sql;
        if(/FROM\s+issues i/.test(sql)) return issues.filter(i => i.status === 'valid').map(i => Object.assign({}, i, {
            tick: TICKERS[i.tick_id], callback_tick: i.callback_tick_ref ? TICKERS[i.callback_tick_ref] : null,
            owner: ADDRESSES[i.source_addr_id], transfer: i.transfer_addr_id ? ADDRESSES[i.transfer_addr_id] : null,
            bridged: 0, block_index: 1 }));
        if(/SELECT id FROM tokens/.test(sql)) return [{ id: 1 }];
        if(/^\s*UPDATE\s+tokens/.test(sql)){ captured = args; sourceFold.updateSql = sql; return {}; }
        return [];
    };
    let data = await db.getTokenInfo(TICKERS[TICK]);
    sourceFold.infoKeys = Object.keys(data);
    sourceFold.config = config;
    await db.createToken(data);
    // Bind each fold column by its name in the captured SET list, never by position.
    let columns = updateColumns(sourceFold.updateSql);
    let out = {};
    for(let c of tokenRefold.FOLD_COLUMNS) out[c] = captured[columns.indexOf(c)];
    return out;
}

// The SET column names of an UPDATE, in bind order.
function updateColumns(sql){
    let m = /\bSET (.*) WHERE /i.exec(String(sql).replace(/\s+/g, ' '));
    assert.ok(m, 'not an UPDATE ... SET ... WHERE statement: ' + sql);
    return m[1].split(',').map(c => c.trim().replace(/ ?= ?\?$/, ''));
}

// Rolls back from action 500, orphaning `edit`; `surviving` is the history the reorg keeps.
async function reorg(edit, rowAfterEdit, surviving = [GENESIS, SURVIVING_EDIT]){
    let { db, world } = replica(surviving.concat([edit]), rowAfterEdit, 500);
    await new ClientRollback(db, new Utility(), 'BTC', 'regtest').rollback(1000);
    return world;
}

function indexerOrSkip(ctx){
    let root = indexerRoot();
    let ok = root.usable && fs.existsSync(path.join(root.path, 'src/db/issues/token_info.js'));
    if(!skipOrFail(ctx, ok ? root : { usable: false, reason: root.reason || 'token_info.js absent' }, 'the indexer fold parity guard')) return null;
    return root.path;
}

describe('ClientRollback token refold (reverse leg of updated_rows class 7)', function(){

    beforeEach(function(){ sinon.stub(console, 'log'); sinon.stub(console, 'error'); });
    afterEach(function(){ sinon.restore(); });

    const SCENARIOS = {
        'ownership transfer': [issue(500, { transfer_addr_id: 2 }), editedRow({ owner_id: 2 })],
        'lock edit':          [issue(500, { lock_mint: '1', lock_description: '1', lock_callback: '1' }),
                               editedRow({ lock_mint: 1, lock_description: 1, lock_callback: 1 })],
        'bridge-policy edit': [issue(500, { bridge_chains: '-', min_depth: '12', lock_bridge: '1' }),
                               editedRow({ bridge_chains: null, min_depth: 12, lock_bridge: 1 })],
        'callback, list and mint-window edit': [
            issue(500, { callback_block: '999', callback_amount: '2', block_list: 7, mint_stop_block: '5000', max_mint: '20' }),
            editedRow({ callback_block: '999', callback_amount: '2', block_list: 7, mint_stop_block: '5000', max_mint: '20.00000000' })],
    };

    for(let name of Object.keys(SCENARIOS)){
        it('a reorg of the ' + name + ' leaves the replica row equal to the refold of the surviving issues', async function(){
            let [edit, row] = SCENARIOS[name];
            let world = await reorg(edit, row);
            assert.strictEqual(world.updates, 1);
            assert.deepStrictEqual(stored(world.token), stored(EXPECTED));
            // Columns the fold does not own are left alone (supply is recomputed separately).
            assert.strictEqual(world.token.supply, '5');
            assert.strictEqual(world.token.action_index, 100);
            assert.strictEqual(world.token.bridged, 0);
        });

        it('a reorg of the ' + name + ' matches the xchain-indexer fold byte for byte', async function(){
            this.timeout(30000);
            let root = indexerOrSkip(this);
            if(!root) return;
            let [edit, row] = SCENARIOS[name];
            let world = await reorg(edit, row);
            let source = await sourceFold(root, [GENESIS, SURVIVING_EDIT]);
            assert.deepStrictEqual(stored(world.token), stored(source));
        });
    }
});

describe('ClientRollback token refold: list detach sentinel', function(){

    beforeEach(function(){ sinon.stub(console, 'log'); sinon.stub(console, 'error'); });
    afterEach(function(){ sinon.restore(); });

    // A list detach (value 0) must sit in the SURVIVING history to reach either fold:
    // the orphaned edit at 500 is deleted before the refold reads anything.
    const ORPHANED_TRANSFER = issue(500, { transfer_addr_id: 2 });
    const DETACH_SCENARIOS = {
        'allow-list detach': [
            [GENESIS, SURVIVING_EDIT, issue(400, { allow_list: 0 })],
            editedRow({ owner_id: 2, allow_list: null, last_action_index: 500 }),
            editedRow({ allow_list: null, last_action_index: 400 })],
        'block-list detach': [
            [GENESIS, SURVIVING_EDIT, issue(350, { block_list: 7 }), issue(400, { block_list: '0' })],
            editedRow({ owner_id: 2, block_list: null, last_action_index: 500 }),
            editedRow({ block_list: null, last_action_index: 400 })],
    };

    for(let name of Object.keys(DETACH_SCENARIOS)){
        it('a reorg over a surviving ' + name + ' refolds the list to NULL, not 0', async function(){
            let [surviving, row, expected] = DETACH_SCENARIOS[name];
            let world = await reorg(ORPHANED_TRANSFER, row, surviving);
            assert.strictEqual(world.updates, 1);
            assert.deepStrictEqual(stored(world.token), stored(expected));
        });

        it('a reorg over a surviving ' + name + ' matches the xchain-indexer fold byte for byte', async function(){
            let root = indexerOrSkip(this);
            if(!root) return;
            let [surviving, row] = DETACH_SCENARIOS[name];
            let world = await reorg(ORPHANED_TRANSFER, row, surviving);
            let source = await sourceFold(root, surviving);
            assert.deepStrictEqual(stored(world.token), stored(source));
        });
    }
});

describe('ClientRollback token refold: no-op and failure paths', function(){

    beforeEach(function(){ sinon.stub(console, 'log'); sinon.stub(console, 'error'); });
    afterEach(function(){ sinon.restore(); });

    it('a rollback touching no ISSUE issues no tokens UPDATE and changes nothing', async function(){
        let row = editedRow({ last_action_index: 300 });
        let { db, world } = replica([GENESIS, SURVIVING_EDIT], row, 500);
        await new ClientRollback(db, new Utility(), 'BTC', 'regtest').rollback(1000);
        assert.strictEqual(world.updates, 0);
        assert.deepStrictEqual(world.token, row);
        assert.ok(!db.doQuery.getCalls().some(c => /FROM issues i INNER JOIN/.test(c.args[0])));
    });

    it('a tick whose only issues were orphaned is left to the generic delete (no UPDATE)', async function(){
        let { db, world } = replica([issue(600, { decimals: '0' })], editedRow({}), 500);
        await new ClientRollback(db, new Utility(), 'BTC', 'regtest').rollback(1000);
        assert.strictEqual(world.updates, 0);
    });

    it('a replica without the tick\'s first issuance (truncated) skips the refold instead of folding a tail', async function(){
        let row = editedRow({ owner_id: 2 });
        let { db, world } = replica([SURVIVING_EDIT, issue(500, { transfer_addr_id: 2 })], row, 500);
        await new ClientRollback(db, new Utility(), 'BTC', 'regtest').rollback(1000);
        assert.strictEqual(world.updates, 0);
        assert.deepStrictEqual(world.token, row);
    });

    it('a failed refold aborts the rollback transaction', async function(){
        let { db } = replica([GENESIS, issue(500, { transfer_addr_id: 2 })], editedRow({}), 500);
        let inner = db.doQuery;
        db.doQuery = sinon.stub().callsFake(async (sql, args) => {
            if(/^UPDATE tokens SET max_supply=\?/.test(sql)){ let e = new Error('lock wait'); e.errno = 1205; throw e; }
            return inner(sql, args);
        });
        await assert.rejects(new ClientRollback(db, new Utility(), 'BTC', 'regtest').rollback(1000), /lock wait/);
        assert.ok(db.rollbackTransaction.calledOnce);
        assert.ok(db.commitTransaction.notCalled);
    });
});

describe('foldIssueRows', function(){
    it('keeps a set lock, never lowers decimals, and inherits empty fields', function(){
        let out = tokenRefold.foldIssueRows([
            issue(1, { decimals: '8', lock_mint: '1', description: 'a', max_supply: '5' }),
            issue(2, { decimals: '2', lock_mint: '0', description: '' }),
        ]).get(String(TICK));
        assert.strictEqual(out.lock_mint, 1);
        assert.strictEqual(out.decimals, 8);
        assert.strictEqual(out.description, 'a');
        assert.strictEqual(out.max_supply, '5.00000000');
        assert.strictEqual(out.last_action_index, 2);
    });

    // Fold one tick's issues and return its column set.
    function fold(rows){ return tokenRefold.foldIssueRows(rows).get(String(TICK)); }

    for(let [col, id] of [['allow_list', 42], ['block_list', 7]]){
        it('reads a ' + col + ' of 0 (number, string or BigInt) as a detach to NULL', function(){
            for(let zero of [0, '0', 0n]){
                let out = fold([issue(1, { decimals: '8', [col]: id }), issue(2, { [col]: zero })]);
                assert.strictEqual(out[col], null, col + ' after a detach of ' + typeof zero);
            }
        });

        it('keeps a detached ' + col + ' NULL through later empty issues', function(){
            let out = fold([issue(1, { decimals: '8', [col]: id }), issue(2, { [col]: 0 }), issue(3, { description: 'x' })]);
            assert.strictEqual(out[col], null);
        });

        it('lets a later ' + col + ' re-attach after a detach', function(){
            let out = fold([issue(1, { decimals: '8', [col]: id }), issue(2, { [col]: 0 }), issue(3, { [col]: 55 })]);
            assert.strictEqual(out[col], 55);
        });
    }
});

describe('BlockHasher.computeTokenFoldChecksum (advisory token fold parity)', function(){
    function hasherOver(rows, maxAction){
        let db = { doQuery: sinon.stub().callsFake(async (sql) => {
            if(/MAX\(action_index\)/.test(sql)) return [{ m: maxAction }];
            if(/FROM tokens ORDER BY tick_id/.test(sql)) return rows.map(r => Object.assign({}, r));
            return [];
        }) };
        return new BlockHasher(db, new Utility());
    }
    const A = Object.assign(editedRow({ last_action_index: 300 }), { tick_id: 10 });
    const B = Object.assign(editedRow({ last_action_index: 800 }), { tick_id: 12 });

    it('agrees when the follower holds the source rows, leaving out ticks the source edited past the height', async function(){
        let source = await hasherOver([A, B], 500).computeTokenFoldChecksum(1000);
        assert.deepStrictEqual(source.ahead, ['12']);
        let follower = await hasherOver([A, Object.assign({}, B, { last_action_index: 400, owner_id: 3 })], 999)
            .computeTokenFoldChecksum(1000, { exclude: source.ahead });
        assert.strictEqual(follower.h, source.h);
    });

    it('detects a replica that kept an orphaned edit', async function(){
        let source   = await hasherOver([A], 500).computeTokenFoldChecksum(1000);
        let follower = await hasherOver([editedRow({ owner_id: 2, last_action_index: 500 })], 500)
            .computeTokenFoldChecksum(1000, { exclude: source.ahead });
        assert.notStrictEqual(follower.h, source.h);
    });
});

// Reduce a replay SELECT to joins, WHERE and ORDER BY, folding away the only allowed
// differences: the source's tokens join (it feeds bridged alone), = ? versus IN (?,..)
// on the tick, and the replica's leading tick_id sort key. Also returns its i.* columns.
function replayShape(sql){
    let s = String(sql).replace(/\s+/g, ' ').toLowerCase().replace(/ ?([(=,]) ?/g, '$1').replace(/ \)/g, ')').trim();
    let [from, where, order] = [s.indexOf(' from '), s.indexOf(' where '), s.indexOf(' order by ')];
    assert.ok(from > 0 && where > from && order > where, 'not a replay SELECT: ' + sql);
    return { columns: s.slice(7, from).split(',').filter(c => c.startsWith('i.')),
        joins: s.slice(from + 6, where).split(/ (?=(?:inner|left) join )/).filter(j => j !== 'left join tokens tk on(tk.tick_id=i.tick_id)'),
        where: s.slice(where + 7, order).replace(/i\.tick_id(?:=\?| in\(\?(?:,\?)*\))/, 'i.tick_id=<ticks>'),
        orderBy: s.slice(order + 10).replace(/^i\.tick_id asc,/, '') };
}

// The one replay SELECT the replica's refold issues for a reorg.
async function replicaReplaySql(){
    let { db } = replica([GENESIS, SURVIVING_EDIT, issue(500, { transfer_addr_id: 2 })], editedRow({ owner_id: 2 }), 500);
    await new ClientRollback(db, new Utility(), 'BTC', 'regtest').rollback(1000);
    let calls = db.doQuery.getCalls().filter(c => /FROM issues i /.test(c.args[0]));
    assert.strictEqual(calls.length, 1, 'expected exactly one replay SELECT from the refold');
    return calls[0].args[0];
}

// The fold depends on the row set AND order, so the replica's refold SELECT must keep
// the source replay SELECT's joins, filter and order (only replayShape's folds differ).
describe('ClientRollback token refold: replay SELECT parity with the indexer', function(){
    beforeEach(function(){ sinon.stub(console, 'log'); sinon.stub(console, 'error'); });
    afterEach(function(){ sinon.restore(); });

    it('selects the same issues, in the same order, as the indexer replay SELECT', async function(){
        let root = indexerOrSkip(this);
        if(!root) return;
        sourceFold.replaySql = null;
        await sourceFold(root, [GENESIS, SURVIVING_EDIT]);
        assert.ok(sourceFold.replaySql, 'the indexer fold issued no replay SELECT');
        let [src, rep] = [replayShape(sourceFold.replaySql), replayShape(await replicaReplaySql())];
        assert.deepStrictEqual(src.columns.filter(c => !rep.columns.includes(c)), [], 'the replica omits issue columns the source folds');
        delete src.columns; delete rep.columns;
        assert.deepStrictEqual(rep, src, 'foldRowsSql (src/db/token_refold.js) no longer matches issueReplay.rowsQuery (xchain-indexer ' +
            'src/db/issues/token_info.js); the replica refold would replay a different set or order of issues');
    });

    it('the shape keeps what it compares, and changes under every drift it must catch', async function(){
        let sql = await replicaReplaySql();
        let shape = replayShape(sql);
        assert.ok(shape.joins.includes('inner join actions a1 on(a1.action_index=i.action_index)'), JSON.stringify(shape.joins));
        assert.deepStrictEqual([shape.where, shape.orderBy], ["s1.status='valid' and i.tick_id=<ticks>", 'i.action_index asc']);
        for(let [name, mutated] of Object.entries({
            'an extra INNER JOIN': sql.replace('WHERE ', 'INNER JOIN foo f ON (f.id=i.id) WHERE '),
            'an extra WHERE bound': sql.replace(' ORDER BY', ' AND t1.block_index <= ? ORDER BY'),
            'a reversed order': sql.replace(/i\.action_index ASC$/, 'i.action_index DESC'),
            'an INNER JOIN turned LEFT': sql.replace('INNER JOIN transactions', 'LEFT JOIN transactions') })){
            assert.notStrictEqual(mutated, sql, name + ' did not apply to the captured SQL');
            assert.notDeepStrictEqual(replayShape(mutated), shape, name + ' is invisible to replayShape');
        }
    });
});

// The replica restates the source's wire-field lists, decimal bounds and UPDATE column
// set by hand, so each is compared with the sibling indexer rather than trusted.
describe('ClientRollback token refold: restated constants parity with the indexer', function(){
    beforeEach(function(){ sinon.stub(console, 'log'); sinon.stub(console, 'error'); });
    afterEach(function(){ sinon.restore(); });

    // Run the indexer fold once and return its config and the keys its token info carries.
    async function sourceView(root){
        await sourceFold(root, [GENESIS, SURVIVING_EDIT]);
        let keys = new Set(tokenRefold.RESTATED.FOLD_KEYS.concat(sourceFold.infoKeys));
        return { config: sourceFold.config, pick: (list) => list.filter(k => keys.has(k)).sort() };
    }

    it('restates the indexer number, list, lock and integer field lists over the fold keys', async function(){
        let root = indexerOrSkip(this);
        if(!root) return;
        let { config, pick } = await sourceView(root);
        let R = tokenRefold.RESTATED;
        for(let name of ['NUMBER_FIELDS', 'LIST_FIELDS', 'LOCK_FIELDS'])
            assert.deepStrictEqual(R[name].slice().sort(), pick(config[name]), name + ' in src/db/token_refold.js no longer matches the indexer wire_fields.js');
        let ints = pick(Object.keys(config.INTEGER_FIELDS));
        assert.deepStrictEqual(R.INTEGER_FIELDS.slice().sort(), ints, 'INTEGER_FIELDS no longer matches the indexer wire_fields.js');
        for(let k of ints) assert.strictEqual(config.INTEGER_FIELDS[k], R.U64_MAX, k + ' is no longer bounded at u64 on the indexer');
    });

    it('restates the indexer token decimal bounds', async function(){
        let root = indexerOrSkip(this);
        if(!root) return;
        let { config } = await sourceView(root);
        assert.deepStrictEqual([tokenRefold.RESTATED.MIN_TOKEN_DECIMALS, tokenRefold.RESTATED.MAX_TOKEN_DECIMALS],
            [config.MIN_TOKEN_DECIMALS, config.MAX_TOKEN_DECIMALS], 'the replica decimal bounds no longer match the indexer token_limits.js');
    });

    it('binds exactly the fold columns plus supply in the indexer tokens UPDATE', async function(){
        let root = indexerOrSkip(this);
        if(!root) return;
        await sourceFold(root, [GENESIS, SURVIVING_EDIT]);
        let columns = updateColumns(sourceFold.updateSql);
        assert.strictEqual(columns.filter(c => c === 'supply').length, 1, 'expected one supply column: ' + columns.join(','));
        assert.deepStrictEqual(columns.filter(c => c !== 'supply'), tokenRefold.FOLD_COLUMNS.slice(),
            'FOLD_COLUMNS (src/db/token_refold.js) no longer matches the indexer tokens UPDATE column list');
    });

    it('the UPDATE column reader changes under an added or reordered column', function(){
        let sql = 'UPDATE tokens SET max_supply=?, supply=?, owner_id=? WHERE tick_id=?';
        assert.deepStrictEqual(updateColumns(sql), ['max_supply', 'supply', 'owner_id']);
        assert.deepStrictEqual(updateColumns(sql.replace(' WHERE', ', extra=? WHERE')), ['max_supply', 'supply', 'owner_id', 'extra']);
        assert.deepStrictEqual(updateColumns(sql.replace('max_supply=?, supply=?', 'supply=?, max_supply=?')), ['supply', 'max_supply', 'owner_id']);
    });
});

// Each normalize branch, fed a malformed surviving issue, must fold identically on both sides.
describe('ClientRollback token refold: malformed value parity with the indexer', function(){
    beforeEach(function(){ sinon.stub(console, 'log'); sinon.stub(console, 'error'); });
    afterEach(function(){ sinon.restore(); });

    const ORPHANED_TRANSFER = issue(500, { transfer_addr_id: 2 });
    const MALFORMED = {
        'out-of-range decimals': issue(400, { decimals: '20' }),
        'a lock value of 2': issue(400, { lock_sleep: '2', lock_mint: '2' }),
        'a non-numeric limit': issue(400, { max_mint: 'abc', mint_address_max: 'x1' }),
        'a callback block above u64': issue(400, { callback_block: '18446744073709551616' }),
    };

    for(let name of Object.keys(MALFORMED)){
        it('a surviving issue with ' + name + ' folds the same as the xchain-indexer', async function(){
            let root = indexerOrSkip(this);
            if(!root) return;
            let surviving = [GENESIS, SURVIVING_EDIT, MALFORMED[name]];
            let world = await reorg(ORPHANED_TRANSFER, editedRow({ owner_id: 2, last_action_index: 500 }), surviving);
            assert.strictEqual(world.updates, 1);
            assert.deepStrictEqual(stored(world.token), stored(await sourceFold(root, surviving)));
        });
    }
});
