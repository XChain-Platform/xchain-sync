// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available separately.

'use strict';

const assert = require('assert');
const path = require('path');
const sinon = require('sinon');
const tokenRefold = require('../../../src/db/token_refold');
const lifecycle = require('../../../src/table_lifecycle');
const { siblingCheckout, skipOrFail } = require('../../helpers/sibling_checkout.js');

const TICK_TABLES = ['credits', 'debits', 'escrows', 'contract_stakes', 'contract_unstakes',
    'contract_delegations', 'airdrops', 'destroys', 'mints', 'sends', 'fees', 'issues', 'swaps'];

function issue(actionIndex, fields = {}){
    return Object.assign({ tick_id: 10, action_index: actionIndex, source_addr_id: 1,
        transfer_addr_id: null, callback_tick_ref: null }, fields);
}

function collectorDb(rowsByTable, survivingIssues = []){
    let updateArgs = null;
    const doQuery = sinon.stub().callsFake(async (sql, args) => {
        const table = /FROM ([a-z_]+) m/.exec(sql);
        if(/^SELECT DISTINCT m\./.test(sql)) return rowsByTable[table[1]] || [];
        if(/FROM issues i INNER JOIN actions a1/.test(sql)) return survivingIssues;
        if(/^SELECT tick_id, action_index FROM tokens/.test(sql)) return [{ tick_id: 10, action_index: 100 }];
        if(/^UPDATE tokens SET max_supply=\?/.test(sql)){ updateArgs = args; return { affectedRows: 1 }; }
        return [];
    });
    return { doQuery, get updateArgs(){ return updateArgs; } };
}

describe('token refold affected-tick parity', function(){
    it('tracks every ticker-bearing table in the indexer rollback read phase', function(){
        const root = siblingCheckout(__dirname, '../../../../xchain-indexer');
        if(!skipOrFail(this, root, 'the indexer affected-tick table guard')) return;
        const readSql = require(path.join(root.path, 'src/db/rollback/read_phase.js'));
        const queryFor = (table) => readSql.entityQueryForLedgerTables(table) ||
            readSql.entityQueryForTransferTables(table) || readSql.entityQueryForDexTables(table) ||
            readSql.entityQueryForCoinpayTables(table);
        const sourceTables = lifecycle.replicaRollbackTables().dataTables.filter(table => {
            const query = queryFor(table);
            const select = query ? query.slice(0, query.toUpperCase().indexOf('FROM')) : '';
            return /\bt[12]\.tick\b/.test(select);
        });
        assert.deepStrictEqual(sourceTables.slice().sort(), TICK_TABLES.slice().sort());
        assert.strictEqual(sourceTables.length, 13, 'the source guard must exercise a nonempty ticker set');
    });

    it('collects every source ticker-bearing table and ignores market-only tables', async function(){
        const db = collectorDb(Object.fromEntries(TICK_TABLES.map((table, i) => [table, [{ tick_id: i + 1 }]])));
        const ids = await tokenRefold.collectRefoldTickIds(db, 500, TICK_TABLES.concat(['orders', 'order_matches']));
        assert.deepStrictEqual(ids, TICK_TABLES.map((_, i) => i + 1));
        const queried = db.doQuery.getCalls().map(c => /FROM ([a-z_]+) m/.exec(c.args[0]))
            .filter(Boolean).map(m => m[1]);
        assert.deepStrictEqual(queried, TICK_TABLES);
    });

    it('uses the source inclusion joins and swap tick column', async function(){
        const db = collectorDb({});
        await tokenRefold.collectRefoldTickIds(db, 500, ['credits', 'contract_stakes', 'mints', 'issues', 'swaps']);
        const sql = Object.fromEntries(db.doQuery.getCalls().map(c => [/FROM ([a-z_]+) m/.exec(c.args[0])[1], c.args[0]]));
        assert.match(sql.credits, /index_addresses a1 ON \(a1\.id=m\.address_id\)/);
        assert.match(sql.contract_stakes, /index_addresses a1 ON \(a1\.id=m\.source_id\)/);
        assert.match(sql.mints, /transactions t1 ON \(t1\.tx_index=a1\.tx_index\)/);
        assert.match(sql.issues, /LEFT JOIN index_addresses a4 ON \(a4\.id=m\.transfer_supply_id\)/);
        assert.match(sql.swaps, /m\.give_tick_id AS tick_id/);
    });

    it('refolds a tick touched only by an orphaned mint', async function(){
        const surviving = [issue(100, { decimals: '8', max_supply: '1000' }), issue(300, { decimals: '2' })];
        const db = collectorDb({ mints: [{ tick_id: 10 }] }, surviving);
        const ids = await tokenRefold.collectRefoldTickIds(db, 500, ['mints', 'issues']);
        assert.deepStrictEqual(ids, [10]);
        assert.deepStrictEqual(await tokenRefold.refoldTokenRows(db, ids), { refolded: 1, skipped: [] });
        assert.strictEqual(db.updateArgs[tokenRefold.FOLD_COLUMNS.indexOf('decimals')], 8);
        assert.strictEqual(db.updateArgs[tokenRefold.FOLD_COLUMNS.indexOf('max_supply')], '1000.00000000');
    });

    it('skips a missing legacy table but rejects an operational query failure', async function(){
        const db = collectorDb({ sends: [{ tick_id: 10 }] });
        db.doQuery.callsFake(async (sql) => {
            if(/FROM mints m/.test(sql)) throw Object.assign(new Error('missing'), { errno: 1146 });
            if(/FROM fees m/.test(sql)) throw Object.assign(new Error('lost'), { errno: 2002 });
            if(/FROM sends m/.test(sql)) return [{ tick_id: 10 }];
            return [];
        });
        assert.deepStrictEqual(await tokenRefold.collectRefoldTickIds(db, 500, ['mints', 'sends']), [10]);
        await assert.rejects(tokenRefold.collectRefoldTickIds(db, 500, ['fees']), /lost/);
    });
});

// An affected-tick query reduced to what decides the set: its FROM, the ordered joins, the
// WHERE and the m.<column> each selected ticker alias resolves to. The SELECT lists
// differ by design (the source reads tick text and addresses, the replica reads ids), so
// only the tick column is lifted out of them.
function tickQueryShape(sql){
    const s = String(sql).replace(/\s+/g, ' ').toLowerCase().replace(/ ?([(=,<>]) ?/g, '$1').replace(/ \)/g, ')').trim();
    const from = s.indexOf(' from '), where = s.indexOf(' where ');
    assert.ok(from > 0 && where > from, 'not an affected-tick SELECT: ' + sql);
    const select = s.slice(0, from), body = s.slice(from + 6, where).split(/ (?=(?:inner|left) join )/);
    const aliases = [...select.matchAll(/\b(t\d+)\.tick\b/g)].map(m => m[1]);
    const own = /^select distinct m\.(\w+) as tick_id$/.exec(select);
    const tickColumns = own ? [own[1]] : aliases.map(alias => {
        const join = body.map(j => new RegExp('^(?:inner|left) join index_tickers ' + alias + ' on\\(' + alias + '\\.id=m\\.(\\w+)\\)$').exec(j)).find(Boolean);
        assert.ok(join, 'no index_tickers join for ' + alias + ' in ' + sql);
        return join[1];
    }).sort();
    return { from: body[0], joins: body.slice(1), where: s.slice(where + 7), tickColumns };
}

async function replicaTickSql(){
    const db = collectorDb({});
    await tokenRefold.collectRefoldTickIds(db, 500, TICK_TABLES);
    return Object.fromEntries(db.doQuery.getCalls().map(c => [/FROM ([a-z_]+) m/.exec(c.args[0])[1], c.args[0]]));
}

describe('token refold affected-tick query shape parity', function(){
    it('restates the indexer joins, filter and tick column for every ticker-bearing table', async function(){
        const root = siblingCheckout(__dirname, '../../../../xchain-indexer');
        if(!skipOrFail(this, root, 'the indexer affected-tick shape guard')) return;
        const readSql = require(path.join(root.path, 'src/db/rollback/read_phase.js'));
        const queryFor = (table) => readSql.entityQueryForLedgerTables(table) ||
            readSql.entityQueryForTransferTables(table) || readSql.entityQueryForDexTables(table) ||
            readSql.entityQueryForCoinpayTables(table);
        const replica = await replicaTickSql();
        assert.strictEqual(Object.keys(replica).length, 13, 'the shape guard must compare a nonempty ticker set');
        for(const table of TICK_TABLES){
            assert.ok(queryFor(table), table + ' has no indexer affected-tick query');
            assert.deepStrictEqual(tickQueryShape(replica[table]), tickQueryShape(queryFor(table)),
                table + ': refoldTickQuery (src/db/token_refold.js) no longer matches the indexer entityQueryFor*Tables ' +
                '(src/db/rollback/read_phase.js); the replica would refold a different set of ticks');
        }
    });

    it('the shape keeps what it compares, and changes under every drift it must catch', async function(){
        const sql = await replicaTickSql(), shape = tickQueryShape(sql.credits);
        assert.ok(shape.joins.includes('inner join index_addresses a1 on(a1.id=m.address_id)'), JSON.stringify(shape.joins));
        assert.deepStrictEqual([shape.where, shape.tickColumns], ['m.action_index>=?', ['tick_id']]);
        const drifts = {
            'an INNER JOIN turned LEFT': [sql.credits, 'INNER JOIN index_addresses', 'LEFT JOIN index_addresses'],
            'an extra INNER JOIN': [sql.credits, 'WHERE ', 'INNER JOIN foo f ON (f.id=m.id) WHERE '],
            'a different address column': [sql.credits, 'a1.id=m.address_id', 'a1.id=m.source_id'],
            'an extra WHERE bound': [sql.credits, 'WHERE m.action_index >= ?', 'WHERE m.action_index >= ? AND m.block_index <= ?'],
            'a different tick column': [sql.swaps, 'm.give_tick_id', 'm.get_tick_id'],
        };
        for(const [name, [base, from, to]] of Object.entries(drifts)){
            const mutated = base.replace(from, to);
            assert.notStrictEqual(mutated, base, name + ' did not apply to the captured SQL');
            assert.notDeepStrictEqual(tickQueryShape(mutated), tickQueryShape(base), name + ' is invisible to tickQueryShape');
        }
    });

    it('reads a second selected ticker alias as a second tick column', async function(){
        const sql = await replicaTickSql(), source = 'SELECT t2.tick, t3.tick, a2.address FROM swaps m ' +
            sql.swaps.slice(sql.swaps.indexOf('INNER JOIN actions')).replace('WHERE ',
                'INNER JOIN index_tickers t3 ON (t3.id=m.get_tick_id) WHERE ');
        assert.deepStrictEqual(tickQueryShape(source).tickColumns, ['get_tick_id', 'give_tick_id']);
        assert.deepStrictEqual(tickQueryShape(sql.swaps).tickColumns, ['give_tick_id']);
    });
});

describe('token refold skips a collected tick whose history is not held', function(){
    function tokensDb(anchorRows, issueRows = []){
        const updates = [];
        const doQuery = sinon.stub().callsFake(async (sql, args) => {
            if(/FROM issues i INNER JOIN actions a1/.test(sql)) return issueRows;
            if(/^SELECT tick_id, action_index FROM tokens/.test(sql)) return anchorRows.filter(r => args.map(Number).includes(r.tick_id));
            if(/^UPDATE tokens SET/.test(sql)){ updates.push(args); return { affectedRows: 1 }; }
            return [];
        });
        return { doQuery, updates };
    }

    it('reports an anchored tick that folds to nothing and leaves its row alone', async function(){
        const db = tokensDb([{ tick_id: 10, action_index: 100 }]);
        assert.deepStrictEqual(await tokenRefold.refoldTokenRows(db, [10]), { refolded: 0, skipped: ['10'] });
        assert.strictEqual(db.updates.length, 0);
    });

    it('stays silent for a tick whose tokens row went with the delete', async function(){
        const db = tokensDb([]);
        assert.deepStrictEqual(await tokenRefold.refoldTokenRows(db, [10]), { refolded: 0, skipped: [] });
        assert.strictEqual(db.updates.length, 0);
    });

    it('reports unfolded and tail-only ticks together in tick id order and still refolds the whole one', async function(){
        const db = tokensDb([{ tick_id: 10, action_index: 100 }, { tick_id: 11, action_index: 100 }, { tick_id: 12, action_index: 100 },
            { tick_id: 13, action_index: 100 }], [issue(300, { tick_id: 11 }), issue(100, { tick_id: 12, decimals: '8' })]);
        const out = await tokenRefold.refoldTokenRows(db, [10, 11, 12, 13]);
        assert.deepStrictEqual(out, { refolded: 1, skipped: ['10', '11', '13'] });
        assert.deepStrictEqual(db.updates.map(a => a[a.length - 1]), ['12']);
    });
});

describe('token refold unsigned boundary parity', function(){
    it('nulls a non-integer callback block at the u64 half-unit boundary', function(){
        const out = tokenRefold.foldIssueRows([issue(100, { decimals: '8' }),
            issue(300, { callback_block: '18446744073709551615.6' })]).get('10');
        assert.strictEqual(out.callback_block, 0);
    });

    it('keeps a non-integer callback block below the u64 half-unit boundary', function(){
        const out = tokenRefold.foldIssueRows([issue(100, { decimals: '8' }),
            issue(300, { callback_block: '18446744073709551615.4' })]).get('10');
        assert.strictEqual(out.callback_block, '18446744073709551615.4');
    });
});
