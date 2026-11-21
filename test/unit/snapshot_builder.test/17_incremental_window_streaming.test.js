// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const { assert, sinon, Readable, SnapshotBuilder, Utility } = require('./helpers/support');
const { windowFakeDb, windowCtx, baseRows } = require('./helpers/window_fake_db');
const { collectMaturedCooldownCredits } = require('../../../src/server/cooldown_credits');

function deferred(){
    let resolve;
    const promise = new Promise(r => { resolve = r; });
    return { promise, resolve };
}

function selectCtx(){
    return {
        dbType: 'indexer', skipLookups: false, lookupSet: new Set(),
        decoderFullDump: new Set(), indexerFullDump: new Set(), conn: null,
    };
}

function registerIncrementalStreamTests(getBuilder){
    it('writes the first window row before the database stream finishes', async function(){
        const builder = getBuilder();
        const release = deferred();
        const firstWritten = deferred();
        let sourceFinished = false;
        const stream = Readable.from((async function*(){
            yield { action_index: 1 };
            await release.promise;
            sourceFinished = true;
            yield { action_index: 2 };
        })());
        const writer = { write: sinon.stub().callsFake(async chunk => {
            if(chunk.includes('action_index') && chunk.includes('1')) firstWritten.resolve();
        }) };

        const write = builder.writeIncrementalTableRows(writer, 'actions', { stream }, true);
        await firstWritten.promise;
        assert.strictEqual(sourceFinished, false, 'source remains paused when row one is written');
        release.resolve();
        assert.strictEqual(await write, false);
        assert.strictEqual(stream.destroyed, true, 'the consumed stream is destroyed in finally');
    });

    it('keeps base order and appends only unseen merge rows', async function(){
        const builder = getBuilder();
        const chunks = [];
        const writer = { write: async chunk => { chunks.push(chunk); } };
        const selection = {
            stream: Readable.from([{ id: 1 }, { id: 2 }]),
            extras: [{ id: 2 }, { id: 3 }],
            dedupeKey: row => String(row.id),
        };

        assert.strictEqual(await builder.writeIncrementalTableRows(writer, 'rows', selection, true), false);
        assert.deepStrictEqual(JSON.parse('{' + chunks.join('') + '}').rows, [{ id: 1 }, { id: 2 }, { id: 3 }]);
    });
}

function registerIncrementalGapTests(getBuilder){
    it('omits a schema-gap table when its stream fails before the first row', async function(){
        const builder = getBuilder();
        const gap = Object.assign(new Error('missing table'), { errno: 1146 });
        const stream = Readable.from((async function*(){ throw gap; })());
        sinon.stub(builder, 'selectIncrementalRows').resolves({ stream });
        sinon.stub(builder, 'tolerateIncrementalTableError').callThrough();
        const writer = { write: sinon.stub().resolves() };

        assert.strictEqual(await builder.streamIncrementalTable(writer, {}, 'actions', selectCtx(), true), true);
        assert.strictEqual(writer.write.called, false);
        assert.strictEqual(stream.destroyed, true);
        assert.strictEqual(builder.tolerateIncrementalTableError.calledWith(gap, 'actions', false), true);
    });

    it('rejects a schema-gap fault after the table header and first row', async function(){
        const builder = getBuilder();
        const gap = Object.assign(new Error('missing column'), { errno: 1054 });
        const stream = Readable.from((async function*(){ yield { id: 1 }; throw gap; })());
        sinon.stub(builder, 'selectIncrementalRows').resolves({ stream });
        const writer = { write: sinon.stub().resolves() };

        await assert.rejects(
            () => builder.streamIncrementalTable(writer, {}, 'actions', selectCtx(), true),
            e => e === gap && e.incrementalTableStarted === true
        );
        assert.strictEqual(stream.destroyed, true);
        assert.ok(writer.write.calledWith('"actions":['), 'table header was written before failure');
        assert.strictEqual(writer.write.calledWith(']'), false, 'truncated table is never closed as valid JSON');
    });
}

describe('SnapshotBuilder incremental window streaming', function(){
    let builder;
    beforeEach(function(){ builder = new SnapshotBuilder(new Utility()); });
    afterEach(function(){ sinon.restore(); });
    registerIncrementalStreamTests(() => builder);
    registerIncrementalGapTests(() => builder);
});

const vr = (block_index, source_id, round_reference) => ({
    id: block_index * 10 + source_id, block_index, source_id, signing_pubkey_id: 7,
    reward_type: 'anchor', round_reference, round_qualifier: null,
});
const led = (action_index, address_id) => ({ action_index, address_id, tick_id: 1, amount: action_index });

// Base rows sit at or above the cursor; every side set mixes rows below it
// (the only ones the base stream cannot carry) with rows that duplicate a
// base row, plus a cross-leg duplicate.
function windowFixture(){
    const ledger = [led(10, 1), led(11, 2), led(12, 3), led(14, 4)];
    return {
        credits: ledger, escrows: ledger,
        capCredits: [led(5, 9), led(12, 3)], conCredits: [led(5, 9), led(7, 8)],
        capEscrows: [led(6, 9), led(11, 2)], conEscrows: [led(3, 8)],
        validator_rewards: [vr(100, 1, 'r1'), vr(101, 2, 'r2'), vr(103, 3, 'r3')],
        redriven: [vr(90, 4, 'r4'), vr(101, 2, 'r2')],
        derived: [vr(90, 4, 'r4'), vr(95, 5, 'r5'), vr(103, 3, 'r3')],
    };
}

async function streamedTable(builder, db, table, ctx){
    const chunks = [];
    const writer = { write: async chunk => { chunks.push(chunk); } };
    const selection = await builder.selectIncrementalRows(db, table, ctx);
    await builder.writeIncrementalTableRows(writer, table, selection, true);
    return chunks.length ? JSON.parse('{' + chunks.join('') + '}')[table] : undefined;
}

async function legacyTable(builder, fx, db, table, ctx){
    const base = (table === 'credits' || table === 'escrows') && ctx.firstActionIndex === null
        ? [] : baseRows(fx, 'FROM `' + table + '` WHERE ' +
            (table === 'validator_rewards' ? 'block_index' : 'action_index') + ' >= ?',
        [table === 'validator_rewards' ? ctx.sinceBlock : ctx.firstActionIndex]);
    const rows = table === 'validator_rewards'
        ? await builder.mergeValidatorRewardRows(db, base, ctx.sinceBlock, ctx.lastBlock, ctx.conn)
        : await builder.mergeMaturedCooldownRows(db, table, base, ctx.sinceBlock, ctx.lastBlock, ctx.conn);
    return rows && rows.length ? JSON.parse(JSON.stringify(rows)) : undefined;
}

function registerSideSetReadTests(getBuilder){
    it('reads every side set before opening the base stream on the snapshot connection', async function(){
        const builder = getBuilder();
        for(const table of ['credits', 'escrows', 'validator_rewards']){
            const { db, conn, calls } = windowFakeDb(windowFixture());
            await streamedTable(builder, db, table, windowCtx(conn, 100, 10));
            const kinds = calls.map(c => c.kind);
            assert.strictEqual(kinds[kinds.length - 1], 'stream', table + ': base stream opens last');
            assert.ok(kinds.filter(k => k === 'query').length >= 1, table + ': side sets were read');
        }
    });

    it('bounds every side-set read below the base window', async function(){
        const builder = getBuilder();
        const { db, conn, calls } = windowFakeDb(windowFixture());
        await streamedTable(builder, db, 'credits', windowCtx(conn, 100, 10));
        await streamedTable(builder, db, 'validator_rewards', windowCtx(conn, 100, 10));
        const side = calls.filter(c => c.kind === 'query');
        assert.strictEqual(side.length, 4);
        for(const c of side.slice(0, 2)){
            assert.ok(/ AND c\.action_index < \?$/.test(c.sql), c.sql);
            assert.strictEqual(c.args[c.args.length - 1], 10);
        }
        for(const c of side.slice(2)){
            assert.ok(/ AND vr\.block_index < \?$/.test(c.sql), c.sql);
            assert.strictEqual(c.args[c.args.length - 1], 100);
        }
    });
}

function registerSideSetMergeTests(getBuilder){
    it('leaves the per-block collector SQL unbounded when no bound is passed', async function(){
        const { db, calls } = windowFakeDb(windowFixture());
        await collectMaturedCooldownCredits(db, 5, 5);
        for(const c of calls){
            assert.ok(/cooldown_end_block BETWEEN \? AND \?$/.test(c.sql), c.sql);
            assert.strictEqual(c.args.includes(undefined), false);
        }
    });

    it('streams the same rows the legacy array merge produced', async function(){
        const builder = getBuilder();
        const cases = [['credits', 10], ['escrows', 10], ['validator_rewards', 10], ['credits', null], ['escrows', null]];
        for(const [table, firstActionIndex] of cases){
            const fx = windowFixture();
            const { db, conn } = windowFakeDb(fx);
            const ctx = windowCtx(conn, 100, firstActionIndex);
            const expected = await legacyTable(builder, fx, db, table, ctx);
            assert.ok(expected && expected.length > 0, table + ': fixture exercises the merge');
            assert.deepStrictEqual(await streamedTable(builder, db, table, ctx), expected, table + '/' + firstActionIndex);
        }
    });

    it('frees the snapshot connection when the client aborts mid-table', async function(){
        const builder = getBuilder();
        const { db, conn } = windowFakeDb(windowFixture());
        const selection = await builder.selectIncrementalRows(db, 'credits', windowCtx(conn, 100, 10));
        const abort = Object.assign(new Error('client gone'), { aborted: true });
        let rows = 0;
        const writer = { write: async chunk => { if(chunk.startsWith('{') && ++rows === 2) throw abort; } };

        await assert.rejects(() => builder.writeIncrementalTableRows(writer, 'credits', selection, true), e => e === abort);
        assert.strictEqual(conn.held(), false, 'abandoned stream released the connection');
        await db.doQuery('ROLLBACK', [], conn);
    });
}

function registerResidentWindowTest(getBuilder){
    async function maxResident(rowCount){
        const builder = getBuilder();
        let yielded = 0;
        let written = 0;
        let peak = 0;
        const stream = Readable.from((async function*(){
            for(let i = 0; i < rowCount; i++){ yielded++; yield { action_index: i }; }
        })());
        const writer = { write: async chunk => {
            if(chunk.startsWith('{')) written++;
            peak = Math.max(peak, yielded - written);
            await new Promise(setImmediate);
        } };
        await builder.writeIncrementalTableRows(writer, 'actions', { open: () => stream }, true);
        assert.strictEqual(written, rowCount);
        return peak;
    }

    it('keeps resident window rows flat as the window grows', async function(){
        const small = await maxResident(50);
        const large = await maxResident(5000);
        assert.ok(small <= 20, 'small window resident rows ' + small);
        assert.ok(large <= small + 1, 'large window resident rows ' + large + ' vs ' + small);
    });
}

describe('SnapshotBuilder incremental window side sets', function(){
    let builder;
    beforeEach(function(){ builder = new SnapshotBuilder(new Utility()); });
    registerSideSetReadTests(() => builder);
    registerSideSetMergeTests(() => builder);
    registerResidentWindowTest(() => builder);
});
