'use strict';

// The replica's schema changes live in dated files under src/sql/migrations/ and run through
// src/sql/migrations/runner.js. These tests pin that every file parses, every key a call
// site names exists, and the runner executes exactly the statement a key declares.

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');

const runner = require('../../src/sql/migrations/runner');

const SRC = path.resolve(__dirname, '../../src');
const MIGRATION_DIR = path.join(SRC, 'sql', 'migrations');
const FILE_RE = /^\d{4}-\d{2}-\d{2}-[a-z0-9-]+\.sql$/;

function keysByFile(){
    const out = new Map();
    for(const file of fs.readdirSync(MIGRATION_DIR).filter(f => f.endsWith('.sql')).sort()){
        const text = fs.readFileSync(path.join(MIGRATION_DIR, file), 'utf8');
        out.set(file, Array.from(text.matchAll(/^\s*--\s*migration-key:\s*([a-z0-9-]+)\s*$/gmi), m => m[1]));
    }
    return out;
}

function fakeDb(){
    const calls = [];
    return Object.assign({
        calls,
        async doQuery(sql, params, cb, opts){ calls.push({ fn: 'doQuery', sql, opts }); return []; },
        async doQueryStrict(sql){ calls.push({ fn: 'doQueryStrict', sql }); return []; },
    }, runner);
}

describe('replica SQL migrations', function(){
    const keys = keysByFile();

    it('keeps every migration file dated and every file non-empty', function(){
        assert.ok(keys.size > 0);
        for(const [file, list] of keys){
            assert.match(file, FILE_RE);
            assert.ok(list.length > 0, file + ' declares no migration-key');
        }
    });

    it('has unique keys inside each file', function(){
        for(const [file, list] of keys) assert.deepStrictEqual(list, Array.from(new Set(list)), file);
    });

    it('runs every declared key as one statement without leftover placeholders', async function(){
        for(const [file, list] of keys){
            const raw = fs.readFileSync(path.join(MIGRATION_DIR, file), 'utf8');
            const values = {};
            for(const m of raw.matchAll(/\{\{([a-z_]+)\}\}/g)) values[m[1]] = 'x';
            for(const key of list){
                const db = fakeDb();
                await db.runSqlMigrationStatement(file, key, values);
                assert.strictEqual(db.calls.length, 1, file + '#' + key);
                assert.ok(!/\{\{/.test(db.calls[0].sql), file + '#' + key + ' kept a placeholder');
                assert.ok(!db.calls[0].sql.includes(';'), file + '#' + key + ' carries a terminator');
            }
        }
    });

    it('resolves every literal file and key a src call site names', function(){
        const re = /runSqlMigrationStatement\(\s*'([^']+\.sql)'\s*,\s*'([a-z0-9-]+)'/g;
        let seen = 0;
        const walk = (dir) => fs.readdirSync(dir, { withFileTypes: true }).forEach(ent => {
            const p = path.join(dir, ent.name);
            if(ent.isDirectory()) return walk(p);
            if(!ent.name.endsWith('.js')) return;
            for(const m of fs.readFileSync(p, 'utf8').matchAll(re)){
                seen++;
                assert.ok((keys.get(m[1]) || []).includes(m[2]), p + ' names missing ' + m[1] + '#' + m[2]);
            }
        });
        walk(SRC);
        assert.ok(seen > 0);
    });

    it('substitutes replacements and refuses a missing one', async function(){
        const db = fakeDb();
        await db.runSqlMigrationStatement('2026-10-19-replica-columns.sql', 'repair-auto-increment', { table: 'credits' });
        assert.strictEqual(db.calls[0].sql, 'ALTER TABLE `credits` MODIFY id BIGINT NOT NULL AUTO_INCREMENT');
        await assert.rejects(() => db.runSqlMigrationStatement('2026-10-19-replica-columns.sql', 'repair-auto-increment'),
            /Missing SQL migration replacement: table/);
    });

    it('routes a strict run through doQueryStrict and a rethrow run through doQuery', async function(){
        const db = fakeDb();
        await db.runSqlMigrationStatement('2026-10-19-sync-state.sql', 'sync-state', undefined, { strict: true });
        await db.runSqlMigrationStatement('2026-10-19-sync-state.sql', 'sync-state', undefined, { rethrow: true });
        assert.deepStrictEqual(db.calls.map(c => c.fn), ['doQueryStrict', 'doQuery']);
        assert.deepStrictEqual(db.calls[1].opts, { rethrow: true });
    });

    it('refuses an unknown key and a file name outside the dated pattern', async function(){
        const db = fakeDb();
        await assert.rejects(() => db.runSqlMigrationStatement('2026-10-19-sync-state.sql', 'nope'), /key not found/);
        await assert.rejects(() => db.runSqlMigrationStatement('../runner.js', 'x'), /Invalid SQL migration filename/);
        await assert.rejects(() => db.runSqlMigrationStatement('sync-state.sql', 'x'), /Invalid SQL migration filename/);
    });

    it('adds the key-rebuild precondition column only for a declared table and column', async function(){
        const db = fakeDb();
        assert.strictEqual(await db.ensureKeyRebuildColumn('credits', 'id'), false);
        assert.strictEqual(db.calls.length, 0);
        assert.strictEqual(await db.ensureKeyRebuildColumn('anchor_actions', 'section_index'), true);
        assert.match(db.calls[0].sql, /^ALTER TABLE `anchor_actions` ADD COLUMN IF NOT EXISTS `section_index`/);
    });
});
