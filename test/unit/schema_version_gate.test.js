// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.
//
// The gate behind src/schema/version.js: a migration that lands DDL against a
// wire-replicated table decides what a follower can STORE, so it must arrive with
// a SCHEMA_VERSION bump for that dbType or followers apply rows into a schema that
// cannot hold them (Unknown column, ER_DUP_ENTRY, a truncated 4-byte character)
// instead of refusing the snapshot. Nothing enforced that before: the version
// history drifted 20+ migrations behind the ledger because bumping was a habit
// rather than a check. Here the frontier in schema-version.js is the claim and the
// sibling migration ledgers are the evidence.
//
// Pure-DML backfills are NOT flagged: they change rows, not the shape a follower
// needs. DDL against a table this dbType never ships is not flagged either.
//
// Per the operator ruling of 2026-09-09 a red gate for payload-affecting DDL is
// answered by a bump carried on the next fleet release: the version decides what
// peers ACCEPT, so a drive-by edit strands the fleet. A reviewed secondary-index-
// only migration can instead advance the frontier with an index-only accounting
// comment because it changes neither replicated columns nor payload shape.

'use strict';

const assert = require('assert');
const crypto = require('crypto');
const fs     = require('fs');
const os     = require('os');
const path   = require('path');

const lifecycle        = require('../../src/table_lifecycle');
const replicatedTables = require('../../src/schema/replicated_tables');
const { MIGRATION_FRONTIER } = require('../../src/schema/version');

const MIGRATION_DIRS = {
    indexer: process.env.XCHAIN_INDEXER_SQL_PATH
        ? path.join(process.env.XCHAIN_INDEXER_SQL_PATH, 'migrations')
        : path.join(__dirname, '../../../xchain-indexer/src/sql/migrations'),
    decoder: process.env.XCHAIN_DECODER_SQL_PATH
        ? path.join(process.env.XCHAIN_DECODER_SQL_PATH, 'migrations')
        : path.join(__dirname, '../../../xchain-decoder/src/sql/migrations')
};
const SIBLING_REQUIRED = process.env.XCHAIN_REQUIRE_SIBLINGS === '1';

function contentHash(text){
    return crypto.createHash('sha256').update(text).digest('hex');
}

// Tables xchain-sync carries over the wire for each dbType, same derivation as
// replicatedDatetimeColumns.test.js: the indexer registry's stream:* classes plus
// the snapshot-channel tables (indexer `events` is replication:'snapshot' and is not
// in the stream topology), and the literal decoder topology. Operator-local,
// hub-mirror and follower-derived tables never ship, so DDL on them is not a
// replication concern.
function wireTables(dbType){
    if(dbType === 'decoder') return new Set(replicatedTables.getReplicatedTables('decoder'));
    return new Set(lifecycle.tablesWhere(t => t.owner === 'indexer'
        && (/^stream:/.test(t.replication) || t.replication === 'snapshot')));
}

// Statements that change a table's SHAPE. Deliberately excludes INSERT/UPDATE/DELETE:
// a backfill cannot make a follower unable to store a streamed row. IF NOT EXISTS /
// IF EXISTS and the UNIQUE/FULLTEXT/SPATIAL index qualifiers are all in use in the
// ledger, so every form has to be matched or the guard reads a real index change as
// prose (that is how the standalone `CREATE UNIQUE INDEX IF NOT EXISTS ... ON` files
// would slip past).
const DDL_PATTERNS = [
    /\bALTER\s+TABLE\s+`?(\w+)`?/gi,
    /\bCREATE\s+(?:TEMPORARY\s+)?TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+`?(\w+)`?/gi,
    /\bDROP\s+TABLE(?:\s+IF\s+EXISTS)?\s+`?(\w+)`?/gi,
    /\bRENAME\s+TABLE\s+`?(\w+)`?/gi,
    /\bCREATE\s+(?:UNIQUE\s+|FULLTEXT\s+|SPATIAL\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?`?\w+`?\s+ON\s+`?(\w+)`?/gi,
    /\bDROP\s+INDEX\s+(?:IF\s+EXISTS\s+)?`?\w+`?\s+ON\s+`?(\w+)`?/gi
];

// Migration bodies carry long `--` prose blocks that name tables and quote DDL, so
// comments are stripped before matching or every essay counts as a schema change.
function ddlTables(sqlText){
    const body = sqlText
        .replace(/\/\*[\s\S]*?\*\//g, ' ')
        .replace(/^\s*--.*$/gm, ' ')
        .replace(/--[^\n]*/g, ' ');
    const tables = new Set();
    for(const re of DDL_PATTERNS){
        for(const m of body.matchAll(re)) tables.add(m[1]);
    }
    return tables;
}

function isNonUniqueSecondaryIndex(statement, tables){
    if(tables.length !== 1) return false;
    const table = tables[0].replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const createIndex = new RegExp('^\\s*CREATE\\s+INDEX(?:\\s+IF\\s+NOT\\s+EXISTS)?'
        + '\\s+`?\\w+`?\\s+ON\\s+`?' + table + '`?\\s+\\([^;]+\\)\\s*$', 'i');
    const alterTable = new RegExp('^\\s*ALTER\\s+TABLE\\s+`?' + table + '`?\\s+'
        + 'ADD\\s+(?:INDEX|KEY)(?:\\s+IF\\s+NOT\\s+EXISTS)?'
        + '\\s+`?\\w+`?\\s*\\([^;]+\\)\\s*$', 'i');
    return createIndex.test(statement) || alterTable.test(statement);
}

function unsafeIndexOnlyTables(sqlText, wire){
    const body = sqlText
        .replace(/\/\*[\s\S]*?\*\//g, ' ')
        .replace(/^\s*--.*$/gm, ' ')
        .replace(/--[^\n]*/g, ' ');
    const unsafe = new Set();
    for(const statement of body.split(';').map(s => s.trim()).filter(Boolean)){
        const tables = [...ddlTables(statement)].filter(t => wire.has(t)).sort();
        if(tables.length && !isNonUniqueSecondaryIndex(statement, tables)){
            for(const table of tables) unsafe.add(table);
        }
    }
    return [...unsafe].sort();
}

// Every migration not recorded by the frontier that changes the shape of a
// wire-replicated table. `before` records earlier replicated-DDL filenames so a new
// back-dated file cannot hide behind the date cursor. `accounted` covers the same-day
// tail, and `hashes` fixes the bytes reviewed when each accounted file was recorded.
// A filename with no parseable date is reported too: the gate cannot place it
// relative to the cursor, and guessing safe would hide it.
function unaccountedReplicatedDdl(dir, frontier, wire){
    const findings = [];
    for(const file of fs.readdirSync(dir).filter(f => f.endsWith('.sql')).sort()){
        const date = (file.match(/^(\d{4}-\d{2}-\d{2})-/) || [])[1];
        const sqlText = fs.readFileSync(path.join(dir, file), 'utf8');
        if(date && date < frontier.through && (frontier.before || []).includes(file)) continue;
        const isAccounted = date && date === frontier.through && frontier.accounted.includes(file);
        const isIndexOnly = isAccounted && (frontier.indexOnly || []).includes(file);
        const hashMismatch = isAccounted
            && (!frontier.hashes || contentHash(sqlText) !== frontier.hashes[file]);
        if(isAccounted && !isIndexOnly && !hashMismatch) continue;
        const hits = isIndexOnly
            ? unsafeIndexOnlyTables(sqlText, wire)
            : [...ddlTables(sqlText)].filter(t => wire.has(t)).sort();
        if(hits.length || hashMismatch){
            const finding = { file, tables: hits, undated: !date };
            if(hashMismatch) finding.hashMismatch = true;
            if(date && date < frontier.through) finding.backdated = true;
            findings.push(finding);
        }
    }
    return findings;
}

describe('replicated-DDL migrations cannot land without a SCHEMA_VERSION bump @regression', function(){

    describe('the gate detects what it claims to detect', function(){

        let dir;
        const foldedSql = 'ALTER TABLE attests ADD COLUMN folded INT NULL;\n';
        const frontier = {
            through: '2026-09-11',
            before: ['2026-09-10-sends-old.sql'],
            accounted: ['2026-09-11-already-folded-in.sql'],
            hashes: {
                '2026-09-11-already-folded-in.sql': contentHash(foldedSql)
            }
        };
        const wire = new Set(['sends', 'attests']);

        function write(name, sql){ fs.writeFileSync(path.join(dir, name), sql); }

        beforeEach(function(){
            dir = fs.mkdtempSync(path.join(os.tmpdir(), 'xchain-sync-migration-gate-'));
        });

        afterEach(function(){
            fs.rmSync(dir, { recursive: true, force: true });
        });

        it('flags an ALTER TABLE on a replicated table dated past the frontier', function(){
            write('2026-09-12-sends-add-column.sql', 'ALTER TABLE sends ADD COLUMN leg_note VARCHAR(32) NULL;\n');
            assert.deepStrictEqual(unaccountedReplicatedDdl(dir, frontier, wire),
                [{ file: '2026-09-12-sends-add-column.sql', tables: ['sends'], undated: false }]);
        });

        it('flags a standalone CREATE UNIQUE INDEX IF NOT EXISTS ... ON, backticks and all', function(){
            write('2026-09-14-attests-index.sql',
                'CREATE UNIQUE INDEX IF NOT EXISTS `relay_identity` ON `attests` (origin_chain);\n');
            const found = unaccountedReplicatedDdl(dir, frontier, wire);
            assert.deepStrictEqual(found.map(f => f.tables), [['attests']]);
        });

        it('flags a DROP INDEX and a CREATE TABLE of a replicated table', function(){
            write('2026-09-13-drop-index.sql', 'DROP INDEX action_index ON sends;\n');
            write('2026-09-15-new-table.sql', 'CREATE TABLE IF NOT EXISTS attests (id BIGINT UNSIGNED NOT NULL);\n');
            assert.deepStrictEqual(unaccountedReplicatedDdl(dir, frontier, wire).map(f => f.file),
                ['2026-09-13-drop-index.sql', '2026-09-15-new-table.sql']);
        });

        it('flags an undated filename carrying replicated DDL rather than skipping it', function(){
            write('hotfix-sends.sql', 'ALTER TABLE sends MODIFY memo VARCHAR(64) NULL;\n');
            assert.deepStrictEqual(unaccountedReplicatedDdl(dir, frontier, wire),
                [{ file: 'hotfix-sends.sql', tables: ['sends'], undated: true }]);
        });

        it('flags a back-dated replicated DDL migration not recorded before the frontier', function(){
            write('2026-09-09-sends-backdated.sql',
                'ALTER TABLE sends ADD COLUMN escaped_version_gate INT NULL;\n');
            assert.deepStrictEqual(unaccountedReplicatedDdl(dir, frontier, wire), [{
                file: '2026-09-09-sends-backdated.sql', tables: ['sends'], undated: false, backdated: true
            }]);
        });

        it('does not flag a back-dated pure-DML migration', function(){
            write('2026-09-09-sends-backfill.sql', 'UPDATE sends SET memo_id = NULL;\n');
            assert.deepStrictEqual(unaccountedReplicatedDdl(dir, frontier, wire), []);
        });
    });
});

describe('replicated-DDL migrations cannot land without a SCHEMA_VERSION bump @regression', function(){

    describe('the gate detects what it claims to detect', function(){

        let dir;
        const foldedSql = 'ALTER TABLE attests ADD COLUMN folded INT NULL;\n';
        const frontier = {
            through: '2026-09-11',
            before: ['2026-09-10-sends-old.sql'],
            accounted: ['2026-09-11-already-folded-in.sql'],
            hashes: {
                '2026-09-11-already-folded-in.sql': contentHash(foldedSql)
            }
        };
        const wire = new Set(['sends', 'attests']);

        function write(name, sql){ fs.writeFileSync(path.join(dir, name), sql); }

        beforeEach(function(){
            dir = fs.mkdtempSync(path.join(os.tmpdir(), 'xchain-sync-migration-gate-'));
        });

        afterEach(function(){
            fs.rmSync(dir, { recursive: true, force: true });
        });

        it('does not flag a pure-DML backfill on a replicated table', function(){
            write('2026-09-12-sends-backfill.sql',
                'UPDATE sends SET memo_id = NULL WHERE memo_id = 0;\nDELETE FROM sends WHERE block_index < 0;\n');
            assert.deepStrictEqual(unaccountedReplicatedDdl(dir, frontier, wire), []);
        });

        it('does not flag DDL on a table this dbType never ships', function(){
            write('2026-09-12-local-table.sql', 'ALTER TABLE state_checkpoints ADD COLUMN seq BIGINT NULL;\n');
            assert.deepStrictEqual(unaccountedReplicatedDdl(dir, frontier, wire), []);
        });

        it('does not flag DDL quoted inside the migration prose', function(){
            write('2026-09-12-prose-only.sql', [
                '-- Migration: none. WHY: the earlier file ran',
                '--   ALTER TABLE sends ADD COLUMN leg_ordinal TINYINT NOT NULL DEFAULT 0;',
                '-- and this one only records that it applied.',
                '/* CREATE INDEX idx ON attests (request_id); */',
                'UPDATE schema_notes SET note = 1;'
            ].join('\n') + '\n');
            assert.deepStrictEqual(unaccountedReplicatedDdl(dir, frontier, wire), []);
        });

        it('does not flag a recorded migration before the frontier, nor an unchanged accounted tail', function(){
            write('2026-09-10-sends-old.sql', 'ALTER TABLE sends ADD COLUMN old_col INT NULL;\n');
            write('2026-09-11-already-folded-in.sql', foldedSql);
            assert.deepStrictEqual(unaccountedReplicatedDdl(dir, frontier, wire), []);
        });

        it('flags an accounted migration whose content changed after accounting', function(){
            write('2026-09-11-already-folded-in.sql',
                'ALTER TABLE attests ADD COLUMN changed_after_accounting INT NULL;\n');
            assert.deepStrictEqual(unaccountedReplicatedDdl(dir, frontier, wire), [{
                file: '2026-09-11-already-folded-in.sql',
                tables: ['attests'],
                undated: false,
                hashMismatch: true
            }]);
        });

        it('flags a same-day migration the accounted tail does not name', function(){
            write('2026-09-11-snuck-in.sql', 'ALTER TABLE sends ADD COLUMN snuck INT NULL;\n');
            assert.deepStrictEqual(unaccountedReplicatedDdl(dir, frontier, wire).map(f => f.file),
                ['2026-09-11-snuck-in.sql']);
        });

        it('failure text names where to record a back-dated file, the digest to pin and the frontier move', function(){
            write('2026-09-09-sends-backdated.sql', 'ALTER TABLE sends ADD COLUMN gate_hint INT NULL;\n');
            const edited = 'ALTER TABLE attests ADD COLUMN edited_for_hint INT NULL;\n';
            write('2026-09-11-already-folded-in.sql', edited);
            const text = frontierMessage('indexer', dir, frontier, unaccountedReplicatedDdl(dir, frontier, wire));
            assert.ok(text.includes('[dated before the frontier: if an earlier frontier already accounted it,'
                + ' add it to MIGRATION_FRONTIER.indexer.before]'));
            assert.ok(text.includes('sha256 ' + contentHash(edited)
                + ': if this edit is reviewed, set MIGRATION_FRONTIER.indexer.hashes]'));
            assert.ok(text.includes('move each previously accounted file with replicated DDL into before'
                + ' and pin each new accounted file in hashes.'));
        });
    });
});

describe('replicated-DDL migrations cannot land without a SCHEMA_VERSION bump @regression', function(){

    describe('the gate detects what it claims to detect', function(){

        let dir;
        const frontier = {
            through: '2026-09-12',
            accounted: ['2026-09-12-sends-covering-index.sql'],
            indexOnly: ['2026-09-12-sends-covering-index.sql'],
            hashes: {
                '2026-09-12-sends-covering-index.sql': contentHash(
                    'ALTER TABLE sends ADD INDEX IF NOT EXISTS sends_covering (action_index, block_index);\n')
            }
        };
        const wire = new Set(['sends', 'attests']);

        function write(sql){
            fs.writeFileSync(path.join(dir, '2026-09-12-sends-covering-index.sql'), sql);
        }

        beforeEach(function(){
            dir = fs.mkdtempSync(path.join(os.tmpdir(), 'xchain-sync-migration-gate-'));
        });

        afterEach(function(){
            fs.rmSync(dir, { recursive: true, force: true });
        });

        it('does not flag an index-only migration explicitly recorded at an advanced frontier', function(){
            write('ALTER TABLE sends ADD INDEX IF NOT EXISTS sends_covering (action_index, block_index);\n');
            assert.deepStrictEqual(unaccountedReplicatedDdl(dir, frontier, wire), []);
        });

        it('flags payload-affecting DDL substituted into an index-only migration', function(){
            write('ALTER TABLE sends ADD COLUMN payload_shape_changed INT NULL;\n');
            assert.deepStrictEqual(unaccountedReplicatedDdl(dir, frontier, wire), [{
                file: '2026-09-12-sends-covering-index.sql',
                tables: ['sends'],
                undated: false,
                hashMismatch: true
            }]);
        });

        it('flags a unique index substituted into an index-only migration', function(){
            write('CREATE UNIQUE INDEX sends_covering ON sends (action_index, block_index);\n');
            assert.deepStrictEqual(unaccountedReplicatedDdl(dir, frontier, wire), [{
                file: '2026-09-12-sends-covering-index.sql',
                tables: ['sends'],
                undated: false,
                hashMismatch: true
            }]);
        });
    });
});

// The failure text for a set of findings. A back-dated file and an edited accounted
// file are usually frontier bookkeeping, not a missing version bump, so the hint
// says where to record them and prints the digest to pin.
function frontierMessage(dbType, dir, frontier, findings){
    return 'replicated DDL is not covered by the ' + dbType + ' migration frontier ('
        + frontier.through + ') with no SCHEMA_VERSION.' + dbType + ' bump:\n'
        + findings.map(f => '  ' + f.file + ' -> ' + f.tables.join(', ')
            + (f.undated ? '  [filename carries no date]' : '')
            + (f.backdated ? '  [dated before the frontier: if an earlier frontier already accounted it,'
                + ' add it to MIGRATION_FRONTIER.' + dbType + '.before]' : '')
            + (f.hashMismatch ? '  [content differs from frontier hash; sha256 '
                + contentHash(fs.readFileSync(path.join(dir, f.file), 'utf8'))
                + ': if this edit is reviewed, set MIGRATION_FRONTIER.' + dbType + '.hashes]' : '')).join('\n')
        + '\nIf the DDL changes replicated columns or payload shape, bump SCHEMA_VERSION.'
        + dbType + ' in src/schema/version.js with a history entry naming what a follower'
        + ' on the previous version cannot store, move the frontier to the newest'
        + ' migration date, and land it with the next fleet release. If it adds only'
        + ' non-unique secondary indexes, leave the version unchanged and move the'
        + ' frontier with an index-only accounting comment naming the migration.'
        + ' When moving the frontier, move each previously accounted file with replicated DDL'
        + ' into before and pin each new accounted file in hashes.';
}

describe('replicated-DDL migrations cannot land without a SCHEMA_VERSION bump @regression', function(){

    describe('the sibling migration ledgers are level with SCHEMA_VERSION', function(){

        for(const dbType of Object.keys(MIGRATION_DIRS)){

            it(dbType + ': every replicated-DDL migration is covered by the frontier', function(){
                const dir = MIGRATION_DIRS[dbType];
                if(!fs.existsSync(dir)){
                    if(SIBLING_REQUIRED) throw new Error('sibling migration ledger missing at ' + dir);
                    this.skip(); return;
                }
                const wire = wireTables(dbType);
                assert.ok(wire.size, dbType + ' wire-replicated table set came back empty');

                const findings = unaccountedReplicatedDdl(dir, MIGRATION_FRONTIER[dbType], wire);
                assert.deepStrictEqual(findings, [], findings.length
                    ? frontierMessage(dbType, dir, MIGRATION_FRONTIER[dbType], findings)
                    : '');
            });
        }
    });
});
