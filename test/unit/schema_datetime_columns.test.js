'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');

const {
    DATETIME_COLUMNS,
    byTable,
    modifyClause,
    needsRetype,
} = require('../../src/schema/datetime_columns');

const SQL_DIR = path.join(__dirname, '../../src/sql');

function sqlWithoutComments(table){
    const filename = path.join(SQL_DIR, table + '.sql');
    return fs.readFileSync(filename, 'utf8').replace(/--.*$/gm, '');
}

function declaredTimestampColumns(sql){
    const columns = [];
    for(const line of sql.split('\n')){
        const match = line.match(/^\s*`?([A-Za-z0-9_]+)`?\s+TIMESTAMP\b/i);
        if(match) columns.push(match[1]);
    }
    return columns;
}

describe('DATETIME column inventory', function(){
    it('uses DATETIME definitions without TIMESTAMP type tokens', function(){
        for(const entry of DATETIME_COLUMNS){
            assert.match(entry.columnDef, /^DATETIME\b/);
            assert.doesNotMatch(entry.columnDef, /\bTIMESTAMP\b/i);
        }
    });

    it('matches a temporal declaration in each table definition', function(){
        for(const entry of DATETIME_COLUMNS){
            const sql = sqlWithoutComments(entry.table);
            const declaration = new RegExp(
                '^\\s*`?' + entry.column + '`?\\s+(?:TIMESTAMP|DATETIME)\\b',
                'im'
            );
            assert.match(sql, declaration, entry.table + '.' + entry.column);
        }
    });

    it('accounts for every TIMESTAMP column in the sync SQL files', function(){
        const inventory = new Set(DATETIME_COLUMNS.map(entry => entry.table + '.' + entry.column));
        const sqlFiles = fs.readdirSync(SQL_DIR).filter(filename => filename.endsWith('.sql'));
        for(const filename of sqlFiles){
            const table = path.basename(filename, '.sql');
            const sql = sqlWithoutComments(table);
            for(const column of declaredTimestampColumns(sql)){
                assert(inventory.has(table + '.' + column), table + '.' + column);
            }
        }
    });
});

describe('DATETIME column helpers', function(){
    it('contains exactly three frozen entries', function(){
        assert.strictEqual(DATETIME_COLUMNS.length, 3);
        assert(Object.isFrozen(DATETIME_COLUMNS));
        assert(DATETIME_COLUMNS.every(Object.isFrozen));
        assert.deepStrictEqual(DATETIME_COLUMNS, [
            { table: 'merkle_epochs', column: 'created_at', columnDef: 'DATETIME DEFAULT CURRENT_TIMESTAMP', scope: 'sync-owned' },
            { table: 'merkle_reorgs', column: 'detected_at', columnDef: 'DATETIME DEFAULT CURRENT_TIMESTAMP', scope: 'sync-owned' },
            { table: 'state_tree_roots', column: 'computed_at', columnDef: 'DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP', scope: 'follower-derived' },
        ]);
    });

    it('groups tables according to follower-derived scope', function(){
        const syncOwned = byTable({ includeFollowerDerived: false });
        assert.deepStrictEqual([...syncOwned.keys()], ['merkle_epochs', 'merkle_reorgs']);
        assert.strictEqual(syncOwned.has('state_tree_roots'), false);

        const all = byTable({ includeFollowerDerived: true });
        assert.deepStrictEqual([...all.keys()], [
            'merkle_epochs',
            'merkle_reorgs',
            'state_tree_roots',
        ]);
    });

    it('builds MODIFY clauses and identifies TIMESTAMP metadata', function(){
        assert.strictEqual(
            modifyClause(DATETIME_COLUMNS[0]),
            'MODIFY COLUMN `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP'
        );
        assert.strictEqual(needsRetype({ DATA_TYPE: 'timestamp' }), true);
        assert.strictEqual(needsRetype({ DATA_TYPE: 'TIMESTAMP' }), true);
        assert.strictEqual(needsRetype({ data_type: 'Timestamp' }), true);
        assert.strictEqual(needsRetype({ DATA_TYPE: 'datetime' }), false);
        assert.strictEqual(needsRetype({}), false);
        assert.strictEqual(needsRetype(null), false);
    });
});
