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
// Shape of the replication schema gate itself: the two dbType keys a snapshot
// can be stamped with, and the migration frontier the gate test in
// schema-version-gate.test.js measures the sibling migration ledgers against.
// A frontier that names a date no migration bears, or an `accounted` tail that
// has drifted off the frontier date, would make that gate pass vacuously.

'use strict';

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');

const { SCHEMA_VERSION, MIGRATION_FRONTIER } = require('../../src/schema-version');

const DB_TYPES = ['indexer', 'decoder'];

const MIGRATION_DIRS = {
    indexer: process.env.XCHAIN_INDEXER_SQL_PATH
        ? path.join(process.env.XCHAIN_INDEXER_SQL_PATH, 'migrations')
        : path.resolve(__dirname, '..', '..', '..', 'xchain-indexer', 'src', 'sql', 'migrations'),
    decoder: process.env.XCHAIN_DECODER_SQL_PATH
        ? path.join(process.env.XCHAIN_DECODER_SQL_PATH, 'migrations')
        : path.resolve(__dirname, '..', '..', '..', 'xchain-decoder', 'src', 'sql', 'migrations')
};
const SIBLING_REQUIRED = process.env.XCHAIN_REQUIRE_SIBLINGS === '1';

describe('replication schema version and migration frontier @regression', function(){

    it('carries one positive integer version per replicated dbType, and no others', function(){
        assert.deepStrictEqual(Object.keys(SCHEMA_VERSION).sort(), DB_TYPES.slice().sort());
        for(const dbType of DB_TYPES){
            const v = SCHEMA_VERSION[dbType];
            assert.ok(Number.isInteger(v) && v > 0, dbType + ' version must be a positive integer, got ' + v);
        }
    });

    it('carries a frontier for exactly the same dbTypes', function(){
        assert.deepStrictEqual(Object.keys(MIGRATION_FRONTIER).sort(), Object.keys(SCHEMA_VERSION).sort());
    });

    for(const dbType of DB_TYPES){

        it(dbType + ': the frontier date is a well-formed migration date', function(){
            const through = MIGRATION_FRONTIER[dbType].through;
            assert.ok(/^\d{4}-\d{2}-\d{2}$/.test(through),
                dbType + ' frontier.through must be YYYY-MM-DD, got ' + through);
            assert.ok(!Number.isNaN(Date.parse(through + 'T00:00:00Z')),
                dbType + ' frontier.through is not a real date: ' + through);
        });

        it(dbType + ': every accounted file bears the frontier date and exists in the migration ledger', function(){
            const dir = MIGRATION_DIRS[dbType];
            if(!fs.existsSync(dir)){
                if(SIBLING_REQUIRED) throw new Error('sibling migration ledger missing at ' + dir);
                this.skip(); return;
            }
            const { through, accounted } = MIGRATION_FRONTIER[dbType];
            assert.ok(Array.isArray(accounted), dbType + ' frontier.accounted must be an array');
            for(const file of accounted){
                assert.ok(file.startsWith(through + '-'),
                    'accounted file ' + file + ' does not bear the ' + dbType + ' frontier date ' + through);
                assert.ok(fs.existsSync(path.join(dir, file)),
                    'accounted file ' + file + ' is not in the ' + dbType + ' migration ledger ' + dir
                    + ' (a renamed or reverted migration leaves the frontier naming a file that no longer exists)');
            }
        });

        it(dbType + ': the accounted tail covers every migration dated on the frontier date', function(){
            const dir = MIGRATION_DIRS[dbType];
            if(!fs.existsSync(dir)){
                if(SIBLING_REQUIRED) throw new Error('sibling migration ledger missing at ' + dir);
                this.skip(); return;
            }
            const { through, accounted } = MIGRATION_FRONTIER[dbType];
            const onDate = fs.readdirSync(dir)
                .filter(f => f.endsWith('.sql') && f.startsWith(through + '-'))
                .sort();
            assert.deepStrictEqual(onDate, accounted.slice().sort(),
                dbType + ': frontier.accounted must list every migration dated ' + through
                + '; otherwise a same-day migration hides behind the date cursor');
        });

        it(dbType + ': the frontier does not run ahead of the migration ledger', function(){
            const dir = MIGRATION_DIRS[dbType];
            if(!fs.existsSync(dir)){
                if(SIBLING_REQUIRED) throw new Error('sibling migration ledger missing at ' + dir);
                this.skip(); return;
            }
            const dates = fs.readdirSync(dir)
                .filter(f => f.endsWith('.sql'))
                .map(f => (f.match(/^(\d{4}-\d{2}-\d{2})-/) || [])[1])
                .filter(Boolean)
                .sort();
            assert.ok(dates.length, 'no dated migrations found in ' + dir);
            assert.ok(MIGRATION_FRONTIER[dbType].through <= dates[dates.length - 1],
                dbType + ': frontier.through ' + MIGRATION_FRONTIER[dbType].through
                + ' is newer than the newest migration ' + dates[dates.length - 1]
                + '; a frontier ahead of the ledger silences the gate for every migration up to that date');
        });
    }
});
