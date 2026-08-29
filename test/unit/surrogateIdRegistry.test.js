// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');

const ClientApplier = require('../../src/ClientApplier');
const Utility       = require('../../src/utility');

// The indexer schema, resolved the way the other cross-repo guards here do
// (see generatedColumns.test.js). Absent by default in a standalone checkout;
// XCHAIN_REQUIRE_SIBLINGS=1 turns green-by-skip into a failure, and
// xchain-indexer is declared in .ci-siblings so the push gate ships it.
const INDEXER_SQL = process.env.XCHAIN_INDEXER_SQL_PATH
    || path.resolve(__dirname, '..', '..', '..', 'xchain-indexer', 'src', 'sql');
const SIBLING_REQUIRED = process.env.XCHAIN_REQUIRE_SIBLINGS === '1';

// Tables whose CREATE TABLE declares an AUTO_INCREMENT column, keyed by table.
// Line-scoped on purpose: `[^\n,]` stops a match from running out of a column
// declaration and swallowing the DROP TABLE / CREATE TABLE lines above it.
function tablesWithAutoIncrement(sqlDir){
    const found = {};
    for(const file of fs.readdirSync(sqlDir).filter(f => f.endsWith('.sql'))){
        const text  = fs.readFileSync(path.join(sqlDir, file), 'utf8');
        const body  = text.replace(/^\s*--.*$/gm, '');
        const table = (body.match(/CREATE TABLE(?:\s+IF NOT EXISTS)?\s+`?(\w+)`?/i) || [])[1];
        if(!table) continue;
        for(const m of body.matchAll(/^[ \t]*`?(\w+)`?[ \t]+[A-Za-z]+[^\n,]*AUTO_INCREMENT/gim))
            (found[table] = found[table] || []).push(m[1]);
    }
    return found;
}

// Scope, stated so nobody widens this by reflex. The wedge this guards is the
// UPSERT one: upsertFullDumpTables builds `ON DUPLICATE KEY UPDATE` over every
// carried column, so an unstripped AUTO_INCREMENT id becomes `id` = VALUES(`id`)
// against the replica's PRIMARY KEY, and on a source id another surviving row
// already holds that is ER_DUP_ENTRY 1062 (outside ClientSync._healSchemaIfStale's
// {1146, 1054} heal set, so the apply aborts and re-fails forever). That is how
// attest_validator_stats broke when indexer migration
// 2026-08-19-attest-validator-stats-surrogate-id gave it a surrogate id.
//
// The plain-INSERT tables carry the same id but never REWRITE one, and their
// replicas take the source id from first insert, so a guard spanning them would
// be a ~35-entry allow-list nobody reads. Two entries is a guard; thirty-five is
// a file someone silences.
const UPSERT_ID_ALLOWED = Object.freeze({
    markets: 'id space is source-assigned end to end (sync has no local markets INSERT path), and its ' +
             'uq_markets_pair natural key arrives from indexer migration 2026-07-15-markets-dedup-unique-pair ' +
             'which secondary-index propagation does not carry, so an aged replica may hold markets with no ' +
             'unique key but the id. Stripping the id there would let a re-dump append duplicates silently.'
});

describe('ClientApplier surrogate-id registry matches the indexer DDL @regression', function(){

    let applier;
    beforeEach(function(){ applier = new ClientApplier({}, new Utility()); });

    it('strips or explicitly allows the id of every upsert full-dump table that declares one', function(){
        if(!fs.existsSync(INDEXER_SQL)){
            if(SIBLING_REQUIRED)
                throw new Error('XCHAIN_REQUIRE_SIBLINGS=1 but the indexer schema is absent: ' + INDEXER_SQL);
            this.skip();
            return;
        }
        const derived = tablesWithAutoIncrement(INDEXER_SQL);

        // The scan must actually find something, or a regex that stopped matching
        // would pass this test by deriving an empty world.
        assert.ok(Object.keys(derived).length > 0,
            'derived no AUTO_INCREMENT columns at all from ' + INDEXER_SQL + '; the DDL scan is broken');
        for(const table of applier.upsertFullDumpTables){
            assert.ok(derived[table],
                table + ' is an upsert full-dump table but the scan found no AUTO_INCREMENT column for it; ' +
                'either the DDL moved or the scan is no longer reading this table');
        }

        for(const table of applier.upsertFullDumpTables){
            if(!(derived[table] || []).includes('id')) continue;
            const stripped = applier.localSurrogateIdTables.has(table)
                          || applier.localSurrogateIdOnlyTables.has(table);
            assert.ok(stripped || UPSERT_ID_ALLOWED[table],
                table + ' upserts a full dump and the indexer declares its `id` AUTO_INCREMENT, so the applier ' +
                'would emit `id` = VALUES(`id`) against the replica PRIMARY KEY (ER_DUP_ENTRY 1062, which ' +
                'aborts the apply transaction and re-fails on every retry). Register it in ' +
                'localSurrogateIdOnlyTables, or add it to UPSERT_ID_ALLOWED with the reason its id space is ' +
                'safe to replicate.');
        }
    });

    it('keeps attest_validator_stats in the strip-only class, not the DELETE class', function(){
        // localSurrogateIdTables drives DELETE ... WHERE <single key> IN (...), and this
        // table natural key is the composite UNIQUE (validator_pubkey, provider_id): a
        // delete by either half alone removes rows that are not being replaced.
        assert.ok(applier.localSurrogateIdOnlyTables.has('attest_validator_stats'));
        assert.ok(!applier.localSurrogateIdTables.has('attest_validator_stats'),
            'a single-column DELETE would drop that validator rows for every other provider');
    });

    it('every allow-listed table carries a reason, so the list cannot grow silently', function(){
        for(const table of Object.keys(UPSERT_ID_ALLOWED))
            assert.ok(typeof UPSERT_ID_ALLOWED[table] === 'string' && UPSERT_ID_ALLOWED[table].length > 40,
                table + ' needs a real reason, not a placeholder');
    });
});
